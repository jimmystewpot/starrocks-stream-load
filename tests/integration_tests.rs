use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
    redact_sensitive_info,
};

#[test]
fn test_log_redaction() {
    let sensitive_err = "http://admin:secret_pass@127.0.0.1:8030/api/db/table/_stream_load failed with Authorization: Basic YWRtaW46c2VjcmV0X3Bhc3M= and password=secret_pass";
    let redacted = redact_sensitive_info(sensitive_err);
    assert!(!redacted.contains("secret_pass"));
    assert!(!redacted.contains("YWRtaW46c2VjcmV0X3Bhc3M="));
    assert!(redacted.contains("password=***"));

    // Test multiple inline URLs
    let multi_url_err =
        "failed connecting to http://user1:pass1@host1:8030 and http://user2:pass2@host2:8030";
    let multi_redacted = redact_sensitive_info(multi_url_err);
    assert!(!multi_redacted.contains("pass1"));
    assert!(!multi_redacted.contains("pass2"));
    assert!(multi_redacted.contains("http://user1:***@host1:8030"));
    assert!(multi_redacted.contains("http://user2:***@host2:8030"));
}

#[tokio::test]
async fn test_v1_stream_load_with_redirect() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // Mock FE redirect response
    Mock::given(method("PUT"))
        .and(path("/api/test_db/test_tbl/_stream_load"))
        .respond_with(
            ResponseTemplate::new(307).insert_header("Location", &format!("{mock_uri}/be_load")),
        )
        .mount(&mock_server)
        .await;

    // Mock BE actual upload response
    Mock::given(method("PUT"))
        .and(path("/be_load"))
        .and(header("Authorization", "Basic YWRtaW46cGFzc3dvcmQ="))
        .and(header("Expect", "100-continue"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "Success",
            "TxnId": 42,
            "Label": "test_label",
            "Message": "Load OK",
            "NumberTotalRows": 10,
            "NumberLoadedRows": 10,
            "NumberFilteredRows": 0,
            "NumberUnselectedRows": 0,
            "LoadBytes": 100,
            "LoadTimeMs": 25
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder()
        .table("test_tbl")
        .format(DataFormat::CSV)
        .build();

    let manager = StreamLoadManager::new(config, props).unwrap();
    let res = manager
        .send_single_batch("test_label", bytes::Bytes::from("col1,col2\nval1,val2"))
        .await
        .unwrap();

    assert_eq!(res.status, "Success");
    assert_eq!(res.txn_id, Some(42));
    assert_eq!(res.number_loaded_rows, Some(10));
}

#[tokio::test]
async fn test_v2_transaction_single_table() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // 1. Begin Transaction Mock
    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .and(header("label", "label_123"))
        .and(header("db", "test_db"))
        .and(header("table", "test_tbl"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK",
            "TxnId": 123
        })))
        .mount(&mock_server)
        .await;

    // 2. Load Transaction Data Mock
    Mock::given(method("PUT"))
        .and(path("/api/transaction/load"))
        .and(header("label", "label_123"))
        .and(header("db", "test_db"))
        .and(header("table", "test_tbl"))
        .and(header("channel_num", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    // 3. Prepare Transaction Mock
    Mock::given(method("POST"))
        .and(path("/api/transaction/prepare"))
        .and(header("label", "label_123"))
        .and(header("db", "test_db"))
        .and(header("table", "test_tbl"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    // 4. Commit Transaction Mock
    Mock::given(method("POST"))
        .and(path("/api/transaction/commit"))
        .and(header("label", "label_123"))
        .and(header("db", "test_db"))
        .and(header("table", "test_tbl"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder()
        .table("test_tbl")
        .build();

    let manager = StreamLoadManager::new(config, props).unwrap();

    let txn_id = manager.begin_transaction("label_123").await.unwrap();
    assert_eq!(txn_id, 123);

    let load_res = manager
        .load_transaction_data(
            "label_123",
            "test_db",
            "test_tbl",
            0,
            bytes::Bytes::from("data"),
        )
        .await
        .unwrap();
    assert_eq!(load_res.status, "OK");

    let prep_res = manager.prepare_transaction("label_123").await.unwrap();
    assert_eq!(prep_res.status, "OK");

    let commit_res = manager.commit_transaction("label_123").await.unwrap();
    assert_eq!(commit_res.status, "OK");
}

#[tokio::test]
async fn test_v2_transaction_multi_table() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // 1. Begin Transaction Mock
    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .and(header("label", "multi_label"))
        .and(header("db", "test_db"))
        .and(header("transaction_type", "multi"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK",
            "TxnId": 777
        })))
        .mount(&mock_server)
        .await;

    // 2. Load Transaction Data Table A Mock
    Mock::given(method("PUT"))
        .and(path("/api/transaction/load"))
        .and(header("label", "multi_label"))
        .and(header("db", "test_db"))
        .and(header("table", "table_a"))
        .and(header("channel_num", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    // 3. Load Transaction Data Table B Mock
    Mock::given(method("PUT"))
        .and(path("/api/transaction/load"))
        .and(header("label", "multi_label"))
        .and(header("db", "test_db"))
        .and(header("table", "table_b"))
        .and(header("channel_num", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    // 4. Commit Transaction Mock
    Mock::given(method("POST"))
        .and(path("/api/transaction/commit"))
        .and(header("label", "multi_label"))
        .and(header("db", "test_db"))
        .and(header("transaction_type", "multi"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .enable_multi_table_transaction(true)
            .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let txn_id = manager.begin_transaction("multi_label").await.unwrap();
    assert_eq!(txn_id, 777);

    manager
        .load_transaction_data(
            "multi_label",
            "test_db",
            "table_a",
            0,
            bytes::Bytes::from("dataA"),
        )
        .await
        .unwrap();

    manager
        .load_transaction_data(
            "multi_label",
            "test_db",
            "table_b",
            1,
            bytes::Bytes::from("dataB"),
        )
        .await
        .unwrap();

    let commit_res = manager.commit_transaction("multi_label").await.unwrap();
    assert_eq!(commit_res.status, "OK");
}

#[tokio::test]
async fn test_v2_transaction_rollback() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // Rollback Mock
    Mock::given(method("POST"))
        .and(path("/api/transaction/rollback"))
        .and(header("label", "rollback_label"))
        .and(header("db", "test_db"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let rollback_res = manager
        .rollback_transaction("rollback_label")
        .await
        .unwrap();
    assert_eq!(rollback_res.status, "OK");
}

#[tokio::test]
async fn test_client_node_failover() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // Mock response on active server
    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .and(header("label", "failover_label"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "OK",
            "TxnId": 999
        })))
        .mount(&mock_server)
        .await;

    // First server is unroutable / invalid; second server is correct mock server
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:1".to_string(), mock_uri],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("password")
    .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let txn_id = manager.begin_transaction("failover_label").await.unwrap();
    assert_eq!(txn_id, 999);
}

#[tokio::test]
async fn test_v2_transaction_error_handling() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "LABEL_ALREADY_EXISTS",
            "Message": "Label already used"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let err = manager
        .begin_transaction("duplicate_label")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("LABEL_ALREADY_EXISTS"));
}

#[tokio::test]
async fn test_header_validation_failure() {
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("password")
    .build();

    // Header values cannot contain control characters like newlines
    let props = StreamLoadTableProperties::builder()
        .column_separator("val\nwith\nnewlines")
        .build();

    let manager = StreamLoadManager::new(config, props).unwrap();
    let err = manager
        .send_single_batch("label_abc", bytes::Bytes::from("data"))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("Invalid character in header"));
}

#[test]
fn test_delimiter_conversion() {
    use starrocks_stream_load::convert_delimiter;
    assert_eq!(convert_delimiter("\\x01").unwrap(), "\u{1}");
    assert_eq!(convert_delimiter("0x01").unwrap(), "\u{1}");
    assert_eq!(convert_delimiter("\\x0a").unwrap(), "\n");
    assert_eq!(convert_delimiter("\\x0A").unwrap(), "\n");
    assert_eq!(convert_delimiter("abc").unwrap(), "abc");

    // Errors
    assert!(convert_delimiter("").is_err());
    assert!(convert_delimiter("\\x").is_err());
    assert!(convert_delimiter("\\x1").is_err());
    assert!(convert_delimiter("\\xzz").is_err());
}

#[test]
fn test_error_log_url_extraction() {
    use starrocks_stream_load::try_get_error_log_url_from_txn_abort_reason;

    let aborted_reason_35 = "There is data quality issue, please check the tracking url for details. Max filter ratio: 0.0. \
                             The tracking url: http://127.0.0.1:8040/api/_load_error_log?file=error_log_19bbc3f6ae0754f_932e963c5ec44399";
    let aborted_reason_40 = "There is a data quality issue. Please check the tracking URL or SQL for details. \
                             Tracking URL: http://127.0.0.1:8040/api/_load_error_log?file=error_log_19bbc3f6ae0754f_932e963c5ec44399. \
                             Tracking SQL: SELECT tracking_log FROM information_schema.load_tracking_logs WHERE JOB_ID=12345";
    let expected =
        "http://127.0.0.1:8040/api/_load_error_log?file=error_log_19bbc3f6ae0754f_932e963c5ec44399";

    assert_eq!(
        try_get_error_log_url_from_txn_abort_reason(aborted_reason_35).unwrap(),
        expected
    );
    assert_eq!(
        try_get_error_log_url_from_txn_abort_reason(aborted_reason_40).unwrap(),
        expected
    );
    assert_eq!(try_get_error_log_url_from_txn_abort_reason("no url"), None);
}

#[test]
fn test_error_log_sanitization() {
    use starrocks_stream_load::sanitize_error_log;

    // Empty handling
    assert_eq!(sanitize_error_log(""), "");
    assert_eq!(sanitize_error_log("   "), "   ");

    // Basic Column Values Redaction
    let input_1 =
        "Value ''secret_stuff'' invalid format\nValue 'more_secrets'\nValue \"even_more\" too long";
    let expected_1 = "column value invalid format\ncolumn value\ncolumn value too long";
    assert_eq!(sanitize_error_log(input_1), expected_1);

    // Row Redaction
    let input_2 =
        "Row: [123, \"secret_row_val\", true]\nRow: {\"field\": \"secret\"}\nRow: general data";
    let expected_2 = "Data validation errors detected. Row data has been redacted for security.";
    assert_eq!(sanitize_error_log(input_2), expected_2);

    // Real world scenario with mixture of metadata and row details
    let input_3 = "Error parsing line 10\nValue 'user_pwd' is not valid for type INT\nRow: [10, \"user_pwd\", \"admin\"]\nError parsing line 11";
    let expected_3 =
        "Error parsing line 10\ncolumn value is not valid for type INT\nError parsing line 11";
    assert_eq!(sanitize_error_log(input_3), expected_3);
}

#[test]
fn test_response_timing_deserialization() {
    use starrocks_stream_load::StreamLoadResponse;

    let json_data = r#"{
        "TxnId": 22736752,
        "Label": "119d4ca5-a920-4dbb-84ad-64e062a449c5",
        "Status": "Success",
        "Message": "OK",
        "NumberTotalRows": 93,
        "NumberLoadedRows": 93,
        "NumberFilteredRows": 0,
        "NumberUnselectedRows": 0,
        "LoadBytes": 17227,
        "LoadTimeMs": 17575,
        "BeginTxnTimeMs": 12,
        "StreamLoadPlanTimeMs": 1,
        "ReadDataTimeMs": 34,
        "WriteDataTimeMs": 17487,
        "CommitAndPublishTimeMs": 86
    }"#;

    let resp: StreamLoadResponse = serde_json::from_str(json_data).unwrap();
    assert_eq!(resp.txn_id, Some(22_736_752));
    assert_eq!(resp.status, "Success");
    assert_eq!(resp.begin_txn_time_ms, Some(12));
    assert_eq!(resp.stream_load_plan_time_ms, Some(1));
    assert_eq!(resp.read_data_time_ms, Some(34));
    assert_eq!(resp.write_data_time_ms, Some(17487));
    assert_eq!(resp.commit_and_publish_time_ms, Some(86));
}

#[tokio::test]
async fn test_error_log_fetch_and_redaction() {
    use starrocks_stream_load::{StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    let raw_error_log = "Error parsing row\nValue 'super_secret'\nRow: [123, \"secret\"]";
    let sanitized_expected = "Error parsing row\ncolumn value";

    Mock::given(method("GET"))
        .and(path("/api/_load_error_log"))
        .respond_with(ResponseTemplate::new(200).set_body_string(raw_error_log))
        .mount(&mock_server)
        .await;

    let config = StreamLoadConfig::builder(
        vec![mock_uri.clone()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("password")
    .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let error_url = format!("{mock_uri}/api/_load_error_log?file=error_log_123");

    // Test fetch without sanitization
    let fetched_raw = manager.get_error_log(&error_url, false).await.unwrap();
    assert_eq!(fetched_raw, raw_error_log);

    // Test fetch with sanitization
    let fetched_sanitized = manager.get_error_log(&error_url, true).await.unwrap();
    assert_eq!(fetched_sanitized, sanitized_expected);

    // Test get error log for merge commit using abort reason parsing
    let abort_reason = format!("Tracking URL: {error_url}");
    let fetched_via_abort = manager
        .try_get_error_log_for_merge_commit(&abort_reason, true)
        .await
        .unwrap();
    assert_eq!(fetched_via_abort, sanitized_expected);
}

#[tokio::test]
async fn test_v1_stream_load_with_relative_redirect() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // Mock FE redirect response with relative path
    Mock::given(method("PUT"))
        .and(path("/api/test_db/test_tbl/_stream_load"))
        .respond_with(ResponseTemplate::new(307).insert_header("Location", "/be_load_relative"))
        .mount(&mock_server)
        .await;

    // Mock BE actual upload response
    Mock::given(method("PUT"))
        .and(path("/be_load_relative"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "Success",
            "TxnId": 43,
            "Label": "test_label_rel",
            "Message": "Load OK"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder()
        .table("test_tbl")
        .build();

    let manager = StreamLoadManager::new(config, props).unwrap();
    let res = manager
        .send_single_batch("test_label_rel", bytes::Bytes::from("data"))
        .await
        .unwrap();

    assert_eq!(res.status, "Success");
    assert_eq!(res.txn_id, Some(43));
}

#[tokio::test]
async fn test_get_load_status_mocked() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    Mock::given(method("GET"))
        .and(path("/api/test_db/get_load_state"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "Success",
            "TxnId": 111,
            "Label": "label_status"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let res = manager.get_load_status("label_status").await.unwrap();
    assert_eq!(res.status, "Success");
    assert_eq!(res.txn_id, Some(111));
}

#[tokio::test]
async fn test_cancel_load_mocked() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    Mock::given(method("POST"))
        .and(path("/api/test_db/test_tbl/_cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Status": "Success",
            "Message": "Cancelled"
        })))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let res = manager
        .cancel_load("label_cancel", "test_db", "test_tbl")
        .await
        .unwrap();
    assert_eq!(res.status, "Success");
    assert_eq!(res.message, Some("Cancelled".to_string()));
}

#[tokio::test]
async fn test_json_deserialization_failure_handling() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .respond_with(ResponseTemplate::new(200).set_body_string("invalid_json_payload"))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .password("password")
            .build();

    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let err = manager
        .begin_transaction("label_invalid")
        .await
        .unwrap_err();
    assert!(matches!(err, starrocks_stream_load::Error::Json(_)));
}

#[tokio::test]
async fn test_empty_load_urls_error() {
    let config =
        StreamLoadConfig::builder(vec![], "test_db".to_string(), "admin".to_string()).build();
    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    // Verify execute_request returns Error::Transaction when load_urls is empty
    let err = manager.begin_transaction("label").await.unwrap_err();
    assert!(err.to_string().contains("No configured load URLs"));

    // Verify get_available_fe falls back to http://127.0.0.1:8030
    assert_eq!(
        manager.client().get_available_fe(),
        "http://127.0.0.1:8030/"
    );
}

#[tokio::test]
async fn test_malformed_redirect_location() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // Location header has a control character, which is invalid, or it's empty
    Mock::given(method("PUT"))
        .and(path("/api/test_db/test_tbl/_stream_load"))
        .respond_with(ResponseTemplate::new(307)) // no location header
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "test_db".to_string(), "admin".to_string())
            .build();
    let props = StreamLoadTableProperties::builder()
        .table("test_tbl")
        .build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    // It should return the 307 response directly which is a StarRocksFailure
    let err = manager
        .send_single_batch("label", bytes::Bytes::from("data"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("307"));
}

#[tokio::test]
async fn test_get_error_log_failures() {
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "db".to_string(),
        "admin".to_string(),
    )
    .build();
    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    // Rejects non-HTTP(S) URLs
    let err_url = manager
        .get_error_log("ftp://some/url", false)
        .await
        .unwrap_err();
    assert!(err_url.to_string().contains("Invalid error log URL"));

    // 404 response
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();
    Mock::given(method("GET"))
        .and(path("/not_found"))
        .respond_with(ResponseTemplate::new(404).set_body_string("Resource not found"))
        .mount(&mock_server)
        .await;

    let error_url = format!("{mock_uri}/not_found");
    let err_status = manager.get_error_log(&error_url, false).await.unwrap_err();
    assert!(err_status.to_string().contains("404"));
}

#[tokio::test]
async fn test_all_apis_non_ok_responses() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal error PUT"))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal error POST"))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal error GET"))
        .mount(&mock_server)
        .await;

    let config =
        StreamLoadConfig::builder(vec![mock_uri], "db".to_string(), "admin".to_string()).build();
    let props = StreamLoadTableProperties::builder().table("tbl").build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    // 1. send_single_batch
    let err = manager
        .send_single_batch("label", bytes::Bytes::from("data"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("500"));

    // 2. begin_transaction
    let err = manager.begin_transaction("label").await.unwrap_err();
    assert!(err.to_string().contains("500"));

    // 3. load_transaction_data
    let err = manager
        .load_transaction_data("label", "db", "tbl", 0, bytes::Bytes::from("data"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("500"));

    // 4. prepare_transaction
    let err = manager.prepare_transaction("label").await.unwrap_err();
    assert!(err.to_string().contains("500"));

    // 5. commit_transaction
    let err = manager.commit_transaction("label").await.unwrap_err();
    assert!(err.to_string().contains("500"));

    // 6. rollback_transaction
    let err = manager.rollback_transaction("label").await.unwrap_err();
    assert!(err.to_string().contains("500"));

    // 7. get_load_status
    let err = manager.get_load_status("label").await.unwrap_err();
    assert!(err.to_string().contains("500"));

    // 8. cancel_load
    let err = manager.cancel_load("label", "db", "tbl").await.unwrap_err();
    assert!(err.to_string().contains("500"));
}

#[tokio::test]
async fn test_anonymous_authentication() {
    let mock_server = MockServer::start().await;
    let mock_uri = mock_server.uri();

    // Verify that the request received by the mock server DOES NOT have the AUTHORIZATION header.
    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .and(|request: &wiremock::Request| {
            !request.headers.contains_key("Authorization")
                && !request.headers.contains_key("authorization")
        })
        .respond_with(ResponseTemplate::new(200).set_body_json(
            starrocks_stream_load::types::StreamLoadResponse {
                status: "OK".to_string(),
                txn_id: Some(999),
                label: Some("label_anon".to_string()),
                ..Default::default()
            },
        ))
        .mount(&mock_server)
        .await;

    // Config with an empty username
    let config = StreamLoadConfig::builder(vec![mock_uri], "db".to_string(), String::new()).build();
    let props = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, props).unwrap();

    let res = manager.begin_transaction("label_anon").await.unwrap();
    assert_eq!(res, 999);
}
