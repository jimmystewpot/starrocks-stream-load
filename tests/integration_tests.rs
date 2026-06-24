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
