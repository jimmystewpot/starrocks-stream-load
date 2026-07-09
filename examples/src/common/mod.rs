#![allow(warnings)]
// Common utilities for testing examples
use serde_json::json;
pub use starrocks_stream_load::{DataFormat, StreamLoadResponse};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Setup a mock `StarRocks` server for testing
pub async fn setup_mock_starrocks() -> MockServer {
    let mock_server = MockServer::start().await;

    // Mock successful stream load response
    Mock::given(method("PUT"))
        .and(path("/api/test_db/test_table/_stream_load"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Status": "Success",
            "TxnId": 12345,
            "Label": "test_label",
            "NumberTotalRows": 3,
            "NumberLoadedRows": 3,
            "LoadBytes": 1500,
            "LoadTimeMs": 42
        })))
        .mount(&mock_server)
        .await;

    // Mock successful transaction begin
    Mock::given(method("POST"))
        .and(path("/api/transaction/begin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Status": "OK",
            "TxnId": 12345,
            "Label": "test_txn_label"
        })))
        .mount(&mock_server)
        .await;

    // Mock successful transaction load
    Mock::given(method("PUT"))
        .and(path("/api/transaction/load"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Status": "OK",
            "TxnId": 12345,
            "NumberTotalRows": 3,
            "NumberLoadedRows": 3
        })))
        .mount(&mock_server)
        .await;

    // Mock successful transaction prepare
    Mock::given(method("POST"))
        .and(path("/api/transaction/prepare"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Status": "OK",
            "TxnId": 12345
        })))
        .mount(&mock_server)
        .await;

    // Mock successful transaction commit
    Mock::given(method("POST"))
        .and(path("/api/transaction/commit"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Status": "OK",
            "TxnId": 12345,
            "NumberTotalRows": 3,
            "NumberLoadedRows": 3
        })))
        .mount(&mock_server)
        .await;

    // Mock successful transaction rollback
    Mock::given(method("POST"))
        .and(path("/api/transaction/rollback"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Status": "OK",
            "TxnId": 12345
        })))
        .mount(&mock_server)
        .await;

    mock_server
}

/// Generate CSV test data with specified number of rows
#[must_use]
pub fn generate_csv_data(rows: usize) -> bytes::Bytes {
    let mut csv = String::from("id,name,value\n");

    for i in 0..rows {
        use std::fmt::Write;
        let _ = writeln!(csv, "{},user_{},{}", i + 1, i, (i * 10) + 5);
    }

    bytes::Bytes::from(csv)
}

/// Generate JSON test data with specified number of rows
///
/// # Panics
///
/// Panics if JSON serialization fails
#[must_use]
pub fn generate_json_data(rows: usize) -> bytes::Bytes {
    let mut json_array = Vec::new();

    for i in 0..rows {
        let obj = json!({
            "id": i + 1,
            "name": format!("user_{}", i),
            "value": (i * 10) + 5
        });
        json_array.push(obj);
    }

    let json_string = serde_json::to_vec(&json_array).unwrap();
    bytes::Bytes::from(json_string)
}

/// Generate Arrow-style binary data placeholder
#[must_use]
pub fn generate_arrow_data(rows: usize) -> bytes::Bytes {
    // This is a placeholder - real Arrow data would require proper serialization
    let message = format!("Arrow data placeholder for {rows} rows");
    bytes::Bytes::from(message)
}

/// Assert that a stream load response indicates success
///
/// # Panics
///
/// Panics if response status is not Success or OK, or if loaded rows are 0
pub fn assert_success_response(response: &StreamLoadResponse) {
    assert!(
        response.status == "Success" || response.status == "OK",
        "Expected success status, got: {}",
        response.status
    );

    if let Some(loaded) = response.number_loaded_rows {
        assert!(loaded > 0, "Expected loaded rows > 0, got: {loaded}");
    }
}

/// Generate a random test label with timestamp
#[must_use]
pub fn generate_test_label(prefix: &str) -> String {
    use chrono::Utc;
    use rand::Rng;
    let random_suffix: u32 = rand::rng().random_range(100_000..1_000_000);
    format!(
        "{}_{}_{}",
        prefix,
        Utc::now().timestamp_millis(),
        random_suffix
    )
}

/// Setup basic tracing for examples
pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_csv_data() {
        let csv = generate_csv_data(3);
        let csv_str = std::str::from_utf8(&csv).unwrap();

        assert!(csv_str.contains("id,name,value"));
        assert!(csv_str.contains("1,user_0,5"));
        assert!(csv_str.contains("3,user_2,25"));
    }

    #[test]
    fn test_generate_json_data() {
        let json = generate_json_data(2);
        let json_str: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();

        assert_eq!(json_str.len(), 2);
        assert_eq!(json_str[0]["id"], 1);
        assert_eq!(json_str[1]["name"], "user_1");
    }

    #[test]
    fn test_generate_test_label() {
        let label1 = generate_test_label("test");
        let label2 = generate_test_label("test");

        assert!(label1.starts_with("test_"));
        assert_ne!(label1, label2); // Should be different due to timestamps
    }

    #[test]
    fn test_assert_success_response() {
        let success_response = StreamLoadResponse {
            status: "Success".to_string(),
            message: Some("Test success".to_string()),
            txn_id: Some(12345),
            label: Some("test_label".to_string()),
            number_total_rows: Some(3),
            number_loaded_rows: Some(3),
            number_filtered_rows: Some(0),
            number_unselected_rows: Some(0),
            load_bytes: Some(1500),
            load_time_ms: Some(42),
            error_log_url: None,
            state: Some("COMMITTED".to_string()),
            existing_job_status: None,
            begin_txn_time_ms: Some(10),
            stream_load_plan_time_ms: Some(15),
            read_data_time_ms: Some(20),
            write_data_time_ms: Some(25),
            commit_and_publish_time_ms: Some(30),
        };

        assert_success_response(&success_response);
    }
}
