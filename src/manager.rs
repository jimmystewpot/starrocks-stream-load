use bytes::Bytes;
use reqwest::Method;
use reqwest::header::{EXPECT, HeaderMap, HeaderValue};

use crate::config::{DataFormat, StreamLoadConfig, StreamLoadTableProperties};
use crate::error::{Error, Result};
use crate::http::StarRocksHttpClient;
use crate::types::StreamLoadResponse;

/// Main manager for `StarRocks` stream load operations.
///
/// This struct provides the primary interface for both V1 (Direct Load) and V2 (2PC Transaction)
/// APIs, handling connection management, retry logic, and 307 redirect interception.
///
/// # Creating a Manager
///
/// ```rust,no_run
/// use starrocks_stream_load::{DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties};
///
/// let config = StreamLoadConfig::builder(
///     vec!["http://127.0.0.1:8030".to_string()],
///     "my_database".to_string(),
///     "admin".to_string(),
/// )
/// .password("your_password")
/// .build();
///
/// let properties = StreamLoadTableProperties::builder()
///     .table("my_table")
///     .format(DataFormat::CSV)
///     .column_separator(",")
///     .build();
///
/// let manager = StreamLoadManager::new(config, properties)?;
/// ```
///
/// # Thread Safety
///
/// The manager can be safely shared across threads using `Arc`. All HTTP operations
/// are performed asynchronously and are thread-safe.
///
/// # Connection Management
///
/// The manager maintains internal state for connection pooling and failover handling.
/// It automatically handles HTTP 307 redirects to backend nodes and retries failed
/// requests according to the configured retry settings.
pub struct StreamLoadManager {
    /// Internal HTTP client for network operations
    http_client: StarRocksHttpClient,
    /// Table-specific properties for data loading
    properties: StreamLoadTableProperties,
}

impl StreamLoadManager {
    /// Creates a new [`StreamLoadManager`] with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Connection and retry configuration
    /// * `properties` - Table-specific loading properties
    ///
    /// # Returns
    ///
    /// Returns `Result<StreamLoadManager, Error>` indicating success or failure.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The provided URLs are invalid
    /// - Network configuration fails
    /// - Connection setup encounters issues
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use starrocks_stream_load::{DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties};
    ///
    /// let config = StreamLoadConfig::builder(
    ///     vec!["http://127.0.0.1:8030".to_string()],
    ///     "my_database".to_string(),
    ///     "admin".to_string(),
    /// )
    /// .password("your_password")
    /// .build();
    ///
    /// let properties = StreamLoadTableProperties::builder()
    ///     .table("my_table")
    ///     .format(DataFormat::CSV)
    ///     .column_separator(",")
    ///     .build();
    ///
    /// let manager = StreamLoadManager::new(config, properties)?;
    /// ```
    pub fn new(config: StreamLoadConfig, properties: StreamLoadTableProperties) -> Result<Self> {
        Ok(Self {
            http_client: StarRocksHttpClient::new(config)?,
            properties,
        })
    }

    /// Provides access to the internal HTTP client.
    ///
    /// # Returns
    ///
    /// A reference to the [`StarRocksHttpClient`] used for network operations.
    ///
    /// # Use Cases
    ///
    /// This method can be useful for advanced scenarios where direct access to
    /// the HTTP client is needed, such as custom request handling or debugging.
    #[must_use]
    pub fn client(&self) -> &StarRocksHttpClient {
        &self.http_client
    }

    /// Provides access to the table properties configuration.
    ///
    /// # Returns
    ///
    /// A reference to the [`StreamLoadTableProperties`] used for data loading.
    ///
    /// # Use Cases
    ///
    /// Useful for inspecting current configuration or dynamically modifying
    /// table-level settings between operations.
    #[must_use]
    pub fn properties(&self) -> &StreamLoadTableProperties {
        &self.properties
    }

    /// Determines the default database name for operations.
    ///
    /// Uses the table properties database if set, otherwise falls back to
    /// the connection configuration database.
    fn default_db(&self) -> &str {
        self.properties
            .database
            .as_deref()
            .unwrap_or(&self.http_client.config().database)
    }

    /// Determines the default table name for operations.
    ///
    /// Returns the table name from properties if set, otherwise returns an empty string.
    fn default_table(&self) -> &str {
        self.properties.table.as_deref().unwrap_or("")
    }

    /// V1 API - Direct Standard Synchronous Load
    ///
    /// Sends a single batch of data to `StarRocks` using the synchronous V1 API.
    /// This method attempts to load the data immediately and blocks until completion.
    ///
    /// # Arguments
    ///
    /// * `label` - Unique identifier for this load operation (must not contain slashes)
    /// * `data` - Raw data bytes to load
    ///
    /// # Returns
    ///
    /// Returns `Result<StreamLoadResponse, Error>` containing the load result.
    ///
    /// # Notes
    ///
    /// - This is the V1 API suitable for simple, non-critical data loading
    /// - For ACID guarantees, use the V2 API (`begin_transaction`, `prepare_transaction`, etc.)
    /// - Label uniqueness is important - duplicate labels may cause conflicts
    /// - The default database and table from properties are used
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use starrocks_stream_load::StreamLoadManager;
    /// use bytes::Bytes;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     // Assume manager is already configured
    ///     let manager: StreamLoadManager = /* ... */;
    ///
    ///     let data = Bytes::from("1,John,Doe\n2,Jane,Smith\n");
    ///     let response = manager.send_single_batch("batch_001", data).await?;
    ///
    ///     assert_eq!(response.status, "Success");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Header building fails
    /// - Label contains invalid characters
    /// - Network request fails
    /// - Stream load operation fails on the server
    pub async fn send_single_batch(&self, label: &str, data: Bytes) -> Result<StreamLoadResponse> {
        let db = self.default_db();
        let table = self.default_table();
        let path = format!("/api/{db}/{table}/_stream_load");

        let mut headers = build_headers(&self.properties)?;
        headers.insert(EXPECT, HeaderValue::from_static("100-continue"));
        headers.insert(
            "label",
            HeaderValue::from_str(label)
                .map_err(|_| Error::Transaction("Invalid label name".to_string()))?,
        );

        let response = self
            .http_client
            .execute_request(Method::PUT, &path, headers, Some(data))
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        if resp.status != "Success" && resp.status != "OK" && resp.status != "Publish Timeout" {
            return Err(Error::StarRocksFailure {
                status: resp.status,
                message: resp.message.unwrap_or_default(),
                error_log_url: resp.error_log_url,
            });
        }

        Ok(resp)
    }

    /// V2 API - Begin 2PC Transaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Label contains invalid characters  
    /// - Network request fails
    /// - Transaction creation fails on the server
    /// - Response parsing fails
    pub async fn begin_transaction(&self, label: &str) -> Result<i64> {
        let db = self.default_db();
        let path = "/api/transaction/begin";

        let mut headers = HeaderMap::new();
        headers.insert(
            "label",
            HeaderValue::from_str(label)
                .map_err(|_| Error::Transaction("Invalid label name".to_string()))?,
        );
        headers.insert(
            "db",
            HeaderValue::from_str(db)
                .map_err(|_| Error::Transaction("Invalid db name".to_string()))?,
        );

        let config = self.http_client.config();
        if config.enable_multi_table_transaction {
            headers.insert("transaction_type", HeaderValue::from_static("multi"));
        } else {
            let table = self.default_table();
            headers.insert(
                "table",
                HeaderValue::from_str(table)
                    .map_err(|_| Error::Transaction("Invalid table name".to_string()))?,
            );
        }

        // Add timeout header
        let timeout_secs = self.properties.timeout.unwrap_or(600);
        headers.insert("timeout", HeaderValue::from(u64::from(timeout_secs)));

        let response = self
            .http_client
            .execute_request(Method::POST, path, headers, None)
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        if resp.status != "OK" && resp.status != "Success" {
            return Err(Error::StarRocksFailure {
                status: resp.status,
                message: resp.message.unwrap_or_default(),
                error_log_url: resp.error_log_url,
            });
        }

        resp.txn_id.ok_or_else(|| {
            Error::Transaction("Transaction response did not contain TxnId".to_string())
        })
    }

    /// V2 API - Load block chunk inside transaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Label contains invalid characters
    /// - Header building fails
    /// - Network request fails
    /// - Data loading fails on the server
    /// - Response parsing fails
    pub async fn load_transaction_data(
        &self,
        label: &str,
        database: &str,
        table: &str,
        sequence: usize,
        data: Bytes,
    ) -> Result<StreamLoadResponse> {
        let path = "/api/transaction/load";

        let mut headers = build_headers(&self.properties)?;
        headers.insert(EXPECT, HeaderValue::from_static("100-continue"));
        headers.insert(
            "label",
            HeaderValue::from_str(label)
                .map_err(|_| Error::Transaction("Invalid label name".to_string()))?,
        );
        headers.insert(
            "db",
            HeaderValue::from_str(database)
                .map_err(|_| Error::Transaction("Invalid db name".to_string()))?,
        );
        headers.insert(
            "table",
            HeaderValue::from_str(table)
                .map_err(|_| Error::Transaction("Invalid table name".to_string()))?,
        );
        headers.insert("channel_num", HeaderValue::from(sequence as u64));

        let response = self
            .http_client
            .execute_request(Method::PUT, path, headers, Some(data))
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        if resp.status != "OK" && resp.status != "Success" {
            return Err(Error::StarRocksFailure {
                status: resp.status,
                message: resp.message.unwrap_or_default(),
                error_log_url: resp.error_log_url,
            });
        }

        Ok(resp)
    }

    /// V2 API - Pre-commit / Flush to immutable state
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Label contains invalid characters
    /// - Network request fails
    /// - Transaction preparation fails on the server
    /// - Response parsing fails
    pub async fn prepare_transaction(&self, label: &str) -> Result<StreamLoadResponse> {
        let db = self.default_db();
        let path = "/api/transaction/prepare";

        let mut headers = HeaderMap::new();
        headers.insert(
            "label",
            HeaderValue::from_str(label)
                .map_err(|_| Error::Transaction("Invalid label name".to_string()))?,
        );
        headers.insert(
            "db",
            HeaderValue::from_str(db)
                .map_err(|_| Error::Transaction("Invalid db name".to_string()))?,
        );

        let config = self.http_client.config();
        if config.enable_multi_table_transaction {
            headers.insert("transaction_type", HeaderValue::from_static("multi"));
        } else {
            let table = self.default_table();
            headers.insert(
                "table",
                HeaderValue::from_str(table)
                    .map_err(|_| Error::Transaction("Invalid table name".to_string()))?,
            );
        }

        let response = self
            .http_client
            .execute_request(Method::POST, path, headers, None)
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        if resp.status != "OK" && resp.status != "Success" {
            return Err(Error::StarRocksFailure {
                status: resp.status,
                message: resp.message.unwrap_or_default(),
                error_log_url: resp.error_log_url,
            });
        }

        Ok(resp)
    }

    /// V2 API - Commit changes safely to storage engine
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Label contains invalid characters
    /// - Network request fails
    /// - Transaction commit fails on the server
    /// - Response parsing fails
    pub async fn commit_transaction(&self, label: &str) -> Result<StreamLoadResponse> {
        let db = self.default_db();
        let path = "/api/transaction/commit";

        let mut headers = HeaderMap::new();
        headers.insert(
            "label",
            HeaderValue::from_str(label)
                .map_err(|_| Error::Transaction("Invalid label name".to_string()))?,
        );
        headers.insert(
            "db",
            HeaderValue::from_str(db)
                .map_err(|_| Error::Transaction("Invalid db name".to_string()))?,
        );

        let config = self.http_client.config();
        if config.enable_multi_table_transaction {
            headers.insert("transaction_type", HeaderValue::from_static("multi"));
        } else {
            let table = self.default_table();
            headers.insert(
                "table",
                HeaderValue::from_str(table)
                    .map_err(|_| Error::Transaction("Invalid table name".to_string()))?,
            );
        }

        if let Some(ref timeout) = config.publish_timeout {
            let secs = timeout.as_secs().max(1);
            headers.insert("timeout", HeaderValue::from(secs));
        }

        let response = self
            .http_client
            .execute_request(Method::POST, path, headers, None)
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        if resp.status != "OK" && resp.status != "Success" {
            return Err(Error::StarRocksFailure {
                status: resp.status,
                message: resp.message.unwrap_or_default(),
                error_log_url: resp.error_log_url,
            });
        }

        Ok(resp)
    }

    /// V2 API - Abort ongoing transactional block
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Label contains invalid characters
    /// - Network request fails
    /// - Transaction rollback fails on the server
    /// - Response parsing fails
    pub async fn rollback_transaction(&self, label: &str) -> Result<StreamLoadResponse> {
        let db = self.default_db();
        let path = "/api/transaction/rollback";

        let mut headers = HeaderMap::new();
        headers.insert(
            "label",
            HeaderValue::from_str(label)
                .map_err(|_| Error::Transaction("Invalid label name".to_string()))?,
        );
        headers.insert(
            "db",
            HeaderValue::from_str(db)
                .map_err(|_| Error::Transaction("Invalid db name".to_string()))?,
        );

        let config = self.http_client.config();
        if config.enable_multi_table_transaction {
            headers.insert("transaction_type", HeaderValue::from_static("multi"));
        } else {
            let table = self.default_table();
            headers.insert(
                "table",
                HeaderValue::from_str(table)
                    .map_err(|_| Error::Transaction("Invalid table name".to_string()))?,
            );
        }

        let response = self
            .http_client
            .execute_request(Method::POST, path, headers, None)
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        if resp.status != "OK" && resp.status != "Success" {
            return Err(Error::StarRocksFailure {
                status: resp.status,
                message: resp.message.unwrap_or_default(),
                error_log_url: resp.error_log_url,
            });
        }

        Ok(resp)
    }

    /// Retrieve Status for a given label
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network request fails
    /// - Status retrieval fails on the server
    /// - Response parsing fails
    pub async fn get_load_status(&self, label: &str) -> Result<StreamLoadResponse> {
        let db = self.default_db();
        let path = format!("/api/{db}/get_load_state?label={label}");

        let response = self
            .http_client
            .execute_request(Method::GET, &path, HeaderMap::new(), None)
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        Ok(resp)
    }

    /// Cancel a load transaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network request fails
    /// - Load cancellation fails on the server
    /// - Response parsing fails
    pub async fn cancel_load(
        &self,
        label: &str,
        database: &str,
        table: &str,
    ) -> Result<StreamLoadResponse> {
        let path = format!("/api/{database}/{table}/_cancel?label={label}");

        let response = self
            .http_client
            .execute_request(Method::POST, &path, HeaderMap::new(), None)
            .await?;

        let status_code = response.status();
        let body_bytes = response.bytes().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: String::from_utf8_lossy(&body_bytes).into_owned(),
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_slice(&body_bytes)?;
        Ok(resp)
    }

    /// Fetch the error log from the given URL. If sanitize is true, it will redact sensitive row/column details.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - URL is invalid
    /// - Network request fails
    /// - Response text retrieval fails
    pub async fn get_error_log(&self, error_url: &str, sanitize: bool) -> Result<String> {
        if !error_url.starts_with("http://") && !error_url.starts_with("https://") {
            return Err(Error::Transaction("Invalid error log URL".to_string()));
        }

        let response = self.http_client.get_request(error_url).await?;
        let status = response.status();
        let mut body = response.text().await?;

        if status != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status.to_string(),
                message: body,
                error_log_url: None,
            });
        }

        if body.len() > 3000 {
            let mut new_len = 3000;
            while !body.is_char_boundary(new_len) {
                new_len -= 1;
            }
            body.truncate(new_len);
        }

        if sanitize {
            Ok(crate::error::sanitize_error_log(&body))
        } else {
            Ok(body)
        }
    }

    /// Try to parse error log URL from txn abort reason, fetch it, and optionally sanitize it.
    pub async fn try_get_error_log_for_merge_commit(
        &self,
        txn_abort_reason: &str,
        sanitize: bool,
    ) -> Option<String> {
        let url = crate::error::try_get_error_log_url_from_txn_abort_reason(txn_abort_reason)?;
        self.get_error_log(&url, sanitize).await.ok()
    }
}

fn to_header_val(name: &str, val: &str) -> Result<HeaderValue> {
    if (name == "row_delimiter" || name == "column_separator")
        && (val == "\n" || val == "\r" || val == "\t" || val == "\r\n")
    {
        let escaped = val
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t");
        HeaderValue::from_str(&escaped)
            .map_err(|e| Error::Transaction(format!("Invalid character in header '{name}': {e}")))
    } else {
        HeaderValue::from_str(val)
            .map_err(|e| Error::Transaction(format!("Invalid character in header '{name}': {e}")))
    }
}

#[doc(hidden)]
pub fn build_headers(props: &StreamLoadTableProperties) -> Result<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();

    if let Some(format) = &props.format {
        let fmt_str = match format {
            DataFormat::CSV => "csv",
            DataFormat::JSON => "json",
            DataFormat::ARROW => "arrow",
        };
        headers.insert("format", reqwest::header::HeaderValue::from_static(fmt_str));
    }
    if let Some(sep) = &props.column_separator {
        headers.insert("column_separator", to_header_val("column_separator", sep)?);
    }
    if let Some(delim) = &props.row_delimiter {
        headers.insert("row_delimiter", to_header_val("row_delimiter", delim)?);
    }
    if let Some(cols) = &props.columns {
        headers.insert("columns", to_header_val("columns", cols)?);
    }
    if let Some(paths) = &props.jsonpaths {
        headers.insert("jsonpaths", to_header_val("jsonpaths", paths)?);
    }
    if let Some(strip) = props.strip_outer_array {
        headers.insert(
            "strip_outer_array",
            reqwest::header::HeaderValue::from_static(if strip { "true" } else { "false" }),
        );
    }
    if let Some(ignore) = props.ignore_json_size {
        headers.insert(
            "ignore_json_size",
            reqwest::header::HeaderValue::from_static(if ignore { "true" } else { "false" }),
        );
    }
    if let Some(ratio) = props.max_filter_ratio {
        headers.insert(
            "max_filter_ratio",
            to_header_val("max_filter_ratio", &ratio.to_string())?,
        );
    }
    if let Some(strict) = props.strict_mode {
        headers.insert(
            "strict_mode",
            reqwest::header::HeaderValue::from_static(if strict { "true" } else { "false" }),
        );
    }
    if let Some(timeout) = props.timeout {
        headers.insert(
            "timeout",
            reqwest::header::HeaderValue::from(u64::from(timeout)),
        );
    }
    if let Some(comp) = &props.compression {
        headers.insert("compression", to_header_val("compression", comp)?);
    }
    if let Some(skip) = props.skip_header {
        headers.insert(
            "skip_header",
            reqwest::header::HeaderValue::from(u64::from(skip)),
        );
    }
    if let Some(wh) = &props.where_clause {
        headers.insert("where", to_header_val("where", wh)?);
    }
    if let Some(parts) = &props.partitions {
        headers.insert("partitions", to_header_val("partitions", parts)?);
    }
    if let Some(neg) = props.negative {
        headers.insert(
            "negative",
            reqwest::header::HeaderValue::from_static(if neg { "true" } else { "false" }),
        );
    }
    if let Some(tz) = &props.timezone {
        headers.insert("timezone", to_header_val("timezone", tz)?);
    }
    for (k, v) in &props.custom_headers {
        let key = reqwest::header::HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| Error::Transaction(format!("Invalid custom header name '{k}': {e}")))?;
        let val = to_header_val(k, v)?;
        headers.insert(key, val);
    }

    Ok(headers)
}

/// Converts a delimiter configuration to the proper format.
///
/// # Errors
///
/// Returns an error if:
/// - The delimiter is null or empty
/// - The delimiter format is invalid
pub fn convert_delimiter(origin_str: &str) -> Result<String> {
    if origin_str.is_empty() {
        return Err(Error::Transaction(
            "The delimiter can't be null or empty".to_string(),
        ));
    }

    let upper = origin_str.to_uppercase();
    if upper.starts_with("\\X") || upper.starts_with("0X") {
        let hex_str = &origin_str[2..];
        if hex_str.is_empty() {
            return Err(Error::Transaction(format!(
                "Invalid delimiter '{origin_str}': empty hex string"
            )));
        }
        if hex_str.len() % 2 != 0 {
            return Err(Error::Transaction(format!(
                "Invalid delimiter '{origin_str}': hex length must be an even number"
            )));
        }

        let mut bytes = Vec::new();
        let mut chars = hex_str.chars();
        while let (Some(c1), Some(c2)) = (chars.next(), chars.next()) {
            let h1 = c1.to_digit(16).ok_or_else(|| {
                Error::Transaction(format!(
                    "Invalid delimiter '{origin_str}': invalid hex format"
                ))
            })?;
            let h2 = c2.to_digit(16).ok_or_else(|| {
                Error::Transaction(format!(
                    "Invalid delimiter '{origin_str}': invalid hex format"
                ))
            })?;
            #[allow(clippy::cast_possible_truncation)]
            bytes.push((h1 << 4 | h2) as u8);
        }

        let s: String = bytes.into_iter().map(|b| b as char).collect();
        Ok(s)
    } else {
        Ok(origin_str.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DataFormat, StreamLoadTableProperties};

    #[test]
    fn test_build_headers_all_permutations() {
        // Test basic format headers
        let props_csv = StreamLoadTableProperties::builder()
            .format(DataFormat::CSV)
            .build();
        let hdrs_csv = build_headers(&props_csv).unwrap();
        assert_eq!(hdrs_csv.get("format").unwrap(), "csv");

        let props_json = StreamLoadTableProperties::builder()
            .format(DataFormat::JSON)
            .build();
        let hdrs_json = build_headers(&props_json).unwrap();
        assert_eq!(hdrs_json.get("format").unwrap(), "json");

        let props_arrow = StreamLoadTableProperties::builder()
            .format(DataFormat::ARROW)
            .build();
        let hdrs_arrow = build_headers(&props_arrow).unwrap();
        assert_eq!(hdrs_arrow.get("format").unwrap(), "arrow");

        // Test escaping of column separator and row delimiter
        let props_delim = StreamLoadTableProperties::builder()
            .column_separator("\t")
            .row_delimiter("\n")
            .build();
        let hdrs_delim = build_headers(&props_delim).unwrap();
        assert_eq!(hdrs_delim.get("column_separator").unwrap(), "\\t");
        assert_eq!(hdrs_delim.get("row_delimiter").unwrap(), "\\n");

        // Test other table properties
        let props_full = StreamLoadTableProperties::builder()
            .columns("c1,c2")
            .jsonpaths("$.c1,$.c2")
            .strip_outer_array(true)
            .ignore_json_size(false)
            .max_filter_ratio(0.12)
            .strict_mode(true)
            .timeout(180)
            .compression("gzip")
            .skip_header(2)
            .where_clause("c1 > 0")
            .partitions("p1,p2")
            .negative(true)
            .timezone("Asia/Shanghai")
            .header("X-Custom", "Val")
            .build();

        let hdrs_full = build_headers(&props_full).unwrap();
        assert_eq!(hdrs_full.get("columns").unwrap(), "c1,c2");
        assert_eq!(hdrs_full.get("jsonpaths").unwrap(), "$.c1,$.c2");
        assert_eq!(hdrs_full.get("strip_outer_array").unwrap(), "true");
        assert_eq!(hdrs_full.get("ignore_json_size").unwrap(), "false");
        assert_eq!(hdrs_full.get("max_filter_ratio").unwrap(), "0.12");
        assert_eq!(hdrs_full.get("strict_mode").unwrap(), "true");
        assert_eq!(hdrs_full.get("timeout").unwrap(), "180");
        assert_eq!(hdrs_full.get("compression").unwrap(), "gzip");
        assert_eq!(hdrs_full.get("skip_header").unwrap(), "2");
        assert_eq!(hdrs_full.get("where").unwrap(), "c1 > 0");
        assert_eq!(hdrs_full.get("partitions").unwrap(), "p1,p2");
        assert_eq!(hdrs_full.get("negative").unwrap(), "true");
        assert_eq!(hdrs_full.get("timezone").unwrap(), "Asia/Shanghai");
        assert_eq!(hdrs_full.get("X-Custom").unwrap(), "Val");
    }

    #[test]
    fn test_build_headers_invalid_custom_header() {
        // Custom header name containing invalid character (e.g., control characters)
        let props = StreamLoadTableProperties::builder()
            .header("X-Header\nName", "Val")
            .build();
        let res = build_headers(&props);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Invalid custom header name")
        );
    }

    #[test]
    fn test_convert_delimiter_direct() {
        // Uppercase 0X variant
        assert_eq!(convert_delimiter("0X0A").unwrap(), "\n");

        // Length not even error
        let err_len = convert_delimiter("0x1").unwrap_err();
        assert!(
            err_len
                .to_string()
                .contains("hex length must be an even number")
        );

        // Invalid hex character error
        let err_hex = convert_delimiter("0x1g").unwrap_err();
        assert!(err_hex.to_string().contains("invalid hex format"));
    }
}
