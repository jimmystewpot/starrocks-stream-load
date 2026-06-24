use bytes::Bytes;
use reqwest::Method;
use reqwest::header::{EXPECT, HeaderMap, HeaderValue};

use crate::config::{DataFormat, StreamLoadConfig, StreamLoadTableProperties};
use crate::error::{Error, Result};
use crate::http::StarRocksHttpClient;
use crate::types::StreamLoadResponse;

pub struct StreamLoadManager {
    http_client: StarRocksHttpClient,
    properties: StreamLoadTableProperties,
}

impl StreamLoadManager {
    pub fn new(config: StreamLoadConfig, properties: StreamLoadTableProperties) -> Result<Self> {
        Ok(Self {
            http_client: StarRocksHttpClient::new(config)?,
            properties,
        })
    }

    #[must_use]
    pub fn client(&self) -> &StarRocksHttpClient {
        &self.http_client
    }

    #[must_use]
    pub fn properties(&self) -> &StreamLoadTableProperties {
        &self.properties
    }

    fn default_db(&self) -> &str {
        self.properties
            .database
            .as_deref()
            .unwrap_or(&self.http_client.config().database)
    }

    fn default_table(&self) -> &str {
        self.properties.table.as_deref().unwrap_or("")
    }

    /// V1 API - Direct Standard Synchronous Load
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
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
    pub async fn get_load_status(&self, label: &str) -> Result<StreamLoadResponse> {
        let db = self.default_db();
        let path = format!("/api/{db}/get_load_state?label={label}");

        let response = self
            .http_client
            .execute_request(Method::GET, &path, HeaderMap::new(), None)
            .await?;

        let status_code = response.status();
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
        Ok(resp)
    }

    /// Cancel a load transaction
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
        let body_str = response.text().await?;

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::StarRocksFailure {
                status: status_code.to_string(),
                message: body_str,
                error_log_url: None,
            });
        }

        let resp: StreamLoadResponse = serde_json::from_str(&body_str)?;
        Ok(resp)
    }

    /// Fetch the error log from the given URL. If sanitize is true, it will redact sensitive row/column details.
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
            body.truncate(3000);
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
    HeaderValue::from_str(val)
        .map_err(|e| Error::Transaction(format!("Invalid character in header '{name}': {e}")))
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
        headers.insert("timeout", to_header_val("timeout", &timeout.to_string())?);
    }
    if let Some(comp) = &props.compression {
        headers.insert("compression", to_header_val("compression", comp)?);
    }
    if let Some(skip) = props.skip_header {
        headers.insert(
            "skip_header",
            to_header_val("skip_header", &skip.to_string())?,
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
        let key = reqwest::header::HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
            Error::Transaction(format!("Invalid custom header name '{k}': {e}"))
        })?;
        let val = to_header_val(k, v)?;
        headers.insert(key, val);
    }

    Ok(headers)
}

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
        if !hex_str.len().is_multiple_of(2) {
            return Err(Error::Transaction(format!(
                "Invalid delimiter '{origin_str}': hex length must be a even number"
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
