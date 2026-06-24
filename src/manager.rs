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

        let mut headers = build_headers(&self.properties);
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

        let mut headers = build_headers(&self.properties);
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
}

#[doc(hidden)]
pub fn build_headers(props: &StreamLoadTableProperties) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();

    if let Some(format) = &props.format {
        let fmt_str = match format {
            DataFormat::CSV => "csv",
            DataFormat::JSON => "json",
            DataFormat::ARROW => "arrow",
        };
        headers.insert("format", reqwest::header::HeaderValue::from_static(fmt_str));
    }
    if let Some(val) = props
        .column_separator
        .as_ref()
        .and_then(|sep| reqwest::header::HeaderValue::from_str(sep).ok())
    {
        headers.insert("column_separator", val);
    }
    if let Some(val) = props
        .row_delimiter
        .as_ref()
        .and_then(|delim| reqwest::header::HeaderValue::from_str(delim).ok())
    {
        headers.insert("row_delimiter", val);
    }
    if let Some(val) = props
        .columns
        .as_ref()
        .and_then(|cols| reqwest::header::HeaderValue::from_str(cols).ok())
    {
        headers.insert("columns", val);
    }
    if let Some(val) = props
        .jsonpaths
        .as_ref()
        .and_then(|paths| reqwest::header::HeaderValue::from_str(paths).ok())
    {
        headers.insert("jsonpaths", val);
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
    if let Some(val) = props
        .max_filter_ratio
        .and_then(|ratio| reqwest::header::HeaderValue::from_str(&ratio.to_string()).ok())
    {
        headers.insert("max_filter_ratio", val);
    }
    if let Some(strict) = props.strict_mode {
        headers.insert(
            "strict_mode",
            reqwest::header::HeaderValue::from_static(if strict { "true" } else { "false" }),
        );
    }
    if let Some(val) = props
        .timeout
        .and_then(|timeout| reqwest::header::HeaderValue::from_str(&timeout.to_string()).ok())
    {
        headers.insert("timeout", val);
    }
    if let Some(val) = props
        .compression
        .as_ref()
        .and_then(|comp| reqwest::header::HeaderValue::from_str(comp).ok())
    {
        headers.insert("compression", val);
    }
    if let Some(val) = props
        .skip_header
        .and_then(|skip| reqwest::header::HeaderValue::from_str(&skip.to_string()).ok())
    {
        headers.insert("skip_header", val);
    }
    if let Some(val) = props
        .where_clause
        .as_ref()
        .and_then(|wh| reqwest::header::HeaderValue::from_str(wh).ok())
    {
        headers.insert("where", val);
    }
    if let Some(val) = props
        .partitions
        .as_ref()
        .and_then(|parts| reqwest::header::HeaderValue::from_str(parts).ok())
    {
        headers.insert("partitions", val);
    }
    if let Some(neg) = props.negative {
        headers.insert(
            "negative",
            reqwest::header::HeaderValue::from_static(if neg { "true" } else { "false" }),
        );
    }
    if let Some(val) = props
        .timezone
        .as_ref()
        .and_then(|tz| reqwest::header::HeaderValue::from_str(tz).ok())
    {
        headers.insert("timezone", val);
    }
    for (k, v) in &props.custom_headers {
        if let (Ok(key), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(v),
        ) {
            headers.insert(key, val);
        }
    }

    headers
}
