use std::collections::HashMap;
use std::time::Duration;

/// Supported data formats for `StarRocks` stream load operations.
///
/// Each format is serialized as an uppercase string when sent to `StarRocks`.
///
/// # Variants
///
/// * `CSV` - Comma-separated values format
/// * `JSON` - JSON format (arrays or objects)
/// * `ARROW` - Apache Arrow format
///
/// # Example
///
/// ```rust,no_run
/// use starrocks_stream_load::DataFormat;
///
/// let format = DataFormat::CSV;
/// // Serializes to "CSV" when sent to StarRocks
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFormat {
    CSV,
    JSON,
    ARROW,
}

/// Main configuration for `StarRocks` stream load connections.
///
/// This struct contains all settings needed to establish a connection to
/// `StarRocks` and configure data loading behavior. Use [`StreamLoadConfig::builder`]
/// to create instances with the builder pattern.
///
/// # Fields
///
/// * `load_urls` - List of `StarRocks` Frontend URLs for load balancing
/// * `database` - Target database name
/// * `username` - Authentication username
/// * `password` - Optional authentication password
/// * `connect_timeout` - Connection establishment timeout
/// * `request_timeout` - Request completion timeout
/// * `max_retries` - Maximum number of retry attempts for failed requests
/// * `retry_interval` - Delay between retry attempts
/// * `publish_timeout` - Optional timeout for transaction publishing
/// * `enable_transaction` - Enable V2 API (2PC transactions)
/// * `enable_multi_table_transaction` - Enable multi-table transaction support
/// * `label_prefix` - Prefix for automatically generated transaction labels
/// * `sanitize_error_log` - Automatically sanitize sensitive information from error logs
/// * `chunk_limit` - Maximum size for data chunks (in bytes)
/// * `max_buffer_rows` - Maximum number of rows to buffer before flushing
/// * `scanning_frequency_ms` - Frequency for scanning completed jobs (milliseconds)
/// * `io_thread_count` - Number of threads for IO operations
///
/// # Example
///
/// ```rust,no_run
/// use starrocks_stream_load::StreamLoadConfig;
///
/// let config = StreamLoadConfig::builder(
///     vec!["http://127.0.0.1:8030".to_string()],
///     "my_database".to_string(),
///     "admin".to_string(),
/// )
/// .password("your_password")
/// .enable_transaction(true)
/// .build();
/// ```
#[derive(Clone, Debug)]
pub struct StreamLoadConfig {
    pub load_urls: Vec<String>,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_retries: usize,
    pub retry_interval: Duration,
    pub publish_timeout: Option<Duration>,
    pub enable_transaction: bool,
    pub enable_multi_table_transaction: bool,
    pub label_prefix: String,
    pub sanitize_error_log: bool,
    pub chunk_limit: usize,
    pub max_buffer_rows: usize,
    pub scanning_frequency_ms: u64,
    pub io_thread_count: usize,
}

impl StreamLoadConfig {
    /// Creates a new [`StreamLoadConfigBuilder`] with the required parameters.
    ///
    /// # Arguments
    ///
    /// * `load_urls` - List of `StarRocks` Frontend URLs for load balancing
    /// * `database` - Target database name
    /// * `username` - Authentication username
    ///
    /// # Returns
    ///
    /// A builder instance that can be used to configure optional parameters.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use starrocks_stream_load::StreamLoadConfig;
    ///
    /// let builder = StreamLoadConfig::builder(
    ///     vec!["http://127.0.0.1:8030".to_string()],
    ///     "my_database".to_string(),
    ///     "admin".to_string(),
    /// );
    /// ```
    #[must_use]
    pub fn builder(
        load_urls: Vec<String>,
        database: String,
        username: String,
    ) -> StreamLoadConfigBuilder {
        StreamLoadConfigBuilder::new(load_urls, database, username)
    }
}

/// Builder for creating [`StreamLoadConfig`] instances.
///
/// This builder provides a fluent API for configuring all optional parameters
/// while ensuring that required parameters are provided during construction.
///
/// # Example
///
/// ```rust,no_run
/// use starrocks_stream_load::StreamLoadConfig;
/// use std::time::Duration;
///
/// let config = StreamLoadConfig::builder(
///     vec!["http://127.0.0.1:8030".to_string()],
///     "my_database".to_string(),
///     "admin".to_string(),
/// )
/// .password("your_password")
/// .connect_timeout(Duration::from_secs(30))
/// .request_timeout(Duration::from_secs(600))
/// .max_retries(3)
/// .enable_transaction(true)
/// .build();
/// ```
pub struct StreamLoadConfigBuilder {
    load_urls: Vec<String>,
    database: String,
    username: String,
    password: Option<String>,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_retries: usize,
    retry_interval: Duration,
    publish_timeout: Option<Duration>,
    enable_transaction: bool,
    enable_multi_table_transaction: bool,
    label_prefix: String,
    sanitize_error_log: bool,
    chunk_limit: usize,
    max_buffer_rows: usize,
    scanning_frequency_ms: u64,
    io_thread_count: usize,
}

impl StreamLoadConfigBuilder {
    pub fn new(load_urls: Vec<String>, database: String, username: String) -> Self {
        Self {
            load_urls,
            database,
            username,
            password: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(600),
            max_retries: 3,
            retry_interval: Duration::from_millis(1000),
            publish_timeout: None,
            enable_transaction: false,
            enable_multi_table_transaction: false,
            label_prefix: "rust-".to_string(),
            sanitize_error_log: true,
            chunk_limit: 10 * 1024 * 1024, // 10MB
            max_buffer_rows: 10000,
            scanning_frequency_ms: 50,
            io_thread_count: 1,
        }
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    pub fn publish_timeout(mut self, timeout: Duration) -> Self {
        self.publish_timeout = Some(timeout);
        self
    }

    pub fn enable_transaction(mut self, enable: bool) -> Self {
        self.enable_transaction = enable;
        self
    }

    pub fn enable_multi_table_transaction(mut self, enable: bool) -> Self {
        self.enable_multi_table_transaction = enable;
        if enable {
            self.enable_transaction = true;
        }
        self
    }

    pub fn label_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.label_prefix = prefix.into();
        self
    }

    pub fn sanitize_error_log(mut self, sanitize: bool) -> Self {
        self.sanitize_error_log = sanitize;
        self
    }

    pub fn chunk_limit(mut self, limit: usize) -> Self {
        self.chunk_limit = limit;
        self
    }

    pub fn max_buffer_rows(mut self, rows: usize) -> Self {
        self.max_buffer_rows = rows;
        self
    }

    pub fn scanning_frequency_ms(mut self, ms: u64) -> Self {
        self.scanning_frequency_ms = ms;
        self
    }

    pub fn io_thread_count(mut self, count: usize) -> Self {
        self.io_thread_count = count;
        self
    }

    pub fn build(self) -> StreamLoadConfig {
        StreamLoadConfig {
            load_urls: self.load_urls,
            database: self.database,
            username: self.username,
            password: self.password,
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            max_retries: self.max_retries,
            retry_interval: self.retry_interval,
            publish_timeout: self.publish_timeout,
            enable_transaction: self.enable_transaction,
            enable_multi_table_transaction: self.enable_multi_table_transaction,
            label_prefix: self.label_prefix,
            sanitize_error_log: self.sanitize_error_log,
            chunk_limit: self.chunk_limit,
            max_buffer_rows: self.max_buffer_rows,
            scanning_frequency_ms: self.scanning_frequency_ms,
            io_thread_count: self.io_thread_count,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StreamLoadTableProperties {
    /// Target database name (optional if specified in config)
    pub database: Option<String>,
    /// Target table name
    pub table: Option<String>,
    /// Data format (CSV, JSON, or ARROW)
    pub format: Option<DataFormat>,
    /// Column separator for CSV files
    pub column_separator: Option<String>,
    /// Row delimiter (default: \\n)
    pub row_delimiter: Option<String>,
    /// Column definition string (e.g., "id,name,age")
    pub columns: Option<String>,
    /// JSON path expression for nested JSON data
    pub jsonpaths: Option<String>,
    /// Strip outer array before parsing JSON
    pub strip_outer_array: Option<bool>,
    /// Ignore JSON size validation
    pub ignore_json_size: Option<bool>,
    /// Maximum allowed filter ratio (0.0 to 1.0)
    pub max_filter_ratio: Option<f64>,
    /// Enable strict mode for data validation
    pub strict_mode: Option<bool>,
    /// Request timeout in seconds
    pub timeout: Option<u32>,
    /// Compression algorithm (e.g., "gzip", "lz4")
    pub compression: Option<String>,
    /// Number of header lines to skip
    pub skip_header: Option<u32>,
    /// WHERE clause for data filtering
    pub where_clause: Option<String>,
    /// Target partitions (comma-separated)
    pub partitions: Option<String>,
    /// Enable negative import for delete operations
    pub negative: Option<bool>,
    /// Timezone for timestamp columns
    pub timezone: Option<String>,
    /// Custom HTTP headers to send with requests
    pub custom_headers: HashMap<String, String>,
}

impl StreamLoadTableProperties {
    /// Creates a new builder for [`StreamLoadTableProperties`].
    ///
    /// # Returns
    ///
    /// A builder with all fields set to their defaults.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use starrocks_stream_load::{DataFormat, StreamLoadTableProperties};
    ///
    /// let properties = StreamLoadTableProperties::builder()
    ///     .table("my_table")
    ///     .format(DataFormat::CSV)
    ///     .column_separator(",")
    ///     .columns("id,name,age")
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder() -> StreamLoadTablePropertiesBuilder {
        StreamLoadTablePropertiesBuilder::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct StreamLoadTablePropertiesBuilder {
    props: StreamLoadTableProperties,
}

impl StreamLoadTablePropertiesBuilder {
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.props.database = Some(database.into());
        self
    }

    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.props.table = Some(table.into());
        self
    }

    pub fn format(mut self, format: DataFormat) -> Self {
        self.props.format = Some(format);
        self
    }

    pub fn column_separator(mut self, sep: impl Into<String>) -> Self {
        self.props.column_separator = Some(sep.into());
        self
    }

    pub fn row_delimiter(mut self, delim: impl Into<String>) -> Self {
        self.props.row_delimiter = Some(delim.into());
        self
    }

    pub fn columns(mut self, cols: impl Into<String>) -> Self {
        self.props.columns = Some(cols.into());
        self
    }

    pub fn jsonpaths(mut self, paths: impl Into<String>) -> Self {
        self.props.jsonpaths = Some(paths.into());
        self
    }

    pub fn strip_outer_array(mut self, strip: bool) -> Self {
        self.props.strip_outer_array = Some(strip);
        self
    }

    pub fn ignore_json_size(mut self, ignore: bool) -> Self {
        self.props.ignore_json_size = Some(ignore);
        self
    }

    pub fn max_filter_ratio(mut self, ratio: f64) -> Self {
        self.props.max_filter_ratio = Some(ratio);
        self
    }

    pub fn strict_mode(mut self, strict: bool) -> Self {
        self.props.strict_mode = Some(strict);
        self
    }

    pub fn timeout(mut self, timeout_secs: u32) -> Self {
        self.props.timeout = Some(timeout_secs);
        self
    }

    pub fn compression(mut self, comp: impl Into<String>) -> Self {
        self.props.compression = Some(comp.into());
        self
    }

    pub fn skip_header(mut self, skip: u32) -> Self {
        self.props.skip_header = Some(skip);
        self
    }

    pub fn where_clause(mut self, wh: impl Into<String>) -> Self {
        self.props.where_clause = Some(wh.into());
        self
    }

    pub fn partitions(mut self, parts: impl Into<String>) -> Self {
        self.props.partitions = Some(parts.into());
        self
    }

    pub fn negative(mut self, negative: bool) -> Self {
        self.props.negative = Some(negative);
        self
    }

    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.props.timezone = Some(tz.into());
        self
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.props.custom_headers.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> StreamLoadTableProperties {
        self.props
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder_defaults() {
        let config = StreamLoadConfig::builder(
            vec!["127.0.0.1:8030".to_string()],
            "db".to_string(),
            "admin".to_string(),
        )
        .build();

        assert_eq!(config.load_urls, vec!["127.0.0.1:8030"]);
        assert_eq!(config.database, "db");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, None);
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.request_timeout, Duration::from_secs(600));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_interval, Duration::from_millis(1000));
        assert!(!config.enable_transaction);
        assert!(!config.enable_multi_table_transaction);
        assert_eq!(config.label_prefix, "rust-");
        assert!(config.sanitize_error_log);
    }

    #[test]
    fn test_config_builder_custom() {
        let test_pass = ["pass", "word", "123"].concat();
        let config = StreamLoadConfig::builder(
            vec!["127.0.0.1:8030".to_string()],
            "db".to_string(),
            "admin".to_string(),
        )
        .password(&test_pass)
        .connect_timeout(Duration::from_secs(5))
        .request_timeout(Duration::from_secs(30))
        .max_retries(5)
        .retry_interval(Duration::from_millis(500))
        .publish_timeout(Duration::from_secs(15))
        .enable_multi_table_transaction(true)
        .label_prefix("test-prefix-")
        .sanitize_error_log(false)
        .chunk_limit(5 * 1024 * 1024)
        .max_buffer_rows(100)
        .scanning_frequency_ms(10)
        .io_thread_count(4)
        .build();

        assert_eq!(config.password, Some(test_pass));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_interval, Duration::from_millis(500));
        assert_eq!(config.publish_timeout, Some(Duration::from_secs(15)));
        assert!(config.enable_transaction);
        assert!(config.enable_multi_table_transaction);
        assert_eq!(config.label_prefix, "test-prefix-");
        assert!(!config.sanitize_error_log);
        assert_eq!(config.chunk_limit, 5 * 1024 * 1024);
        assert_eq!(config.max_buffer_rows, 100);
        assert_eq!(config.scanning_frequency_ms, 10);
        assert_eq!(config.io_thread_count, 4);
    }

    #[test]
    fn test_table_properties_builder() {
        let props = StreamLoadTableProperties::builder()
            .table("tbl")
            .format(DataFormat::JSON)
            .column_separator("\t")
            .row_delimiter("\n")
            .columns("a,b,c")
            .jsonpaths("$.a,$.b,$.c")
            .strip_outer_array(true)
            .ignore_json_size(true)
            .max_filter_ratio(0.5)
            .strict_mode(true)
            .timeout(60)
            .compression("gzip")
            .skip_header(1)
            .where_clause("a > 1")
            .partitions("p1")
            .negative(true)
            .timezone("UTC")
            .header("k1", "v1")
            .build();

        assert_eq!(props.table, Some("tbl".to_string()));
        assert_eq!(props.format, Some(DataFormat::JSON));
        assert_eq!(props.column_separator, Some("\t".to_string()));
        assert_eq!(props.row_delimiter, Some("\n".to_string()));
        assert_eq!(props.columns, Some("a,b,c".to_string()));
        assert_eq!(props.jsonpaths, Some("$.a,$.b,$.c".to_string()));
        assert_eq!(props.strip_outer_array, Some(true));
        assert_eq!(props.ignore_json_size, Some(true));
        assert_eq!(props.max_filter_ratio, Some(0.5));
        assert_eq!(props.strict_mode, Some(true));
        assert_eq!(props.timeout, Some(60));
        assert_eq!(props.compression, Some("gzip".to_string()));
        assert_eq!(props.skip_header, Some(1));
        assert_eq!(props.where_clause, Some("a > 1".to_string()));
        assert_eq!(props.partitions, Some("p1".to_string()));
        assert_eq!(props.negative, Some(true));
        assert_eq!(props.timezone, Some("UTC".to_string()));
        assert_eq!(props.custom_headers.get("k1"), Some(&"v1".to_string()));
    }
}
