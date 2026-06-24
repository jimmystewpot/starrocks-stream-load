use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFormat {
    CSV,
    JSON,
    ARROW,
}

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
    #[must_use]
    pub fn builder(
        load_urls: Vec<String>,
        database: String,
        username: String,
    ) -> StreamLoadConfigBuilder {
        StreamLoadConfigBuilder::new(load_urls, database, username)
    }
}

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
    pub database: Option<String>,
    pub table: Option<String>,
    pub format: Option<DataFormat>,
    pub column_separator: Option<String>,
    pub row_delimiter: Option<String>,
    pub columns: Option<String>,
    pub jsonpaths: Option<String>,
    pub strip_outer_array: Option<bool>,
    pub ignore_json_size: Option<bool>,
    pub max_filter_ratio: Option<f64>,
    pub strict_mode: Option<bool>,
    pub timeout: Option<u32>,
    pub compression: Option<String>,
    pub skip_header: Option<u32>,
    pub where_clause: Option<String>,
    pub partitions: Option<String>,
    pub negative: Option<bool>,
    pub timezone: Option<String>,
    pub custom_headers: HashMap<String, String>,
}

impl StreamLoadTableProperties {
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
        let config = StreamLoadConfig::builder(
            vec!["127.0.0.1:8030".to_string()],
            "db".to_string(),
            "admin".to_string(),
        )
        .password("password123")
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

        assert_eq!(config.password, Some("password123".to_string()));
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
