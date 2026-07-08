#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::module_name_repetitions,
    clippy::missing_panics_doc,
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::collapsible_if,
    clippy::if_not_else
)]

//! # `StarRocks` Stream Load Rust SDK
//!
//! A production-ready Rust client for loading data into `StarRocks` via the Stream Load API.
//! This SDK supports both V1 (Direct Load) and V2 (2PC Transaction) APIs, providing ACID
//! guarantees for critical data ingestion workflows.
//!
//! ## Features
//!
//! - **V1 API (Direct Load)**: Simple single-shot data loading with automatic retry
//! - **V2 API (2PC Transactions)**: Two-phase commit for exactly-once semantics
//! - **High Availability**: Multi-Fe configuration with automatic failover
//! - **Error Handling**: Comprehensive error types and sensitive information redaction
//! - **Configurable**: Builder patterns for flexible configuration
//! - **Type-Safe**: Strongly typed response structures and data formats
//!
//! ## Quick Start
//!
//! ### V1 Direct Load
//!
//! ```rust,no_run
//! use starrocks_stream_load::{
//!     DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
//! };
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = StreamLoadConfig::builder(
//!         vec!["http://127.0.0.1:8030".to_string()],
//!         "my_database".to_string(),
//!         "admin".to_string(),
//!     )
//!     .password("your_password")
//!     .build();
//!
//!     let properties = StreamLoadTableProperties::builder()
//!         .table("my_table")
//!         .format(DataFormat::CSV)
//!         .column_separator(",")
//!         .build();
//!
//!     let manager = StreamLoadManager::new(config, properties)?;
//!
//!     let data = Bytes::from("1,John,Doe\n2,Jane,Smith\n");
//!     let response = manager.send_single_batch("test_label_001", data).await?;
//!
//!     assert_eq!(response.status, "Success");
//!     Ok(())
//! }
//! ```
//!
//! ### V2 2PC Transaction
//!
//! ```rust,no_run
//! use starrocks_stream_load::{
//!     DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
//! };
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = StreamLoadConfig::builder(
//!         vec!["http://127.0.0.1:8030".to_string()],
//!         "my_database".to_string(),
//!         "admin".to_string(),
//!     )
//!     .password("your_password")
//!     .enable_transaction(true)
//!     .build();
//!
//!     let properties = StreamLoadTableProperties::builder()
//!         .table("my_table")
//!         .format(DataFormat::CSV)
//!         .column_separator(",")
//!         .build();
//!
//!     let manager = StreamLoadManager::new(config, properties)?;
//!
//!     // Begin transaction
//!     let label = "txn_2024_07_07_001";
//!     let txn_id = manager.begin_transaction(label).await?;
//!
//!     // Load data in transaction
//!     let data1 = Bytes::from("1,John,Doe\n");
//!     manager.load_transaction_data(label, "my_database", "my_table", 0, data1).await?;
//!
//!     let data2 = Bytes::from("2,Jane,Smith\n");
//!     manager.load_transaction_data(label, "my_database", "my_table", 1, data2).await?;
//!
//!     // Prepare and commit
//!     manager.prepare_transaction(label).await?;
//!     manager.commit_transaction(label).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! The SDK is organized into several key modules:
//!
//! - [`config`] - Configuration builders for load settings and table properties
//! - [`manager`] - Core `StreamLoadManager` with HTTP 307 redirect handling
//! - [`types`] - Response types and data structures
//! - [`error`] - Comprehensive error handling and utilities
//! - [`http`] - Low-level HTTP client with failover support
//!
//! ## Production Considerations
//!
//! This SDK provides building block functionality for `StarRocks` stream loading.
//! For production deployments, the application layer should implement:
//!
//! - **Retry strategies**: Exponential backoff with jitter
//! - **Circuit breakers**: Prevent cascading failures
//! - **Metrics collection**: Performance monitoring and alerting
//! - **Transaction recovery**: State management for 2PC transactions
//! - **Error handling**: Appropriate logging and recovery procedures
//!
//! See the [examples](https://github.com/your-repo/starrocks-stream-load/tree/main/examples)
//! directory for comprehensive production-ready implementations.
//!
//! ## API Documentation
//!
//! For detailed API documentation, see:
//! - [`StreamLoadConfig`] - Main configuration builder
//! - [`StreamLoadManager`] - Core data loading operations
//! - [`StreamLoadResponse`] - Response structure
//! - [`Error`] - Error types and handling
//!
//! ## License
//!
//! This project is licensed under the MIT License - see the LICENSE file for details.

#[cfg(all(feature = "rustls", feature = "native-tls"))]
compile_error!("Features 'rustls' and 'native-tls' are mutually exclusive; select only one.");

pub mod config;
pub mod error;
pub mod http;
pub mod manager;
pub mod types;

pub use config::{
    DataFormat, StreamLoadConfig, StreamLoadConfigBuilder, StreamLoadTableProperties,
    StreamLoadTablePropertiesBuilder,
};
pub use error::{
    Error, Result, redact_sensitive_info, sanitize_error_log,
    try_get_error_log_url_from_txn_abort_reason,
};
pub use http::StarRocksHttpClient;
pub use manager::{StreamLoadManager, build_headers, convert_delimiter};
pub use types::StreamLoadResponse;
