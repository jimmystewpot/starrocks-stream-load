# StarRocks Stream Load Rust SDK

A production-grade, memory-safe, and high-performance asynchronous Rust SDK for StarRocks Stream Load. Aligned with the StarRocks Flink connector specification, this library provides support for both synchronous direct loads (V1 API) and two-phase commit (2PC) multi-stage transactions (V2 API).

## Features

- **Asynchronous Network Core**: Powered by `tokio` and `reqwest` for scalable, concurrent throughput.
- **V1 Synchronous Loading**: Simple one-shot stream uploads for CSV, JSON, and Arrow formats.
- **V2 Two-Phase Commit (2PC)**: Complete transactional control over multi-table or multi-batch transactions using `begin`, `load`, `prepare`, `commit`, and `rollback`.
- **Custom Redirect Handling**: Custom-built redirection engine that intercepts `307 Temporary Redirect` status codes, ensuring sensitive authentication headers (e.g. Basic Auth) are retained and payload streams can be successfully re-sent to target Backend (BE) nodes.
- **Security-First Logging**: Native redaction helper (`redact_sensitive_info`) automatically scrubs passwords and authorization headers from error messages and logs.
- **Round-Robin Failover**: Client automatically maintains node health tracking and handles automatic routing failover across multiple configured Frontend (FE) load URLs.

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
starrocks-stream-load = { git = "https://github.com/jimmystewpot/starrocks-stream-load" }
tokio = { version = "1.38", features = ["full"] }
bytes = "1.6"
```

## Quick Start

### 1. V1 API: Synchronous Direct Load

Best for simple, one-shot loading tasks:

```rust
use starrocks_stream_load::{DataFormat, StreamLoadConfig, StreamLoadTableProperties, StreamLoadManager};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure the connection
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()], // FrontEnd URLs
        "my_database".to_string(),
        "root".to_string(),
    )
    .password("my_password")
    .build();

    // 2. Set table-specific loading options
    let properties = StreamLoadTableProperties::builder()
        .table("my_table")
        .format(DataFormat::CSV)
        .column_separator(",")
        .build();

    // 3. Instantiate the manager
    let manager = StreamLoadManager::new(config, properties)?;

    // 4. Send stream data
    let payload = Bytes::from("1,Alice,20\n2,Bob,25\n");
    let response = manager.send_single_batch("label_2026_06_24", payload).await?;

    println!("Load status: {}", response.status);
    println!("Loaded rows: {:?}", response.number_loaded_rows);
    Ok(())
}
```

### 2. V2 API: Two-Phase Commit (2PC) Transactions

Required for exactly-once semantics, multi-table transactions, or loading large datasets split into multiple chunks:

```rust
use starrocks_stream_load::{StreamLoadConfig, StreamLoadTableProperties, StreamLoadManager};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "my_database".to_string(),
        "root".to_string(),
    )
    .password("my_password")
    .enable_multi_table_transaction(true) // Enables transactions across multiple tables
    .build();

    let properties = StreamLoadTableProperties::builder().build();
    let manager = StreamLoadManager::new(config, properties)?;

    let label = "txn_label_999";

    // Step 1: Begin the transaction
    let txn_id = manager.begin_transaction(label).await?;
    println!("Transaction started with ID: {txn_id}");

    // Step 2: Load chunked data into different tables
    manager.load_transaction_data(label, "my_database", "table_a", 0, Bytes::from("data_a_chunk_0")).await?;
    manager.load_transaction_data(label, "my_database", "table_b", 1, Bytes::from("data_b_chunk_0")).await?;

    // Step 3: Pre-commit (Prepare) the transaction
    let prep_res = manager.prepare_transaction(label).await?;
    println!("Prepare status: {}", prep_res.status);

    // Step 4: Commit the transaction
    let commit_res = manager.commit_transaction(label).await?;
    println!("Commit status: {}", commit_res.status);

    Ok(())
}
```

## Detailed Configuration Parameters

### Connection Configuration (`StreamLoadConfigBuilder`)
- `database(String)`: Target database name.
- `username(String)`: Username.
- `password(String)`: Optional password.
- `connect_timeout(Duration)`: TCP connection timeout. Default is 10s.
- `request_timeout(Duration)`: Request/read timeout. Default is 600s.
- `max_retries(usize)`: Maximum times to retry failed network calls. Default is 3.
- `retry_interval(Duration)`: Delay between retries. Default is 1s.
- `enable_transaction(bool)`: Enable transactional V2 API capabilities. Default is false.
- `enable_multi_table_transaction(bool)`: Allow transactional inserts across multiple target tables under a single label. Default is false.

### Table Loading Properties (`StreamLoadTablePropertiesBuilder`)
- `format(DataFormat)`: Input format (`CSV`, `JSON`, `ARROW`).
- `column_separator(String)`: Column separator for CSV.
- `row_delimiter(String)`: Row delimiter for CSV.
- `columns(String)`: List of columns mapped from source input (e.g. `col1, col2, col3`).
- `jsonpaths(String)`: JSON path query configurations.
- `max_filter_ratio(f64)`: Percentage of rows that can fail validation/parsing without failing the load task.
- `strict_mode(bool)`: Enable strict parsing mode.
- `timeout(u32)`: Ingestion timeout limit in seconds.
- `timezone(String)`: Configure session timezone for datetime columns.

## Testing & Benchmarks

### Running Tests
To run unit and mock integration tests verifying 2PC flows, redirects, and log sanitization:
```bash
cargo test
```

### Formatting & Linting
To check lint violations under the strict pedantic guidelines:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

### Running Benchmarks
To run the micro-benchmark suite measuring serialization, header-building, and log redaction throughput:
```bash
cargo bench
```

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
