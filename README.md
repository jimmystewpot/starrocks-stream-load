[![Latest Version](https://img.shields.io/crates/v/starrocks-stream-load)](https://crates.io/crates/starrocks-stream-load)
[![Documentation](https://docs.rs/starrocks-stream-load/badge.svg)](https://docs.rs/starrocks-stream-load)
[![codecov](https://codecov.io/github/jimmystewpot/starrocks-stream-load/graph/badge.svg?token=EZKEKTR0F1)](https://codecov.io/github/jimmystewpot/starrocks-stream-load)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=jimmystewpot_starrocks-stream-load&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=jimmystewpot_starrocks-stream-load)

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

## Production Best Practices

This SDK provides core building blocks for StarRocks stream loading, but for production deployments, applications should implement additional resilience patterns:

### Retry Strategy
Implement exponential backoff with jitter to handle transient failures:

```rust
async fn with_backoff<F, Fut, T>(mut f: F, max_attempts: usize) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    for attempt in 0..max_attempts {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) if attempt < max_attempts - 1 => {
                let delay_ms = 100 * 2u64.pow(attempt as u32);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
```

### Circuit Breaker
Prevent cascading failures by implementing circuit breakers around critical operations:

```rust
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct CircuitBreaker {
    state: AtomicU8, // 0=closed, 1=open, 2=half-open
    failures: AtomicU64,
    last_failure: AtomicU64,
}

impl CircuitBreaker {
    async fn call<F, Fut, T>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        if !self.can_proceed() {
            return Err(Error::Transaction("Circuit breaker open".to_string()));
        }
        
        let result = f().await;
        match &result {
            Ok(_) => self.record_success(),
            Err(_) => self.record_failure(),
        }
        result
    }
    
    fn can_proceed(&self) -> bool {
        // Circuit breaker logic
        true
    }
    
    fn record_success(&self) {
        self.state.store(0, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
    }
    
    fn record_failure(&self) {
        let failures = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= 5 {
            self.state.store(1, Ordering::Relaxed);
        }
    }
}
```

### Monitoring & Metrics
Track request success rates, latencies, and error rates:

```rust
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

struct LoadMetrics {
    requests: AtomicUsize,
    successes: AtomicUsize,
    failures: AtomicUsize,
    latency_ms: AtomicU64,
}

impl LoadMetrics {
    fn track<F, Fut, T>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        
        let result = f().await;
        let elapsed = start.elapsed().as_millis() as u64;
        
        match result {
            Ok(value) => {
                self.successes.fetch_add(1, Ordering::Relaxed);
                self.latency_ms.fetch_add(elapsed, Ordering::Relaxed);
                Ok(value)
            }
            Err(e) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }
    
    fn success_rate(&self) -> f64 {
        let total = self.requests.load(Ordering::Relaxed);
        if total == 0 { return 0.0; }
        (self.successes.load(Ordering::Relaxed) as f64 / total as f64) * 100.0
    }
}
```

### Transaction State Management
For 2PC transactions, implement proper state tracking and recovery procedures:

```rust
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone)]
enum TxnState {
    Begun(i64),
    Prepared,
    Committed,
    Aborted,
}

struct TransactionTracker {
    transactions: Mutex<HashMap<String, TxnState>>,
}

impl TransactionTracker {
    fn begin(&self, label: String, txn_id: i64) {
        self.transactions.lock().unwrap()
            .insert(label, TxnState::Begun(txn_id));
    }
    
    fn prepare(&self, label: String) -> Result<(), Error> {
        let mut txns = self.transactions.lock().unwrap();
        match txns.get_mut(&label) {
            Some(state) => {
                *state = TxnState::Prepared;
                Ok(())
            }
            None => Err(Error::Transaction("Unknown transaction".to_string())),
        }
    }
    
    fn commit(&self, label: String) -> Result<(), Error> {
        let mut txns = self.transactions.lock().unwrap();
        match txns.get_mut(&label) {
            Some(state) => {
                *state = TxnState::Committed;
                Ok(())
            }
            None => Err(Error::Transaction("Unknown transaction".to_string())),
        }
    }
}
```

See [AGENTS.md](AGENTS.md) for complete production implementation examples and operational guidelines.

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
To run unit and mock integration tests:
```bash
cargo test
```

To run the automated E2E integration tests against a local StarRocks 4.0 Docker instance:
```bash
./tests/run_e2e.sh
```
For manual/step-by-step setup details of E2E testing, see [DEVELOPING.md](DEVELOPING.md).

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
