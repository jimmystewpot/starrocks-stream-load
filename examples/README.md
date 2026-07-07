# StarRocks Stream Load SDK Examples

Comprehensive production-grade examples demonstrating StarRocks Stream Load SDK capabilities for Rust applications.

## 🚀 Overview

This collection of examples showcases practical implementation patterns for StarRocks data ingestion, from basic operations to advanced production architectures. Each example is designed to be educational, executable, and immediately applicable to real-world scenarios.

## 📚 Example Categories

### 🟢 Basic Examples (`examples/basic/`)
Perfect for getting started and understanding fundamentals.

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| [`v1_direct_load.rs`](basic/v1_direct_load.rs) | Simple one-shot data loading using V1 API | Basic configuration, CSV data loading, response handling |
| [`v2_transaction_basic.rs`](basic/v2_transaction_basic.rs) | Two-phase commit transaction flow | Transaction lifecycle, atomic operations, error handling |
| [`data_formats.rs`](basic/data_formats.rs) | Various data format support | CSV, JSON, Arrow, custom delimiters, data transformation |

### 🔵 Production Examples (`examples/production/`)
Essential patterns for production deployment and resilience.

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| [`exponential_backoff.rs`](production/exponential_backoff.rs) | Retry strategy with exponential backoff | Retry logic, jitter, backoff caps, error classification |
| [`circuit_breaker.rs`](production/circuit_breaker.rs) | Circuit breaker for failure prevention | Circuit states, failover, recovery testing, cascading failure prevention |
| [`metrics_monitoring.rs`](production/metrics_monitoring.rs) | Comprehensive metrics collection | Performance tracking, throughput metrics, error rates, observability |
| [`transaction_state.rs`](production/transaction_state.rs) | Transaction state management | State machines, recovery, timeout handling, conflict detection |

### 🟣 Advanced Examples (`examples/advanced/`)
Complex scenarios and architectural patterns.

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| [`multi_table_transaction.rs`](advanced/multi_table_transaction.rs) | Atomic multi-table operations | Cross-table transactions, consistency guarantees, complex rollback |
| [`error_handling_recovery.rs`](advanced/error_handling_recovery.rs) | Advanced error handling | Error classification, data validation, recovery strategies, dead letter queues |
| [`high_availability.rs`](advanced/high_availability.rs) | High availability and failover | Multi-Fe failover, health monitoring, geographic redundancy |
| [`data_pipeline.rs`](advanced/data_pipeline.rs) | Complete data pipeline integration | ETL processing, data quality, batch/stream processing, pipeline monitoring |

### 🟠 Integration Testing (`examples/integration/`)
Real-world integration scenarios and testing.

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| [`concurrent_loads.rs`](integration/concurrent_loads.rs) | High-throughput concurrent operations | Concurrent execution, rate limiting, performance optimization, resource management |

## 🛠️ Installation and Setup

### Prerequisites

- Rust 1.85+ (with 2024 edition support)
- StarRocks cluster (v2.3+ recommended)
- Basic understanding of Rust async programming

### Running Examples

**1. Navigate to examples directory:**
```bash
cd examples
```

**2. Set up environment:**
```bash
# Install dependencies
cargo build

# Or test specific example
cargo test --bin v1_direct_load
```

**3. Run specific example:**
```bash
# Basic examples
cargo run --bin v1_direct_load
cargo run --bin v2_transaction_basic
cargo run --bin data_formats

# Production examples
cargo run --bin exponential_backoff
cargo run --bin circuit_breaker

# Advanced examples
cargo run --bin multi_table_transaction
cargo run --bin concurrent_loads
```

**4. Configure StarRocks connection:**

Each example requires StarRocks connection details. The examples use placeholder configuration:

```rust
// Update these values based on your StarRocks environment
let config = StreamLoadConfig::builder(
    vec!["http://your-fe-host:8030".to_string()],
    "your_database".to_string(),
    "your_username".to_string(),
)
.password("your_password")
.build();
```

### Example-Specific Setup

Some examples have additional requirements:

**StarRocks Tables Setup:**
```sql
-- Create test database
CREATE DATABASE IF NOT EXISTS test_db;

-- Create test tables
CREATE TABLE IF NOT EXISTS test_db.simple_users (
    id INT,
    name VARCHAR(50),
    value INT
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

## 📖 Learning Path

We recommend following this path to build comprehensive understanding:

### 1. Getting Started
1. **`v1_direct_load.rs`** - Understand basic API usage
2. **`data_formats.rs`** - Learn about data format options
3. **`v2_transaction_basic.rs`** - Grasp transactional concepts

### 2. Production Patterns
4. **`exponential_backoff.rs`** - Implement retry logic
5. **`circuit_breaker.rs`** - Add failure prevention
6. **`metrics_monitoring.rs`** - Add observability
7. **`transaction_state.rs`** - Manage transaction lifecycle

### 3. Advanced Concepts
8. **`multi_table_transaction.rs`** - Handle cross-table operations
9. **`error_handling_recovery.rs`** - Build comprehensive error handling
10. **`high_availability.rs`** - Implement HA patterns
11. **`data_pipeline.rs`** - Design complete pipelines

### 4. Production Integration
12. **`concurrent_loads.rs`** - Optimize for high throughput

## 🏗️ Architecture Patterns

### SDK Design Philosophy

The StarRocks Stream Load SDK follows a **building block design**:

```
Application Layer (Your Code)
├── Retry Strategy (exponential backoff)
├── Circuit Breaker (failure prevention)
├── Metrics Collection (observability)
├── Transaction Management (state tracking)
└── Error Handling (recovery mechanisms)

SDK Layer
├── V1 Direct Load API
├── V2 Transaction API
├── HTTP Client with Failover
└── Type-safe Response Models
```

### Production Architecture Example

```rust
// Combine multiple production patterns
use starrocks_stream_load::*;

async fn production_load() -> Result<(), Box<dyn Error>> {
    // 1. Configure SDK
    let config = StreamLoadConfig::builder(...)
        .max_retries(0) // Handle at app layer
        .build();
    
    let manager = Arc::new(StreamLoadManager::new(config, properties)?);
    
    // 2. Add resilience patterns
    let circuit_breaker = Arc::new(CircuitBreaker::new(5, Duration::from_secs(60)));
    let metrics = Arc::new(Metrics::new());
    let retry_config = BackoffConfig::default();
    
    // 3. Execute with all protections
    let result = exponential_backoff(&retry_config, || async {
        if !circuit_breaker.can_proceed() {
            return Err(Error::CircuitBreakerOpen);
        }
        
        manager.send_single_batch(&label, data).await
            .map(|response| {
                circuit_breaker.record_success();
                metrics.record_success(response);
                response
            })
            .map_err(|error| {
                circuit_breaker.record_failure();
                metrics.record_failure();
                error
            })
    }).await?;
    
    Ok(())
}
```

## 📊 Production Deployment

### Configuration Recommendations

| Setting | Development | Production |
|---------|-------------|------------|
| Max Retries | 2 | 5+ (with exponential backoff) |
| Connection Timeout | 10s | 30s |
| Request Timeout | 300s | 600s+ |
| Circuit Breaker Threshold | 5 failures | 3 failures |
| Metrics Retention | In-memory | Persistent storage |
| Logging | Debug | Info/Warning with structured logs |

### Monitoring and Alerting

**Essential Metrics to Monitor:**
- Success rate (target: >99.5%)
- Average latency (baseline: <500ms)
- Circuit breaker status
- Throughput (ops/sec)
- Error types and frequencies

**Alert Thresholds:**
- Success rate < 95%
- Latency P99 > 2s
- Circuit breaker open
- Error rate > 5%

## 🛡️ Best Practices

### Performance Optimization

1. **Batch Processing**: Load data in batches rather than individual records
2. **Connection Reuse**: Maintain long-lived connections
3. **Concurrency**: Use controlled concurrency (5-10 concurrent ops)
4. **Compression**: Compress large payloads (gzip, zstd)

### Error Handling

1. **Always Use Retry Logic**: Implement exponential backoff for transient failures
2. **Validate Data**: Pre-validate data before sending to StarRocks
3. **Monitor Errors**: Track and categorize errors for proactive resolution
4. **Circuit Breaking**: Prevent cascading failures during outages

### Security

1. **Credentials**: Use environment variables or secret management
2. **TLS**: Enable SSL/TLS for all connections
3. **Audit Logging**: Log all operations for security auditing
4. **Network Security**: Use VPCs and proper network isolation

## 🧪 Testing

The examples include test utilities in [`common/mod.rs`](common/mod.rs):

```rust
// Mock StarRocks server for testing
let mock_server = common::setup_mock_starrocks().await;

// Generate test data
let csv_data = common::generate_csv_data(100);
let json_data = common::generate_json_data(50);

// Assert success responses
common::assert_success_response(&response);
```

## 📝 Common Patterns

### Basic Retry Pattern

```rust
use std::time::{Duration, sleep};
use std::future::Future;

async fn with_retry<F, Fut, T>(
    mut operation: F,
    max_attempts: usize,
) -> Result<T, Box<dyn Error>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error>>>,
{
    let mut last_error = None;
    
    for attempt in 0..max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                last_error = Some(error);
                if attempt < max_attempts - 1 {
                    let delay = Duration::from_millis(100 * 2u32.pow(attempt as u32) as u64);
                    sleep(delay).await;
                }
            }
        }
    }
    
    Err(last_error.unwrap())
}
```

### Metrics Tracking

```rust
let start = Instant::now();

match manager.send_single_batch(&label, data).await {
    Ok(response) => {
        metrics.record_success(start.elapsed(), data.len(), response.number_loaded_rows.unwrap_or(0));
    }
    Err(error) => {
        metrics.record_error(&error);
    }
}
```

## 🚨 Troubleshooting

### Common Issues

**Connection Timeout:**
- Check StarRocks FE nodes are reachable
- Verify firewall rules allow connections
- Increase connection timeout in config

**Load Failures:**
- Validate data format matches table schema
- Check for appropriate permissions
- Review StarRocks BE logs for详细信息

**Transaction Conflicts:**
- Use unique labels per transaction
- Implement proper rollback logic
- Monitor transaction state

### Debug Mode

Enable detailed logging:

```rust
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

## 🤝 Contributing

When adding new examples, follow these guidelines:

1. **Documentation**: Include comprehensive comments and explanations
2. **Error Handling**: Demonstrate proper error handling patterns
3. **Metrics**: Add relevant metrics tracking where applicable
4. **Testing**: Include test cases where possible
5. **Examples**: Use realistic, production-ready code

## 📚 Additional Resources

- [StarRocks Documentation](https://docs.starrocks.io)
- [Stream Load Guide](https://docs.starrocks.io/docs/loading/StreamLoad/)
- [SDK README](../README.md)
- [Developer Documentation](../AGENTS.md)

## ⚖️ License

These examples follow the same license as the main SDK project (Apache-2.0).

## 🆘 Support

For issues with the examples:
1. Check existing issues in the repository
2. Review StarRocks documentation
3. Enable debug logging for troubleshooting
4. Submit issue with detailed reproduction steps

---

**Note:** These examples are designed for educational purposes and production reference. Always adapt configurations and patterns to your specific requirements and environment.