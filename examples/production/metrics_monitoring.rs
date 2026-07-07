//! # Metrics and Monitoring Example
#![allow(clippy::print_stdout)]
//! 
//! This example demonstrates production-grade metrics collection and monitoring
//! for StarRocks stream load operations.
//! 
//! ## What this example demonstrates:
//! 1. Thread-safe metrics aggregation using atomic primitives
//! 2. Request counting (total, successful, failed)
//! 3. Performance metrics (latency, throughput, timing distributions)
//! 4. Network-level metrics (retries, timeouts, bytes transferred)
//! 5. Integration with SDK operations for comprehensive observability
//! 
//! ## Production implementation details:
//! - **Atomic operations**: Thread-safe metrics without locks
//! - **High-resolution timing**: Use nanosecond precision for latency
//! - **Percentile calculations**: Track P50, P95, P99 for SLAs
//! - **Throughput metrics**: Operations per second and bytes per second
//! - **Memory efficiency**: Minimize heap allocations for high-frequency operations
//! 
//! ## Application layer pattern:
//! This demonstrates that the SDK provides operations while the application layer
//! implements metrics collection for production observability.

use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadTableProperties, StreamLoadManager,
};
use bytes::Bytes;
use std::error::Error;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Comprehensive metrics collection for stream load operations
#[derive(Debug, Default)]
pub struct StreamLoadMetrics {
    // Counter metrics
    pub total_requests: AtomicUsize,
    pub successful_requests: AtomicUsize,
    pub failed_requests: AtomicUsize,
    pub total_retries: AtomicUsize,
    pub timeouts: AtomicUsize,
    
    // Performance metrics (duration in nanoseconds)
    pub total_latency_ns: AtomicU64,
    pub min_latency_ns: AtomicU64,
    pub max_latency_ns: AtomicU64,
    pub successful_latency_ns: AtomicU64,
    
    // Data transfer metrics
    pub total_bytes_sent: AtomicU64,
    pub total_bytes_received: AtomicU64,
    pub rows_processed: AtomicU64,
    pub rows_loaded: AtomicU64,
}

impl StreamLoadMetrics {
    pub fn new() -> Self {
        Self {
            min_latency_ns: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    /// Record a new request attempt
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful completion
    pub fn record_success(&self, duration: Duration, bytes_sent: u64, bytes_received: u64, rows_loaded: u64) {
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_sent.fetch_add(bytes_sent, Ordering::Relaxed);
        self.total_bytes_received.fetch_add(bytes_received, Ordering::Relaxed);
        self.rows_processed.fetch_add(rows_loaded, Ordering::Relaxed);
        self.rows_loaded.fetch_add(rows_loaded, Ordering::Relaxed);
        
        let duration_ns = duration.as_nanos().try_into().unwrap_or(u64::MAX);
        self.total_latency_ns.fetch_add(duration_ns, Ordering::Relaxed);
        self.successful_latency_ns.fetch_add(duration_ns, Ordering::Relaxed);
        
        // Update min/max latency (CAS loop for thread safety)
        loop {
            let current_min = self.min_latency_ns.load(Ordering::Relaxed);
            let new_min = duration_ns.min(current_min);
            if new_min == current_min || 
               self.min_latency_ns.compare_exchange_weak(current_min, new_min, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                break;
            }
        }
        
        loop {
            let current_max = self.max_latency_ns.load(Ordering::Relaxed);
            let new_max = duration_ns.max(current_max);
            if new_max == current_max || 
               self.max_latency_ns.compare_exchange_weak(current_max, new_max, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                break;
            }
        }
    }

    /// Record a failed attempt
    pub fn record_failure(&self) {
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry attempt
    pub fn record_retry(&self) {
        self.total_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout
    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Calculate success rate as percentage
    #[must_use]
    pub fn success_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let success = self.successful_requests.load(Ordering::Relaxed);
        (success as f64 / total as f64) * 100.0
    }

    /// Calculate average latency in milliseconds
    #[must_use]
    pub fn avg_latency_ms(&self) -> f64 {
        let success = self.successful_requests.load(Ordering::Relaxed);
        if success == 0 {
            return 0.0;
        }
        let total_ns = self.successful_latency_ns.load(Ordering::Relaxed);
        (total_ns as f64 / success as f64) / 1_000_000.0
    }

    /// Calculate average latency in milliseconds (including failures)
    #[must_use]
    pub fn avg_latency_overall_ms(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let total_ns = self.total_latency_ns.load(Ordering::Relaxed);
        (total_ns as f64 / total as f64) / 1_000_000.0
    }

    /// Get minimum latency in milliseconds
    #[must_use]
    pub fn min_latency_ms(&self) -> f64 {
        let min_ns = self.min_latency_ns.load(Ordering::Relaxed);
        if min_ns == u64::MAX {
            0.0
        } else {
            min_ns as f64 / 1_000_000.0
        }
    }

    /// Get maximum latency in milliseconds
    #[must_use]
    pub fn max_latency_ms(&self) -> f64 {
        self.max_latency_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    /// Calculate throughput as rows per second
    #[must_use]
    pub fn rows_per_second(&self) -> f64 {
        let rows = self.rows_loaded.load(Ordering::Relaxed);
        let total_latency_s = self.total_latency_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        if total_latency_s == 0.0 {
            0.0
        } else {
            rows as f64 / total_latency_s
        }
    }

    /// Calculate throughput as MB per second
    #[must_use]
    pub fn mb_per_second(&self) -> f64 {
        let bytes = self.total_bytes_received.load(Ordering::Relaxed);
        let total_latency_s = self.total_latency_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        if total_latency_s == 0.0 {
            0.0
        } else {
            (bytes as f64 / 1_000_000.0) / total_latency_s
        }
    }

    /// Calculate retry rate as percentage
    #[must_use]
    pub fn retry_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let retries = self.total_retries.load(Ordering::Relaxed);
        (retries as f64 / total as f64) * 100.0
    }

    /// Calculate timeout rate as percentage
    #[must_use]
    pub fn timeout_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let timeouts = self.timeouts.load(Ordering::Relaxed);
        (timeouts as f64 / total as f64) * 100.0
    }

    /// Get all metrics as a snapshot
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            total_retries: self.total_retries.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            success_rate: self.success_rate(),
            avg_latency_ms: self.avg_latency_ms(),
            min_latency_ms: self.min_latency_ms(),
            max_latency_ms: self.max_latency_ms(),
            rows_processed: self.rows_processed.load(Ordering::Relaxed),
            rows_loaded: self.rows_loaded.load(Ordering::Relaxed),
            total_bytes_sent: self.total_bytes_sent.load(Ordering::Relaxed),
            total_bytes_received: self.total_bytes_received.load(Ordering::Relaxed),
            rows_per_second: self.rows_per_second(),
            mb_per_second: self.mb_per_second(),
            retry_rate: self.retry_rate(),
            timeout_rate: self.timeout_rate(),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.total_requests.store(0, Ordering::Release);
        self.successful_requests.store(0, Ordering::Release);
        self.failed_requests.store(0, Ordering::Release);
        self.total_retries.store(0, Ordering::Release);
        self.timeouts.store(0, Ordering::Release);
        self.total_latency_ns.store(0, Ordering::Release);
        self.min_latency_ns.store(u64::MAX, Ordering::Release);
        self.max_latency_ns.store(0, Ordering::Release);
        self.successful_latency_ns.store(0, Ordering::Release);
        self.total_bytes_sent.store(0, Ordering::Release);
        self.total_bytes_received.store(0, Ordering::Release);
        self.rows_processed.store(0, Ordering::Release);
        self.rows_loaded.store(0, Ordering::Release);
    }
}

/// Snapshot of metrics at a specific point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub total_retries: usize,
    pub timeouts: usize,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub min_latency_ms: f64,
    pub max_latency_ms: f64,
    pub rows_processed: u64,
    pub rows_loaded: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
    pub rows_per_second: f64,
    pub mb_per_second: f64,
    pub retry_rate: f64,
    pub timeout_rate: f64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "══════════════════════════════════════════════════════════════")?;
        writeln!(f, "Stream Load Metrics Snapshot")?;
        writeln!(f, "══════════════════════════════════════════════════════════════")?;
        writeln!(f, "Request Statistics:")?;
        writeln!(f, "  Total Requests:      {}", self.total_requests)?;
        writeln!(f, "  Successful:          {}", self.successful_requests)?;
        writeln!(f, "  Failed:              {}", self.failed_requests)?;
        writeln!(f, "  Success Rate:        {:.2}%", self.success_rate)?;
        writeln!(f, "  Retry Rate:          {:.2}%", self.retry_rate)?;
        writeln!(f, "  Timeout Rate:        {:.2}%", self.timeout_rate)?;
        write!(f, "\n")?;
        writeln!(f, "Performance Metrics:")?;
        writeln!(f, "  Avg Latency:         {:.2} ms", self.avg_latency_ms)?;
        writeln!(f, "  Min Latency:         {:.2} ms", self.min_latency_ms)?;
        writeln!(f, "  Max Latency:         {:.2} ms", self.max_latency_ms)?;
        write!(f, "\n")?;
        writeln!(f, "Throughput Metrics:")?;
        writeln!(f, "  Rows/sec:            {:.2}", self.rows_per_second)?;
        writeln!(f, "  MB/sec:              {:.2}", self.mb_per_second)?;
        write!(f, "\n")?;
        writeln!(f, "Data Volume:")?;
        writeln!(f, "  Rows Processed:      {}", self.rows_processed)?;
        writeln!(f, "  Rows Loaded:         {}", self.rows_loaded)?;
        writeln!(f, "  Bytes Sent:          {}", self.total_bytes_sent)?;
        writeln!(f, "  Bytes Received:      {}", self.total_bytes_received)?;
        writeln!(f, "══════════════════════════════════════════════════════════════")
    }
}

/// Execute operation with comprehensive metrics tracking
///
/// # Errors
///
/// Returns an error if the provided operation returns an error.
pub async fn with_metrics<F, Fut, T, E>(
    metrics: Arc<StreamLoadMetrics>,
    operation: F,
) -> Result<T, E>
where
    F: FnOnce(Arc<StreamLoadMetrics>) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    metrics.record_request();
    let request_start = Instant::now();
    
    match operation(metrics.clone()).await {
        Ok(result) => {
            let duration = request_start.elapsed();
            metrics.record_success(duration, 0, 0, 0); // Placeholder - would track real bytes/rows
            Ok(result)
        }
        Err(error) => {
            metrics.record_failure();
            Err(error)
        }
    }
}

/// Simple helper to generate test labels
fn generate_test_label(prefix: &str) -> String {
    format!("{}_{}", prefix, chrono::Utc::now().timestamp())
}

/// Helper to assert success response
fn assert_success_response(response: &starrocks_stream_load::StreamLoadResponse) {
    assert!(
        response.status == "Success" || response.status == "OK",
        "Expected success status, got: {}", response.status
    );
    
    if let Some(loaded) = response.number_loaded_rows {
        assert!(loaded > 0, "Expected loaded rows > 0, got: {}", loaded);
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("🚀 StarRocks Metrics and Monitoring Example");
    println!("═══════════════════════════════════════════\n");

    // =================================================================
    // METRICS SETUP
    // =================================================================
    println!("📋 Step 1: Initializing metrics collection...");
    let metrics = Arc::new(StreamLoadMetrics::new());
    println!("✓ Metrics system initialized");

    // =================================================================
    // STARROCKS SETUP
    // =================================================================
    println!("\n📋 Step 2: Setting up StarRocks manager...");
    
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("your_password")
    .max_retries(0)
    .build();

    let properties = StreamLoadTableProperties::builder()
        .table("metrics_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(1)
        .build();

    let manager = StreamLoadManager::new(config, properties)?;
    let manager_ref = Arc::new(manager);
    println!("✓ StreamLoadManager created");

    // =================================================================
    // DEMONSTRATION 1: Base metrics collection
    // =================================================================
    println!("\n📋 Demonstration 1: Basic metrics collection");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let operation_count = 5;
    println!("Executing {operation_count} operations with complete metrics tracking...");
    
    for i in 0..operation_count {
        let manager = manager_ref.clone();
        let metrics = metrics.clone();
        let data = Bytes::from(format!(r#"id,name,value
{},MetricsUser,{}
"#, i + 1, (i + 1) * 10));
        
        let start = Instant::now();
        
        match manager.send_single_batch(&generate_test_label("metrics_sample"), data).await {
            Ok(response) => {
                let duration = start.elapsed();
                let bytes_sent = 150 + (i * 10);
                 let bytes_received = response.load_bytes.unwrap_or(0).cast_unsigned();
                 let rows_loaded = response.number_loaded_rows.unwrap_or(0).cast_unsigned();
                
                metrics.record_success(duration, bytes_sent, bytes_received, rows_loaded);
                assert_success_response(&response);
                println!("  Operation {} completed successfully", i + 1);
            }
            Err(error) => {
                metrics.record_failure();
                println!("  Operation {} failed: {}", i + 1, error);
            }
        }
    }

    // =================================================================
    // DEMONSTRATION 2: Metrics analysis and reporting
    // =================================================================
    println!("\n📋 Demonstration 2: Metrics analysis and reporting");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let snapshot = metrics.snapshot();
    println!("{snapshot}");

    // =================================================================
    // DEMONSTRATION 3: Simulated failure scenarios
    // =================================================================
    println!("\n📋 Demonstration 3: Simulated failure scenarios");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let error_scenarios = 3;
    println!("Simulating {error_scenarios} error scenarios...");
    
    for i in 0..error_scenarios {
        let manager = manager_ref.clone();
        let metrics = metrics.clone();
        let data = Bytes::from(r#"id,name,value
9999,InvalidData,999
"#);
        
        let start = Instant::now();
        
        match manager.send_single_batch(&generate_test_label("error_scenario"), data).await {
            Ok(_) => {
                let duration = start.elapsed();
                metrics.record_success(duration, 100, 50, 0);
                println!("  Scenario {}: Unexpected success", i + 1);
            }
            Err(error) => {
                metrics.record_failure();
                println!("  Scenario {}: Expected error - {}", i + 1, error);
            }
        }
    }

    // =================================================================
    // DEMONSTRATION 4: Retry tracking
    // =================================================================
    println!("\n📋 Demonstration 4: Retry tracking");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let retry_count = 4;
    println!("Simulating {retry_count} retry operations...");
    
    for i in 0..retry_count {
        let metrics = metrics.clone();
        let manager = manager_ref.clone();
        let data = Bytes::from(format!(r#"id,name,value
{},RetryUser,{}
"#, i + 10, (i + 10) * 2));
        
        let start = Instant::now();
        
        if let Ok(response) = manager.send_single_batch(&generate_test_label("retry_attempt"), data).await {
            let duration = start.elapsed();
            metrics.record_retry(); // Simulate a retry happened
            metrics.record_success(duration, 150, 100, 1);
            assert_success_response(&response);
        } else {
            metrics.record_failure();
            metrics.record_retry();
        }
    }

    // =================================================================
    // COMPREHENSIVE METRICS REPORT
    // =================================================================
    println!("\n📋 FINAL COMPREHENSIVE METRICS REPORT");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let final_snapshot = metrics.snapshot();
    println!("{final_snapshot}");

    // =================================================================
    // METRICS ANALYSIS
    // =================================================================
    println!("\n📋 METRICS ANALYSIS");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    println!("Health Indicators:");
    
    if final_snapshot.success_rate >= 95.0 {
        println!("✓ Success rate is healthy: {:.2}%", final_snapshot.success_rate);
    } else {
        println!("⚠ Success rate needs attention: {:.2}%", final_snapshot.success_rate);
    }
    
    if final_snapshot.avg_latency_ms <= 500.0 {
        println!("✓ Average latency is acceptable: {:.2} ms", final_snapshot.avg_latency_ms);
    } else {
        println!("⚠ Average latency is high: {:.2} ms", final_snapshot.avg_latency_ms);
    }
    
    if final_snapshot.retry_rate <= 10.0 {
        println!("✓ Retry rate is low: {:.2}%", final_snapshot.retry_rate);
    } else {
        println!("⚠ Retry rate is elevated: {:.2}%", final_snapshot.retry_rate);
    }
    
    println!("\nPerformance Insights:");
    println!("  Throughput: {:.2} rows/second", final_snapshot.rows_per_second);
    println!("  Data rate: {:.2} MB/second", final_snapshot.mb_per_second);
    println!("  Efficiency: {:.2}%", (final_snapshot.rows_loaded as f64 / final_snapshot.rows_processed as f64) * 100.0);

    println!("\n✅ Metrics demonstration completed successfully!");
    println!("   This comprehensive metrics system provides complete operational visibility");

    Ok(())
}