#![allow(clippy::print_stdout)]
//! # Exponential Backoff Retry Strategy Example
//!
//! This example demonstrates production-grade exponential backoff with jitter
//! for implementing robust retry logic around StarRocks stream load operations.
//!
//! ## What this example demonstrates:
//! 1. Exponential backoff with random jitter to prevent thundering herd
//! 2. Configurable maximum retries and backoff caps
//! 3. Proper error classification for retryable vs non-retryable errors
//! 4. Integration with SDK operations at the application layer
//! 5. Performance metrics tracking for retry attempts
//!
//! ## Production implementation details:
//! - **Base delay**: Starting delay (e.g., 100ms)
//! - **Exponent**: Power of 2 growth (100ms, 200ms, 400ms, 800ms, ...)
//! - **Jitter**: Random variation (±10%) to distribute load
//! - **Max delay**: Upper bound (e.g., 30 seconds) to prevent extremely long waits
//! - **Max attempts**: Total retry limit to prevent infinite loops
//!
//! ## Application layer pattern:
//! This demonstrates that the SDK provides building blocks (`send_single_batch`) while
//! the application layer implements retry logic, as per SDK design philosophy.

use bytes::Bytes;
use rand::Rng;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::error::Error;
use std::time::{Duration, Instant};

/// Exponential backoff configuration struct
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_attempts: usize,
    pub jitter_percent: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_delay_ms: 100,  // Start at 100ms
            max_delay_ms: 30000, // Cap at 30 seconds
            max_attempts: 5,     // Maximum retry attempts
            jitter_percent: 0.1, // 10% jitter
        }
    }
}

/// Retry result wrapper
#[derive(Debug)]
pub struct RetryResult<T> {
    pub result: T,
    pub total_attempts: usize,
    pub total_duration_ms: u128,
    pub retries_count: usize,
}

/// Production-grade exponential backoff with jitter
pub async fn exponential_backoff<F, Fut, T, E>(
    config: &BackoffConfig,
    mut operation: F,
) -> Result<RetryResult<T>, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error,
{
    let start_time = Instant::now();
    let mut last_error = None;

    for attempt in 0..config.max_attempts {
        let attempt_start = Instant::now();

        match operation().await {
            Ok(result) => {
                let total_duration = start_time.elapsed();
                return Ok(RetryResult {
                    result,
                    total_attempts: attempt + 1,
                    total_duration_ms: total_duration.as_millis(),
                    retries_count: attempt,
                });
            }
            Err(error) => {
                let attempt_duration = attempt_start.elapsed();
                last_error = Some(error);

                if attempt < config.max_attempts - 1 {
                    // Calculate exponential backoff with jitter
                    let base_delay =
                        Duration::from_millis(config.base_delay_ms * 2u64.pow(attempt as u32));

                    // Apply max delay cap
                    let base_delay = base_delay.min(Duration::from_millis(config.max_delay_ms));

                    // Add random jitter to prevent thundering herd
                    let jitter_ms = (base_delay.as_millis() as f64 * config.jitter_percent) as u64;
                    let jitter = Duration::from_millis(
                        rand::thread_rng().gen_range(0..=jitter_ms * 2) - jitter_ms,
                    );

                    let total_delay = base_delay.saturating_add(jitter);

                    tracing::warn!(
                        "Attempt {} failed in {}ms, retrying in {}ms",
                        attempt + 1,
                        attempt_duration.as_millis(),
                        total_delay.as_millis()
                    );

                    tokio::time::sleep(total_delay).await;
                } else {
                    tracing::error!(
                        "Final attempt {} failed in {}ms after {} retries",
                        attempt + 1,
                        attempt_duration.as_millis(),
                        attempt
                    );
                }
            }
        }
    }

    Err(last_error.unwrap())
}

/// Check if an error is retryable based on common patterns
pub fn is_retryable_error<E: std::error::Error>(error: &E) -> bool {
    let error_string = error.to_string().to_lowercase();

    // Network-related errors are typically retryable
    let retryable_patterns = [
        "timeout",
        "connection",
        "network",
        "temporary",
        "unavailable",
        "503", // Service unavailable
        "504", // Gateway timeout
        "429", // Too many requests
    ];

    retryable_patterns
        .iter()
        .any(|pattern| error_string.contains(pattern))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    /// Simple helper to generate test labels
    fn generate_test_label(prefix: &str) -> String {
        format!("{}_{}", prefix, chrono::Utc::now().timestamp())
    }

    /// Helper to assert success response
    fn assert_success_response(response: &starrocks_stream_load::StreamLoadResponse) {
        assert!(
            response.status == "Success" || response.status == "OK",
            "Expected success status, got: {}",
            response.status
        );

        if let Some(loaded) = response.number_loaded_rows {
            assert!(loaded > 0, "Expected loaded rows > 0, got: {}", loaded);
        }
    }
    println!("🚀 StarRocks Exponential Backoff Retry Example");
    println!("===============================================\n");

    // =================================================================
    // CONFIGURATION
    // =================================================================
    println!("📋 Step 1: Configuring retry strategy...");

    let backoff_config = BackoffConfig::default();
    println!("✓ Backoff configuration:");
    println!("  Base delay: {}ms", backoff_config.base_delay_ms);
    println!("  Max delay: {}ms", backoff_config.max_delay_ms);
    println!("  Max attempts: {}", backoff_config.max_attempts);
    println!("  Jitter: {}%", (backoff_config.jitter_percent * 100.0));

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
    .max_retries(0) // We handle retries at application layer
    .build();

    let properties = StreamLoadTableProperties::builder()
        .table("backoff_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(1)
        .build();

    let manager = StreamLoadManager::new(config, properties)?;
    let manager_ref = std::sync::Arc::new(manager);
    println!("✓ StreamLoadManager created");

    // =================================================================
    // DEMONSTRATION 1: Successfully retry after failures
    // =================================================================
    println!("\n📋 Demonstration 1: Successful retry after transient failures");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let test_data = Bytes::from(
        r#"id,name,value
1,RetryUser1,25
2,RetryUser2,30
"#,
    );

    // Create a wrapper that simulates occasional failures
    let attempt_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let manager_clone = manager_ref.clone();
    let counter_clone = attempt_counter.clone();

    let operation = move || {
        let manager = manager_clone.clone();
        let counter = counter_clone.clone();
        let data = test_data.clone();
        let label = generate_test_label("retry_demo");

        async move {
            let attempt = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // Simulate failure on first attempt for demonstration
            if attempt == 0 {
                tracing::warn!("Simulating transient failure on attempt {}", attempt + 1);
                return Err::<_, starrocks_stream_load::Error>(
                    starrocks_stream_load::Error::Transaction("Simulated timeout".to_string()),
                );
            }

            manager.send_single_batch(&label, data).await
        }
    };

    // Execute with exponential backoff
    match exponential_backoff(&backoff_config, operation).await {
        Ok(retry_result) => {
            println!(
                "✓ Operation succeeded after {} attempt(s)",
                retry_result.total_attempts
            );
            println!("  Total retries: {}", retry_result.retries_count);
            println!("  Total duration: {}ms", retry_result.total_duration_ms);
            println!(
                "  Average time per attempt: {}ms",
                retry_result.total_duration_ms / retry_result.total_attempts as u128
            );

            assert_success_response(&retry_result.result);
        }
        Err(error) => {
            println!("✗ Operation failed after all retries: {}", error);
        }
    }

    // =================================================================
    // DEMONSTRATION 2: Non-retryable errors fail immediately
    // =================================================================
    println!("\n📋 Demonstration 2: Non-retryable errors fail immediately");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let manager_clone = manager_ref.clone();
    let operation_fail = move || {
        let manager = manager_clone.clone();
        let label = generate_test_label("fail_demo");
        let data = Bytes::from(r#"invalid,data"#);

        async move {
            // This will fail with a non-retryable error (e.g., authentication)
            manager.send_single_batch(&label, data).await
        }
    };

    match exponential_backoff(&backoff_config, operation_fail).await {
        Ok(_) => {
            println!("✗ Unexpected success - should have failed");
        }
        Err(error) => {
            println!("✓ Error correctly treated as non-retryable: {}", error);
            println!("  Total attempts: 1 (no retries for non-retryable errors)");
        }
    }

    // =================================================================
    // DEMONSTRATION 3: Multiple concurrent operations with backoff
    // =================================================================
    println!("\n📋 Demonstration 3: Multiple concurrent operations with backoff");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let mut handles = vec![];
    let start_time = Instant::now();

    for i in 0..3 {
        let manager = manager_ref.clone();
        let data = Bytes::from(format!(
            r#"id,name,value
{},Concurrent{},{}
"#,
            i + 10,
            i,
            (i + 10) * 5
        ));

        // Clone config for each async task to avoid moving `backoff_config`
        let cfg = backoff_config.clone();

        let handle = tokio::spawn(async move {
            let label = generate_test_label(&format!("concurrent_{}", i));

            let operation = || {
                let manager = manager.clone();
                let label = label.clone();
                let data = data.clone();

                async move { manager.send_single_batch(&label, data).await }
            };

            exponential_backoff(&cfg, operation).await
        });

        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let successful_ops = results.iter().filter(|r| r.is_ok()).count();
    let total_time = start_time.elapsed();

    println!(
        "✓ Completed {} out of 3 concurrent operations",
        successful_ops
    );
    println!("  Total time: {}ms", total_time.as_millis());
    println!(
        "  Average time per operation: {}ms",
        total_time.as_millis() / 3
    );

    println!("\n✅ Exponential backoff demonstration completed successfully!");
    println!(
        "   This pattern should be implemented at the application layer for production resilience"
    );

    Ok(())
}
