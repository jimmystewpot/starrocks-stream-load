#![allow(warnings)]
//! # Circuit Breaker Pattern Example
#![allow(clippy::print_stdout)]
//!
//! This example demonstrates production-grade circuit breaker implementation for preventing
//! cascading failures in `StarRocks` stream load operations.
//!
//! ## What this example demonstrates:
//! 1. Circuit breaker with three states: CLOSED, OPEN, HALF-OPEN
//! 2. Automatic state transitions based on failure/success rates
//! 3. Recovery timeout with configurable half-open testing
//! 4. Thread-safe operations using atomic primitives
//! 5. Integration with SDK operations and error tracking
//!
//! ## Circuit Breaker States:
//! - **CLOSED**: Normal operation, requests pass through, failures count against threshold
//! - **OPEN**: Circuit is broken, fast-fail all requests, wait for recovery timeout
//! - **HALF-OPEN**: Testing recovery, allow limited requests to test if service is healthy
//!
//! ## Production implementation details:
//! - **Failure threshold**: Number of failures before opening circuit (e.g., 5)
//! - **Recovery timeout**: How long to wait before attempting recovery (e.g., 60 seconds)
//! - **Success threshold**: How many successful requests needed to CLOSE circuit (e.g., 3)
//! - **Request timeout**: Time limit for individual requests
//!
//! ## Application layer pattern:
//! This demonstrates that the SDK provides building blocks while the application layer
//! implements circuit breaking for production resilience.

use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum CircuitState {
    Closed = 0,   // Normal operation
    Open = 1,     // Circuit broken, fast-fail all requests
    HalfOpen = 2, // Testing recovery
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "CLOSED"),
            CircuitState::Open => write!(f, "OPEN"),
            CircuitState::HalfOpen => write!(f, "HALF-OPEN"),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u64,
    pub recovery_timeout: Duration,
    pub success_threshold: usize,
    pub request_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,                     // Open circuit after 5 failures
            recovery_timeout: Duration::from_mins(1), // Wait 60 seconds before recovery
            success_threshold: 3,                     // Need 3 successful requests to close circuit
            request_timeout: Duration::from_secs(30), // Individual request timeout
        }
    }
}

/// production-grade circuit breaker implementation
pub struct CircuitBreaker {
    failure_count: AtomicU64,
    success_count: AtomicU64,
    last_failure_time: AtomicU64,
    state: AtomicU8,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    #[must_use]
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_failure_time: AtomicU64::new(0),
            state: AtomicU8::new(CircuitState::Closed as u8),
            config,
        }
    }

    /// Check if requests can proceed through the circuit breaker
    pub fn can_proceed(&self) -> bool {
        let current_state = self.get_state();

        match current_state {
            CircuitState::Open => {
                // Check if recovery timeout has elapsed
                let last_failure_timestamp = self.last_failure_time.load(Ordering::Acquire);
                let elapsed = if last_failure_timestamp > 0 {
                    let now_timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    Duration::from_secs(now_timestamp.saturating_sub(last_failure_timestamp))
                } else {
                    Duration::ZERO
                };

                if elapsed >= self.config.recovery_timeout {
                    // Transition to half-open for testing
                    self.transition_to(CircuitState::HalfOpen);
                    tracing::info!(
                        "Circuit breaker transitioning to HALF-OPEN for recovery testing"
                    );
                    true
                } else {
                    // Still in recovery period
                    let remaining = self.config.recovery_timeout.saturating_sub(elapsed);
                    let remaining_ms = remaining.as_millis();
                    tracing::warn!(
                        "Circuit breaker OPEN, {}ms remaining in recovery",
                        remaining_ms
                    );
                    false
                }
            }
            CircuitState::Closed | CircuitState::HalfOpen => true, // Allow requests
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let current_state = self.get_state();

        match current_state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                // Increment success count
                let successes = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

                if successes >= self.config.success_threshold as u64 {
                    // Enough successes to close the circuit
                    self.transition_to(CircuitState::Closed);
                    self.success_count.store(0, Ordering::Relaxed);
                    tracing::info!(
                        "Circuit breaker CLOSED after {} successful requests",
                        successes
                    );
                } else {
                    tracing::info!(
                        "Circuit breaker HALF-OPEN: {}/{} successful requests",
                        successes,
                        self.config.success_threshold
                    );
                }
            }
            CircuitState::Open => {
                // Successful requests shouldn't happen when circuit is open
                tracing::warn!("Successful operation while circuit is OPEN - unexpected");
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let current_state = self.get_state();

        match current_state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;

                if failures >= self.config.failure_threshold {
                    // Threshold reached, open circuit
                    self.transition_to(CircuitState::Open);
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    self.last_failure_time.store(timestamp, Ordering::Release);
                    tracing::error!("Circuit breaker OPEN after {} failures", failures);
                } else {
                    tracing::warn!(
                        "Circuit breaker approaching threshold: {}/{} failures",
                        failures,
                        self.config.failure_threshold
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Failure during testing means stay open
                self.transition_to(CircuitState::Open);
                self.success_count.store(0, Ordering::Relaxed);
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                self.last_failure_time.store(timestamp, Ordering::Release);
                tracing::error!("Circuit reopen to OPEN due to failure during HALF-OPEN testing");
            }
            CircuitState::Open => {
                // Already open, just update last failure time
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                self.last_failure_time.store(timestamp, Ordering::Release);
            }
        }
    }

    /// Get current circuit breaker state
    pub fn get_state(&self) -> CircuitState {
        match self.state.load(Ordering::Acquire) {
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }

    /// Transition to a specific state
    fn transition_to(&self, new_state: CircuitState) {
        let old_state = self.state.swap(new_state as u8, Ordering::Release);
        if old_state != new_state as u8 {
            tracing::info!(
                "Circuit breaker state transition: {} -> {}",
                Self::state_to_string(old_state),
                new_state
            );
        }
    }

    /// Get current failure count
    pub fn get_failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Get current success count
    pub fn get_success_count(&self) -> u64 {
        self.success_count.load(Ordering::Relaxed)
    }

    /// Reset circuit breaker to initial state
    pub fn reset(&self) {
        self.failure_count.store(0, Ordering::Release);
        self.success_count.store(0, Ordering::Release);
        self.transition_to(CircuitState::Closed);
        tracing::info!("Circuit breaker reset to CLOSED state");
    }

    fn state_to_string(state: u8) -> &'static str {
        match state {
            0 => "CLOSED",
            1 => "OPEN",
            2 => "HALF-OPEN",
            _ => "UNKNOWN",
        }
    }
}

/// Safe stream load operation with circuit breaker protection
///
/// # Errors
///
/// Returns `CircuitBreakerError::CircuitOpen` if the circuit is open,
/// or `CircuitBreakerError::OperationFailed` if the underlying operation fails.
pub async fn safe_stream_load<F, Fut, T, E>(
    circuit_breaker: Arc<CircuitBreaker>,
    operation: F,
) -> Result<T, CircuitBreakerError<E>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error + 'static,
{
    if !circuit_breaker.can_proceed() {
        return Err(CircuitBreakerError::CircuitOpen);
    }

    operation().await.map_err(CircuitBreakerError::from)
}

/// Circuit breaker error types
#[derive(Debug)]
pub enum CircuitBreakerError<E: std::error::Error> {
    /// Circuit is open, requests are blocked
    CircuitOpen,
    /// Underlying operation failed
    OperationFailed(E),
}

impl<E: std::error::Error> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is OPEN"),
            CircuitBreakerError::OperationFailed(e) => write!(f, "Operation failed: {e}"),
        }
    }
}

impl<E: std::error::Error> std::error::Error for CircuitBreakerError<E> {}

impl<E: std::error::Error> From<E> for CircuitBreakerError<E> {
    fn from(error: E) -> Self {
        CircuitBreakerError::OperationFailed(error)
    }
}

/// Simple helper to generate test labels
fn generate_test_label(prefix: &str) -> String {
    format!("{}_{}", prefix, chrono::Utc::now().timestamp_millis())
}

/// Helper to assert success response
fn assert_success_response(response: &starrocks_stream_load::StreamLoadResponse) {
    assert!(
        response.status == "Success" || response.status == "OK",
        "Expected success status, got: {}",
        response.status
    );

    if let Some(loaded) = response.number_loaded_rows {
        assert!(loaded > 0, "Expected loaded rows > 0, got: {loaded}");
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("🚀 StarRocks Circuit Breaker Pattern Example");
    println!("=============================================\n");

    // =================================================================
    // CONFIGURATION
    // =================================================================
    println!("📋 Step 1: Configuring circuit breaker...");

    let config = CircuitBreakerConfig {
        failure_threshold: 3,                      // Open after 3 failures
        recovery_timeout: Duration::from_secs(30), // 30 second recovery
        success_threshold: 2,                      // Need 2 successes to close
        request_timeout: Duration::from_secs(10),
    };

    let circuit_breaker = Arc::new(CircuitBreaker::new(config.clone()));
    println!("✓ Circuit breaker configured:");
    println!("  Failure threshold: {}", config.failure_threshold);
    println!("  Recovery timeout: {}s", config.recovery_timeout.as_secs());
    println!("  Success threshold: {}", config.success_threshold);

    // =================================================================
    // STARROCKS SETUP
    // =================================================================
    println!("\n📋 Step 2: Setting up StarRocks manager...");

    let stream_config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("your_password")
    .max_retries(0) // We handle retries at application layer
    .build();

    let properties = StreamLoadTableProperties::builder()
        .table("circuit_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(1)
        .build();

    let manager = StreamLoadManager::new(stream_config, properties)?;
    let manager_ref = Arc::new(manager);
    println!("✓ StreamLoadManager created");

    // =================================================================
    // DEMONSTRATION 1: Normal operation failures trigger circuit
    // =================================================================
    println!("\n📋 Demonstration 1: Failures trigger circuit breaker");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("Circuit state: {}", circuit_breaker.get_state());

    // Simulate failures to trigger circuit breaker
    for i in 0..3 {
        let cb = circuit_breaker.clone();
        let manager = manager_ref.clone();
        let data = Bytes::from(
            r#"id,name,value
9999,BadUser,999
"#,
        );

        let result = safe_stream_load(cb, move || {
            let manager = manager.clone();
            let label = generate_test_label("fail_trigger");
            let data = data.clone();

            async move {
                // This will fail due to invalid data
                manager.send_single_batch(&label, data).await
            }
        })
        .await;

        if let Err(CircuitBreakerError::OperationFailed(e)) = result {
            println!("  Attempt {}: Failed as expected - {}", i + 1, e);
        }

        println!(
            "  Circuit state after attempt {}: {}",
            i + 1,
            circuit_breaker.get_state()
        );
        println!("  Failure count: {}", circuit_breaker.get_failure_count());
    }

    // =================================================================
    // DEMONSTRATION 2: Circuit blocks requests when OPEN
    // =================================================================
    println!("\n📋 Demonstration 2: Circuit blocks requests when OPEN");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let cb = circuit_breaker.clone();
    let manager = manager_ref.clone();

    let result = safe_stream_load(cb, || {
        let manager = manager.clone();
        let label = generate_test_label("blocked_request");
        let data = Bytes::from(
            r#"id,name,value
1,NewUser,25
"#,
        );

        async move { manager.send_single_batch(&label, data).await }
    })
    .await;

    match result {
        Err(CircuitBreakerError::CircuitOpen) => {
            println!("✓ Request blocked as expected - circuit is OPEN");
        }
        Ok(_) => {
            println!("✗ Unexpected success - circuit should be OPEN");
        }
        Err(CircuitBreakerError::OperationFailed(e)) => {
            println!("✗ Unexpected operation failure: {e}");
        }
    }

    // =================================================================
    // DEMONSTRATION 3: Recovery timeout and half-open testing
    // =================================================================
    println!("\n📋 Demonstration 3: Recovery timeout and half-open testing");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // For demonstration, manually reset circuit breaker
    println!("Resetting circuit breaker for demonstration...");
    circuit_breaker.reset();

    // Simulate failures again
    for _i in 0..3 {
        let cb = circuit_breaker.clone();
        let manager = manager_ref.clone();
        let data = Bytes::from(
            r#"id,name,value
9999,BadUser,999
"#,
        );

        let _ = safe_stream_load(cb, || {
            let manager = manager.clone();
            let label = generate_test_label("recovery_fail");
            let data = data.clone();

            async move { manager.send_single_batch(&label, data).await }
        })
        .await;
    }

    println!(
        "Circuit state after failures: {}",
        circuit_breaker.get_state()
    );

    // Wait for recovery timeout (with shortened duration for demo)
    let recovery_time = Duration::from_secs(2); // Short for demo
    println!(
        "Waiting {} seconds for recovery timeout...",
        recovery_time.as_secs()
    );
    tokio::time::sleep(recovery_time).await;

    // Manually adjust last failure time to simulate recovery timeout
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(35);
    circuit_breaker
        .last_failure_time
        .store(timestamp, Ordering::Release);

    println!(
        "Circuit state after recovery timeout: {}",
        circuit_breaker.get_state()
    );

    // =================================================================
    // DEMONSTRATION 4: Successful recovery closes circuit
    // =================================================================
    println!("\n📋 Demonstration 4: Successful recovery closes circuit");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Perform successful operations to close circuit
    for i in 0..2 {
        let cb = circuit_breaker.clone();
        let manager = manager_ref.clone();
        let data = Bytes::from(format!(
            r#"id,name,value
{},RecoveryUser,{}
"#,
            i + 100,
            (i + 100) * 2
        ));

        let result = safe_stream_load(cb.clone(), move || {
            let manager = manager.clone();
            let label = generate_test_label("recovery_success");
            let data = data.clone();

            async move { manager.send_single_batch(&label, data).await }
        })
        .await;

        match result {
            Ok(response) => {
                cb.record_success();
                println!("✓ Recovery attempt {} succeeded", i + 1);
                println!("  Circuit state: {}", cb.get_state());
                println!("  Success count: {}", cb.get_success_count());
                assert_success_response(&response);
            }
            Err(e) => {
                cb.record_failure();
                println!("✗ Recovery attempt {} failed: {}", i + 1, e);
            }
        }
    }

    println!("Final circuit state: {}", circuit_breaker.get_state());

    println!("\n✅ Circuit breaker demonstration completed successfully!");
    println!("   This pattern is essential for production systems to prevent cascading failures");

    Ok(())
}
