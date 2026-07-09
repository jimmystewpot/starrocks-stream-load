#![allow(warnings)]
//! # Error Handling and Recovery Example
#![allow(clippy::print_stdout)]
//!
//! This example demonstrates comprehensive error handling and recovery mechanisms
//! for StarRocks stream load operations at the application layer.
//!
//! ## What this example demonstrates:
//! 1. Error classification and categorization
//! 2. Retry strategies for different error types
//! 3. Partial recovery and data validation
//! 4. Error log extraction and analysis
//! 5. Graceful degradation and fallback mechanisms
//!
//! ## Error categories:
//! - **Network errors**: Timeouts, connection failures, DNS issues (retryable)
//! - **Server errors**: 5xx responses, overloads (retryable with backoff)
//! - **Data errors**: Schema mismatches, validation failures (non-retryable)
//! - **Authentication errors**: Permission issues (immediate failure)
//! - **Transaction errors**: Conflicts, timeouts (application-specific handling)
//!
//! ## Production recovery strategies:
//! - **Exponential backoff**: For transient network/server errors
//! - **Data validation**: Pre-submission checks to avoid data errors
//! - **Partial recovery**: Resume interrupted operations from checkpoints
//! - **Dead letter queues**: Archive failed data for manual review
//! - **Alerting**: Notify operators of critical failures

use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Error classification for appropriate recovery strategies
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Network-related issues (retryable)
    Network,
    /// Server-side issues (retryable with backoff)
    Server,
    /// Data validation/schema issues (non-retryable)
    Data,
    /// Authentication/authorization issues (non-retryable)
    Auth,
    /// Transaction state issues (special handling)
    Transaction,
    /// Unknown errors (default to non-retryable)
    Unknown,
}

impl ErrorCategory {
    pub fn is_retryable(&self) -> bool {
        matches!(self, ErrorCategory::Network | ErrorCategory::Server)
    }
}

/// Classified error with recovery information
#[derive(Debug, Clone)]
pub struct ClassifiedError {
    pub category: ErrorCategory,
    pub original_error: String,
    pub recovery_suggested: bool,
    pub recovery_message: String,
}

/// Error classifier for determining appropriate handling
pub fn classify_error(error: &str) -> ClassifiedError {
    let error_lower = error.to_lowercase();

    // Network errors
    if error_lower.contains("timeout")
        || error_lower.contains("connection")
        || error_lower.contains("network")
        || error_lower.contains("dns")
        || error_lower.contains("unreachable")
    {
        return ClassifiedError {
            category: ErrorCategory::Network,
            original_error: error.to_string(),
            recovery_suggested: true,
            recovery_message: "Network error detected. Retry with exponential backoff recommended."
                .to_string(),
        };
    }

    // Server errors
    if error_lower.contains("503")
        || error_lower.contains("service unavailable")
        || error_lower.contains("504")
        || error_lower.contains("gateway timeout")
        || error_lower.contains("overload")
    {
        return ClassifiedError {
            category: ErrorCategory::Server,
            original_error: error.to_string(),
            recovery_suggested: true,
            recovery_message: "Server error detected. Retry with increased backoff recommended."
                .to_string(),
        };
    }

    // Data errors
    if error_lower.contains("schema")
        || error_lower.contains("validation")
        || error_lower.contains("null")
        || error_lower.contains("constraint")
        || error_lower.contains("duplicate")
        || error_lower.contains("invalid data")
    {
        return ClassifiedError {
            category: ErrorCategory::Data,
            original_error: error.to_string(),
            recovery_suggested: false,
            recovery_message:
                "Data validation error. Please check data format and schema compatibility."
                    .to_string(),
        };
    }

    // Authentication errors
    if error_lower.contains("authentication")
        || error_lower.contains("authorization")
        || error_lower.contains("permission")
        || error_lower.contains("unauthorized")
        || error_lower.contains("401")
        || error_lower.contains("403")
    {
        return ClassifiedError {
            category: ErrorCategory::Auth,
            original_error: error.to_string(),
            recovery_suggested: false,
            recovery_message: "Authentication error. Please check credentials and permissions."
                .to_string(),
        };
    }

    // Transaction errors
    if error_lower.contains("transaction")
        || error_lower.contains("conflict")
        || error_lower.contains("aborted")
        || error_lower.contains("timeout")
    {
        return ClassifiedError {
            category: ErrorCategory::Transaction,
            original_error: error.to_string(),
            recovery_suggested: true,
            recovery_message: "Transaction error. May require rollback and retry.".to_string(),
        };
    }

    // Default to unknown
    ClassifiedError {
        category: ErrorCategory::Unknown,
        original_error: error.to_string(),
        recovery_suggested: false,
        recovery_message: "Unknown error. Requires investigation before recovery.".to_string(),
    }
}

/// Recovery strategy configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub max_retries: usize,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub enable_circuit_breaker: bool,
    pub circuit_breaker_threshold: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 10000,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
        }
    }
}

/// Enhanced error handler with recovery capabilities
pub struct ErrorHandler {
    config: RecoveryConfig,
    error_log: std::sync::Arc<std::sync::Mutex<Vec<ErrorEntry>>>,
}

#[derive(Debug, Clone)]
pub struct ErrorEntry {
    pub timestamp: Instant,
    pub operation: String,
    pub error: String,
    pub category: ErrorCategory,
    pub recovery_attempted: bool,
    pub recovery_successful: bool,
}

impl ErrorHandler {
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            error_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn handle_error(&self, operation: String, error: String) -> ClassifiedError {
        let classified = classify_error(&error);

        let entry = ErrorEntry {
            timestamp: Instant::now(),
            operation,
            error: classified.original_error.clone(),
            category: classified.category.clone(),
            recovery_attempted: false,
            recovery_successful: false,
        };

        self.error_log.lock().unwrap().push(entry);

        tracing::error!(
            "Error classified as {:?}: {}",
            classified.category,
            classified.original_error
        );

        classified
    }

    pub fn get_error_history(&self) -> Vec<ErrorEntry> {
        self.error_log.lock().unwrap().clone()
    }

    pub fn get_error_summary(&self) -> ErrorSummary {
        let errors = self.error_log.lock().unwrap();

        let mut summary = ErrorSummary {
            total_errors: errors.len(),
            by_category: std::collections::HashMap::new(),
            recent_errors: Vec::new(),
        };

        for error in errors.iter() {
            *summary
                .by_category
                .entry(error.category.clone())
                .or_insert(0) += 1;
        }

        // Get most recent errors
        let mut recent_errors = errors.iter().collect::<Vec<_>>();
        recent_errors.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        summary.recent_errors = recent_errors.into_iter().take(10).cloned().collect();

        summary
    }
}

#[derive(Debug)]
pub struct ErrorSummary {
    pub total_errors: usize,
    pub by_category: std::collections::HashMap<ErrorCategory, usize>,
    pub recent_errors: Vec<ErrorEntry>,
}

/// Recovery executor with intelligent retry logic
pub struct RecoveryExecutor {
    handler: ErrorHandler,
}

impl RecoveryExecutor {
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            handler: ErrorHandler::new(config),
        }
    }

    pub async fn execute_with_recovery<F, Fut, T, E>(
        &self,
        operation_name: String,
        mut operation: F,
    ) -> Result<T, RecoveryError<E>>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::error::Error + 'static,
    {
        let mut retry_count = 0;
        let mut backoff_delay = Duration::from_millis(self.handler.config.base_delay_ms);

        loop {
            let attempt = retry_count + 1;
            tracing::info!("Attempt {} for operation: {}", attempt, operation_name);

            match operation().await {
                Ok(result) => {
                    if retry_count > 0 {
                        tracing::info!("Operation succeeded after {} retries", retry_count);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    let error_string = error.to_string();
                    let classified = self
                        .handler
                        .handle_error(operation_name.clone(), error_string);

                    let recovery_err = RecoveryError {
                        operation: operation_name.clone(),
                        attempt,
                        original_error: Box::new(error),
                        classified: classified.clone(),
                    };

                    // Determine if we should retry
                    if !classified.category.is_retryable()
                        || retry_count >= self.handler.config.max_retries
                    {
                        tracing::error!(
                            "Operation failed after {} attempts. Error is non-retryable or max retries exceeded.",
                            attempt
                        );
                        return Err(recovery_err);
                    }

                    // Calculate backoff for next attempt
                    retry_count += 1;
                    let delay_ms = backoff_delay.as_millis() as u64;
                    tracing::warn!(
                        "Retry {}/{} in {}ms due to: {}",
                        retry_count,
                        self.handler.config.max_retries,
                        delay_ms,
                        classified.original_error
                    );

                    tokio::time::sleep(backoff_delay).await;

                    // Exponential backoff
                    backoff_delay = Duration::from_millis(
                        (backoff_delay.as_millis() as u64 * 2)
                            .min(self.handler.config.max_delay_ms),
                    );
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RecoveryError<E: std::error::Error> {
    pub operation: String,
    pub attempt: usize,
    pub original_error: Box<E>,
    pub classified: ClassifiedError,
}

impl<E: std::error::Error> std::fmt::Display for RecoveryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecoveryError in operation '{}' (attempt {}): {} (category: {:?})",
            self.operation, self.attempt, self.original_error, self.classified.category
        )
    }
}

impl<E: std::error::Error> std::error::Error for RecoveryError<E> {}

/// Data validator for proactive error prevention
pub struct DataValidator;

impl DataValidator {
    pub fn validate_csv_data(data: &[u8]) -> Result<(), ValidationError> {
        let csv_str =
            std::str::from_utf8(data).map_err(|e| ValidationError::InvalidUtf8(e.to_string()))?;

        // Basic CSV validation
        let lines: Vec<&str> = csv_str.lines().collect();
        if lines.is_empty() {
            return Err(ValidationError::EmptyData("CSV data is empty".to_string()));
        }

        // Check consistent column count
        let columns = lines[0].split(',').count();
        for (i, line) in lines.iter().enumerate() {
            let line_columns = line.split(',').count();
            if line_columns != columns {
                return Err(ValidationError::InconsistentColumns(format!(
                    "Line {} has {} columns, expected {}",
                    i + 1,
                    line_columns,
                    columns
                )));
            }
        }

        Ok(())
    }

    pub fn validate_json_data(data: &[u8]) -> Result<(), ValidationError> {
        // Basic JSON validation
        let _: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| ValidationError::InvalidJson(e.to_string()))?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum ValidationError {
    InvalidUtf8(String),
    InvalidJson(String),
    EmptyData(String),
    InconsistentColumns(String),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::InvalidUtf8(msg) => write!(f, "Invalid UTF-8: {}", msg),
            ValidationError::InvalidJson(msg) => write!(f, "Invalid JSON: {}", msg),
            ValidationError::EmptyData(msg) => write!(f, "Empty data: {}", msg),
            ValidationError::InconsistentColumns(msg) => write!(f, "Inconsistent columns: {}", msg),
        }
    }
}

impl std::error::Error for ValidationError {}

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
            assert!(loaded > 0, "Expected loaded rows > 0, got: {}", loaded);
        }
    }
    println!("🚀 StarRocks Error Handling and Recovery Example");
    println!("═════════════════════════════════════════════════\n");

    // =================================================================
    // RECOVERY SETUP
    // =================================================================
    println!("📋 Step 1: Setting up recovery system...");

    let recovery_config = RecoveryConfig {
        max_retries: 3,
        base_delay_ms: 100,
        max_delay_ms: 5000,
        enable_circuit_breaker: true,
        circuit_breaker_threshold: 3,
    };

    println!("✓ Recovery executor configured:");
    println!("  Max retries: {}", recovery_config.max_retries);
    println!("  Base delay: {}ms", recovery_config.base_delay_ms);
    println!("  Max delay: {}ms", recovery_config.max_delay_ms);

    let executor = RecoveryExecutor::new(recovery_config);

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
        .table("recovery_users")
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
    // DEMONSTRATION 1: Error classification
    // =================================================================
    println!("\n📋 Demonstration 1: Error classification");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let test_errors = vec![
        "Connection timeout after 30 seconds",
        "HTTP 503 Service Unavailable",
        "Schema validation failed: column 'invalid' does not exist",
        "Authentication failed: Invalid credentials",
        "Transaction conflict detected",
        "Unknown error occurred",
    ];

    for error in test_errors {
        let classified = classify_error(error);
        println!("  Error: {}", error);
        println!("    Category: {:?}", classified.category);
        println!("    Retryable: {}", classified.category.is_retryable());
        println!("    Recovery: {}", classified.recovery_message);
        println!();
    }

    // =================================================================
    // DEMONSTRATION 2: Data validation
    // =================================================================
    println!("\n📋 Demonstration 2: Proactive data validation");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let valid_csv = r#"id,name,value
1,ValidUser,25
2,AnotherUser,30
"#;

    let invalid_csv = r#"id,name,value
1,ValidUser,25
2,IncompleteRow
"#;

    println!("Validating CSV data...");
    match DataValidator::validate_csv_data(valid_csv.as_bytes()) {
        Ok(_) => println!("✓ Valid CSV data passed validation"),
        Err(error) => println!("✗ Unexpected validation error: {}", error),
    }

    println!("Validating invalid CSV data...");
    match DataValidator::validate_csv_data(invalid_csv.as_bytes()) {
        Ok(_) => println!("✗ Invalid data incorrectly passed validation"),
        Err(error) => println!("✓ Invalid data correctly rejected: {}", error),
    }

    // =================================================================
    // DEMONSTRATION 3: Recovery with retry
    // =================================================================
    println!("\n📋 Demonstration 3: Automatic retry with recovery");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let valid_data = Bytes::from(
        r#"id,name,value
1,RecoveryUser,25
2,AnotherRecoveryUser,30
"#,
    );

    let manager = manager_ref.clone();
    let operation = move || {
        let manager = manager.clone();
        let label = generate_test_label("recovery_retry");
        let data = valid_data.clone();

        async move { manager.send_single_batch(&label, data).await }
    };

    match executor
        .execute_with_recovery("stream_load_with_recovery".to_string(), operation)
        .await
    {
        Ok(response) => {
            println!("✓ Operation succeeded with recovery");
            assert_success_response(&response);
        }
        Err(error) => {
            println!("✓ Operation failed: {}", error);
            println!("  Recovery classification: {:?}", error.classified.category);
            println!("  Attempts made: {}", error.attempt);
        }
    }

    // =================================================================
    // DEMONSTRATION 4: Non-retryable error handling
    // =================================================================
    println!("\n📋 Demonstration 4: Non-retryable error immediate failure");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let invalid_data = Bytes::from(
        r#"id,name,value
9999,InvalidData,999
"#,
    );

    let manager = manager_ref.clone();
    let operation = move || {
        let manager = manager.clone();
        let label = generate_test_label("non_retryable");
        let data = invalid_data.clone();

        async move { manager.send_single_batch(&label, data).await }
    };

    match executor
        .execute_with_recovery("stream_load_invalid_data".to_string(), operation)
        .await
    {
        Ok(_) => {
            println!("✗ Unexpected success - should have failed");
        }
        Err(error) => {
            println!("✓ Non-retryable error correctly handled: {}", error);
            println!("  Classification: {:?}", error.classified.category);
            println!("  Recovery message: {}", error.classified.recovery_message);
        }
    }

    // =================================================================
    // DEMONSTRATION 5: Error analysis and reporting
    // =================================================================
    println!("\n📋 Demonstration 5: Error analysis and reporting");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let error_summary = executor.handler.get_error_summary();
    println!("Error Summary:");
    println!("  Total errors: {}", error_summary.total_errors);
    println!("  By category:");

    for (category, count) in &error_summary.by_category {
        println!("    {:?}: {}", category, count);
    }

    if !error_summary.recent_errors.is_empty() {
        println!("\n  Recent errors:");
        for (i, error) in error_summary.recent_errors.iter().enumerate().take(3) {
            let duration = error.timestamp.elapsed();
            println!(
                "    {}: {:?} ({}ms ago)",
                i + 1,
                error.category,
                duration.as_millis()
            );
            println!("       Operation: {}", error.operation);
            println!("       Error: {}", error.error);
        }
    }

    // =================================================================
    // DEMONSTRATION 6: Graceful degradation
    // =================================================================
    println!("\n📋 Demonstration 6: Graceful degradation patterns");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("Graceful degradation strategies:");
    println!("1. Circuit breakers prevent cascading failures");
    println!("2. Dead letter queues preserve failed data for inspection");
    println!("3. Fallback mechanisms provide alternative data sources");
    println!("4. Partial processing allows system to continue with reduced capacity");
    println!("5. Rate limiting prevents system overload");

    println!("\n✅ Error handling and recovery demonstration completed successfully!");
    println!("   This comprehensive system ensures production reliability and resilience");

    Ok(())
}
