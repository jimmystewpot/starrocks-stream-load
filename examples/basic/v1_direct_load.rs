#![allow(warnings)]
#![allow(clippy::all, clippy::print_stdout)]
//! # V1 Direct Load Example
//!
//! This example demonstrates the basic usage of StarRocks Stream Load SDK V1 API
//! for simple one-shot data loading scenarios.
//!
//! ## What this example demonstrates:
//! 1. Basic connection configuration using StreamLoadConfig builder
//! 2. Table-specific loading properties configuration
//! 3. Loading CSV data into StarRocks using send_single_batch()
//! 4. Basic error handling and response validation
//! 5. Response parsing and result interpretation
//!
//! ## When to use V1 API:
//! - Simple one-shot data loading tasks
//! - Non-critical data ingestion
//! - Cases where transactional guarantees aren't required
//! - Applications that can handle retry logic independently
//!
//! ## Production considerations:
//! - This example uses basic error handling - production code should implement:
//!   - Exponential backoff retry strategies
//!   - Circuit breakers for cascading failure prevention
//!   - Comprehensive metrics and monitoring
//!   - Transaction state management if needed

use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::error::Error;

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
    println!("🚀 StarRocks V1 Direct Load Example");
    println!("======================================\n");

    // Step 1: Configure the connection to StarRocks
    println!("📋 Step 1: Configuring StarRocks connection...");
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()], // Frontend URLs
        "test_db".to_string(),                     // Target database
        "admin".to_string(),                       // Username
    )
    .password("your_password") // Optional password
    .connect_timeout(std::time::Duration::from_secs(10))
    .request_timeout(std::time::Duration::from_secs(600))
    .max_retries(2) // Basic retry at network level
    .build();

    println!("✓ Connection configured for database: {}", config.database);

    // Step 2: Configure table-specific loading options
    println!("\n📋 Step 2: Configuring table loading properties...");
    let properties = StreamLoadTableProperties::builder()
        .table("simple_users") // Target table
        .format(DataFormat::CSV) // Data format
        .column_separator(",") // CSV column separator
        .row_delimiter("\n") // Row delimiter
        .columns("id,name,value") // Column mapping
        .skip_header(1) // Skip the header row
        .build();

    println!("✓ Table properties configured for: {properties:#?}");

    // Step 3: Create the StreamLoadManager
    println!("\n📋 Step 3: Creating StreamLoadManager...");
    let manager = StreamLoadManager::new(config, properties)?;
    println!("✓ StreamLoadManager created successfully");

    // Step 4: Prepare test data
    println!("\n📋 Step 4: Preparing sample CSV data...");
    let csv_data = r"id,name,value
1,Alice,25
2,Bob,30
3,Charlie,35
";
    let data_bytes = Bytes::from(csv_data);
    println!("✓ Prepared {} bytes of CSV data", data_bytes.len());

    // Step 5: Execute the stream load
    println!("\n📋 Step 5: Loading data into StarRocks...");
    let label = generate_test_label("v1_load");

    println!("  Using label: {label}");
    let load_start = std::time::Instant::now();

    match manager.send_single_batch(&label, data_bytes).await {
        Ok(response) => {
            let load_duration = load_start.elapsed();

            // Step 6: Parse and display results
            println!("\n📋 Step 6: Processing load results...");
            println!("✓ Load completed successfully!");
            println!("  Status: {}", response.status);
            println!("  Load time: {}ms", load_duration.as_millis());

            if let Some(txn_id) = response.txn_id {
                println!("  Transaction ID: {txn_id}");
            }

            if let Some(total_rows) = response.number_total_rows {
                println!("  Total rows: {total_rows}");
            }

            if let Some(loaded_rows) = response.number_loaded_rows {
                println!("  Loaded rows: {loaded_rows}");
            }

            if let Some(filtered_rows) = response.number_filtered_rows {
                println!("  Filtered rows: {filtered_rows}");
            }

            if let Some(load_bytes) = response.load_bytes {
                println!("  Load size: {load_bytes} bytes");
            }

            if let Some(load_time_ms) = response.load_time_ms {
                println!("  Server processing time: {load_time_ms}ms");
            }

            // Success validation
            assert_success_response(&response);
            println!("\n✅ Example completed successfully!");
        }
        Err(error) => {
            let load_duration = load_start.elapsed();
            println!("\n❌ Load failed after {}ms", load_duration.as_millis());
            println!("  Error: {error}");

            // In production, you would implement retry logic here
            // See production/exponential_backoff.rs for examples
            return Err(error.into());
        }
    }

    Ok(())
}
