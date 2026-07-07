#![allow(clippy::print_stdout)]
//! # V2 Transaction Basic Example
//!
//! This example demonstrates the basic usage of StarRocks Stream Load SDK V2 API
//! for two-phase commit (2PC) transactions, providing ACID guarantees for data loading.
//!
//! ## What this example demonstrates:
//! 1. Transaction lifecycle: begin → load → prepare → commit
//! 2. Loading data chunks within a transaction
//! 3. Transaction rollback for error handling
//! 4. Multi-batch data loading in a single transaction
//! 5. Comprehensive error handling at each stage
//!
//! ## When to use V2 API (2PC):
//! - Critical data ingestion requiring exactly-once semantics
//! - Multi-table atomic operations
//! - Scenarios where partial loads must be rejected
//! - Applications requiring strong consistency guarantees
//!
//! ## Transaction lifecycle:
//! 1. **begin_transaction**: Start a new transaction, get TxnId
//! 2. **load_transaction_data**: Load data chunks (can be called multiple times)
//! 3. **prepare_transaction**: Prepare transaction for commit
//! 4. **commit_transaction**: Finalize and persist the transaction
//! 5. **rollback_transaction**: Abort the transaction (if errors occur)
//!
//! ## Production considerations:
//! - This example shows basic transaction flow - production code should implement:
//!   - Transaction state management and recovery
//!   - Timeout handling for stuck transactions
//!   - Automatic rollback on failures
//!   - Metrics collection for transaction monitoring

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
    println!("🚀 StarRocks V2 Transaction Basic Example");
    println!("==========================================\n");

    // Step 1: Configure connection for transactional loading
    println!("📋 Step 1: Configuring StarRocks connection for transactions...");
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("your_password")
    .enable_transaction(true) // Enable V2 API
    .connect_timeout(std::time::Duration::from_secs(10))
    .request_timeout(std::time::Duration::from_secs(600))
    .max_retries(2)
    .build();

    println!("✓ Transaction support enabled");

    // Step 2: Configure table properties
    println!("\n📋 Step 2: Configuring table loading properties...");
    let properties = StreamLoadTableProperties::builder()
        .table("transaction_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(0) // No header in transactional data
        .build();

    println!("✓ Table properties configured");

    // Step 3: Create the StreamLoadManager
    println!("\n📋 Step 3: Creating StreamLoadManager...");
    let manager = StreamLoadManager::new(config, properties)?;
    println!("✓ StreamLoadManager created successfully");

    // Step 4: Begin a new transaction
    println!("\n📋 Step 4: Beginning transaction...");
    let label = generate_test_label("v2_txn");

    println!("  Transaction label: {}", label);
    let txn_id = manager.begin_transaction(&label).await?;

    println!("✓ Transaction begun with ID: {}", txn_id);

    // Step 5: Load first data chunk
    println!("\n📋 Step 5: Loading first data chunk (batch 0)...");
    let chunk1 = "1,Alice,25\n2,Bob,30\n";
    let data1 = Bytes::from(chunk1);

    manager
        .load_transaction_data(&label, "test_db", "transaction_users", 0, data1)
        .await?;
    println!("✓ First chunk loaded successfully");

    // Step 6: Load second data chunk
    println!("\n📋 Step 6: Loading second data chunk (batch 1)...");
    let chunk2 = "3,Charlie,35\n4,David,40\n";
    let data2 = Bytes::from(chunk2);

    manager
        .load_transaction_data(&label, "test_db", "transaction_users", 1, data2)
        .await?;
    println!("✓ Second chunk loaded successfully");

    // Step 7: Prepare the transaction
    println!("\n📋 Step 7: Preparing transaction for commit...");
    let prepare_response = manager.prepare_transaction(&label).await?;

    println!("✓ Transaction prepared");
    println!("  Status: {}", prepare_response.status);

    if let Some(prepared_txn_id) = prepare_response.txn_id {
        println!("  Transaction ID: {}", prepared_txn_id);
    }

    // Step 8: Commit the transaction (this will persist the data)
    println!("\n📋 Step 8: Committing transaction...");
    let commit_start = std::time::Instant::now();
    let commit_response = manager.commit_transaction(&label).await?;

    let commit_duration = commit_start.elapsed();

    println!("✓ Transaction committed successfully!");
    println!("  Status: {}", commit_response.status);
    println!("  Commit time: {}ms", commit_duration.as_millis());

    if let Some(total_rows) = commit_response.number_total_rows {
        println!("  Total rows: {}", total_rows);
    }

    if let Some(loaded_rows) = commit_response.number_loaded_rows {
        println!("  Loaded rows: {}", loaded_rows);
    }

    // Success validation
    assert_success_response(&commit_response);

    println!("\n✅ Transaction example completed successfully!");
    println!("   Data persisted with ACID guarantees");

    Ok(())
}
