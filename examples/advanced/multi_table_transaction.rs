//! # Multi-Table Transaction Example
#![allow(clippy::print_stdout)]
//! 
//! This example demonstrates atomic operations across multiple `StarRocks` tables
//! using 2PC transactions, ensuring data consistency across table boundaries.
//!
//! ## What this example demonstrates:
//! 1. Coordinated transaction spanning multiple tables
//! 2. Atomic all-or-nothing semantics across tables
//! 3. Data integrity with referential consistency
//! 4. Complex transaction state management
//! 5. Error handling and rollback across multiple tables
//!
//! ## Use cases for multi-table transactions:
//! - **Data migration**: Move related data between tables atomically
//! - **Cross-table updates**: Maintain referential integrity
//! - **Complex data pipelines**: Process related entities atomically
//! - **Transactional consistency**: Ensure related tables stay synchronized
//!
//! ## Production considerations:
//! - **Performance overhead**: Multi-table transactions have increased latency
//! - **Conflict detection**: Handle concurrent modifications across tables
//! - **Rollback complexity**: Ensure proper cleanup across all tables
//! - **Monitoring**: Track cross-table transaction success rates

use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadTableProperties, StreamLoadManager,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

/// Represents a single table's data within a multi-table transaction
#[derive(Debug, Clone)]
pub struct TableData {
    pub table_name: String,
    pub database: String,
    pub data: Vec<Bytes>,
    pub properties: StreamLoadTableProperties,
}

/// Multi-table transaction coordinator
pub struct MultiTableTransaction {
    manager: Arc<StreamLoadManager>,
    tables: HashMap<String, TableData>,
    label: String,
    txn_id: Option<i64>,
    committed: bool,
    rolled_back: bool,
}

impl MultiTableTransaction {
    /// Create a new multi-table transaction
    pub fn new(label: String, manager: Arc<StreamLoadManager>) -> Self {
        Self {
            manager,
            tables: HashMap::new(),
            label,
            txn_id: None,
            committed: false,
            rolled_back: false,
        }
    }

    /// Add a table to the transaction with its configuration
    pub fn add_table(&mut self, table_data: TableData) {
        let table_key = format!("{}.{}", table_data.database, table_data.table_name);
        self.tables.insert(table_key, table_data);
    }

    /// Begin the multi-table transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction has already been begun or if any of the
    /// data loading operations for the associated tables fail.
    ///
    /// # Panics
    ///
    /// Panics if the transaction ID cannot be retrieved after beginning the transaction.
    pub async fn begin(&mut self) -> Result<(), Box<dyn Error>> {
        if self.txn_id.is_some() {
            return Err("Transaction already begun".into());
        }

        println!("🔄 Beginning multi-table transaction: {}", self.label);
        self.txn_id = Some(self.manager.begin_transaction(&self.label).await?);
        println!("✓ Transaction begun with ID: {}", self.txn_id.unwrap());
        
        // Load data for all tables
        let mut sequence = 0;
        for table_data in self.tables.values() {
            println!("  Loading data into {}.{}...", table_data.database, table_data.table_name);
            
            for data_chunk in &table_data.data {
                self.manager.load_transaction_data(
                    &self.label,
                    &table_data.database,
                    &table_data.table_name,
                    sequence,
                    data_chunk.clone(),
                ).await?;
                sequence += 1;
            }
            
            println!("  ✓ {}.{}: {} data chunks loaded", 
                    table_data.database, table_data.table_name, table_data.data.len());
        }

        Ok(())
    }

    /// Prepare all tables for commit
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction has not been begun or if the
    /// `StarRocks` prepare operation fails.
    pub async fn prepare(&self) -> Result<(), Box<dyn Error>> {
        if self.txn_id.is_none() {
            return Err("Transaction not begun".into());
        }

        println!("🔄 Preparing multi-table transaction...");
        
        let prepare_response = self.manager.prepare_transaction(&self.label).await?;
        println!("✓ Transaction prepared for {} tables", self.tables.len());
        println!("  Response status: {}", prepare_response.status);
        
        Ok(())
    }

    /// Commit the multi-table transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction has already been committed or if the
    /// `StarRocks` commit operation fails.
    pub async fn commit(&mut self) -> Result<(), Box<dyn Error>> {
        if self.committed {
            return Err("Transaction already committed".into());
        }

        println!("🔄 Committing multi-table transaction...");
        
        let commit_start = std::time::Instant::now();
        let commit_response = self.manager.commit_transaction(&self.label).await?;
        let commit_duration = commit_start.elapsed();
        
        self.committed = true;
        
        println!("✓ Multi-table transaction committed successfully!");
        println!("  Commit time: {}ms", commit_duration.as_millis());
        println!("  Tables affected: {}", self.tables.len());
        
        if let Some(loaded_rows) = commit_response.number_loaded_rows {
            println!("  Total rows loaded: {loaded_rows}");
        }

        Ok(())
    }

    /// Rollback the multi-table transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction has already been rolled back or if the
    /// `StarRocks` rollback operation fails.
    pub async fn rollback(&mut self) -> Result<(), Box<dyn Error>> {
        if self.rolled_back {
            return Err("Transaction already rolled back".into());
        }

        println!("🔄 Rolling back multi-table transaction...");
        
        let rollback_start = std::time::Instant::now();
        let rollback_response = self.manager.rollback_transaction(&self.label).await?;
        let rollback_duration = rollback_start.elapsed();
        
        self.rolled_back = true;
        
        println!("✓ Multi-table transaction rolled back successfully!");
        println!("  Rollback time: {}ms", rollback_duration.as_millis());
        println!("  Tables affected: {}", self.tables.len());
        println!("  Response status: {}", rollback_response.status);

        Ok(())
    }

    /// Get transaction status
    #[must_use]
    pub fn status(&self) -> MultiTableTransactionStatus {
        MultiTableTransactionStatus {
            label: self.label.clone(),
            txn_id: self.txn_id,
            tables_count: self.tables.len(),
            committed: self.committed,
            rolled_back: self.rolled_back,
            tables: self.tables.keys().cloned().collect(),
        }
    }
}

#[derive(Debug)]
pub struct MultiTableTransactionStatus {
    pub label: String,
    pub txn_id: Option<i64>,
    pub tables_count: usize,
    pub committed: bool,
    pub rolled_back: bool,
    pub tables: Vec<String>,
}

/// Simple helper to generate test labels
fn generate_test_label(prefix: &str) -> String {
    format!("{}_{}", prefix, chrono::Utc::now().timestamp())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("🚀 StarRocks Multi-Table Transaction Example");
    println!("══════════════════════════════════════════════\n");

    // =================================================================
    // CONFIGURATION
    // =================================================================
    println!("📋 Step 1: Configuring StarRocks manager...");
    
    let config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("your_password")
    .enable_transaction(true)
    .max_retries(2)
    .build();

    let base_properties = StreamLoadTableProperties::builder()
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .skip_header(0)
        .build();

    let manager = StreamLoadManager::new(config, base_properties.clone())?;
    let manager_ref = Arc::new(manager);
    println!("✓ StreamLoadManager created with transaction support");

    // =================================================================
    // DEMONSTRATION 1: Successful multi-table transaction
    // =================================================================
    println!("\n📋 Demonstration 1: Successful multi-table transaction");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let label = generate_test_label("multi_table_success");
    println!("Creating multi-table transaction: {label}");
    
    let mut txn = MultiTableTransaction::new(label.clone(), manager_ref.clone());
    
    // Add users table
    let users_properties = StreamLoadTableProperties::builder()
        .table("users")
        .format(DataFormat::CSV)
        .columns("id,username,email")
        .skip_header(0)
        .build();
    
    let users_data = vec![
        Bytes::from("1,user1,test1@example.com\n"),
        Bytes::from("2,user2,test2@example.com\n"),
    ];
    
    txn.add_table(TableData {
        table_name: "users".to_string(),
        database: "test_db".to_string(),
        data: users_data,
        properties: users_properties,
    });
    
    // Add orders table
    let orders_properties = StreamLoadTableProperties::builder()
        .table("orders")
        .format(DataFormat::CSV)
        .columns("id,user_id,product_id,amount")
        .skip_header(0)
        .build();
    
    let orders_data = vec![
        Bytes::from("101,1,prod1,100.50\n"),
        Bytes::from("102,2,prod2,75.25\n"),
    ];
    
    txn.add_table(TableData {
        table_name: "orders".to_string(),
        database: "test_db".to_string(),
        data: orders_data,
        properties: orders_properties,
    });
    
    // Add order_items table
    let order_items_properties = StreamLoadTableProperties::builder()
        .table("order_items")
        .format(DataFormat::CSV)
        .columns("id,order_id,item_id,quantity")
        .skip_header(0)
        .build();
    
    let order_items_data = vec![
        Bytes::from("1001,101,item1,2\n"),
        Bytes::from("1002,102,item2,1\n"),
    ];
    
    txn.add_table(TableData {
        table_name: "order_items".to_string(),
        database: "test_db".to_string(),
        data: order_items_data,
        properties: order_items_properties,
    });
    
    println!("✓ Added 3 tables to transaction: users, orders, order_items");
    
    // Execute transaction
    txn.begin().await?;
    txn.prepare().await?;
    txn.commit().await?;
    
    let status = txn.status();
    println!("✓ Transaction final status: {} with {} tables", 
            if status.committed { "COMMITTED" } else { "NOT COMMITTED" }, 
            status.tables_count);

    // =================================================================
    // DEMONSTRATION 2: Multi-table transaction with rollback
    // =================================================================
    println!("\n📋 Demonstration 2: Multi-table transaction with error handling");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let label = generate_test_label("multi_table_rollback");
    println!("Creating multi-table transaction with potential issues: {label}");
    
    let mut txn = MultiTableTransaction::new(label.clone(), manager_ref.clone());
    
    // Add table with valid data
    let valid_properties = StreamLoadTableProperties::builder()
        .table("valid_table")
        .format(DataFormat::CSV)
        .columns("id,value")
        .skip_header(0)
        .build();
    
    let valid_data = vec![Bytes::from("1,100\n")];
    
    txn.add_table(TableData {
        table_name: "valid_table".to_string(),
        database: "test_db".to_string(),
        data: valid_data,
        properties: valid_properties,
    });
    
    // Add table with problematic data (will force rollback)
    let invalid_properties = StreamLoadTableProperties::builder()
        .table("invalid_table")
        .format(DataFormat::CSV)
        .columns("id,value")
        .skip_header(0)
        .build();
    
    let invalid_data = vec![Bytes::from("9999,invalid_data\n")]; // Likely to fail
    
    txn.add_table(TableData {
        table_name: "invalid_table".to_string(),
        database: "test_db".to_string(),
        data: invalid_data,
        properties: invalid_properties,
    });
    
    println!("✓ Added 2 tables to transaction: valid_table, invalid_table");
    println!("ℹ  Note: invalid_table contains data that may trigger rollback");
    
    // Attempt transaction
    match txn.begin().await {
        Ok(_) => {
            println!("✓ Transaction begun with data loaded to both tables");
            
            // Try to prepare - may fail due to invalid data
            match txn.prepare().await {
                Ok(()) => {
                    println!("✓ Transaction prepared successfully");
                    // If we get here, invalid data was somehow acceptable
                    match txn.commit().await {
                Ok(()) => {
                            println!("✗ Unexpected success - invalid data should have failed");
                        }
                        Err(error) => {
                            println!("✓ Commit failed as expected: {error}");
                            println!("  Attempting rollback...");
                            
                            if let Err(rollback_error) = txn.rollback().await {
                                println!("⚠  Rollback failed (may not be critical): {rollback_error}");
                            }
                        }
                    }
                }
                Err(error) => {
                    println!("✓ Preparation failed as expected: {error}");
                    println!("  Attempting rollback...");
                    
                    if let Err(rollback_error) = txn.rollback().await {
                        println!("⚠  Rollback may have issues: {rollback_error}");
                    }
                }
            }
        }
        Err(error) => {
            println!("✓ Transaction failed during begin phase: {error}");
        }
    }
    
    let status = txn.status();
    println!("✓ Transaction final status: {}", 
            if status.rolled_back { "ROLLED BACK" } else { "FAILED" });

    // =================================================================
    // DEMONSTRATION 3: Large multi-table transaction
    // =================================================================
    println!("\n📋 Demonstration 3: Large multi-table transaction");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    let label = generate_test_label("multi_table_large");
    println!("Creating large multi-table transaction: {label}");
    
    let mut txn = MultiTableTransaction::new(label.clone(), manager_ref.clone());
    
    // Create multiple tables with data
    for i in 0..5 {
        let table_properties = StreamLoadTableProperties::builder()
            .table(format!("table_{i}"))
            .format(DataFormat::CSV)
            .columns("id,value")
            .skip_header(0)
            .build();
        
        let table_data = vec![
            Bytes::from(format!("{i}0,valueA{i}\n")),
            Bytes::from(format!("{i}1,valueB{i}\n")),
        ];
        
        txn.add_table(TableData {
             table_name: format!("table_{i}"),
            database: "test_db".to_string(),
            data: table_data,
            properties: table_properties,
        });
    }
    
    println!("✓ Added 5 tables to transaction with 2 chunks each");
    
    // Execute transaction
    let start = std::time::Instant::now();
    txn.begin().await?;
    txn.prepare().await?;
    txn.commit().await?;
    let total_duration = start.elapsed();
    
    println!("✓ Large multi-table transaction completed in {}ms", total_duration.as_millis());
    
    let status = txn.status();
    println!("✓ Transaction final status: COMMITTED with {} tables", status.tables_count);

    // =================================================================
    // DEMONSTRATION 4: Transaction consistency analysis
    // =================================================================
    println!("\n📋 Demonstration 4: Transaction consistency analysis");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    println!("Multi-table transaction consistency guarantees:");
    println!("✓ All tables in a transaction succeed or fail together");
    println!("✓ No partial commits across tables");
    println!("✓ Atomic all-or-nothing semantics");
    println!("✓ Complete rollback on any failure");
    println!("✓ Transaction isolation from other operations");
    
    println!("\nPerformance considerations:");
    println!("⚠  Increased latency with more tables");
    println!("⚠  Higher memory usage during transaction");
    println!("⚠  Extended lock durations on affected tables");
    println!("⚠  Potential for deadlocks with concurrent transactions");

    println!("\n✅ Multi-table transaction demonstration completed successfully!");
    println!("   This pattern ensures strong consistency across complex data operations");

    Ok(())
}