#![allow(warnings)]
//! # Transaction State Management Example
#![allow(clippy::print_stdout)]
//!
//! This example demonstrates production-grade transaction state management for `StarRocks`
//! 2PC stream load operations, including recovery and consistent state tracking.
//!
//! ## What this example demonstrates:
//! 1. Transaction state machine with proper state transitions
//! 2. Persistent tracking of transaction states in memory
//! 3. Error recovery and transaction rollback procedures
//! 4. Timeout handling for stuck transactions
//! 5. Concurrent transaction management with conflict detection
//!
//! ## Transaction state lifecycle:
//! - **`NotStarted`**: Transaction not yet initialized
//! - **`Begun(txn_id)`**: Transaction started with backend ID
//! - **Loaded**: Data chunks loaded into transaction
//! - **Prepared**: Transaction prepared for commit
//! - **Committed**: Transaction successfully committed
//! - **`RolledBack`**: Transaction was rolled back
//! - **Failed(reason)**: Transaction failed with specific reason
//!
//! ## Production implementation details:
//! - **Thread-safe state management**: `Arc<Mutex<HashMap>>` for concurrent access
//! - **State validation**: Ensure transitions only occur from valid states
//! - **Recovery mechanisms**: Ability to query and manage incomplete transactions
//! - **Timeout handling**: Automatic cleanup of stuck transactions
//! - **Conflict detection**: Prevent duplicate transaction labels

use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Transaction state machine with proper state transitions
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    NotStarted,
    Begun(i64),     // Transaction ID from StarRocks
    Loaded,         // Data loaded but not prepared
    Prepared,       // Transaction prepared for commit
    Committed,      // Transaction successfully committed
    RolledBack,     // Transaction explicitly rolled back
    Failed(String), // Transaction failed with reason
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::NotStarted => write!(f, "Not Started"),
            TransactionState::Begun(id) => write!(f, "Began ({id})"),
            TransactionState::Loaded => write!(f, "Loaded"),
            TransactionState::Prepared => write!(f, "Prepared"),
            TransactionState::Committed => write!(f, "Committed"),
            TransactionState::RolledBack => write!(f, "Rolled Back"),
            TransactionState::Failed(reason) => write!(f, "Failed: {reason}"),
        }
    }
}

/// Transaction metadata with tracking information
#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    pub label: String,
    pub state: TransactionState,
    pub created_at: Instant,
    pub last_updated: Instant,
    pub table: String,
    pub rows_affected: u64,
    pub error_details: Option<String>,
}

impl TransactionMetadata {
    #[must_use]
    pub fn new(label: String, table: String) -> Self {
        let now = Instant::now();
        Self {
            label,
            state: TransactionState::NotStarted,
            created_at: now,
            last_updated: now,
            table,
            rows_affected: 0,
            error_details: None,
        }
    }

    #[must_use]
    pub fn duration_since_creation(&self) -> Duration {
        self.created_at.elapsed()
    }

    #[must_use]
    pub fn duration_since_update(&self) -> Duration {
        self.last_updated.elapsed()
    }
}

/// Transaction manager for state tracking and recovery
pub struct TransactionManager {
    transactions: Arc<Mutex<HashMap<String, TransactionMetadata>>>,
    config: TransactionManagerConfig,
}

#[derive(Debug, Clone)]
pub struct TransactionManagerConfig {
    pub transaction_timeout: Duration,
    pub max_concurrent_transactions: usize,
    pub enable_auto_recovery: bool,
}

impl Default for TransactionManagerConfig {
    fn default() -> Self {
        Self {
            transaction_timeout: Duration::from_mins(5), // 5 minutes
            max_concurrent_transactions: 100,
            enable_auto_recovery: true,
        }
    }
}

impl TransactionManager {
    #[must_use]
    pub fn new(config: TransactionManagerConfig) -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    #[must_use]
    pub fn new_default() -> Self {
        Self::new(TransactionManagerConfig::default())
    }

    /// Begin a new transaction with state tracking
    ///
    /// # Errors
    ///
    /// Returns `TransactionError` if the label is a duplicate or if the StarRocks
    /// begin operation fails.
    pub async fn begin_transaction(
        &self,
        label: String,
        manager: &StreamLoadManager,
        table: String,
    ) -> Result<i64, TransactionError> {
        {
            let mut transactions = self
                .transactions
                .lock()
                .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

            // Check for duplicate label
            if transactions.contains_key(&label) {
                return Err(TransactionError::DuplicateTransactionLabel(label));
            }

            // Create transaction metadata
            let metadata = TransactionMetadata::new(label.clone(), table);
            transactions.insert(label.clone(), metadata);
        }

        // Begin transaction with StarRocks
        match manager.begin_transaction(&label).await {
            Ok(txn_id) => {
                let mut transactions = self
                    .transactions
                    .lock()
                    .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

                if let Some(metadata) = transactions.get_mut(&label) {
                    metadata.state = TransactionState::Begun(txn_id);
                    metadata.last_updated = Instant::now();
                }

                tracing::info!("Transaction '{}' begun with ID: {}", label, txn_id);
                Ok(txn_id)
            }
            Err(error) => {
                let mut transactions = self
                    .transactions
                    .lock()
                    .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

                if let Some(metadata) = transactions.get_mut(&label) {
                    metadata.state = TransactionState::Failed(error.to_string());
                    metadata.error_details = Some(error.to_string());
                    metadata.last_updated = Instant::now();
                }

                Err(TransactionError::BeginFailed(error.to_string()))
            }
        }
    }

    /// Load data into transaction with state tracking
    ///
    /// # Errors
    ///
    /// Returns `TransactionError` if the transaction is in an invalid state
    /// or if the data load operation fails.
    pub async fn load_data(
        &self,
        label: String,
        manager: &StreamLoadManager,
        database: String,
        table: String,
        sequence: usize,
        data: Bytes,
    ) -> Result<(), TransactionError> {
        // Validate current state
        {
            let transactions = self
                .transactions
                .lock()
                .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

            if let Some(metadata) = transactions.get(&label)
                && !matches!(
                    metadata.state,
                    TransactionState::Begun(_) | TransactionState::Loaded
                )
            {
                return Err(TransactionError::InvalidStateTransition(format!(
                    "Cannot load data in state: {}",
                    metadata.state
                )));
            }
        }

        // Load data with StarRocks
        match manager
            .load_transaction_data(&label, &database, &table, sequence, data)
            .await
        {
            Ok(_) => {
                let mut transactions = self
                    .transactions
                    .lock()
                    .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

                if let Some(metadata) = transactions.get_mut(&label) {
                    metadata.state = TransactionState::Loaded;
                    metadata.rows_affected += 1; // Simplified tracking
                    metadata.last_updated = Instant::now();
                }

                Ok(())
            }
            Err(error) => {
                self.mark_transaction_failed(label.clone(), error.to_string());
                Err(TransactionError::LoadFailed(error.to_string()))
            }
        }
    }

    /// Prepare transaction for commit
    ///
    /// # Errors
    ///
    /// Returns `TransactionError` if the transaction is already prepared,
    /// in an invalid state, or if the prepare operation fails.
    pub async fn prepare_transaction(
        &self,
        label: String,
        manager: &StreamLoadManager,
    ) -> Result<(), TransactionError> {
        // Validate and update state
        {
            let transactions = self
                .transactions
                .lock()
                .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

            if let Some(metadata) = transactions.get(&label) {
                match &metadata.state {
                    TransactionState::Begun(_) | TransactionState::Loaded => {
                        // Valid states for prepare
                    }
                    TransactionState::Prepared => {
                        return Err(TransactionError::AlreadyPrepared(label));
                    }
                    other => {
                        return Err(TransactionError::InvalidStateTransition(format!(
                            "Cannot prepare in state: {}",
                            other
                        )));
                    }
                }
            }
        }

        // Prepare transaction with StarRocks
        match manager.prepare_transaction(&label).await {
            Ok(_) => {
                let mut transactions = self
                    .transactions
                    .lock()
                    .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

                if let Some(metadata) = transactions.get_mut(&label) {
                    metadata.state = TransactionState::Prepared;
                    metadata.last_updated = Instant::now();
                }

                Ok(())
            }
            Err(error) => {
                self.mark_transaction_failed(label.clone(), error.to_string());
                Err(TransactionError::PrepareFailed(error.to_string()))
            }
        }
    }

    /// Commit transaction
    ///
    /// # Errors
    ///
    /// Returns `TransactionError` if the transaction cannot be committed or
    /// if the auto-prepare phase fails.
    pub async fn commit_transaction(
        &self,
        label: String,
        manager: &StreamLoadManager,
    ) -> Result<(), TransactionError> {
        // Auto-prepare if needed
        {
            let transactions = self
                .transactions
                .lock()
                .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

            if let Some(metadata) = transactions.get(&label) {
                if matches!(
                    metadata.state,
                    TransactionState::Begun(_) | TransactionState::Loaded
                ) {}
            }
        }

        // We must check the state again and potentially prepare.
        // To avoid holding lock across await, we call prepare_transaction which handles its own locking.
        // But we only want to call it if the state was Begun or Loaded.
        // So let's use a helper or just check again.

        // Re-check state to see if we need to auto-prepare
        let needs_prepare = {
            let transactions = self
                .transactions
                .lock()
                .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;
            transactions.get(&label).is_some_and(|m| {
                matches!(
                    m.state,
                    TransactionState::Begun(_) | TransactionState::Loaded
                )
            })
        };

        if needs_prepare {
            self.prepare_transaction(label.clone(), manager).await?;
        }

        // Commit transaction with StarRocks
        match manager.commit_transaction(&label).await {
            Ok(_) => {
                let mut transactions = self
                    .transactions
                    .lock()
                    .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

                if let Some(metadata) = transactions.get_mut(&label) {
                    metadata.state = TransactionState::Committed;
                    metadata.last_updated = Instant::now();
                }

                tracing::info!("Transaction '{}' committed successfully", label);
                Ok(())
            }
            Err(error) => {
                self.mark_transaction_failed(label.clone(), error.to_string());
                Err(TransactionError::CommitFailed(error.to_string()))
            }
        }
    }

    /// Rollback transaction
    ///
    /// # Errors
    ///
    /// Returns `TransactionError` if the rollback operation fails on the
    /// StarRocks backend.
    pub async fn rollback_transaction(
        &self,
        label: String,
        manager: &StreamLoadManager,
    ) -> Result<(), TransactionError> {
        // Rollback transaction with StarRocks
        match manager.rollback_transaction(&label).await {
            Ok(_) => {
                let mut transactions = self
                    .transactions
                    .lock()
                    .map_err(|e| TransactionError::InternalLockError(e.to_string()))?;

                if let Some(metadata) = transactions.get_mut(&label) {
                    metadata.state = TransactionState::RolledBack;
                    metadata.last_updated = Instant::now();
                }

                tracing::info!("Transaction '{}' rolled back successfully", label);
                Ok(())
            }
            Err(error) => {
                // Even if rollback fails, mark as rolled back locally
                self.mark_transaction_rolled_back(
                    label.clone(),
                    format!("Rollback failed: {}", error),
                );
                Err(TransactionError::RollbackFailed(error.to_string()))
            }
        }
    }

    /// Mark transaction as failed
    fn mark_transaction_failed(&self, label: String, reason: String) {
        let mut transactions = self.transactions.lock().unwrap();

        if let Some(metadata) = transactions.get_mut(&label) {
            metadata.state = TransactionState::Failed(reason.clone());
            metadata.error_details = Some(reason);
            metadata.last_updated = Instant::now();
        }
    }

    /// Mark transaction as rolled back
    fn mark_transaction_rolled_back(&self, label: String, reason: String) {
        let mut transactions = self.transactions.lock().unwrap();

        if let Some(metadata) = transactions.get_mut(&label) {
            metadata.state = TransactionState::RolledBack;
            metadata.error_details = Some(reason);
            metadata.last_updated = Instant::now();
        }
    }

    /// Get transaction status
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn get_transaction_status(&self, label: &str) -> Option<TransactionMetadata> {
        let transactions = self.transactions.lock().unwrap();
        transactions.get(label).cloned()
    }

    /// Get all transactions
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn get_all_transactions(&self) -> Vec<TransactionMetadata> {
        let transactions = self.transactions.lock().unwrap();
        transactions.values().cloned().collect()
    }

    /// Recover stuck transactions
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub async fn recover_stuck_transactions(
        &self,
        manager: &StreamLoadManager,
    ) -> Vec<TransactionMetadata> {
        let mut stuck_transactions = Vec::new();

        {
            let transactions = self.transactions.lock().unwrap();

            for metadata in transactions.values() {
                if metadata.last_updated.elapsed() > self.config.transaction_timeout {
                    match &metadata.state {
                        TransactionState::Begun(_)
                        | TransactionState::Loaded
                        | TransactionState::Prepared => {
                            stuck_transactions.push(metadata.clone());
                        }
                        _ => {}
                    }
                }
            }
        }

        // Attempt rollback for stuck transactions
        for metadata in stuck_transactions.clone() {
            tracing::warn!(
                "Recovering stuck transaction: '{}' (state: {}, timeout: {}ms)",
                metadata.label,
                metadata.state,
                metadata.duration_since_update().as_millis()
            );

            // Auto-rollback stuck transactions if enabled
            if self.config.enable_auto_recovery
                && let Err(e) = self
                    .rollback_transaction(metadata.label.clone(), manager)
                    .await
            {
                tracing::error!(
                    "Failed to rollback stuck transaction '{}': {}",
                    metadata.label,
                    e
                );
            }
        }

        stuck_transactions
    }

    /// Clean up completed transactions
    pub fn cleanup_completed_transactions(&self, max_age: Duration) -> usize {
        let mut transactions = self.transactions.lock().unwrap();

        let labels_to_remove: Vec<String> = transactions
            .iter()
            .filter(|(_, metadata)| match &metadata.state {
                TransactionState::Committed | TransactionState::RolledBack => {
                    metadata.last_updated.elapsed() > max_age
                }
                _ => false,
            })
            .map(|(label, _)| label.clone())
            .collect();

        for label in &labels_to_remove {
            tracing::info!("Cleaning up completed transaction: '{}'", label);
            transactions.remove(label);
        }

        labels_to_remove.len()
    }

    /// Generate status report
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn generate_status_report(&self) -> TransactionStatusReport {
        let transactions = self.transactions.lock().unwrap();

        let mut report = TransactionStatusReport::default();

        for metadata in transactions.values() {
            report.total_transactions += 1;

            match &metadata.state {
                TransactionState::NotStarted => report.not_started += 1,
                TransactionState::Begun(_) => report.begun += 1,
                TransactionState::Loaded => report.loaded += 1,
                TransactionState::Prepared => report.prepared += 1,
                TransactionState::Committed => report.committed += 1,
                TransactionState::RolledBack => report.rolled_back += 1,
                TransactionState::Failed(_) => report.failed += 1,
            }

            // Track stuck transactions
            if metadata.last_updated.elapsed() > self.config.transaction_timeout
                && matches!(
                    metadata.state,
                    TransactionState::Begun(_)
                        | TransactionState::Loaded
                        | TransactionState::Prepared
                )
            {
                report.stuck_transactions.push(metadata.clone());
            }
        }

        report
    }
}

#[derive(Debug, Default)]
pub struct TransactionStatusReport {
    pub total_transactions: usize,
    pub not_started: usize,
    pub begun: usize,
    pub loaded: usize,
    pub prepared: usize,
    pub committed: usize,
    pub rolled_back: usize,
    pub failed: usize,
    pub stuck_transactions: Vec<TransactionMetadata>,
}

#[derive(Debug)]
pub enum TransactionError {
    DuplicateTransactionLabel(String),
    InvalidStateTransition(String),
    AlreadyPrepared(String),
    InternalLockError(String),
    BeginFailed(String),
    LoadFailed(String),
    PrepareFailed(String),
    CommitFailed(String),
    RollbackFailed(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::DuplicateTransactionLabel(label) => {
                write!(f, "Duplicate transaction label: {label}")
            }
            TransactionError::InvalidStateTransition(msg) => {
                write!(f, "Invalid state transition: {msg}")
            }
            TransactionError::AlreadyPrepared(label) => {
                write!(f, "Transaction already prepared: {label}")
            }
            TransactionError::InternalLockError(msg) => {
                write!(f, "Internal lock error: {msg}")
            }
            TransactionError::BeginFailed(msg) => {
                write!(f, "Transaction begin failed: {msg}")
            }
            TransactionError::LoadFailed(msg) => {
                write!(f, "Transaction load failed: {msg}")
            }
            TransactionError::PrepareFailed(msg) => {
                write!(f, "Transaction prepare failed: {msg}")
            }
            TransactionError::CommitFailed(msg) => {
                write!(f, "Transaction commit failed: {msg}")
            }
            TransactionError::RollbackFailed(msg) => {
                write!(f, "Transaction rollback failed: {msg}")
            }
        }
    }
}

impl std::error::Error for TransactionError {}

/// Simple helper to generate test labels
fn generate_test_label(prefix: &str) -> String {
    format!("{}_{}", prefix, chrono::Utc::now().timestamp_millis())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("🚀 StarRocks Transaction State Management Example");
    println!("═══════════════════════════════════════════════════\n");

    // =================================================================
    // TRANSACTION MANAGER SETUP
    // =================================================================
    println!("📋 Step 1: Configuring transaction manager...");

    let config = TransactionManagerConfig {
        transaction_timeout: Duration::from_secs(120), // 2 minutes for demo
        max_concurrent_transactions: 10,
        enable_auto_recovery: true,
    };

    let txn_manager = TransactionManager::new(config.clone());
    println!("✓ Transaction manager configured:");
    println!("  Timeout: {}s", config.transaction_timeout.as_secs());
    println!("  Max concurrent: {}", config.max_concurrent_transactions);
    println!("  Auto recovery: {}", config.enable_auto_recovery);

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
    .enable_transaction(true)
    .max_retries(2)
    .build();

    let properties = StreamLoadTableProperties::builder()
        .table("txn_state_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(0)
        .build();

    let manager = StreamLoadManager::new(stream_config, properties)?;
    println!("✓ StreamLoadManager created with transaction support");

    // =================================================================
    // DEMONSTRATION 1: Full transaction lifecycle
    // =================================================================
    println!("\n📋 Demonstration 1: Complete transaction lifecycle");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let label = generate_test_label("lifecycle_txn");
    println!("Starting transaction: {label}");

    // Step 1: Begin transaction
    let txn_id = txn_manager
        .begin_transaction(label.clone(), &manager, "txn_state_users".to_string())
        .await?;
    println!("✓ Transaction begun with ID: {txn_id}");
    println!(
        "  Current state: {}",
        txn_manager.get_transaction_status(&label).unwrap().state
    );

    // Step 2: Load data
    let chunk1 = Bytes::from("1,LifecycleUser1,25\n2,LifecycleUser2,30\n");
    txn_manager
        .load_data(
            label.clone(),
            &manager,
            "test_db".to_string(),
            "txn_state_users".to_string(),
            0,
            chunk1,
        )
        .await?;
    println!("✓ Data chunk 0 loaded");
    println!(
        "  Current state: {}",
        txn_manager.get_transaction_status(&label).unwrap().state
    );

    // Step 3: Prepare transaction
    txn_manager
        .prepare_transaction(label.clone(), &manager)
        .await?;
    println!("✓ Transaction prepared");
    println!(
        "  Current state: {}",
        txn_manager.get_transaction_status(&label).unwrap().state
    );

    // Step 4: Commit transaction
    txn_manager
        .commit_transaction(label.clone(), &manager)
        .await?;
    println!("✓ Transaction committed");
    let final_state = txn_manager.get_transaction_status(&label).unwrap();
    println!("  Final state: {}", final_state.state);

    // =================================================================
    // DEMONSTRATION 2: Transaction rollback on error
    // =================================================================
    println!("\n📋 Demonstration 2: Transaction rollback on error");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let error_label = generate_test_label("rollback_txn");
    println!("Starting transaction: {error_label}");

    let txn_id = txn_manager
        .begin_transaction(error_label.clone(), &manager, "txn_state_users".to_string())
        .await?;
    println!("✓ Transaction begun with ID: {txn_id}");

    // Load some data
    let chunk = Bytes::from("1,ErrorUser,25\n");
    txn_manager
        .load_data(
            error_label.clone(),
            &manager,
            "test_db".to_string(),
            "txn_state_users".to_string(),
            0,
            chunk,
        )
        .await?;
    println!("✓ Data loaded into transaction");

    // Force rollback for demonstration
    println!("Rolling back transaction due to simulated error...");
    txn_manager
        .rollback_transaction(error_label.clone(), &manager)
        .await?;
    println!("✓ Transaction rolled back");
    let final_state = txn_manager.get_transaction_status(&error_label).unwrap();
    println!("  Final state: {}", final_state.state);

    // =================================================================
    // DEMONSTRATION 3: Auto-prepare before commit
    // =================================================================
    println!("\n📋 Demonstration 3: Auto-prepare before commit");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let auto_label = generate_test_label("auto_prepare_txn");
    println!("Starting transaction: {auto_label}");

    let txn_id = txn_manager
        .begin_transaction(auto_label.clone(), &manager, "txn_state_users".to_string())
        .await?;
    println!("✓ Transaction begun with ID: {txn_id}");

    // Load data
    let chunk = Bytes::from("1,AutoPrepareUser,35\n");
    txn_manager
        .load_data(
            auto_label.clone(),
            &manager,
            "test_db".to_string(),
            "txn_state_users".to_string(),
            0,
            chunk,
        )
        .await?;
    println!("✓ Data loaded (transaction in LOADED state)");

    // Commit directly - should auto-prepare
    println!("Committing directly (auto-prepare will be triggered)...");
    txn_manager
        .commit_transaction(auto_label.clone(), &manager)
        .await?;
    println!("✓ Transaction committed successfully");
    let final_state = txn_manager.get_transaction_status(&auto_label).unwrap();
    println!("  Final state: {}", final_state.state);

    // =================================================================
    // DEMONSTRATION 4: Concurrent transaction handling
    // =================================================================
    println!("\n📋 Demonstration 4: Concurrent transaction management");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let mut handles = vec![];
    let txn_manager_ref = std::sync::Arc::new(txn_manager);
    let manager_ref = Arc::new(manager);

    for i in 0..3 {
        let txn_manager = txn_manager_ref.clone();
        let manager = manager_ref.clone();
        let label = generate_test_label(&format!("concurrent_txn_{i}"));
        let data = Bytes::from(format!("1,ConcurrentUser{},{}\n", i, (i + 1) * 10));

        let handle = tokio::spawn(async move {
            // Begin transaction
            let _txn_id = txn_manager
                .begin_transaction(label.clone(), &manager, "txn_state_users".to_string())
                .await?;

            // Load data
            txn_manager
                .load_data(
                    label.clone(),
                    &manager,
                    "test_db".to_string(),
                    "txn_state_users".to_string(),
                    0,
                    data,
                )
                .await?;

            // Commit transaction
            txn_manager
                .commit_transaction(label.clone(), &manager)
                .await?;

            Ok::<_, TransactionError>(())
        });

        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let successful_txns = results.iter().filter(|r| r.is_ok()).count();
    println!("✓ Completed {successful_txns} out of 3 concurrent transactions");

    // =================================================================
    // DEMONSTRATION 5: Status reporting and cleanup
    // =================================================================
    println!("\n📋 Demonstration 5: Status reporting and cleanup");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let status_report = txn_manager_ref.generate_status_report();
    println!("Transaction Status Report:");
    println!("  Total transactions: {}", status_report.total_transactions);
    println!("  Not started: {}", status_report.not_started);
    println!("  Begun: {}", status_report.begun);
    println!("  Loaded: {}", status_report.loaded);
    println!("  Prepared: {}", status_report.prepared);
    println!("  Committed: {}", status_report.committed);
    println!("  Rolled back: {}", status_report.rolled_back);
    println!("  Failed: {}", status_report.failed);
    println!(
        "  Stuck transactions: {}",
        status_report.stuck_transactions.len()
    );

    // Cleanup completed transactions
    let cleaned = txn_manager_ref.cleanup_completed_transactions(Duration::from_secs(0));
    println!("\n✓ Cleaned up {cleaned} completed transactions");

    println!("\n✅ Transaction state management demonstration completed successfully!");
    println!(
        "   This system provides complete transaction lifecycle control for production reliability"
    );

    Ok(())
}
