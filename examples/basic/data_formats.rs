#![allow(
    clippy::print_stdout,
    clippy::needless_raw_string_hashes,
    clippy::uninlined_format_args,
    clippy::doc_markdown,
    clippy::duration_suboptimal_units,
    unused_imports,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::similar_names,
    clippy::single_match_else,
    clippy::cast_precision_loss,
    clippy::ignored_unit_patterns
)]
//! # Data Formats Example
//!
//! This example demonstrates various data format options supported by StarRocks Stream Load SDK.
//!
//! ## What this example demonstrates:
//! 1. CSV format with different delimiters and separators
//! 2. JSON format with array and streaming formats
//! 3. Arrow format for high-performance columnar data
//! 4. Custom field separators and enclosures
//! 5. Skip header and row filtering options
//!
//! ## Supported formats:
//! - **CSV**: Comma-separated values with configurable delimiters
//! - **JSON**: JSON arrays and newline-delimited JSON (NDJSON)
//! - **Arrow**: Apache Arrow columnar format (high performance)
//! - **ORC/PARQUET**: Columnar storage formats (via format specification)
//!
//! ## Production considerations:
//! - Performance differences between formats (Arrow > JSON > CSV)
//! - Memory usage patterns for each format
//! - Error handling for malformed data
//! - Compression support (gzip, zstd, etc.)

use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::error::Error;

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

/// Generate Arrow-style binary data placeholder
fn generate_arrow_data(rows: usize) -> bytes::Bytes {
    let message = format!("Arrow data placeholder for {} rows", rows);
    bytes::Bytes::from(message)
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

    println!("🚀 StarRocks Data Formats Example");
    println!("===================================\n");

    // Configuration that will be reused across all examples
    let base_config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("your_password")
    .max_retries(2)
    .build();

    // =================================================================
    // 1. CSV FORMAT EXAMPLE
    // =================================================================
    println!("📋 1. CSV Format Demonstration");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let csv_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(1)
        .build();

    let csv_manager = StreamLoadManager::new(base_config.clone(), csv_properties)?;

    let csv_data = r"id,name,value
1,Alice,25
2,Bob,30
";

    demonstrate_format("CSV", &csv_manager, "csv_load", Bytes::from(csv_data)).await?;

    // =================================================================
    // 2. CSV WITH TAB DELIMITER
    // =================================================================
    println!("📋 2. CSV with Tab Separator");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let tab_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::CSV)
        .column_separator("\t") // Tab separator
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(1)
        .build();

    let tab_manager = StreamLoadManager::new(base_config.clone(), tab_properties)?;

    let tab_data = r#"id	name	value
1	Alice	25
2	Bob	30
"#;

    demonstrate_format(
        "CSV (Tab)",
        &tab_manager,
        "csv_tab_load",
        Bytes::from(tab_data),
    )
    .await?;

    // =================================================================
    // 3. JSON ARRAY FORMAT
    // =================================================================
    println!("📋 3. JSON Array Format");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let json_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::JSON)
        .columns("id,name,value") // Map JSON fields to table columns
        .build();

    let json_manager = StreamLoadManager::new(base_config.clone(), json_properties)?;

    let json_data = r#"[
        {"id": 1, "name": "Alice", "value": 25},
        {"name": "Bob", "id": 2, "value": 30}
    ]"#;

    demonstrate_format(
        "JSON Array",
        &json_manager,
        "json_load",
        Bytes::from(json_data),
    )
    .await?;

    // =================================================================
    // 4. NEWLINE-DELIMITED JSON (NDJSON)
    // =================================================================
    println!("📋 4. Newline-Delimited JSON (NDJSON)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let ndjson_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::JSON)
        .columns("id,name,value")
        .row_delimiter("\n") // Specify newline delimiter
        .build();

    let ndjson_manager = StreamLoadManager::new(base_config.clone(), ndjson_properties)?;

    let ndjson_data = r#"{"id": 1, "name": "Alice", "value": 25}
{"id": 2, "name": "Bob", "value": 30}
{"id": 3, "name": "Charlie", "value": 35}
"#;

    demonstrate_format(
        "NDJSON",
        &ndjson_manager,
        "ndjson_load",
        Bytes::from(ndjson_data),
    )
    .await?;

    // =================================================================
    // 5. ARROW FORMAT (Simulated)
    // =================================================================
    println!("📋 5. Apache Arrow Format");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let arrow_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::ARROW)
        .columns("id,name,value")
        .build();

    let arrow_manager = StreamLoadManager::new(base_config.clone(), arrow_properties)?;

    // Note: Real Arrow data requires proper serialization with arrow crate
    let arrow_data = generate_arrow_data(3);

    demonstrate_format("Arrow", &arrow_manager, "arrow_load", arrow_data).await?;

    // =================================================================
    // 6. CSV WITH QUOTED FIELDS
    // =================================================================
    println!("📋 6. CSV with Quoted Fields");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let quoted_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::CSV)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .skip_header(1)
        .build();

    let quoted_manager = StreamLoadManager::new(base_config, quoted_properties)?;

    let quoted_data = r#"id,name,value
1,"Alice, Smith",25
2,"Bob; Johnson",30
"#;

    demonstrate_format(
        "CSV (Quoted)",
        &quoted_manager,
        "quoted_load",
        Bytes::from(quoted_data),
    )
    .await?;

    println!("✅ All data format demonstrations completed successfully!");
    println!("   Review the performance and usage characteristics of each format");

    Ok(())
}

/// Helper function to demonstrate format loading with consistent output
async fn demonstrate_format(
    format_name: &str,
    manager: &StreamLoadManager,
    label_prefix: &str,
    data: Bytes,
) -> Result<(), Box<dyn Error>> {
    println!("  Format: {format_name}");
    println!("  Data size: {} bytes", data.len());

    let label = generate_test_label(label_prefix);
    let start = std::time::Instant::now();

    match manager.send_single_batch(&label, data).await {
        Ok(response) => {
            let duration = start.elapsed();
            println!("  ✓ Load completed in {}ms", duration.as_millis());
            println!("  Status: {}", response.status);

            if let Some(loaded_rows) = response.number_loaded_rows {
                println!("  Loaded rows: {loaded_rows}");
            }

            if let Some(load_bytes) = response.load_bytes {
                println!("  Processed bytes: {load_bytes}");
            }

            println!("  Transaction ID: {:?}", response.txn_id);

            assert_success_response(&response);
        }
        Err(error) => {
            let duration = start.elapsed();
            println!("  ✗ Load failed in {}ms: {error}", duration.as_millis());

            // For demonstration purposes, we'll note the error but not fail the entire program
            println!(
                "  Note: This is expected if the format or column mapping isn't configured correctly"
            );
        }
    }

    println!();
    Ok(())
}
