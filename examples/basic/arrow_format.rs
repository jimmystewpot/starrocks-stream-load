#![allow(warnings)]
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
//! # Arrow Format Example
//!
//! This example demonstrates how to serialize data into the Apache Arrow IPC stream format
//! and load it into StarRocks using the `DataFormat::ARROW` configuration.
//!
//! ## What this example demonstrates:
//! 1. Creating an Arrow schema.
//! 2. Building an Arrow `RecordBatch` containing column data.
//! 3. Serializing the `RecordBatch` into an in-memory Arrow IPC stream.
//! 4. Configuring the SDK to handle `DataFormat::ARROW`.
//! 5. Mapping the Arrow schema to StarRocks table columns using the `columns` property.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::error::Error;
use std::sync::Arc;

/// Simple helper to generate test labels
fn generate_test_label(prefix: &str) -> String {
    format!("{}_{}", prefix, chrono::Utc::now().timestamp_millis())
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

    println!("🚀 StarRocks Apache Arrow Format Example");
    println!("========================================\n");

    // =================================================================
    // 1. GENERATE ARROW DATA
    // =================================================================
    println!("📦 1. Building Arrow RecordBatch and IPC Stream");

    // Define the Arrow Schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Build Column Arrays
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let value_array = Int32Array::from(vec![25, 30, 35]);

    // Create the RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )?;

    // Serialize to Arrow IPC Stream in-memory
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let arrow_payload = Bytes::from(buffer);

    println!(
        "  ✓ Successfully serialized Arrow IPC Stream: {} bytes",
        arrow_payload.len()
    );

    // =================================================================
    // 2. CONFIGURE STREAM LOAD
    // =================================================================
    println!("\n⚙️ 2. Configuring Stream Load Manager");

    let base_config = StreamLoadConfig::builder(
        vec!["http://127.0.0.1:8030".to_string()],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("your_password")
    .max_retries(2)
    .build();

    // The key here is DataFormat::ARROW.
    // StarRocks will automatically parse the IPC stream schema to determine field names.
    // We can use `.columns()` to explicitly map these Arrow fields to the target StarRocks table columns.
    let arrow_properties = StreamLoadTableProperties::builder()
        .table("format_users")
        .format(DataFormat::ARROW)
        .columns("id,name,value")
        .build();

    let manager = StreamLoadManager::new(base_config, arrow_properties)?;

    // =================================================================
    // 3. EXECUTE STREAM LOAD
    // =================================================================
    println!("\n🚀 3. Executing Stream Load");

    let label = generate_test_label("arrow_example");
    let start = std::time::Instant::now();

    match manager.send_single_batch(&label, arrow_payload).await {
        Ok(response) => {
            let duration = start.elapsed();
            println!("  ✓ Load completed in {}ms", duration.as_millis());
            println!("  Status: {}", response.status);
            if let Some(loaded_rows) = response.number_loaded_rows {
                println!("  Loaded rows: {}", loaded_rows);
            }
            if let Some(load_bytes) = response.load_bytes {
                println!("  Processed bytes: {}", load_bytes);
            }

            assert!(
                response.status == "Success" || response.status == "OK",
                "Expected success status, got: {}",
                response.status
            );
        }
        Err(error) => {
            let duration = start.elapsed();
            println!("  ✗ Load failed in {}ms: {}", duration.as_millis(), error);
            println!(
                "  Note: This is expected if you don't have a local StarRocks instance running."
            );
        }
    }

    Ok(())
}
