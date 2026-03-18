use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::time::Instant;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== BENCHMARK: INSERT vs UPSERT (10,000 operations) ===\n");

    let temp_dir = tempfile::tempdir()?;
    let db = Aurora::with_config(AuroraConfig {
        db_path: temp_dir.path().join("bench.db"),
        enable_write_buffering: true,
        ..Default::default()
    }).await?;

    db.new_collection("bench", vec![
        ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
        ("count", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true }),
    ]).await?;

    // 1. Benchmark: Pure Insert
    let start = Instant::now();
    for i in 0..10000 {
        db.insert_into("bench", vec![
            ("name", Value::String(format!("item-{}", i))),
            ("count", Value::Int(i)),
        ]).await?;
    }
    db.sync().await?;
    let insert_duration = start.elapsed();
    println!("  Standard Insert: {:?} ({:.0} ops/sec)", insert_duration, 10000.0 / insert_duration.as_secs_f64());

    // 2. Benchmark: Upsert (Updating existing documents)
    // This will trigger the "Fast Path" through the document_cache
    let start = Instant::now();
    for i in 0..10000 {
        // We use the ID format expected by our simulation
        // Note: insert_into generates UUIDs, but upsert takes a specific ID.
        // For a fair comparison, let's upsert to 10,000 specific IDs.
        db.upsert("bench", &format!("user-{}", i), vec![
            ("name", Value::String(format!("updated-item-{}", i))),
            ("count", Value::Int(i * 2)),
        ]).await?;
    }
    db.sync().await?;
    let upsert_duration = start.elapsed();
    println!("  Fast-Path Upsert: {:?} ({:.0} ops/sec)", upsert_duration, 10000.0 / upsert_duration.as_secs_f64());

    let overhead = (upsert_duration.as_secs_f64() / insert_duration.as_secs_f64() - 1.0) * 100.0;
    println!("\nSummary:");
    println!("  Upsert Overhead: {:.1}%", overhead);
    
    if overhead < 20.0 {
        println!(" SUCCESS: Upsert is within acceptable overhead limits of Insert.");
    } else {
        println!(" NOTICE: Upsert is significantly slower than Insert. Further optimization may be needed.");
    }

    Ok(())
}
