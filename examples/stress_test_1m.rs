use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = PathBuf::from("stress_test.db");
    
    // 1. Setup Database with optimized config for high ingestion
    let config = AuroraConfig {
        db_path: db_path.clone(),
        hot_cache_size_mb: 256,
        enable_write_buffering: true,
        write_buffer_size: 100_000, // Large buffer
        ..Default::default()
    };
    
    let db = Aurora::with_config(config).await?;
    
    // 2. Create Collection
    db.new_collection("users", vec![
        ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
        ("age", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true }),
        ("city", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
        ("active", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_BOOL, unique: false, indexed: false, nullable: true }),
        ("payload", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
    ]).await?;

    println!("Starting 1M document ingestion...");
    let start = Instant::now();
    
    let cities = vec!["London", "New York", "Paris", "Berlin", "Tokyo", "Lagos", "Accra"];
    let batch_size = 10_000;
    let total_docs = 1_000_000;
    
    // Payload to reach ~1.2KB per doc — shared via Arc to avoid 1M allocations
    let payload = Arc::new("A".repeat(1100));

    for i in 0..(total_docs / batch_size) {
        let mut batch = Vec::with_capacity(batch_size);
        for j in 0..batch_size {
            let id = i * batch_size + j;
            let mut doc = HashMap::new();
            doc.insert("name".to_string(), Value::String(format!("User {}", id)));
            doc.insert("age".to_string(), Value::Int((id % 100) as i64));
            doc.insert("city".to_string(), Value::String(cities[id % cities.len()].to_string()));
            doc.insert("active".to_string(), Value::Bool(id % 2 == 0));
            doc.insert("payload".to_string(), Value::String((*payload).clone()));
            batch.push(doc);
        }
        db.batch_insert("users", batch).await?;
        if (i + 1) % 10 == 0 {
            println!("Inserted {} documents...", (i + 1) * batch_size);
        }
    }

    let duration = start.elapsed();
    println!("Ingestion Complete: 1,000,000 docs in {:?}", duration);
    println!("Throughput: {} docs/sec", total_docs as f64 / duration.as_secs_f64());

    db.flush()?;
    Ok(())
}
