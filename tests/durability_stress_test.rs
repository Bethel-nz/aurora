use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_brutal_crash_recovery() {
    println!("\n=== DURABILITY TEST: Brutal Crash Recovery ===");
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("durability.db");
    
    let config = AuroraConfig {
        db_path: db_path.clone(),
        enable_wal: true,
        cold_flush_interval_ms: Some(10000), // Long flush interval so data stays in WAL
        ..Default::default()
    };

    // 1. Initial Data Load
    let num_docs = 10_000;
    {
        println!("Opening database and inserting {} docs...", num_docs);
        let db = Aurora::with_config(config.clone()).await.unwrap();
        
        db.new_collection("users", vec![
            ("email", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: true, nullable: true, validations: vec![] }),
            ("age", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ]).await.unwrap();

        for i in 0..num_docs {
            db.insert_into("users", vec![
                ("email", Value::String(format!("user{}@example.com", i))),
                ("age", Value::Int(i as i64)),
            ]).await.unwrap();
        }
        
        // Wait briefly for the WAL Group Commit (10ms) but NOT for the 10s checkpoint
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        println!("CRASH! (Dropping database instance without graceful shutdown)");
        // Drop DB here. Since checkpoint is 10s, data MUST be in WAL, not in Hot/Cold.
    }

    // Wait for background tasks to release locks
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Recovery Phase
    println!("Reopening database. Recovery should trigger...");
    let start = std::time::Instant::now();
    let db = Aurora::with_config(config).await.unwrap();
    println!("Recovery completed in {:?}", start.elapsed());

    // 3. Verification
    println!("Verifying data integrity...");
    let count = db.query("users").count().await.unwrap();
    
    println!("Found {}/{} documents.", count, num_docs);
    assert_eq!(count, num_docs, "WAL recovery failed to restore all documents!");

    // Spot check
    let sample = db.query("users")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("email", format!("user{}@example.com", num_docs / 2)))
        .first_one().await.unwrap().unwrap();
    
    assert_eq!(sample.data.get("age"), Some(&Value::Int((num_docs / 2) as i64)));
    println!("SUCCESS: All data recovered and verified.");
}

#[tokio::test]
async fn test_partial_wal_recovery() {
    println!("\n=== DURABILITY TEST: Partial WAL Recovery (Interrupted Write) ===");
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("partial_durability.db");
    
    let config = AuroraConfig {
        db_path: db_path.clone(),
        enable_wal: true,
        cold_flush_interval_ms: Some(10000), 
        ..Default::default()
    };

    // 1. Initial Load
    {
        let db = Aurora::with_config(config.clone()).await.unwrap();
        db.new_collection("items", vec![("n", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] })]).await.unwrap();
        
        for i in 0..100 {
            db.insert_into("items", vec![("n", Value::Int(i as i64))]).await.unwrap();
        }
        db.sync().await.unwrap();
        // Database is dropped here at the end of scope
    }

    // Wait for background tasks to release locks
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Corrupt the WAL
    // The WAL is stored at {db_path}.wal (based on WriteAheadLog implementation)
    let mut wal_path = db_path.clone();
    wal_path.set_extension("wal");
    println!("Corrupting WAL at: {:?}", wal_path);
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        // Write a partial length (4 bytes) but no data, or just random garbage
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(); 
        file.flush().unwrap();
    }

    // 3. Recover
    println!("Reopening database with corrupted trailing WAL data...");
    let db = Aurora::with_config(config).await.unwrap();

    // 4. Verify
    let count = db.query("items").count().await.unwrap();
    println!("Found {} valid documents (Expected 100).", count);
    
    // The recovery logic should stop at the first invalid entry and still return the valid ones
    assert_eq!(count, 100, "Recovery should have restored all 100 valid documents despite trailing garbage!");
    println!("SUCCESS: Aurora gracefully handled partial WAL entry.");
}
