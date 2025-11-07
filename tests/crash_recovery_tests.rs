/// Crash Recovery and Durability Tests
///
/// These tests verify that Aurora correctly handles crash scenarios and can recover data
/// from the Write-Ahead Log (WAL). Tests include:
/// - Random shutdown scenarios
/// - WAL replay correctness
/// - Data integrity after crash
/// - Transaction recovery

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Helper to create a test database with WAL enabled
fn create_test_db(path: PathBuf) -> Result<Aurora, aurora_db::AuroraError> {
    let mut config = AuroraConfig::default();
    config.db_path = path;
    config.enable_wal = true;
    config.enable_write_buffering = true;
    config.checkpoint_interval_ms = 1000; // 1 second checkpoint

    Aurora::with_config(config)
}

#[tokio::test]
async fn test_crash_recovery_basic() {
    println!("\n=== Basic Crash Recovery Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("crash_test.aurora");

    // Phase 1: Write data and simulate crash by dropping DB
    {
        let db = create_test_db(db_path.clone()).unwrap();

        db.new_collection("users", vec![
            ("name", FieldType::String, false),
            ("age", FieldType::Int, false),
        ]).unwrap();

        // Insert some data
        for i in 0..100 {
            db.insert_into("users", vec![
                ("name", Value::String(format!("user_{}", i))),
                ("age", Value::Int(20 + (i % 50))),
            ]).await.unwrap();
        }

        // Explicitly flush to ensure data is written
        db.flush().unwrap();

        println!("Inserted 100 documents, simulating crash...");
        // Drop db to simulate crash (no graceful shutdown)
        drop(db);
    }

    // Small delay to allow file locks to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Reopen database and verify data recovery
    {
        println!("Reopening database and verifying recovery...");
        let db = create_test_db(db_path).unwrap();

        // Query all documents
        let results = db.query("users").collect().await.unwrap();

        println!("Recovered {} documents", results.len());
        assert_eq!(results.len(), 100, "Should recover all 100 documents");

        // Verify some specific documents
        let doc = results.iter().find(|d| {
            d.data.get("name") == Some(&Value::String("user_0".to_string()))
        });
        assert!(doc.is_some(), "Should find user_0");

        println!("✓ Basic crash recovery successful");
    }
}

#[tokio::test]
async fn test_crash_during_writes() {
    println!("\n=== Crash During Writes Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("crash_during_writes.aurora");

    // Phase 1: Start writing data and crash at random point
    {
        let db = Arc::new(create_test_db(db_path.clone()).unwrap());

        db.new_collection("transactions", vec![
            ("tx_id", FieldType::String, false),
            ("amount", FieldType::Int, false),
        ]).unwrap();

        // Insert data concurrently and crash suddenly
        let mut tasks = vec![];
        for i in 0..500 {
            let db_clone = Arc::clone(&db);
            tasks.push(tokio::spawn(async move {
                db_clone.insert_into("transactions", vec![
                    ("tx_id", Value::String(format!("tx_{}", i))),
                    ("amount", Value::Int(i * 100)),
                ]).await
            }));
        }

        // Wait for about half the writes to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("Simulating crash mid-write...");
        // Forcefully drop everything (simulates sudden process termination)
        drop(tasks);
        drop(db);
    }

    // Small delay to allow file locks to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Recover and verify consistency
    {
        println!("Recovering after mid-write crash...");
        let db = create_test_db(db_path).unwrap();

        let results = db.query("transactions").collect().await.unwrap();
        println!("Recovered {} transactions", results.len());

        // Verify data integrity - all recovered records should be valid
        for doc in &results {
            let tx_id = doc.data.get("tx_id").unwrap();
            let amount = doc.data.get("amount").unwrap();

            if let (Value::String(id), Value::Int(amt)) = (tx_id, amount) {
                // Extract number from tx_id
                let num: i64 = id.strip_prefix("tx_").unwrap().parse().unwrap();
                assert_eq!(*amt, num * 100, "Amount should match transaction ID");
            }
        }

        println!("✓ All recovered transactions are valid (recovered {} of 500)", results.len());
        println!("✓ No data corruption detected");
    }
}

#[tokio::test]
async fn test_wal_replay_idempotency() {
    println!("\n=== WAL Replay Idempotency Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("idempotency.aurora");

    // Phase 1: Write known dataset
    {
        let db = create_test_db(db_path.clone()).unwrap();

        db.new_collection("items", vec![
            ("item_id", FieldType::String, true), // unique
            ("count", FieldType::Int, false),
        ]).unwrap();

        // Insert specific items
        for i in 0..50 {
            db.insert_into("items", vec![
                ("item_id", Value::String(format!("item_{}", i))),
                ("count", Value::Int(i)),
            ]).await.unwrap();
        }

        db.flush().unwrap();
        println!("Inserted 50 items");
        drop(db);
    }

    // Small delay to allow file locks to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Reopen multiple times (simulating multiple replay scenarios)
    for attempt in 1..=3 {
        println!("\nReopening database (attempt {})...", attempt);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let db = create_test_db(db_path.clone()).unwrap();

        let results = db.query("items").collect().await.unwrap();
        assert_eq!(results.len(), 50, "Should always have exactly 50 items after replay");

        // Verify no duplicates
        let mut item_ids: Vec<String> = results.iter()
            .filter_map(|doc| {
                if let Some(Value::String(id)) = doc.data.get("item_id") {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        item_ids.sort();
        item_ids.dedup();
        assert_eq!(item_ids.len(), 50, "Should have 50 unique items (no duplicates)");

        drop(db);
    }

    println!("✓ WAL replay is idempotent - no duplicates across multiple recoveries");
}

#[tokio::test]
async fn test_partial_write_crash_recovery() {
    println!("\n=== Partial Write Crash Recovery Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("partial_write.aurora");

    // Phase 1: Write data in batches and crash mid-batch
    {
        let db = Arc::new(create_test_db(db_path.clone()).unwrap());

        db.new_collection("batches", vec![
            ("batch_id", FieldType::Int, false),
            ("item_id", FieldType::Int, false),
        ]).unwrap();

        // Write first batch completely
        for i in 0..50 {
            db.insert_into("batches", vec![
                ("batch_id", Value::Int(1)),
                ("item_id", Value::Int(i)),
            ]).await.unwrap();
        }

        db.flush().unwrap();

        // Start second batch but don't flush
        for i in 0..25 {
            db.insert_into("batches", vec![
                ("batch_id", Value::Int(2)),
                ("item_id", Value::Int(i)),
            ]).await.unwrap();
        }

        println!("Wrote batch 1 (50 items) and partial batch 2 (25 items), simulating crash...");
        // Crash without flushing second batch
        drop(db);
    }

    // Small delay to allow file locks to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Verify at least first batch is recovered
    {
        let db = create_test_db(db_path).unwrap();

        let results = db.query("batches").collect().await.unwrap();
        println!("Recovered {} documents", results.len());

        // Should have at least the first batch
        let batch1_count = results.iter()
            .filter(|doc| matches!(doc.data.get("batch_id"), Some(Value::Int(1))))
            .count();

        assert_eq!(batch1_count, 50, "First batch should be fully recovered");

        // Second batch may or may not be present depending on write buffer timing
        let batch2_count = results.iter()
            .filter(|doc| matches!(doc.data.get("batch_id"), Some(Value::Int(2))))
            .count();

        println!("  Batch 1 (flushed): {} items", batch1_count);
        println!("  Batch 2 (unflushed): {} items", batch2_count);

        println!("✓ Durable data recovered correctly");
    }
}

#[tokio::test]
async fn test_multiple_crash_cycles() {
    println!("\n=== Multiple Crash Cycles Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("multi_crash.aurora");

    let crash_cycles = 5;
    let docs_per_cycle = 20;

    for cycle in 0..crash_cycles {
        println!("\n--- Crash Cycle {} ---", cycle + 1);

        let db = create_test_db(db_path.clone()).unwrap();

        // Create collection on first cycle
        if cycle == 0 {
            db.new_collection("resilient", vec![
                ("data", FieldType::String, false),
                ("cycle", FieldType::Int, false),
            ]).unwrap();
        }

        // Verify previous data survived
        let before_count = db.query("resilient").collect().await.unwrap().len();
        println!("Documents before cycle {}: {}", cycle + 1, before_count);
        assert_eq!(before_count, cycle * docs_per_cycle,
                   "Should have {} documents from previous cycles", cycle * docs_per_cycle);

        // Add new data
        for i in 0..docs_per_cycle {
            db.insert_into("resilient", vec![
                ("data", Value::String(format!("cycle_{}_doc_{}", cycle, i))),
                ("cycle", Value::Int(cycle as i64)),
            ]).await.unwrap();
        }

        db.flush().unwrap();

        // Verify new data was added
        let after_count = db.query("resilient").collect().await.unwrap().len();
        println!("Documents after inserts: {}", after_count);
        assert_eq!(after_count, (cycle + 1) * docs_per_cycle);

        println!("Simulating crash...");
        drop(db);

        // Small delay between crash cycles
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Final verification
    {
        println!("\n--- Final Verification ---");
        let db = create_test_db(db_path).unwrap();

        let final_results = db.query("resilient").collect().await.unwrap();
        println!("Total documents after {} crash cycles: {}", crash_cycles, final_results.len());

        assert_eq!(final_results.len(), crash_cycles * docs_per_cycle,
                   "Should have all documents from all cycles");

        // Verify we have data from each cycle
        for cycle in 0..crash_cycles {
            let cycle_docs: Vec<_> = final_results.iter()
                .filter(|doc| {
                    matches!(doc.data.get("cycle"), Some(Value::Int(c)) if *c == cycle as i64)
                })
                .collect();

            assert_eq!(cycle_docs.len(), docs_per_cycle,
                       "Should have {} docs from cycle {}", docs_per_cycle, cycle);
        }

        println!("✓ All data survived {} crash/recovery cycles", crash_cycles);
    }
}

#[tokio::test]
async fn test_crash_with_large_documents() {
    println!("\n=== Crash Recovery with Large Documents ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("large_docs_crash.aurora");

    // Phase 1: Insert large documents
    {
        let db = create_test_db(db_path.clone()).unwrap();

        db.new_collection("large_docs", vec![
            ("doc_id", FieldType::String, false),
            ("large_data", FieldType::String, false),
        ]).unwrap();

        // Create large documents (100KB each)
        let large_text = "x".repeat(100_000);

        for i in 0..10 {
            db.insert_into("large_docs", vec![
                ("doc_id", Value::String(format!("large_{}", i))),
                ("large_data", Value::String(large_text.clone())),
            ]).await.unwrap();
        }

        db.flush().unwrap();
        println!("Inserted 10 large documents (100KB each)");
        drop(db);
    }

    // Small delay to allow file locks to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Verify large documents are recovered correctly
    {
        let db = create_test_db(db_path).unwrap();

        let results = db.query("large_docs").collect().await.unwrap();
        assert_eq!(results.len(), 10, "Should recover all large documents");

        // Verify a large document's data integrity
        for doc in &results {
            if let Some(Value::String(data)) = doc.data.get("large_data") {
                assert_eq!(data.len(), 100_000, "Large document data should be intact");
            }
        }

        println!("✓ All large documents recovered with correct size");
    }
}

#[tokio::test]
async fn test_checkpoint_after_crash() {
    println!("\n=== Checkpoint After Crash Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("checkpoint_crash.aurora");

    // Phase 1: Write data, trigger checkpoint, then crash
    {
        let db = Arc::new(create_test_db(db_path.clone()).unwrap());

        db.new_collection("checkpointed", vec![
            ("value", FieldType::Int, false),
        ]).unwrap();

        // Insert data
        for i in 0..100 {
            db.insert_into("checkpointed", vec![
                ("value", Value::Int(i)),
            ]).await.unwrap();
        }

        db.flush().unwrap();

        // Wait for checkpoint to occur (configured at 1000ms)
        println!("Waiting for checkpoint...");
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Add more data after checkpoint
        for i in 100..150 {
            db.insert_into("checkpointed", vec![
                ("value", Value::Int(i)),
            ]).await.unwrap();
        }

        println!("Simulating crash after checkpoint...");
        drop(db);
    }

    // Small delay to allow file locks to be released
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Verify all data is recovered
    {
        let db = create_test_db(db_path).unwrap();

        let results = db.query("checkpointed").collect().await.unwrap();
        println!("Recovered {} documents", results.len());

        // We should recover at least the checkpointed data (0-99)
        // Data after checkpoint (100-149) may or may not be present depending on flush timing
        assert!(results.len() >= 100, "Should recover at least checkpointed data");

        println!("✓ Checkpoint recovery successful");
    }
}
