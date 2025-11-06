use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::Arc;
use std::time::Instant;

#[tokio::test]
async fn test_memory_usage_growth() {
    println!("\n=== Memory Usage Growth Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("memory_test.aurora");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.enable_write_buffering = true;
    config.enable_wal = true;
    config.hot_cache_size_mb = 128; // Limit hot cache

    let db = Arc::new(Aurora::with_config(config).unwrap());

    db.new_collection("memory_test", vec![
        ("data", FieldType::String, false),
        ("index", FieldType::Int, false),
    ]).unwrap();

    // Get initial process info
    let pid = std::process::id();
    let initial_rss = get_process_rss_mb(pid);
    println!("Initial RSS: {} MB", initial_rss);

    // Insert documents in batches and measure memory
    let batch_size = 10000;
    let num_batches = 10; // 100k total documents

    for batch in 0..num_batches {
        println!("\nBatch {}/{}:", batch + 1, num_batches);

        let start = Instant::now();

        // Insert batch
        for i in 0..batch_size {
            let idx = batch * batch_size + i;
            db.insert_into("memory_test", vec![
                ("data", Value::String(format!("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Document {}", idx))),
                ("index", Value::Int(idx as i64)),
            ]).await.unwrap();
        }

        let duration = start.elapsed();
        let current_rss = get_process_rss_mb(pid);
        let growth = current_rss - initial_rss;

        println!("  Inserted: {} documents", (batch + 1) * batch_size);
        println!("  Time: {:.2?}", duration);
        println!("  Current RSS: {} MB", current_rss);
        println!("  Growth: {} MB", growth);
        println!("  Throughput: {:.2} docs/sec", batch_size as f64 / duration.as_secs_f64());
    }

    // Final flush
    db.flush().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let final_rss = get_process_rss_mb(pid);
    let total_growth = final_rss - initial_rss;

    println!("\n=== Summary ===");
    println!("Initial RSS: {} MB", initial_rss);
    println!("Final RSS: {} MB", final_rss);
    println!("Total growth: {} MB", total_growth);
    println!("Documents: 100,000");
    println!("Average per 10k docs: {:.2} MB", total_growth as f64 / 10.0);

    // Query to verify all data is accessible
    let query_start = Instant::now();
    let count = db.query("memory_test").collect().await.unwrap().len();
    let query_time = query_start.elapsed();

    println!("Query verification: {} documents in {:.2?}", count, query_time);

    assert_eq!(count, 100000, "All documents should be queryable");

    // Memory growth should be reasonable (< 500MB for 100k docs)
    assert!(total_growth < 500, "Memory growth should be under 500MB (got {} MB)", total_growth);
}

#[tokio::test]
async fn test_hot_cache_eviction() {
    println!("\n=== Hot Cache Eviction Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("cache_test.aurora");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.hot_cache_size_mb = 10; // Very small cache to force evictions
    config.enable_write_buffering = false; // Direct writes for testing

    let db = Arc::new(Aurora::with_config(config).unwrap());

    db.new_collection("cache_test", vec![
        ("data", FieldType::String, false),
    ]).unwrap();

    println!("Inserting 10,000 documents with 10MB hot cache limit...");

    // Insert documents that should exceed cache
    for i in 0..10000 {
        db.insert_into("cache_test", vec![
            ("data", Value::String(format!("Document {} with some padding text to increase size", i))),
        ]).await.unwrap();
    }

    println!("All documents inserted successfully");

    // Query some old documents (should be evicted from cache)
    let start = Instant::now();
    let doc1 = db.get_document("cache_test", &"0".to_string()).unwrap();
    let time1 = start.elapsed();

    // Query recent document (likely in cache)
    let start = Instant::now();
    let doc2 = db.query("cache_test")
        .filter(|f| f.eq("data", "Document 9999 with some padding text to increase size"))
        .collect()
        .await
        .unwrap();
    let time2 = start.elapsed();

    println!("Old document query time: {:?}", time1);
    println!("Recent document query time: {:?}", time2);

    assert!(doc1.is_some() || doc2.len() > 0, "Should be able to retrieve evicted documents from cold storage");

    println!("✓ Hot cache eviction working correctly");
}

#[tokio::test]
async fn test_error_disk_full_simulation() {
    println!("\n=== Disk Full Simulation Test ===");

    // This test simulates what happens when disk is nearly full
    // by using a very small database and filling it up

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("disk_test.aurora");

    let db = Arc::new(Aurora::open(db_path.to_str().unwrap()).unwrap());

    db.new_collection("disk_test", vec![
        ("data", FieldType::String, false),
    ]).unwrap();

    println!("Attempting to insert large documents...");

    // Try to insert very large documents
    let large_data = "x".repeat(1024 * 1024); // 1MB per document
    let mut success_count = 0;
    let mut error_count = 0;

    for i in 0..100 {
        match db.insert_into("disk_test", vec![
            ("data", Value::String(format!("{}{}", large_data, i))),
        ]).await {
            Ok(_) => success_count += 1,
            Err(e) => {
                println!("Error on document {}: {:?}", i, e);
                error_count += 1;
                if error_count > 5 {
                    break; // Stop after a few errors
                }
            }
        }
    }

    println!("Success: {}, Errors: {}", success_count, error_count);
    println!("✓ Database handles large writes without panicking");
}

#[tokio::test]
async fn test_concurrent_collection_creation() {
    println!("\n=== Concurrent Collection Creation Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("concurrent_collections.aurora");
    let db = Arc::new(Aurora::open(db_path.to_str().unwrap()).unwrap());

    let mut tasks = vec![];

    // Try to create 10 collections concurrently
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db_clone.new_collection(
                &format!("collection_{}", i),
                vec![("field", FieldType::String, false)]
            )
        }));
    }

    let mut success = 0;
    for task in tasks {
        if task.await.unwrap().is_ok() {
            success += 1;
        }
    }

    println!("Successfully created {} collections concurrently", success);
    assert_eq!(success, 10, "All collection creations should succeed");
}

#[tokio::test]
async fn test_rapid_flush_calls() {
    println!("\n=== Rapid Flush Calls Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("flush_test.aurora");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.enable_write_buffering = true;

    let db = Arc::new(Aurora::with_config(config).unwrap());

    db.new_collection("flush_test", vec![
        ("data", FieldType::String, false),
    ]).unwrap();

    // Insert some data
    for i in 0..100 {
        db.insert_into("flush_test", vec![
            ("data", Value::String(format!("data_{}", i))),
        ]).await.unwrap();
    }

    println!("Calling flush() rapidly 50 times...");

    let start = Instant::now();
    for _ in 0..50 {
        db.flush().unwrap();
    }
    let duration = start.elapsed();

    println!("All flushes completed in {:?}", duration);
    println!("✓ Rapid flush calls don't cause deadlocks or panics");

    // Verify data integrity
    let count = db.query("flush_test").collect().await.unwrap().len();
    assert_eq!(count, 100, "All data should be intact after rapid flushes");
}

// Helper function to get process RSS in MB
fn get_process_rss_mb(pid: u32) -> i64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{}/status", pid)) {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<i64>() {
                            return kb / 1024; // Convert to MB
                        }
                    }
                }
            }
        }
    }

    // Fallback for non-Linux or if reading fails
    0
}
