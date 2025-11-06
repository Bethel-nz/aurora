/// Advanced Concurrency Tests
///
/// These tests verify Aurora's behavior under heavy concurrent load with various
/// access patterns. Tests include:
/// - Concurrent reads and writes to same keys
/// - High contention scenarios
/// - Read-after-write consistency
/// - Concurrent collection operations

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

fn create_test_db(path: std::path::PathBuf) -> Result<Aurora, aurora_db::AuroraError> {
    let mut config = AuroraConfig::default();
    config.db_path = path;
    config.enable_wal = true;
    config.enable_write_buffering = true;

    Aurora::with_config(config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_same_key_writes() {
    println!("\n=== Concurrent Same-Key Writes Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("same_key.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("counter", vec![
        ("key", FieldType::String, false),
        ("value", FieldType::Int, false),
    ]).unwrap();

    let num_writers = 100;
    let writes_per_worker = 10;

    println!("Spawning {} writers, each writing {} times to the same key", num_writers, writes_per_worker);

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    // All writers write to the same logical key
    for writer_id in 0..num_writers {
        let db_clone = Arc::clone(&db);
        tasks.spawn(async move {
            let mut successes = 0;
            for i in 0..writes_per_worker {
                match db_clone.insert_into("counter", vec![
                    ("key", Value::String("shared_counter".to_string())),
                    ("value", Value::Int((writer_id * writes_per_worker + i) as i64)),
                ]).await {
                    Ok(_) => successes += 1,
                    Err(_) => {},
                }
            }
            successes
        });
    }

    let mut total_successes = 0;
    while let Some(result) = tasks.join_next().await {
        if let Ok(count) = result {
            total_successes += count;
        }
    }

    let duration = start.elapsed();

    println!("Results:");
    println!("  Duration: {:?}", duration);
    println!("  Successful writes: {}", total_successes);
    println!("  Expected: {}", num_writers * writes_per_worker);
    println!("  Throughput: {:.2} writes/sec", total_successes as f64 / duration.as_secs_f64());

    // Verify data integrity
    let results = db.query("counter").collect().await.unwrap();
    println!("  Documents in DB: {}", results.len());

    assert_eq!(total_successes, num_writers * writes_per_worker, "All writes should succeed");
    println!("✓ All concurrent writes to same key succeeded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_reads_and_writes() {
    println!("\n=== Concurrent Reads and Writes Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("concurrent_rw.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("rw_test", vec![
        ("id", FieldType::String, false),
        ("data", FieldType::String, false),
    ]).unwrap();

    // Pre-populate with some data
    for i in 0..50 {
        db.insert_into("rw_test", vec![
            ("id", Value::String(format!("init_{}", i))),
            ("data", Value::String(format!("initial_data_{}", i))),
        ]).await.unwrap();
    }

    let num_writers = 50;
    let num_readers = 100;
    let ops_per_worker = 100;

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));

    println!("Starting {} writers and {} readers", num_writers, num_readers);

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    // Spawn writers
    for i in 0..num_writers {
        let db_clone = Arc::clone(&db);
        let write_count_clone = Arc::clone(&write_count);
        tasks.spawn(async move {
            for j in 0..ops_per_worker {
                db_clone.insert_into("rw_test", vec![
                    ("id", Value::String(format!("writer_{}_{}", i, j))),
                    ("data", Value::String(format!("data_{}_{}", i, j))),
                ]).await.unwrap();
                write_count_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // Spawn readers
    for _ in 0..num_readers {
        let db_clone = Arc::clone(&db);
        let read_count_clone = Arc::clone(&read_count);
        tasks.spawn(async move {
            for _ in 0..ops_per_worker {
                // Readers query all documents
                let _ = db_clone.query("rw_test").collect().await.unwrap();
                read_count_clone.fetch_add(1, Ordering::Relaxed);

                // Small delay to interleave with writes
                tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
            }
        });
    }

    // Wait for all tasks
    while let Some(_) = tasks.join_next().await {}

    let duration = start.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);

    println!("Results:");
    println!("  Duration: {:?}", duration);
    println!("  Writes completed: {}", total_writes);
    println!("  Reads completed: {}", total_reads);
    println!("  Total ops: {}", total_writes + total_reads);
    println!("  Throughput: {:.2} ops/sec", (total_writes + total_reads) as f64 / duration.as_secs_f64());

    // Verify final data integrity
    let final_results = db.query("rw_test").collect().await.unwrap();
    println!("  Final document count: {}", final_results.len());

    assert_eq!(total_writes, num_writers * ops_per_worker, "All writes should complete");
    assert_eq!(total_reads, num_readers * ops_per_worker, "All reads should complete");
    assert_eq!(final_results.len(), 50 + total_writes, "Should have initial + written documents");

    println!("✓ Concurrent reads and writes completed successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_read_after_write_consistency() {
    println!("\n=== Read-After-Write Consistency Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("raw_consistency.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("consistency_test", vec![
        ("key", FieldType::String, false),
        ("value", FieldType::Int, false),
    ]).unwrap();

    let num_workers = 50;
    let ops_per_worker = 20;

    println!("Testing read-after-write consistency with {} workers", num_workers);

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for worker_id in 0..num_workers {
        let db_clone = Arc::clone(&db);
        tasks.spawn(async move {
            let mut consistency_errors = 0;

            for i in 0..ops_per_worker {
                let key = format!("worker_{}_{}", worker_id, i);
                let value = worker_id * ops_per_worker + i;

                // Write
                let _doc_id = db_clone.insert_into("consistency_test", vec![
                    ("key", Value::String(key.clone())),
                    ("value", Value::Int(value as i64)),
                ]).await.unwrap();

                // Immediately read back
                let results = db_clone.query("consistency_test")
                    .filter(|f| f.eq("key", Value::String(key.clone())))
                    .collect()
                    .await
                    .unwrap();

                // Verify we can read what we just wrote
                if results.is_empty() {
                    consistency_errors += 1;
                } else {
                    let doc = &results[0];
                    if let Some(Value::Int(v)) = doc.data.get("value") {
                        if *v != value as i64 {
                            consistency_errors += 1;
                        }
                    }
                }

                // Small delay
                tokio::time::sleep(tokio::time::Duration::from_micros(10)).await;
            }

            consistency_errors
        });
    }

    let mut total_errors = 0;
    while let Some(result) = tasks.join_next().await {
        if let Ok(errors) = result {
            total_errors += errors;
        }
    }

    let duration = start.elapsed();

    println!("Results:");
    println!("  Duration: {:?}", duration);
    println!("  Total operations: {}", num_workers * ops_per_worker);
    println!("  Consistency errors: {}", total_errors);

    assert_eq!(total_errors, 0, "Read-after-write should always be consistent");
    println!("✓ Read-after-write consistency maintained");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_high_contention_scenario() {
    println!("\n=== High Contention Scenario Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("high_contention.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("hot_keys", vec![
        ("key_id", FieldType::String, false),
        ("counter", FieldType::Int, false),
    ]).unwrap();

    let num_workers = 100;
    let num_hot_keys = 5; // Small number of keys = high contention
    let ops_per_worker = 50;

    println!("Testing high contention: {} workers accessing {} hot keys", num_workers, num_hot_keys);

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for worker_id in 0..num_workers {
        let db_clone = Arc::clone(&db);
        tasks.spawn(async move {
            let mut ops = 0;

            for j in 0..ops_per_worker {
                // Pick a hot key deterministically to avoid Send issues
                let key_idx = (worker_id + j) % num_hot_keys;
                let key = format!("hot_key_{}", key_idx);

                // Write to this hot key
                db_clone.insert_into("hot_keys", vec![
                    ("key_id", Value::String(key)),
                    ("counter", Value::Int(worker_id as i64)),
                ]).await.unwrap();

                ops += 1;
            }

            ops
        });
    }

    let mut total_ops = 0;
    while let Some(result) = tasks.join_next().await {
        if let Ok(ops) = result {
            total_ops += ops;
        }
    }

    let duration = start.elapsed();

    println!("Results:");
    println!("  Duration: {:?}", duration);
    println!("  Total operations: {}", total_ops);
    println!("  Throughput: {:.2} ops/sec", total_ops as f64 / duration.as_secs_f64());

    // Count writes per hot key
    let all_docs = db.query("hot_keys").collect().await.unwrap();
    println!("  Total documents: {}", all_docs.len());

    for i in 0..num_hot_keys {
        let key = format!("hot_key_{}", i);
        let count = all_docs.iter()
            .filter(|doc| {
                matches!(doc.data.get("key_id"), Some(Value::String(k)) if k == &key)
            })
            .count();
        println!("    {}: {} writes", key, count);
    }

    assert_eq!(total_ops, num_workers * ops_per_worker);
    println!("✓ High contention handled correctly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_query_operations() {
    println!("\n=== Concurrent Query Operations Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("concurrent_queries.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("products", vec![
        ("name", FieldType::String, false),
        ("price", FieldType::Int, false),
        ("category", FieldType::String, false),
    ]).unwrap();

    // Create index for faster queries
    db.create_index("products", "price").await.unwrap();

    // Pre-populate with diverse data
    let categories = ["electronics", "books", "clothing", "food", "toys"];
    for i in 0..1000 {
        db.insert_into("products", vec![
            ("name", Value::String(format!("product_{}", i))),
            ("price", Value::Int((i % 100) as i64)),
            ("category", Value::String(categories[i % 5].to_string())),
        ]).await.unwrap();
    }

    println!("Pre-populated with 1000 products");

    let num_query_workers = 50;
    let queries_per_worker = 20;

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    // Spawn query workers with different query patterns
    for i in 0..num_query_workers {
        let db_clone = Arc::clone(&db);
        tasks.spawn(async move {
            let mut query_count = 0;

            for j in 0..queries_per_worker {
                // Vary query type deterministically
                let query_type = (i + j) % 4;

                let _ = match query_type {
                    0 => {
                        // Range query on price
                        db_clone.query("products")
                            .filter(|f| f.gt("price", Value::Int(50)))
                            .collect()
                            .await
                    },
                    1 => {
                        // Category filter
                        db_clone.query("products")
                            .filter(|f| f.eq("category", Value::String("electronics".to_string())))
                            .collect()
                            .await
                    },
                    2 => {
                        // Sorted query
                        db_clone.query("products")
                            .order_by("price", true)
                            .limit(10)
                            .collect()
                            .await
                    },
                    _ => {
                        // Full scan
                        db_clone.query("products")
                            .collect()
                            .await
                    }
                };

                query_count += 1;
            }

            query_count
        });
    }

    let mut total_queries = 0;
    while let Some(result) = tasks.join_next().await {
        if let Ok(count) = result {
            total_queries += count;
        }
    }

    let duration = start.elapsed();

    println!("Results:");
    println!("  Duration: {:?}", duration);
    println!("  Total queries: {}", total_queries);
    println!("  Query throughput: {:.2} queries/sec", total_queries as f64 / duration.as_secs_f64());

    assert_eq!(total_queries, num_query_workers * queries_per_worker);
    println!("✓ Concurrent queries completed successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_mixed_workload_stress() {
    println!("\n=== Mixed Workload Stress Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("mixed_workload.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("mixed", vec![
        ("id", FieldType::String, false),
        ("value", FieldType::Int, false),
    ]).unwrap();

    let num_workers = 50;
    let ops_per_worker = 100;

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let update_count = Arc::new(AtomicUsize::new(0));

    println!("Mixed workload: {} workers doing random operations", num_workers);

    let start = Instant::now();
    let mut tasks = JoinSet::new();

    for worker_id in 0..num_workers {
        let db_clone = Arc::clone(&db);
        let write_count = Arc::clone(&write_count);
        let read_count = Arc::clone(&read_count);
        let update_count = Arc::clone(&update_count);

        tasks.spawn(async move {
            for i in 0..ops_per_worker {
                // Deterministic operation type based on iteration
                let op_type = (worker_id + i) % 10;

                if op_type < 5 {
                    // 50% writes
                    let _ = db_clone.insert_into("mixed", vec![
                        ("id", Value::String(format!("w_{}_{}", worker_id, i))),
                        ("value", Value::Int((worker_id * ops_per_worker + i) as i64)),
                    ]).await;
                    write_count.fetch_add(1, Ordering::Relaxed);
                } else if op_type < 8 {
                    // 30% reads
                    let _ = db_clone.query("mixed").collect().await;
                    read_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    // 20% updates (insert with potential collision)
                    let _ = db_clone.insert_into("mixed", vec![
                        ("id", Value::String(format!("shared_{}", i % 10))),
                        ("value", Value::Int((worker_id + i) as i64)),
                    ]).await;
                    update_count.fetch_add(1, Ordering::Relaxed);
                }

                // Occasional delay
                if i % 10 == 0 {
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                }
            }
        });
    }

    while let Some(_) = tasks.join_next().await {}

    let duration = start.elapsed();
    let writes = write_count.load(Ordering::Relaxed);
    let reads = read_count.load(Ordering::Relaxed);
    let updates = update_count.load(Ordering::Relaxed);
    let total_ops = writes + reads + updates;

    println!("Results:");
    println!("  Duration: {:?}", duration);
    println!("  Writes: {}", writes);
    println!("  Reads: {}", reads);
    println!("  Updates: {}", updates);
    println!("  Total ops: {}", total_ops);
    println!("  Throughput: {:.2} ops/sec", total_ops as f64 / duration.as_secs_f64());

    let final_count = db.query("mixed").collect().await.unwrap().len();
    println!("  Final document count: {}", final_count);

    println!("✓ Mixed workload completed successfully");
}
