use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

#[tokio::test]
async fn stress_test_concurrent_writes() {
    let test_cases = vec![
        ("1 request", 1),
        ("10 requests", 10),
        ("100 requests", 100),
        ("1000 requests", 1000),
        ("10000 requests", 10000),
        ("100000 requests", 100000),
        ("1000000 requests", 1000000),
    ];

    for (name, num_requests) in test_cases {
        println!("\n=== Running stress test: {} ===", name);

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("stress_test.aurora");

        let mut config = AuroraConfig::default();
        config.db_path = db_path;
        config.enable_write_buffering = true;
        config.enable_wal = true;
        config.checkpoint_interval_ms = 100;

        let db = Arc::new(Aurora::with_config(config).unwrap());

        // Create collection
        db.new_collection("stress_test", vec![
            ("name", FieldType::String, false),
            ("value", FieldType::Int, false),
        ]).unwrap();

        let start = Instant::now();
        let mut tasks = JoinSet::new();

        // Spawn concurrent insert tasks
        for i in 0..num_requests {
            let db_clone = Arc::clone(&db);
            tasks.spawn(async move {
                db_clone.insert_into("stress_test", vec![
                    ("name", Value::String(format!("item_{}", i))),
                    ("value", Value::Int(i as i64)),
                ]).await
            });
        }

        // Wait for all tasks to complete
        let mut success_count = 0;
        let mut error_count = 0;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(_)) => success_count += 1,
                Ok(Err(_)) => error_count += 1,
                Err(_) => error_count += 1,
            }
        }

        let duration = start.elapsed();

        println!("Results:");
        println!("  Total requests: {}", num_requests);
        println!("  Successful: {}", success_count);
        println!("  Failed: {}", error_count);
        println!("  Duration: {:.2?}", duration);
        println!("  Throughput: {:.2} req/sec", num_requests as f64 / duration.as_secs_f64());

        // Verify data integrity
        let query_start = Instant::now();
        let results = db.query("stress_test").collect().await.unwrap();
        let query_duration = query_start.elapsed();

        println!("  Query time: {:.2?}", query_duration);
        println!("  Documents in DB: {}", results.len());

        assert_eq!(success_count, num_requests, "All requests should succeed");
        assert_eq!(results.len(), num_requests, "All documents should be queryable");
    }
}

#[tokio::test]
async fn stress_test_concurrent_reads_writes() {
    println!("\n=== Concurrent Reads & Writes Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("rw_stress.aurora");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.enable_write_buffering = true;
    config.enable_wal = true;

    let db = Arc::new(Aurora::with_config(config).unwrap());

    db.new_collection("rw_test", vec![
        ("counter", FieldType::Int, false),
    ]).unwrap();

    // Pre-populate with some data
    for i in 0..100 {
        db.insert_into("rw_test", vec![
            ("counter", Value::Int(i)),
        ]).await.unwrap();
    }

    let start = Instant::now();
    let mut write_tasks = JoinSet::new();
    let mut read_tasks = JoinSet::new();

    // Spawn 1000 writers
    for i in 0..1000 {
        let db_clone = Arc::clone(&db);
        write_tasks.spawn(async move {
            db_clone.insert_into("rw_test", vec![
                ("counter", Value::Int(i + 1000)),
            ]).await
        });
    }

    // Spawn 1000 readers
    for _ in 0..1000 {
        let db_clone = Arc::clone(&db);
        read_tasks.spawn(async move {
            db_clone.query("rw_test").collect().await
        });
    }

    let mut write_success = 0;
    let mut read_success = 0;
    let mut errors = 0;

    while let Some(result) = write_tasks.join_next().await {
        match result {
            Ok(Ok(_)) => write_success += 1,
            Ok(Err(_)) => errors += 1,
            Err(_) => errors += 1,
        }
    }

    while let Some(result) = read_tasks.join_next().await {
        match result {
            Ok(Ok(_)) => read_success += 1,
            Ok(Err(_)) => errors += 1,
            Err(_) => errors += 1,
        }
    }

    let duration = start.elapsed();

    println!("Results:");
    println!("  Total operations: 2000 (1000 writes + 1000 reads)");
    println!("  Writes successful: {}", write_success);
    println!("  Reads successful: {}", read_success);
    println!("  Failed: {}", errors);
    println!("  Duration: {:.2?}", duration);
    println!("  Throughput: {:.2} ops/sec", 2000.0 / duration.as_secs_f64());

    assert_eq!(errors, 0, "No operations should fail");
    assert_eq!(write_success, 1000, "All writes should succeed");
    assert_eq!(read_success, 1000, "All reads should succeed");
}

#[tokio::test]
async fn stress_test_pubsub_notifications() {
    println!("\n=== PubSub Stress Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pubsub_stress.aurora");

    let db = Arc::new(Aurora::open(db_path.to_str().unwrap()).unwrap());

    db.new_collection("pubsub_test", vec![
        ("event", FieldType::String, false),
    ]).unwrap();

    // Create 10 listeners
    let mut listeners = vec![];
    for i in 0..10 {
        let mut listener = db.listen("pubsub_test");
        let listener_id = i;
        listeners.push(tokio::spawn(async move {
            let mut count = 0;

            // Listen for up to 2 seconds or 100 events
            let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(2));
            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    result = listener.recv() => {
                        if result.is_ok() {
                            count += 1;
                            if count >= 100 {
                                break;
                            }
                        }
                    }
                    _ = &mut timeout => {
                        break;
                    }
                }
            }

            (listener_id, count)
        }));
    }

    // Give listeners time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Insert 100 documents
    let start = Instant::now();
    for i in 0..100 {
        db.insert_into("pubsub_test", vec![
            ("event", Value::String(format!("event_{}", i))),
        ]).await.unwrap();
    }
    let insert_duration = start.elapsed();

    // Wait for listeners to finish
    let mut total_events_received = 0;
    for listener in listeners {
        let (id, count) = listener.await.unwrap();
        println!("  Listener {}: received {} events", id, count);
        total_events_received += count;
    }

    println!("Results:");
    println!("  Documents inserted: 100");
    println!("  Insert duration: {:.2?}", insert_duration);
    println!("  Total events received across all listeners: {}", total_events_received);
    println!("  Average events per listener: {:.1}", total_events_received as f64 / 10.0);

    // Each listener should have received most/all events
    assert!(total_events_received >= 900, "Should receive most events (got {})", total_events_received);
}
