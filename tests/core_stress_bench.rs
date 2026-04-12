use aurora_db::workers::{Job, WorkerConfig, WorkerSystem};
use aurora_db::{Aurora, FieldType, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_needle_in_haystack_scaling() {
    println!("\n=== BENCHMARK: Needle in a Haystack (Lookup Scaling) ===");
    let temp_dir = TempDir::new().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    let collection = "scaling_test";
    let scales = vec![10_000, 100_000, 1_000_000];

    for scale in scales {
        println!("\nScale: {} documents", format_number(scale));

        // RE-CREATE COLLECTION FOR EACH SCALE (Fixes Unique Constraint Error)
        let _ = db.delete_collection(collection).await;
        db.new_collection(
            collection,
            vec![
                (
                    "email",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: true,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // Indexed
                (
                    "status",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // Unindexed
            ],
        )
        .await
        .unwrap();

        // 1. Ingestion
        let start = Instant::now();
        let mut docs = Vec::with_capacity(10_000);
        for i in 0..scale {
            let mut data = std::collections::HashMap::new();
            data.insert(
                "email".to_string(),
                Value::String(format!("user{}@example.com", i)),
            );
            data.insert(
                "status".to_string(),
                Value::String(if i % 2 == 0 { "active" } else { "inactive" }.to_string()),
            );
            docs.push(data);

            if docs.len() >= 10_000 {
                db.batch_insert(collection, std::mem::take(&mut docs))
                    .await
                    .unwrap();
            }
        }
        if !docs.is_empty() {
            db.batch_insert(collection, docs).await.unwrap();
        }
        println!("  Ingestion time: {:?}", start.elapsed());

        // Target: the last document
        let target_email = format!("user{}@example.com", scale - 1);

        // 2. Indexed Lookup (O(1))
        let start = Instant::now();
        let results = db
            .query(collection)
            .filter(|f: &aurora_db::query::FilterBuilder| f.eq("email", target_email.clone()))
            .collect()
            .await
            .unwrap();
        let indexed_time = start.elapsed();
        assert_eq!(results.len(), 1);
        println!("  Indexed Lookup:  {:?}", indexed_time);

        // 3. Full Table Scan (O(N))
        let target_status = if (scale - 1) % 2 == 0 {
            "active"
        } else {
            "inactive"
        };
        let search_pattern = format!("user{}", scale - 1);
        let start = Instant::now();
        let results = db
            .query(collection)
            .filter(|f: &aurora_db::query::FilterBuilder| {
                f.eq("status", target_status) & f.contains("email", search_pattern.as_str())
            })
            .collect()
            .await
            .unwrap();
        let scan_time = start.elapsed();
        assert!(results.len() >= 1);
        println!("  Full Table Scan: {:?}", scan_time);

        // Wait for all background WAL operations to complete before finishing the test or starting the next scale
        db.sync().await.unwrap();
    }
}

#[tokio::test]
async fn test_worker_queue_throughput() {
    println!("\n=== BENCHMARK: Worker Queue Throughput ===");
    let temp_dir = TempDir::new().unwrap();
    let workers_path = temp_dir.path().join("workers");

    let config = WorkerConfig {
        storage_path: workers_path.to_str().unwrap().to_string(),
        concurrency: 4,
        poll_interval_ms: 10,
        cleanup_interval_seconds: 3600,
    };

    let workers = WorkerSystem::new(config).unwrap();
    let num_jobs = 10_000;

    let completed_count = Arc::new(tokio::sync::Mutex::new(0));
    let cc_clone = Arc::clone(&completed_count);

    workers
        .register_handler("test_job", move |_job| {
            let cc = Arc::clone(&cc_clone);
            async move {
                let mut count = cc.lock().await;
                *count += 1;
                Ok(())
            }
        })
        .await;

    println!("Enqueuing {} jobs...", num_jobs);
    let start = Instant::now();
    for _ in 0..num_jobs {
        workers.enqueue(Job::new("test_job")).await.unwrap();
    }
    let enqueue_time = start.elapsed();
    println!("  Enqueue time: {:?}", enqueue_time);
    println!(
        "  Throughput:   {:.0} jobs/sec",
        num_jobs as f64 / enqueue_time.as_secs_f64()
    );

    println!("Starting 4 workers to clear queue...");
    workers.start().await.unwrap();

    let start = Instant::now();
    loop {
        let count = *completed_count.lock().await;
        if count >= num_jobs {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        if start.elapsed().as_secs() > 20 {
            panic!(
                "Timeout: Workers only cleared {}/{} jobs in 20s",
                count, num_jobs
            );
        }
    }
    let exec_time = start.elapsed();
    println!("  Execution time: {:?}", exec_time);
    println!(
        "  Throughput:     {:.0} jobs/sec",
        num_jobs as f64 / exec_time.as_secs_f64()
    );

    workers.stop().await.unwrap();
}

#[tokio::test]
async fn test_computed_fields_cpu_load() {
    println!("\n=== BENCHMARK: Computed Fields CPU Load ===");
    let temp_dir = TempDir::new().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    let collection = "cpu_test";
    db.new_collection(
        collection,
        vec![
            (
                "first_name",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "last_name",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "salary",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
        ],
    )
    .await
    .unwrap();

    let num_docs = 100_000;
    println!("Ingesting {} documents...", format_number(num_docs));
    let mut docs = Vec::new();
    for i in 0..num_docs {
        let mut data = std::collections::HashMap::new();
        data.insert("first_name".to_string(), Value::String("John".to_string()));
        data.insert("last_name".to_string(), Value::String(format!("Doe{}", i)));
        data.insert("salary".to_string(), Value::Int(50000 + i as i64));
        docs.push(data);

        if docs.len() >= 10_000 {
            db.batch_insert(collection, std::mem::take(&mut docs))
                .await
                .unwrap();
        }
    }
    if !docs.is_empty() {
        db.batch_insert(collection, docs).await.unwrap();
    }

    println!("Executing AQL with complex projections on 100k docs...");

    let starvation_check = Arc::new(tokio::sync::Mutex::new(0));
    let sc_clone = Arc::clone(&starvation_check);
    let barrier = Arc::new(Barrier::new(2));
    let b_clone = Arc::clone(&barrier);

    tokio::spawn(async move {
        b_clone.wait().await;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let mut count = sc_clone.lock().await;
            *count += 1;
        }
    });

    barrier.wait().await;
    let start = Instant::now();

    let aql = r#"
        query {
            cpu_test(limit: 100000) {
                first_name
                last_name
                salary
            }
        }
    "#;

    let result = db.execute(aql).await.unwrap();
    let duration = start.elapsed();

    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        assert_eq!(q.documents.len(), num_docs);
    }

    println!("  AQL Execution time: {:?}", duration);
    println!(
        "  Throughput:         {:.0} docs/sec",
        num_docs as f64 / duration.as_secs_f64()
    );

    let final_sc = *starvation_check.lock().await;
    println!("  Starvation check progress: {}/100", final_sc);
    // Note: Since we haven't fixed the executor yet, this might still fail if it blocks.
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;
    for c in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
        count += 1;
    }
    result.chars().rev().collect()
}
