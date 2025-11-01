use aurora_db::{Aurora, AuroraError, Document, Result};
use aurora_db::{FieldType, Value};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use uuid::Uuid;

const BULK_SIZE: usize = 100;

fn setup() -> Result<Aurora> {
    let db = Aurora::open("bench.db")?;
    Ok(db)
}

fn bench_basic_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("basic_operations");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("single_put", |b| {
        b.iter(|| {
            db.put(
                black_box("test_key".to_string()),
                black_box(b"test_value".to_vec()),
                None,
            )
            .unwrap()
        })
    });

    group.bench_function("single_get", |b| {
        b.iter(|| db.get(black_box("test_key")).unwrap())
    });

    group.bench_function("bulk_put_100", |b| {
        b.iter(|| {
            for i in 0..BULK_SIZE {
                db.put(
                    format!("bulk_key_{}", i),
                    format!("bulk_value_{}", i).into_bytes(),
                    None,
                )
                .unwrap();
            }
        })
    });

    group.bench_function("bulk_get_100", |b| {
        b.iter(|| {
            for i in 0..BULK_SIZE {
                db.get(&format!("bulk_key_{}", i)).unwrap();
            }
        })
    });

    group.bench_function("bulk_put_get_mixed_100", |b| {
        b.iter(|| {
            for i in 0..BULK_SIZE {
                let key = format!("mixed_key_{}", i);
                db.put(key.clone(), format!("mixed_value_{}", i).into_bytes(), None)
                    .unwrap();
                db.get(&key).unwrap();
            }
        })
    });

    group.finish();
}

fn bench_collection_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("collection_operations");
    group.measurement_time(Duration::from_secs(5));

    db.new_collection(
        "users",
        vec![
            ("name", FieldType::String, false),
            ("id", FieldType::Uuid, true),
            ("age", FieldType::Int, false),
        ],
    )
    .unwrap();

    group.bench_function("single_insert", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.insert_into(
                    "users",
                    vec![
                        ("name", Value::String("John Doe".to_string())),
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("age", Value::Int(30 as i64)),
                    ],
                )
                .await
                .unwrap()
            })
        })
    });

    group.bench_function("bulk_insert_100", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                for _ in 0..BULK_SIZE {
                    db.insert_into(
                        "users",
                        vec![
                            ("name", Value::String("John Doe".to_string())),
                            ("id", Value::Uuid(Uuid::new_v4())),
                            ("age", Value::Int(30 as i64)),
                        ],
                    )
                    .await
                    .unwrap();
                }
            })
        })
    });

    group.bench_function("query_all", |b| b.iter(|| db.get_all_collection("users")));

    group.finish();
}

fn bench_blob_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("blob_operations");
    group.measurement_time(Duration::from_secs(5));

    let small_blob = vec![0u8; 1024];
    let medium_blob = vec![0u8; 1024 * 1024];
    let large_blob = vec![0u8; 10 * 1024 * 1024];
    let huge_blob = vec![0u8; 51 * 1024 * 1024];

    group.bench_function("bulk_store_small_blobs_100", |b| {
        b.iter(|| {
            for _ in 0..BULK_SIZE {
                db.put(
                    format!("small_blob:{}", Uuid::new_v4()),
                    small_blob.clone(),
                    None,
                )
                .unwrap();
            }
        })
    });

    group.bench_function("bulk_store_medium_blobs_10", |b| {
        b.iter(|| {
            for _ in 0..5 {
                db.put(
                    format!("medium_blob:{}", Uuid::new_v4()),
                    medium_blob.clone(),
                    None,
                )
                .unwrap();
            }
        })
    });

    group.bench_function("store_large_blob", |b| {
        b.iter(|| {
            db.put(
                format!("large_blob:{}", Uuid::new_v4()),
                large_blob.clone(),
                None,
            )
            .unwrap()
        })
    });

    group.bench_function("store_huge_blob_should_fail", |b| {
        b.iter(|| {
            let result = db.put(
                format!("huge_blob:{}", Uuid::new_v4()),
                huge_blob.clone(),
                None,
            );
            assert!(result.is_err(), "Expected error for blob > 50MB");
            assert!(matches!(
                result.unwrap_err(),
                AuroraError::InvalidOperation(_)
            ));
        })
    });

    group.finish();
}

fn bench_index_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("index_operations");
    group.measurement_time(Duration::from_secs(5));

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        db.new_collection(
            "products",
            vec![
                ("id", FieldType::Uuid, true),
                ("name", FieldType::String, false),
                ("price", FieldType::Float, false),
                ("category", FieldType::String, false),
                ("in_stock", FieldType::Bool, false),
            ],
        )
        .unwrap();

        db.create_index("products", "price").await.unwrap();
        db.create_index("products", "category").await.unwrap();
        db.create_index("products", "name").await.unwrap();
        db.create_index("products", "id").await.unwrap();

        let categories = ["Electronics", "Clothing", "Food", "Books", "Home"];
        for i in 0..500 {
            db.insert_into(
                "products",
                vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("name", Value::String(format!("Product {}", i))),
                    ("price", Value::Float((i as f64) * 10.5)),
                    ("category", Value::String(categories[i % 5].to_string())),
                    ("in_stock", Value::Bool(i % 3 == 0)),
                ],
            )
            .await
            .unwrap();
        }
    });

    group.bench_function("query_btree_index", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("products")
                    .filter(|f| {
                        f.gt("price", Value::Float(1000.0)) && f.lt("price", Value::Float(2000.0))
                    })
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("query_hash_index", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("products")
                    .filter(|f| f.eq("category", Value::String("Electronics".to_string())))
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("query_composite_index", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("products")
                    .filter(|f| {
                        f.eq("category", Value::String("Books".to_string()))
                            && f.lt("price", Value::Float(500.0))
                    })
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("insert_with_indexes", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.insert_into(
                    "products",
                    vec![
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("name", Value::String("Test Product".to_string())),
                        ("price", Value::Float(999.99)),
                        ("category", Value::String("Test".to_string())),
                        ("in_stock", Value::Bool(true)),
                    ],
                )
                .await
                .unwrap()
            })
        })
    });

    group.finish();
}

fn bench_complex_queries(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("complex_queries");
    group.measurement_time(Duration::from_secs(5));

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        db.new_collection(
            "orders",
            vec![
                ("id", FieldType::String, true),
                ("customer_id", FieldType::String, false),
                ("total", FieldType::Float, false),
                ("date", FieldType::String, false),
                ("status", FieldType::String, false),
                ("items", FieldType::Int, false),
            ],
        )
        .unwrap();

        db.create_index("orders", "customer_id").await.unwrap();
        db.create_index("orders", "date").await.unwrap();
        db.create_index("orders", "status").await.unwrap();
        db.create_index("orders", "total").await.unwrap();

        let statuses = ["pending", "processing", "shipped", "delivered", "cancelled"];
        let dates = [
            "2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06",
        ];

        for i in 0..1000 {
            db.insert_into(
                "orders",
                vec![
                    ("id", Value::String(format!("ord-{}", i))),
                    ("customer_id", Value::String(format!("cust-{}", i % 100))),
                    ("total", Value::Float((i as f64) * 5.75 + 10.0)),
                    (
                        "date",
                        Value::String(format!("{}-{:02}", dates[i % 6], (i % 28) + 1)),
                    ),
                    ("status", Value::String(statuses[i % 5].to_string())),
                    ("items", Value::Int(((i % 10) + 1) as i64)),
                ],
            )
            .await
            .unwrap();
        }
    });

    group.bench_function("query_with_multiple_filters", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("orders")
                    .filter(|f| {
                        f.eq("status", Value::String("shipped".to_string()))
                            && f.gt("total", Value::Float(100.0))
                            && f.gt("items", Value::Int(3 as i64))
                    })
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("query_with_sorting", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("orders")
                    .filter(|f| f.eq("customer_id", Value::String("cust-7".to_string())))
                    .order_by("date", true)
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("query_with_limit_offset", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("orders")
                    .filter(|f| f.gt("total", Value::Float(500.0)))
                    .order_by("total", false)
                    .limit(20)
                    .offset(10)
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("query_date_range", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.query("orders")
                    .filter(|f| {
                        f.gt("date", Value::String("2023-03-01".to_string()))
                            && f.lt("date", Value::String("2023-04-01".to_string()))
                    })
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    group.finish();
}

fn bench_cache_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("cache_operations");
    group.measurement_time(Duration::from_secs(5));

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        db.new_collection(
            "cache_test",
            vec![
                ("id", FieldType::String, true),
                ("data", FieldType::String, false),
            ],
        )
        .unwrap();

        for i in 0..1000 {
            db.insert_into(
                "cache_test",
                vec![
                    ("id", Value::String(format!("doc-{}", i))),
                    ("data", Value::String(format!("data-{}", i))),
                ],
            )
            .await
            .unwrap();
        }
    });

    group.bench_function("prewarm_cache_100", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.clear_hot_cache();
                db.prewarm_cache("cache_test", Some(100)).await.unwrap()
            })
        })
    });

    group.bench_function("prewarm_cache_1000", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                db.clear_hot_cache();
                db.prewarm_cache("cache_test", Some(1000)).await.unwrap()
            })
        })
    });

    group.bench_function("cold_read_no_cache", |b| {
        b.iter(|| {
            db.clear_hot_cache();
            db.get("cache_test:doc-500").unwrap()
        })
    });

    group.bench_function("hot_read_from_cache", |b| {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            db.prewarm_cache("cache_test", Some(1000)).await.unwrap();
        });

        b.iter(|| db.get("cache_test:doc-500").unwrap())
    });

    group.finish();
}

fn bench_pubsub_operations(c: &mut Criterion) {
    let db = Arc::new(setup().unwrap());
    let mut group = c.benchmark_group("pubsub_operations");
    group.measurement_time(Duration::from_secs(5));

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        db.new_collection(
            "events",
            vec![
                ("id", FieldType::String, true),
                ("type", FieldType::String, false),
                ("data", FieldType::String, false),
            ],
        )
        .unwrap();
    });

    group.bench_function("pubsub_single_listener", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let _listener = db.listen("events");

                db.insert_into(
                    "events",
                    vec![
                        ("id", Value::String(Uuid::new_v4().to_string())),
                        ("type", Value::String("test".to_string())),
                        ("data", Value::String("benchmark".to_string())),
                    ],
                )
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("pubsub_multiple_listeners", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let _listener1 = db.listen("events");
                let _listener2 = db.listen("events");
                let _listener3 = db.listen("events");

                db.insert_into(
                    "events",
                    vec![
                        ("id", Value::String(Uuid::new_v4().to_string())),
                        ("type", Value::String("test".to_string())),
                        ("data", Value::String("benchmark".to_string())),
                    ],
                )
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_workers(c: &mut Criterion) {
    use aurora_db::workers::{Job, JobPriority, WorkerConfig, WorkerSystem};

    let mut group = c.benchmark_group("workers");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("job_creation", |b| {
        b.iter(|| {
            let _job = Job::new("test")
                .add_field("data", serde_json::json!("test data"))
                .with_priority(JobPriority::Normal)
                .with_max_retries(3);
        })
    });

    group.bench_function("job_enqueue_100", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let config = WorkerConfig {
                    storage_path: format!("./bench_workers_{}", Uuid::new_v4()),
                    concurrency: 4,
                    poll_interval_ms: 100,
                    cleanup_interval_seconds: 3600,
                };

                let worker_system = WorkerSystem::new(config).unwrap();

                for _ in 0..100 {
                    let job = Job::new("benchmark").add_field("iteration", serde_json::json!(1));
                    worker_system.enqueue(job).await.unwrap();
                }
            })
        })
    });

    group.finish();
}

fn bench_computed_fields(c: &mut Criterion) {
    use aurora_db::computed::{ComputedExpression, ComputedFields};
    use aurora_db::types::Document;

    let mut group = c.benchmark_group("computed_fields");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("concat_expression", |b| {
        let expr =
            ComputedExpression::Concat(vec!["first_name".to_string(), "last_name".to_string()]);

        let mut doc = Document::new();
        doc.data
            .insert("first_name".to_string(), Value::String("John".to_string()));
        doc.data
            .insert("last_name".to_string(), Value::String("Doe".to_string()));

        b.iter(|| expr.evaluate(&doc))
    });

    group.bench_function("sum_expression", |b| {
        let expr = ComputedExpression::Sum(vec!["a".to_string(), "b".to_string(), "c".to_string()]);

        let mut doc = Document::new();
        doc.data.insert("a".to_string(), Value::Int(10 as i64));
        doc.data.insert("b".to_string(), Value::Int(20 as i64));
        doc.data.insert("c".to_string(), Value::Int(30 as i64));

        b.iter(|| expr.evaluate(&doc))
    });

    group.bench_function("computed_fields_apply", |b| {
        let mut registry = ComputedFields::new();
        registry.register(
            "users",
            "full_name",
            ComputedExpression::Concat(vec!["first_name".to_string(), "last_name".to_string()]),
        );
        registry.register(
            "users",
            "total_score",
            ComputedExpression::Sum(vec!["score1".to_string(), "score2".to_string()]),
        );

        let mut doc = Document::new();
        doc.data
            .insert("first_name".to_string(), Value::String("Jane".to_string()));
        doc.data
            .insert("last_name".to_string(), Value::String("Smith".to_string()));
        doc.data.insert("score1".to_string(), Value::Int(85 as i64));
        doc.data.insert("score2".to_string(), Value::Int(92 as i64));

        b.iter(|| {
            let mut test_doc = doc.clone();
            registry.apply("users", &mut test_doc).unwrap()
        })
    });

    group.finish();
}

fn bench_write_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_performance");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("bulk_write_100", |b| {
        let db = setup().unwrap();

        db.new_collection(
            "write_test",
            vec![
                ("id", FieldType::String, true),
                ("data", FieldType::String, false),
            ],
        )
        .unwrap();

        b.iter(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                for i in 0..100 {
                    db.insert_into(
                        "write_test",
                        vec![
                            ("id", Value::String(format!("doc-{}", Uuid::new_v4()))),
                            ("data", Value::String(format!("data-{}", i))),
                        ],
                    )
                    .await
                    .unwrap();
                }
            })
        })
    });

    group.bench_function("batch_write_100", |b| {
        let db = setup().unwrap();
        db.new_collection(
            "batch_coll",
            vec![
                ("id", FieldType::String, true),
                ("data", FieldType::String, false),
            ],
        )
        .unwrap();

        b.iter(|| {
            let pairs: Vec<(String, Vec<u8>)> = (0..100)
                .map(|i| {
                    let doc_id = Uuid::new_v4().to_string();
                    let doc = Document {
                        id: doc_id.clone(),
                        data: [
                            ("id".to_string(), Value::String(doc_id.clone())),
                            ("data".to_string(), Value::String(format!("data-{}", i))),
                        ]
                        .iter()
                        .cloned()
                        .collect(),
                    };
                    let key = format!("batch_coll:{}", doc.id);
                    let value = serde_json::to_vec(&doc).unwrap();
                    (key, value)
                })
                .collect();

            db.batch_write(pairs).unwrap()
        })
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500));
    targets = bench_basic_operations, bench_collection_operations, bench_blob_operations,
             bench_index_operations, bench_complex_queries, bench_cache_operations,
             bench_pubsub_operations, bench_workers, bench_computed_fields,
             bench_write_performance
);
criterion_main!(benches);
