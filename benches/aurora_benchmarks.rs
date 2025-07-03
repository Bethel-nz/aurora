use aurora_db::{Aurora, AuroraError, Result};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
// use std::path::Path;
use aurora_db::{FieldType, Value};
use std::time::Duration;
use tokio::runtime::Runtime;
use uuid::Uuid;

// const VIDEO_PATH: &str = "/home/kylo_ren/Videos/Complete Guide to Full Stack Solana Development (2024) [vUHF1X48zM4].webm";
const BULK_SIZE: usize = 100;

fn setup() -> Result<Aurora> {
    let db = Aurora::open("bench.db")?;
    Ok(db)
}

fn bench_basic_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("basic_operations");
    group.measurement_time(Duration::from_secs(10));

    // Single operations
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

    // Bulk operations
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
    group.measurement_time(Duration::from_secs(10));

    // Setup collection
    db.new_collection(
        "users",
        vec![
            ("name".to_string(), FieldType::String, false),
            ("id".to_string(), FieldType::Uuid, true),
            ("age".to_string(), FieldType::Int, false),
        ],
    )
    .unwrap();

    group.bench_function("single_insert", |b| {
        b.iter(|| {
            db.insert_into(
                "users",
                vec![
                    ("name", Value::String("John Doe".to_string())),
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("age", Value::Int(30)),
                ],
            )
            .unwrap()
        })
    });

    group.bench_function("bulk_insert_100", |b| {
        b.iter(|| {
            for _ in 0..BULK_SIZE {
                db.insert_into(
                    "users",
                    vec![
                        ("name", Value::String("John Doe".to_string())),
                        ("id", Value::Uuid(Uuid::new_v4())),
                        ("age", Value::Int(30)),
                    ],
                )
                .unwrap();
            }
        })
    });

    group.bench_function("query_all", |b| b.iter(|| db.get_all_collection("users")));

    group.finish();
}

fn bench_blob_operations(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("blob_operations");
    group.measurement_time(Duration::from_secs(30));

    // Different sized blobs
    let small_blob = vec![0u8; 1024]; // 1KB
    let medium_blob = vec![0u8; 1024 * 1024]; // 1MB
    let large_blob = vec![0u8; 10 * 1024 * 1024]; // 10MB
    let huge_blob = vec![0u8; 51 * 1024 * 1024]; // 51MB - should trigger error

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

    // This should fail with a size error
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
    group.measurement_time(Duration::from_secs(10));

    // Create a runtime for async setup
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Setup collection with indexes
        db.new_collection(
            "products",
            vec![
                ("id".to_string(), FieldType::Uuid, true),
                ("name".to_string(), FieldType::String, false),
                ("price".to_string(), FieldType::Float, false),
                ("category".to_string(), FieldType::String, false),
                ("in_stock".to_string(), FieldType::Boolean, false),
            ],
        )
        .unwrap();

        // Create indexes using await
        db.create_index("products", "price").await.unwrap();
        db.create_index("products", "category").await.unwrap();
        db.create_index("products", "name").await.unwrap();
        db.create_index("products", "id").await.unwrap();

        // Insert test data
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
            .unwrap()
        })
    });

    group.finish();
}

fn bench_complex_queries(c: &mut Criterion) {
    let db = setup().unwrap();
    let mut group = c.benchmark_group("complex_queries");
    group.measurement_time(Duration::from_secs(10));

    // Create a runtime for async setup
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Setup collection
        db.new_collection(
            "orders",
            vec![
                ("id".to_string(), FieldType::String, true),
                ("customer_id".to_string(), FieldType::String, false),
                ("total".to_string(), FieldType::Float, false),
                ("date".to_string(), FieldType::String, false),
                ("status".to_string(), FieldType::String, false),
                ("items".to_string(), FieldType::Int, false),
            ],
        )
        .unwrap();

        // Create indexes with the simple format
        db.create_index("orders", "customer_id").await.unwrap();
        db.create_index("orders", "date").await.unwrap();
        db.create_index("orders", "status").await.unwrap();
        db.create_index("orders", "total").await.unwrap();

        // Insert test data
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
                            && f.gt("items", Value::Int(3))
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

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(5));
    targets = bench_basic_operations, bench_collection_operations, bench_blob_operations,
             bench_index_operations, bench_complex_queries
);
criterion_main!(benches);
