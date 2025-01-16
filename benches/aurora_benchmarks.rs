use criterion::{black_box, criterion_group, criterion_main, Criterion};
use aurora::{error::AuroraError, Aurora, Result};
// use std::path::Path;
use uuid::Uuid;
use aurora::types::{FieldType, Value};
use std::time::Duration;

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
                None
            ).unwrap()
        })
    });

    group.bench_function("single_get", |b| {
        b.iter(|| {
            db.get(black_box("test_key")).unwrap()
        })
    });

    // Bulk operations
    group.bench_function("bulk_put_100", |b| {
        b.iter(|| {
            for i in 0..BULK_SIZE {
                db.put(
                    format!("bulk_key_{}", i),
                    format!("bulk_value_{}", i).into_bytes(),
                    None
                ).unwrap();
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
                db.put(key.clone(), format!("mixed_value_{}", i).into_bytes(), None).unwrap();
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
    db.new_collection("users", vec![
        ("name", FieldType::String, false),
        ("id", FieldType::Uuid, true),
        ("age", FieldType::Int, false),
    ]).unwrap();

    group.bench_function("single_insert", |b| {
        b.iter(|| {
            db.insert_into("users", vec![
                ("name", Value::String("John Doe".to_string())),
                ("id", Value::Uuid(Uuid::new_v4())),
                ("age", Value::Int(30)),
            ]).unwrap()
        })
    });

    group.bench_function("bulk_insert_100", |b| {
        b.iter(|| {
            for _ in 0..BULK_SIZE {
                db.insert_into("users", vec![
                    ("name", Value::String("John Doe".to_string())),
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("age", Value::Int(30)),
                ]).unwrap();
            }
        })
    });

    group.bench_function("query_all", |b| {
        b.iter(|| {
            db.get_all_collection("users")
        })
    });

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
                    None
                ).unwrap();
            }
        })
    });

    group.bench_function("bulk_store_medium_blobs_10", |b| {
        b.iter(|| {
            for _ in 0..5 {
                db.put(
                    format!("medium_blob:{}", Uuid::new_v4()),
                    medium_blob.clone(),
                    None
                ).unwrap();
            }
        })
    });

    group.bench_function("store_large_blob", |b| {
        b.iter(|| {
            db.put(
                format!("large_blob:{}", Uuid::new_v4()),
                large_blob.clone(),
                None
            ).unwrap()
        })
    });

    // This should fail with a size error
    group.bench_function("store_huge_blob_should_fail", |b| {
        b.iter(|| {
            let result = db.put(
                format!("huge_blob:{}", Uuid::new_v4()),
                huge_blob.clone(),
                None
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

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(5));
    targets = bench_basic_operations, bench_collection_operations, bench_blob_operations
);
criterion_main!(benches); 
