use aurora_db::{Aurora, AuroraConfig, Result, Value};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use uuid::Uuid;

async fn setup_baseline(enable_wal: bool) -> Result<(Aurora, TempDir)> {
    let temp_dir = tempfile::tempdir().map_err(|e| {
        aurora_db::AqlError::invalid_operation(format!("Failed to create temp dir: {}", e))
    })?;
    let db_path = temp_dir.path().join("bench.db");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.hot_cache_size_mb = 256;
    config.enable_write_buffering = true;
    config.enable_wal = enable_wal;

    let db = Aurora::with_config(config).await?;
    Ok((db, temp_dir))
}

fn run_baseline_bench(c: &mut Criterion, enable_wal: bool, group_name: &str) {
    use rand::prelude::*;
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    let scales = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for &size in &scales {
        let (db, _temp_dir) = rt.block_on(setup_baseline(enable_wal)).unwrap();

        rt.block_on(async {
            let schema_aql = r#"
                schema {
                    define collection bench {
                        id: String @primary
                        data: String
                        index: Int @indexed
                        timestamp: String
                    }
                }
            "#;
            db.execute(schema_aql).await.unwrap();

            for i in 0..size {
                db.insert_into(
                    "bench",
                    vec![
                        ("id", Value::String(format!("doc_{:08}", i))),
                        ("data", Value::String(format!("Document {} content", i))),
                        ("index", Value::Int(i as i64)),
                        (
                            "timestamp",
                            Value::String("2024-01-01T00:00:00Z".to_string()),
                        ),
                    ],
                )
                .await
                .unwrap();
            }
        });

        // 1. Write (Native)
        group.bench_with_input(BenchmarkId::new("write_native", size), &size, |b, &_s| {
            b.iter(|| {
                rt.block_on(async {
                    let id = Uuid::new_v4().to_string();
                    db.insert_into(
                        "bench",
                        vec![
                            ("id", Value::String(id)),
                            ("data", Value::String("Benchmark Data".to_string())),
                            ("index", Value::Int(9999)),
                            (
                                "timestamp",
                                Value::String("2024-01-01T00:00:00Z".to_string()),
                            ),
                        ],
                    )
                    .await
                    .unwrap();
                })
            })
        });

        // 2. Write (AQL)
        group.bench_with_input(BenchmarkId::new("write_aql", size), &size, |b, &_s| {
            b.iter_batched(
                || {
                    let id = Uuid::new_v4();
                    format!(
                        r#"
                        mutation {{
                            insertInto(collection: "bench", data: {{
                                id: "{}",
                                data: "Benchmark Data",
                                index: 9999,
                                timestamp: "2024-01-01T00:00:00Z"
                            }}) {{
                                id
                            }}
                        }}
                    "#,
                        id
                    )
                },
                |query| {
                    rt.block_on(async {
                        db.execute(query.as_str()).await.unwrap();
                    })
                },
                BatchSize::SmallInput,
            )
        });

        // 3. Read Hot (Native)
        rt.block_on(async {
            db.prewarm_cache("bench", None).await.unwrap();
        });

        group.bench_with_input(BenchmarkId::new("read_hot_native", size), &size, |b, &s| {
            let mut rng = rand::thread_rng();
            b.iter(|| {
                let idx = rng.gen_range(0..s);
                let key = format!("bench:doc_{:08}", idx);
                let _ = db.get(&key).unwrap();
            })
        });

        // 4. Query (Native Builder)
        group.bench_with_input(BenchmarkId::new("query_native", size), &size, |b, &s| {
            b.iter(|| {
                rt.block_on(async {
                    db.query("bench")
                        .filter(|f: &aurora_db::query::FilterBuilder| {
                            f.gt("index", Value::Int((s / 2) as i64))
                        })
                        .limit(100)
                        .collect()
                        .await
                        .unwrap()
                })
            })
        });

        // 5. Query (AQL)
        let query_aql = format!(
            r#"
            query {{
                bench(filter: {{ index: {{ gt: {} }} }}, limit: 100) {{
                    id
                    data
                }}
            }}
        "#,
            (size / 2)
        );

        group.bench_with_input(BenchmarkId::new("query_aql", size), &size, |b, &_s| {
            b.iter(|| {
                rt.block_on(async {
                    db.execute(query_aql.as_str()).await.unwrap();
                })
            })
        });
    }

    group.finish();
}

fn bench_baseline_wal_enabled(c: &mut Criterion) {
    run_baseline_bench(c, true, "baseline_wal_enabled");
}

fn bench_baseline_wal_disabled(c: &mut Criterion) {
    run_baseline_bench(c, false, "baseline_wal_disabled");
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500));
    targets = bench_baseline_wal_enabled, bench_baseline_wal_disabled
);

criterion_main!(benches);
