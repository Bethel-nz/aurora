//! Pure Fluent API vs SQLite Benchmark
//!
//! This benchmark compares the raw internal performance of Aurora DB's Fluent API
//! against SQLite's prepared statements, eliminating all AQL parsing overhead.
//!
//! Run with: `cargo bench --bench comparative_fluent_benchmark`

use aurora_db::{Aurora, AuroraConfig, Value};
use criterion::{Criterion, criterion_group, criterion_main};
use rusqlite::{Connection, params};

use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const INSERT_COUNT: usize = 1000;

// ============================================================================
// Setup Helpers
// ============================================================================

async fn setup_aurora() -> (Aurora, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = AuroraConfig {
        db_path: temp_dir.path().join("aurora.db"),
        enable_write_buffering: false,
        enable_wal: false,
        auto_compact: false,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await.unwrap();
    (db, temp_dir)
}

fn setup_sqlite() -> (Connection, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(temp_dir.path().join("sqlite.db")).unwrap();

    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = MEMORY;",
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            active INTEGER
        )",
        [],
    )
    .unwrap();

    conn.execute("CREATE INDEX idx_email ON users(email)", [])
        .unwrap();
    conn.execute("CREATE INDEX idx_age ON users(age)", [])
        .unwrap();

    (conn, temp_dir)
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_pure_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_insert");
    group.measurement_time(Duration::from_secs(5));

    let rt = Runtime::new().unwrap();
    let (aurora_db, _aurora_temp) = rt.block_on(setup_aurora());

    // Use AQL for schema definition (one-time setup)
    rt.block_on(async {
        aurora_db
            .execute(
                r#"
            schema {
                define collection users {
                    name: String
                    email: String @indexed
                    age: Int @indexed
                    active: Boolean
                }
            }
        "#,
            )
            .await
            .unwrap();
    });

    group.bench_function("aurora_fluent_insert_1000", |b| {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        b.iter(|| {
            let counter = counter.clone();
            rt.block_on(async move {
                for _ in 0..INSERT_COUNT {
                    let id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let doc = vec![
                        ("name", Value::String(format!("User {}", id))),
                        ("email", Value::String(format!("user{}@example.com", id))),
                        ("age", Value::Int(20 + (id % 50) as i64)),
                        ("active", Value::Bool(id % 2 == 0)),
                    ];
                    aurora_db.insert_into("users", doc).await.unwrap();
                }
            })
        })
    });

    let (sqlite_conn, _sqlite_temp) = setup_sqlite();
    // Prepare statement outside of loop
    let mut stmt = sqlite_conn
        .prepare("INSERT INTO users (name, email, age, active) VALUES (?1, ?2, ?3, ?4)")
        .unwrap();

    group.bench_function("sqlite_prepared_insert_1000", |b| {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        b.iter(|| {
            // SQLite insertion loop
            for _i in 0..INSERT_COUNT {
                let id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                stmt.execute(params![
                    format!("User {}", id),
                    format!("user{}@example.com", id),
                    20 + (id % 50),
                    if id % 2 == 0 { 1 } else { 0 }
                ])
                .unwrap();
            }
        })
    });

    group.finish();
}

fn bench_pure_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("pure_query");
    group.measurement_time(Duration::from_secs(5));

    // Setup Aurora
    let rt = Runtime::new().unwrap();
    let (aurora_db, _aurora_temp) = rt.block_on(setup_aurora());

    rt.block_on(async {
        // Schema
        aurora_db
            .execute(
                r#"
            schema {
                define collection users {
                    name: String
                    email: String @indexed
                    age: Int @indexed
                    active: Boolean
                }
            }
        "#,
            )
            .await
            .unwrap();

        // Populate data using AQL for simplicity in setup (we are benchmarking query, not insert here)
        // But to be consistent with "pure", let's use insert_into or batch insert if available.
        // We'll use insert_into loop as in setup.
        for i in 0..INSERT_COUNT {
            let doc = vec![
                ("name", Value::String(format!("User {}", i))),
                ("email", Value::String(format!("user{}@example.com", i))),
                ("age", Value::Int(20 + (i % 50) as i64)),
                ("active", Value::Bool(i % 2 == 0)),
            ];
            aurora_db.insert_into("users", doc).await.unwrap();
        }
        aurora_db.prewarm_cache("users", None).await.unwrap();
    });

    // Setup SQLite
    let (sqlite_conn, _sqlite_temp) = setup_sqlite();
    {
        let mut stmt = sqlite_conn
            .prepare("INSERT INTO users (name, email, age, active) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        sqlite_conn.execute("BEGIN TRANSACTION", []).unwrap();
        for i in 0..INSERT_COUNT {
            stmt.execute(params![
                format!("User {}", i),
                format!("user{}@example.com", i),
                20 + (i % 50),
                if i % 2 == 0 { 1 } else { 0 }
            ])
            .unwrap();
        }
        sqlite_conn.execute("COMMIT", []).unwrap();
    }

    // Benchmark Aurora Fluent Query
    group.bench_function("aurora_fluent_find", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _results = aurora_db
                    .query("users")
                    .filter(|f: &aurora_db::query::FilterBuilder| f.gt("age", Value::Int(30)) & f.lt("age", Value::Int(40)))
                    .collect()
                    .await
                    .unwrap();
            })
        })
    });

    // Benchmark SQLite Prepared Query
    let mut query_stmt = sqlite_conn
        .prepare("SELECT * FROM users WHERE age > 30 AND age < 40")
        .unwrap();

    group.bench_function("sqlite_prepared_query", |b| {
        b.iter(|| {
            let _results: Vec<_> = query_stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i32>(3)?,
                        row.get::<_, i32>(4)?,
                    ))
                })
                .unwrap()
                .collect();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_pure_insert, bench_pure_query);
criterion_main!(benches);
