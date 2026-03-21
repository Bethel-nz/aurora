//! Aurora vs SQLite Comparative Benchmark (Using AQL)
//!
//! This benchmark compares Aurora DB against SQLite for common operations.
//! Run with: `cargo bench --bench comparative_benchmark`
//!
//! Aurora's value proposition isn't raw speed - it's the combination of:
//! - Embedded document database
//! - GraphQL-like query language (AQL)
//! - Built-in reactive subscriptions
//! - Background job processing

use aurora_db::{Aurora, AuroraConfig, Value};
use criterion::{Criterion, criterion_group, criterion_main};
use rusqlite::{Connection, params};
use serde_json::json;
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

    conn.execute(
        "CREATE TABLE users (
            id TEXT PRIMARY KEY,
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
// Benchmarks - Using AQL
// ============================================================================

fn bench_insert(c: &mut Criterion) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static ITERATION: AtomicUsize = AtomicUsize::new(0);

    let mut group = c.benchmark_group("insert_operations");
    group.measurement_time(Duration::from_secs(10));

    // Aurora Insert using AQL
    // Setup happens once, iterations just insert with unique IDs
    let aurora_rt = Runtime::new().unwrap();
    let (aurora_db, _aurora_temp) = aurora_rt.block_on(setup_aurora());

    aurora_rt.block_on(async {
        aurora_db
            .execute(
                r#"
            schema {
                define collection users {
                    name: String!
                    email: String! @indexed
                    age: Int @indexed
                    active: Boolean
                }
            }
        "#,
            )
            .await
            .unwrap();
    });

    group.bench_function("aurora_aql_insert_1000", |b| {
        b.iter(|| {
            let iter_id = ITERATION.fetch_add(1, Ordering::SeqCst);
            aurora_rt.block_on(async {
                for i in 0..INSERT_COUNT {
                    let vars = json!({
                        "name": format!("User {}", i),
                        "email": format!("user{}_{}@example.com", iter_id, i),
                        "age": 20 + (i % 50),
                        "active": i % 2 == 0
                    });

                    aurora_db.execute((r#"
                        mutation InsertUser($name: String, $email: String, $age: Int, $active: Boolean) {
                            insertInto(collection: "users", data: {
                                name: $name,
                                email: $email,
                                age: $age,
                                active: $active
                            }) { id }
                        }
                    "#, vars)).await.unwrap();
                }
            })
        })
    });

    // SQLite Insert - also use unique IDs
    let (sqlite_conn, _sqlite_temp) = setup_sqlite();

    group.bench_function("sqlite_insert_1000", |b| {
        b.iter(|| {
            let iter_id = ITERATION.fetch_add(1, Ordering::SeqCst);
            for i in 0..INSERT_COUNT {
                sqlite_conn.execute(
                    "INSERT INTO users (id, name, email, age, active) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        format!("id-{}-{}", iter_id, i),
                        format!("User {}", i),
                        format!("user{}@example.com", i),
                        20 + (i % 50) as i32,
                        if i % 2 == 0 { 1 } else { 0 }
                    ],
                ).unwrap();
            }
        })
    });

    group.finish();
}

fn bench_query_by_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_query");
    group.measurement_time(Duration::from_secs(10));

    // Setup Aurora with data using AQL
    let rt = Runtime::new().unwrap();
    let (aurora_db, _aurora_temp) = rt.block_on(setup_aurora());

    rt.block_on(async {
        aurora_db
            .execute(
                r#"
            schema {
                define collection users {
                    name: String!
                    email: String! @indexed
                    age: Int @indexed
                    active: Boolean
                }
            }
        "#,
            )
            .await
            .unwrap();

        for i in 0..INSERT_COUNT {
            let vars = json!({
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
                "age": 20 + (i % 50),
                "active": i % 2 == 0
            });

            aurora_db
                .execute((
                    r#"
                mutation ($name: String, $email: String, $age: Int, $active: Boolean) {
                    insertInto(collection: "users", data: {
                        name: $name, email: $email, age: $age, active: $active
                    }) { id }
                }
            "#,
                    vars,
                ))
                .await
                .unwrap();
        }

        // Prewarm cache for fair comparison with SQLite (which keeps data in page cache)
        aurora_db.prewarm_cache("users", None).await.unwrap();
    });

    // Setup SQLite with data
    let (sqlite_conn, _sqlite_temp) = setup_sqlite();
    for i in 0..INSERT_COUNT {
        sqlite_conn
            .execute(
                "INSERT INTO users (id, name, email, age, active) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    format!("id-{}", i),
                    format!("User {}", i),
                    format!("user{}@example.com", i),
                    20 + (i % 50) as i32,
                    if i % 2 == 0 { 1 } else { 0 }
                ],
            )
            .unwrap();
    }

    // Aurora indexed query using AQL
    group.bench_function("aurora_aql_query_by_age_range", |b| {
        b.iter(|| {
            rt.block_on(async {
                aurora_db
                    .execute(
                        r#"
                    query {
                        users(where: { age: { gt: 30, lt: 40 } }) {
                            id
                            name
                            email
                            age
                        }
                    }
                "#,
                    )
                    .await
                    .unwrap()
            })
        })
    });

    // Aurora indexed query using Fluent API (no AQL parsing overhead)
    group.bench_function("aurora_fluent_query_by_age_range", |b| {
        b.iter(|| {
            rt.block_on(async {
                aurora_db
                    .query("users")
                    .filter(|f: &aurora_db::query::FilterBuilder| f.gt("age", Value::Int(30)) & f.lt("age", Value::Int(40)))
                    .collect()
                    .await
                    .unwrap()
            })
        })
    });

    // SQLite indexed query
    group.bench_function("sqlite_query_by_age_range", |b| {
        b.iter(|| {
            let mut stmt = sqlite_conn
                .prepare_cached("SELECT * FROM users WHERE age > 30 AND age < 40")
                .unwrap();
            let _results: Vec<_> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
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

fn bench_full_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_scan");
    group.measurement_time(Duration::from_secs(10));

    // Setup Aurora using AQL
    let rt = Runtime::new().unwrap();
    let (aurora_db, _aurora_temp) = rt.block_on(setup_aurora());

    rt.block_on(async {
        aurora_db
            .execute(
                r#"
            schema {
                define collection users {
                    name: String!
                    email: String! @indexed
                    age: Int
                    active: Boolean
                }
            }
        "#,
            )
            .await
            .unwrap();

        for i in 0..INSERT_COUNT {
            let vars = json!({
                "name": format!("User {}", i),
                "email": format!("user{}@example.com", i),
                "age": 20 + (i % 50),
                "active": i % 2 == 0
            });

            aurora_db
                .execute((
                    r#"
                mutation ($name: String, $email: String, $age: Int, $active: Boolean) {
                    insertInto(collection: "users", data: {
                        name: $name, email: $email, age: $age, active: $active
                    }) { id }
                }
            "#,
                    vars,
                ))
                .await
                .unwrap();
        }

        // Prewarm cache for fair comparison
        aurora_db.prewarm_cache("users", None).await.unwrap();
    });

    // Setup SQLite
    let (sqlite_conn, _sqlite_temp) = setup_sqlite();
    for i in 0..INSERT_COUNT {
        sqlite_conn
            .execute(
                "INSERT INTO users (id, name, email, age, active) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    format!("id-{}", i),
                    format!("User {}", i),
                    format!("user{}@example.com", i),
                    20 + (i % 50) as i32,
                    if i % 2 == 0 { 1 } else { 0 }
                ],
            )
            .unwrap();
    }

    // Aurora full scan using AQL
    group.bench_function("aurora_aql_select_all", |b| {
        b.iter(|| {
            rt.block_on(async {
                aurora_db
                    .execute(
                        r#"
                    query {
                        users {
                            id
                            name
                            email
                            age
                            active
                        }
                    }
                "#,
                    )
                    .await
                    .unwrap()
            })
        })
    });

    // SQLite full scan
    group.bench_function("sqlite_select_all", |b| {
        b.iter(|| {
            let mut stmt = sqlite_conn.prepare_cached("SELECT * FROM users").unwrap();
            let _results: Vec<_> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
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

fn bench_reactive_subscription(c: &mut Criterion) {
    let mut group = c.benchmark_group("reactive_features");
    group.measurement_time(Duration::from_secs(5));

    // Aurora-only - SQLite doesn't have built-in subscriptions
    group.bench_function("aurora_aql_subscription_insert", |b| {
        let rt = Runtime::new().unwrap();
        let (db, _temp) = rt.block_on(setup_aurora());

        rt.block_on(async {
            db.execute(
                r#"
                schema {
                    define collection events {
                        event_type: String!
                        payload: String
                    }
                }
            "#,
            )
            .await
            .unwrap();
        });

        // Create the subscription once — we benchmark notification delivery, not setup cost
        let mut listener = rt.block_on(
            db.stream(
                r#"
                subscription {
                    events {
                        id
                        event_type
                        payload
                    }
                }
            "#,
            )
        ).unwrap();

        b.iter(|| {
            rt.block_on(async {
                // Each iteration measures insert + notification delivery
                db.execute(
                    r#"
                    mutation {
                        insertInto(collection: "events", data: {
                            event_type: "test",
                            payload: "benchmark data"
                        }) { id }
                    }
                "#,
                )
                .await
                .unwrap();
                let _ = listener.recv().await;
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_query_by_index,
    bench_full_table_scan,
    bench_reactive_subscription,
);
criterion_main!(benches);
