// Aurora vs SQLite Performance Benchmark
//
// This benchmark compares Aurora's performance against SQLite using identical test scenarios.
// Tests include: inserts, queries, updates, deletes, transactions, and indexing.

use aurora_db::{Aurora, AuroraConfig, types::Value};
use rusqlite::{Connection, params};
use std::time::{Duration, Instant};
use std::collections::HashMap;

const NUM_DOCS: usize = 100_000;
const BATCH_SIZE: usize = 1000;

#[derive(Debug)]
struct BenchmarkResults {
    aurora: TestResults,
    sqlite: TestResults,
}

#[derive(Debug)]
struct TestResults {
    single_insert: Duration,
    batch_insert: Duration,
    query_no_index: Duration,
    query_with_index: Duration,
    query_with_limit: Duration,
    update: Duration,
    delete: Duration,
    transaction: Duration,
}

impl BenchmarkResults {
    fn print_comparison(&self) {
        println!("\n╔═══════════════════════════════════════════════════════════════════════╗");
        println!("║              AURORA vs SQLITE PERFORMANCE COMPARISON                  ║");
        println!("╠═══════════════════════════════════════════════════════════════════════╣");
        println!("║  Test Scenario                │  Aurora      │  SQLite      │  Winner ║");
        println!("╠═══════════════════════════════════════════════════════════════════════╣");

        self.print_row("Single Insert (1,000 docs)",
            self.aurora.single_insert, self.sqlite.single_insert);

        self.print_row(&format!("Batch Insert ({} docs)", NUM_DOCS),
            self.aurora.batch_insert, self.sqlite.batch_insert);

        self.print_row("Query without Index",
            self.aurora.query_no_index, self.sqlite.query_no_index);

        self.print_row("Query with Index",
            self.aurora.query_with_index, self.sqlite.query_with_index);

        self.print_row("Query with LIMIT 100",
            self.aurora.query_with_limit, self.sqlite.query_with_limit);

        self.print_row("Update (10,000 docs)",
            self.aurora.update, self.sqlite.update);

        self.print_row("Delete (10,000 docs)",
            self.aurora.delete, self.sqlite.delete);

        self.print_row("Transaction (1,000 ops)",
            self.aurora.transaction, self.sqlite.transaction);

        println!("╚═══════════════════════════════════════════════════════════════════════╝");

        // Calculate overall winner
        let aurora_wins = self.count_wins(true);
        let sqlite_wins = self.count_wins(false);

        println!("\n📊 Overall Score:");
        println!("   Aurora: {} wins", aurora_wins);
        println!("   SQLite: {} wins", sqlite_wins);

        if aurora_wins > sqlite_wins {
            println!("\n🏆 Aurora is faster overall!");
        } else if sqlite_wins > aurora_wins {
            println!("\n🏆 SQLite is faster overall!");
        } else {
            println!("\n🤝 It's a tie!");
        }

        // Calculate throughput
        println!("\n📈 Throughput Comparison:");
        let aurora_insert_throughput = NUM_DOCS as f64 / self.aurora.batch_insert.as_secs_f64();
        let sqlite_insert_throughput = NUM_DOCS as f64 / self.sqlite.batch_insert.as_secs_f64();
        println!("   Insert throughput:");
        println!("     Aurora: {:.0} docs/sec", aurora_insert_throughput);
        println!("     SQLite: {:.0} docs/sec", sqlite_insert_throughput);
        println!("     Difference: {:.1}x", aurora_insert_throughput / sqlite_insert_throughput);
    }

    fn print_row(&self, name: &str, aurora_time: Duration, sqlite_time: Duration) {
        let winner = if aurora_time < sqlite_time { "Aurora" } else { "SQLite" };
        let aurora_str = format!("{:.3}s", aurora_time.as_secs_f64());
        let sqlite_str = format!("{:.3}s", sqlite_time.as_secs_f64());

        println!("║ {:<30}│ {:>12} │ {:>12} │ {:>7} ║",
            name, aurora_str, sqlite_str, winner);
    }

    fn count_wins(&self, aurora: bool) -> usize {
        let mut wins = 0;
        let tests = [
            (self.aurora.single_insert, self.sqlite.single_insert),
            (self.aurora.batch_insert, self.sqlite.batch_insert),
            (self.aurora.query_no_index, self.sqlite.query_no_index),
            (self.aurora.query_with_index, self.sqlite.query_with_index),
            (self.aurora.query_with_limit, self.sqlite.query_with_limit),
            (self.aurora.update, self.sqlite.update),
            (self.aurora.delete, self.sqlite.delete),
            (self.aurora.transaction, self.sqlite.transaction),
        ];

        for (aurora_time, sqlite_time) in tests {
            if aurora && aurora_time < sqlite_time {
                wins += 1;
            } else if !aurora && sqlite_time < aurora_time {
                wins += 1;
            }
        }
        wins
    }
}

fn main() {
    println!("🚀 Starting Aurora vs SQLite Benchmark...\n");
    println!("Test parameters:");
    println!("  - Total documents: {}", NUM_DOCS);
    println!("  - Batch size: {}", BATCH_SIZE);
    println!("  - Single insert test: 1,000 docs");
    println!("  - Update/Delete test: 10,000 docs each");
    println!();

    // Create tokio runtime for Aurora's async operations
    let rt = tokio::runtime::Runtime::new().unwrap();

    let results = BenchmarkResults {
        aurora: rt.block_on(async { run_aurora_benchmarks().await }),
        sqlite: run_sqlite_benchmarks(),
    };

    results.print_comparison();
}

async fn run_aurora_benchmarks() -> TestResults {
    println!("📦 Running Aurora benchmarks...");

    // Setup
    let _ = std::fs::remove_file("bench_aurora.db");
    let config = AuroraConfig {
        db_path: "bench_aurora.db".into(),
        ..Default::default()
    };
    let db = Aurora::with_config(config).unwrap();

    db.new_collection("users", vec![
        ("name", aurora_db::types::FieldType::String, false),
        ("age", aurora_db::types::FieldType::Int, false),
        ("email", aurora_db::types::FieldType::String, false),
        ("score", aurora_db::types::FieldType::Int, false),
    ]).unwrap();

    // Test 1: Single inserts
    println!("  [1/8] Single inserts...");
    let single_insert = time_operation_async(|| async {
        for i in 0..1000 {
            let _ = db.insert_into("users", vec![
                ("name", Value::String(format!("User {}", i))),
                ("age", Value::Int((20 + i % 50) as i64)),
                ("email", Value::String(format!("user{}@example.com", i))),
                ("score", Value::Int((i * 10) as i64)),
            ]).await;
        }
    }).await;

    // Test 2: Batch insert
    println!("  [2/8] Batch insert...");
    let batch_insert = time_operation_async(|| async {
        let docs: Vec<HashMap<String, Value>> = (0..NUM_DOCS)
            .map(|i| {
                let mut map = HashMap::new();
                map.insert("name".to_string(), Value::String(format!("User {}", i)));
                map.insert("age".to_string(), Value::Int((20 + i % 50) as i64));
                map.insert("email".to_string(), Value::String(format!("user{}@example.com", i)));
                map.insert("score".to_string(), Value::Int((i * 10) as i64));
                map
            })
            .collect();

        let _ = db.batch_insert("users", docs).await;
    }).await;

    // Test 3: Query without index
    println!("  [3/8] Query without index...");
    let query_no_index = time_operation_async(|| async {
        let _ = db.query("users")
            .filter(|f| f.gt("score", Value::Int(500000)))
            .collect()
            .await;
    }).await;

    // Create index
    db.create_index("users", "score").await.unwrap();

    // Test 4: Query with index
    println!("  [4/8] Query with index...");
    let query_with_index = time_operation_async(|| async {
        let _ = db.query("users")
            .filter(|f| f.gt("score", Value::Int(500000)))
            .collect()
            .await;
    }).await;

    // Test 5: Query with LIMIT (Aurora's early termination advantage)
    println!("  [5/8] Query with LIMIT 100...");
    let query_with_limit = time_operation_async(|| async {
        let _ = db.query("users")
            .filter(|f| f.gt("score", Value::Int(500000)))
            .limit(100)
            .collect()
            .await;
    }).await;

    // Test 6: Update
    println!("  [6/8] Update operations...");
    let update = time_operation_async(|| async {
        for i in 0..10000 {
            let docs = db.query("users")
                .filter(|f| f.eq("score", Value::Int((i * 10) as i64)))
                .collect()
                .await
                .unwrap();

            if let Some(doc) = docs.first() {
                let _ = db.update_document("users", &doc.id, vec![
                    ("score", Value::Int((i * 10 + 5) as i64)),
                ]).await;
            }
        }
    }).await;

    // Test 7: Delete
    println!("  [7/8] Delete operations...");
    let delete = time_operation_async(|| async {
        let docs = db.query("users")
            .limit(10000)
            .collect()
            .await
            .unwrap();

        for doc in docs {
            let _ = db.delete(&format!("users:{}", doc.id)).await;
        }
    }).await;

    // Test 8: Transaction
    println!("  [8/8] Transaction operations...");
    let transaction = time_operation_async(|| async {
        for _ in 0..10 {
            let tx_id = db.begin_transaction();
            for i in 0..100 {
                let _ = db.insert_into("users", vec![
                    ("name", Value::String(format!("TX User {}", i))),
                    ("age", Value::Int(25)),
                    ("email", Value::String(format!("tx{}@example.com", i))),
                    ("score", Value::Int(100)),
                ]).await;
            }
            let _ = db.commit_transaction(tx_id);
        }
    }).await;

    println!("  ✅ Aurora benchmarks complete\n");

    TestResults {
        single_insert,
        batch_insert,
        query_no_index,
        query_with_index,
        query_with_limit,
        update,
        delete,
        transaction,
    }
}

fn run_sqlite_benchmarks() -> TestResults {
    println!("🗄️  Running SQLite benchmarks...");

    // Setup
    let _ = std::fs::remove_file("bench_sqlite.db");
    let conn = Connection::open("bench_sqlite.db").unwrap();

    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            email TEXT,
            score INTEGER
        )",
        [],
    ).unwrap();

    // Test 1: Single inserts
    println!("  [1/8] Single inserts...");
    let single_insert = time_operation(|| {
        for i in 0..1000 {
            conn.execute(
                "INSERT INTO users (name, age, email, score) VALUES (?1, ?2, ?3, ?4)",
                params![format!("User {}", i), 20 + (i % 50), format!("user{}@example.com", i), i * 10],
            ).unwrap();
        }
    });

    // Test 2: Batch insert
    println!("  [2/8] Batch insert...");
    let batch_insert = time_operation(|| {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..NUM_DOCS {
            conn.execute(
                "INSERT INTO users (name, age, email, score) VALUES (?1, ?2, ?3, ?4)",
                params![format!("User {}", i), 20 + (i % 50), format!("user{}@example.com", i), i * 10],
            ).unwrap();
        }
        tx.commit().unwrap();
    });

    // Test 3: Query without index
    println!("  [3/8] Query without index...");
    let query_no_index = time_operation(|| {
        let mut stmt = conn.prepare("SELECT * FROM users WHERE score > 500000").unwrap();
        let _: Vec<_> = stmt.query_map([], |_row| Ok(())).unwrap().collect();
    });

    // Create index
    conn.execute("CREATE INDEX idx_score ON users(score)", []).unwrap();

    // Test 4: Query with index
    println!("  [4/8] Query with index...");
    let query_with_index = time_operation(|| {
        let mut stmt = conn.prepare("SELECT * FROM users WHERE score > 500000").unwrap();
        let _: Vec<_> = stmt.query_map([], |_row| Ok(())).unwrap().collect();
    });

    // Test 5: Query with LIMIT
    println!("  [5/8] Query with LIMIT 100...");
    let query_with_limit = time_operation(|| {
        let mut stmt = conn.prepare("SELECT * FROM users WHERE score > 500000 LIMIT 100").unwrap();
        let _: Vec<_> = stmt.query_map([], |_row| Ok(())).unwrap().collect();
    });

    // Test 6: Update
    println!("  [6/8] Update operations...");
    let update = time_operation(|| {
        for i in 0..10000 {
            conn.execute(
                "UPDATE users SET score = ?1 WHERE score = ?2",
                params![i * 10 + 5, i * 10],
            ).unwrap();
        }
    });

    // Test 7: Delete
    println!("  [7/8] Delete operations...");
    let delete = time_operation(|| {
        conn.execute("DELETE FROM users WHERE id <= 10000", []).unwrap();
    });

    // Test 8: Transaction
    println!("  [8/8] Transaction operations...");
    let transaction = time_operation(|| {
        for _ in 0..10 {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                conn.execute(
                    "INSERT INTO users (name, age, email, score) VALUES (?1, ?2, ?3, ?4)",
                    params![format!("TX User {}", i), 25, format!("tx{}@example.com", i), 100],
                ).unwrap();
            }
            tx.commit().unwrap();
        }
    });

    println!("  ✅ SQLite benchmarks complete\n");

    TestResults {
        single_insert,
        batch_insert,
        query_no_index,
        query_with_index,
        query_with_limit,
        update,
        delete,
        transaction,
    }
}

fn time_operation<F>(operation: F) -> Duration
where
    F: FnOnce(),
{
    let start = Instant::now();
    operation();
    start.elapsed()
}

async fn time_operation_async<F, Fut>(operation: F) -> Duration
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let start = Instant::now();
    operation().await;
    start.elapsed()
}
