//! Aurora Chaos Suite
//!
//! Stress-tests crash durability by:
//! 1. Spawning a child process that writes documents continuously
//! 2. Killing it at a random point (SIGKILL / process::exit)
//! 3. Reopening the database and validating that every committed write
//!    is present and uncorrupted
//!
//! Run with:
//!   cargo run --bin chaos -- [--rounds N] [--writes-per-round N] [--db-path PATH]
//!
//! Environment variable CHAOS_WORKER=1 is set on the child process so it knows
//! to run the writer loop instead of the supervisor loop.

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// ── CLI args ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Config {
    rounds: usize,
    writes_per_round: usize,
    db_path: PathBuf,
    kill_delay_ms_min: u64,
    kill_delay_ms_max: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rounds: 20,
            writes_per_round: 5_000,
            db_path: PathBuf::from("/tmp/aurora_chaos_db"),
            kill_delay_ms_min: 500,
            kill_delay_ms_max: 2000,
        }
    }
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rounds" => { i += 1; cfg.rounds = args[i].parse().unwrap(); }
            "--writes-per-round" => { i += 1; cfg.writes_per_round = args[i].parse().unwrap(); }
            "--db-path" => { i += 1; cfg.db_path = PathBuf::from(&args[i]); }
            "--kill-min-ms" => { i += 1; cfg.kill_delay_ms_min = args[i].parse().unwrap(); }
            "--kill-max-ms" => { i += 1; cfg.kill_delay_ms_max = args[i].parse().unwrap(); }
            _ => {}
        }
        i += 1;
    }
    cfg
}

// ── Shared counter written to a side-channel file ───────────────────────────
// The worker writes its commit counter to a file after each successful write
// so the supervisor knows how many docs should be recoverable.

fn counter_path(db_path: &PathBuf) -> PathBuf {
    db_path.with_extension("counter")
}

fn read_counter(db_path: &PathBuf) -> u64 {
    std::fs::read_to_string(counter_path(db_path))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn write_counter(db_path: &PathBuf, n: u64) {
    let path = counter_path(db_path);
    let tmp = path.with_extension("counter_tmp");
    // Atomic write: write to temp then rename so SIGKILL mid-write can't corrupt
    if std::fs::write(&tmp, n.to_string()).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

// ── Worker (child process) ──────────────────────────────────────────────────

async fn run_worker(db_path: PathBuf) {
    let db = Aurora::with_config(AuroraConfig {
        db_path: db_path.clone(),
        enable_wal: true,
        enable_write_buffering: false, // sync writes so WAL is ground truth
        ..Default::default()
    })
    .await
    .expect("worker: failed to open db");

    // Ensure collection exists
    let _ = db.new_collection(
        "records",
        vec![
            ("seq".to_string(), FieldType::SCALAR_INT, true),
            ("payload".to_string(), FieldType::SCALAR_STRING, false),
        ],
    ).await;

    // Read existing counter so we resume from where we left off
    let start = read_counter(&db_path);
    let mut seq = start;

    loop {
        let mut data = HashMap::new();
        data.insert("seq".to_string(), Value::Int(seq as i64));
        data.insert("payload".to_string(), Value::String(format!("record-{}", seq)));

        if db.insert_map("records", data).await.is_ok() {
            seq += 1;
            write_counter(&db_path, seq);
        }
    }
}

// ── Supervisor (parent process) ─────────────────────────────────────────────

fn random_delay(min: u64, max: u64) -> u64 {
    let range = max - min;
    let noise = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    min + (noise % range)
}

#[allow(dead_code)]
struct RoundResult {
    round: usize,
    committed: u64,
    recovered: u64,
    missing: u64,
    corrupted: u64,
    duration: Duration,
}

async fn verify_database(db_path: &PathBuf, expected_count: u64) -> (u64, u64) {
    let db = Aurora::with_config(AuroraConfig {
        db_path: db_path.clone(),
        enable_wal: true,
        enable_write_buffering: false,
        ..Default::default()
    })
    .await
    .expect("verifier: failed to open db");

    let docs = db.get_all_collection("records").await.unwrap_or_default();

    let mut found_seqs = std::collections::HashSet::new();
    let mut corrupted = 0u64;

    for doc in &docs {
        match doc.data.get("seq") {
            Some(Value::Int(seq)) => { found_seqs.insert(*seq as u64); }
            _ => { corrupted += 1; }
        }
    }

    // Count how many of the expected committed seqs are present
    let recovered = (0..expected_count).filter(|s| found_seqs.contains(s)).count() as u64;
    let missing = expected_count.saturating_sub(recovered);

    (recovered, missing + corrupted)
}

async fn run_supervisor(cfg: Config) {
    println!("╔════════════════════════════════════════════╗");
    println!("║        AURORA CHAOS SUITE                  ║");
    println!("╠════════════════════════════════════════════╣");
    println!("║  Rounds:           {:>6}                  ║", cfg.rounds);
    println!("║  Writes/round:     {:>6}                  ║", cfg.writes_per_round);
    println!("║  Kill window:   {:>4}-{:<4}ms               ║", cfg.kill_delay_ms_min, cfg.kill_delay_ms_max);
    println!("╚════════════════════════════════════════════╝\n");

    // Clean slate
    if cfg.db_path.exists() {
        std::fs::remove_dir_all(&cfg.db_path).expect("failed to clean db dir");
    }
    let _ = std::fs::remove_file(counter_path(&cfg.db_path));

    let binary = std::env::current_exe().expect("cannot find current exe");
    let mut results: Vec<RoundResult> = Vec::new();
    let mut total_data_loss = 0u64;

    for round in 1..=cfg.rounds {
        print!("Round {:>3}/{} — spawning writer... ", round, cfg.rounds);

        let committed_before = read_counter(&cfg.db_path);

        // Spawn child worker
        let mut child = std::process::Command::new(&binary)
            .env("CHAOS_WORKER", "1")
            .env("CHAOS_DB_PATH", cfg.db_path.to_str().unwrap())
            .spawn()
            .expect("failed to spawn worker");

        // Let it write for a random window
        let kill_after = random_delay(cfg.kill_delay_ms_min, cfg.kill_delay_ms_max);
        std::thread::sleep(Duration::from_millis(kill_after));

        // SIGKILL — no cleanup, no flush, just die
        child.kill().ok();
        child.wait().ok();

        let committed_after = read_counter(&cfg.db_path);
        let written_this_round = committed_after.saturating_sub(committed_before);

        let start = Instant::now();
        let (recovered, lost) = verify_database(&cfg.db_path, committed_after).await;
        let duration = start.elapsed();

        total_data_loss += lost;

        println!(
            "killed after {:>4}ms | wrote {:>5} | committed {:>6} | recovered {:>6} | lost {}",
            kill_after, written_this_round, committed_after, recovered, lost
        );

        results.push(RoundResult {
            round,
            committed: committed_after,
            recovered,
            missing: lost,
            corrupted: 0,
            duration,
        });
    }

    // ── Final report ────────────────────────────────────────────────────────
    // Use the max committed/recovered seen across all rounds (cumulative counter
    // may appear lower if a round's kill hit during DB init and wrote nothing)
    let total_committed = results.iter().map(|r| r.committed).max().unwrap_or(0);
    let total_recovered = results.iter().map(|r| r.recovered).max().unwrap_or(0);

    println!("\n╔════════════════════════════════════════════╗");
    println!("║              CHAOS RESULTS                 ║");
    println!("╠════════════════════════════════════════════╣");
    println!("║  Rounds completed:   {:>6}                ║", cfg.rounds);
    println!("║  Total committed:    {:>6}                ║", total_committed);
    println!("║  Total recovered:    {:>6}                ║", total_recovered);
    println!("║  Total lost:         {:>6}                ║", total_data_loss);
    println!("╠════════════════════════════════════════════╣");

    if total_data_loss == 0 {
        println!("║  ✓ PASS — 0.0% data loss across all rounds ║");
    } else {
        let loss_pct = total_data_loss as f64 / total_committed as f64 * 100.0;
        println!("║  ✗ FAIL — {:.4}% data loss detected       ║", loss_pct);
    }
    println!("╚════════════════════════════════════════════╝");

    if total_data_loss > 0 {
        std::process::exit(1);
    }
}

// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    if std::env::var("CHAOS_WORKER").is_ok() {
        let db_path = PathBuf::from(
            std::env::var("CHAOS_DB_PATH").unwrap_or_else(|_| "/tmp/aurora_chaos_db".into()),
        );
        run_worker(db_path).await;
    } else {
        let cfg = parse_args();
        run_supervisor(cfg).await;
    }
}
