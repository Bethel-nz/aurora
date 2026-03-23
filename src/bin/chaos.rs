//! Aurora Chaos Suite
//!
//! Stress-tests crash durability by:
//! 1. Spawning a child process that writes documents continuously
//! 2. Killing it at a random point (SIGKILL / process::exit)
//! 3. Reopening the database and validating that every committed write
//!    is present and uncorrupted
//!
//! Run with:
//!   cargo run --bin chaos -- [--rounds N] [--db-path PATH] [--kill-min-ms N] [--kill-max-ms N]
//!
//! Environment variable CHAOS_WORKER=1 is set on the child process so it knows
//! to run the writer loop instead of the supervisor loop.

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// ── CLI args ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Config {
    rounds: usize,
    db_path: PathBuf,
    kill_delay_ms_min: u64,
    kill_delay_ms_max: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rounds: 20,
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

    let next_val = |i: usize, flag: &str, args: &[String]| -> String {
        if i + 1 >= args.len() {
            eprintln!("error: '{}' requires a value", flag);
            std::process::exit(2);
        }
        args[i + 1].clone()
    };

    while i < args.len() {
        match args[i].as_str() {
            "--rounds" => {
                let v = next_val(i, "--rounds", &args);
                cfg.rounds = v.parse().unwrap_or_else(|_| {
                    eprintln!("error: --rounds must be a positive integer");
                    std::process::exit(2);
                });
                i += 1;
            }
            "--db-path" => {
                let v = next_val(i, "--db-path", &args);
                cfg.db_path = PathBuf::from(v);
                i += 1;
            }
            "--kill-min-ms" => {
                let v = next_val(i, "--kill-min-ms", &args);
                cfg.kill_delay_ms_min = v.parse().unwrap_or_else(|_| {
                    eprintln!("error: --kill-min-ms must be a non-negative integer");
                    std::process::exit(2);
                });
                i += 1;
            }
            "--kill-max-ms" => {
                let v = next_val(i, "--kill-max-ms", &args);
                cfg.kill_delay_ms_max = v.parse().unwrap_or_else(|_| {
                    eprintln!("error: --kill-max-ms must be a non-negative integer");
                    std::process::exit(2);
                });
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }

    if cfg.kill_delay_ms_min >= cfg.kill_delay_ms_max {
        eprintln!(
            "error: --kill-min-ms ({}) must be less than --kill-max-ms ({})",
            cfg.kill_delay_ms_min, cfg.kill_delay_ms_max
        );
        std::process::exit(2);
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
    // Atomic write: write to temp then rename so SIGKILL mid-write can't zero the file
    if std::fs::write(&tmp, n.to_string()).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

// ── Worker (child process) ──────────────────────────────────────────────────

async fn run_worker(db_path: PathBuf) {
    // WAL is Aurora's durability layer: each write is appended to the WAL file
    // before being acknowledged, so SIGKILL after insert_map Ok still leaves a
    // recoverable record. Disable the write buffer so writes bypass the
    // in-memory queue and go directly to the WAL + cold store path.
    let db = Aurora::with_config(AuroraConfig {
        db_path: db_path.clone(),
        enable_wal: true,
        enable_write_buffering: false,
        ..Default::default()
    })
    .await
    .expect("worker: failed to open db");

    // Ensure collection exists. The boolean flag is `unique`; use false so
    // counter divergence can't cause an infinite unique-constraint retry loop.
    let _ = db.new_collection(
        "records",
        vec![
            ("seq".to_string(), FieldType::SCALAR_INT, false),
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
    // Caller guarantees min < max (validated in parse_args), but be safe.
    if min >= max {
        return min;
    }
    rand::thread_rng().gen_range(min..max)
}

/// Remove all Aurora-created files for a given base path so each supervisor
/// run starts with a completely clean slate.
///
/// Aurora cold store lives at `<base>.db/` (sled appends ".db" if absent).
/// The WAL lives at `<base>.wal`.
fn clean_db(base: &PathBuf) {
    let cold_dir = base.with_extension("db");
    let wal_file = base.with_extension("wal");
    let counter  = counter_path(base);

    let _ = std::fs::remove_dir_all(base);      // workers.db directory
    let _ = std::fs::remove_dir_all(&cold_dir);  // sled cold store
    let _ = std::fs::remove_file(&wal_file);
    let _ = std::fs::remove_file(&counter);
}

#[allow(dead_code)]
struct RoundResult {
    round: usize,
    committed: u64,
    recovered: u64,
    duration: Duration,
}

async fn verify_database(db_path: &PathBuf, expected_count: u64) -> Result<(u64, u64), String> {
    let db = Aurora::with_config(AuroraConfig {
        db_path: db_path.clone(),
        enable_wal: true,
        enable_write_buffering: false,
        ..Default::default()
    })
    .await
    .map_err(|e| format!("verifier: failed to open db: {e}"))?;

    let docs = db.get_all_collection("records")
        .await
        .map_err(|e| format!("verifier: get_all_collection failed: {e}"))?;

    let mut found_seqs = std::collections::HashSet::new();
    let mut corrupted = 0u64;

    for doc in &docs {
        match doc.data.get("seq") {
            Some(Value::Int(seq)) => { found_seqs.insert(*seq as u64); }
            _ => { corrupted += 1; }
        }
    }

    let recovered = (0..expected_count).filter(|s| found_seqs.contains(s)).count() as u64;
    let missing   = expected_count.saturating_sub(recovered);

    Ok((recovered, missing + corrupted))
}

async fn run_supervisor(cfg: Config) {
    println!("╔════════════════════════════════════════════╗");
    println!("║        AURORA CHAOS SUITE                  ║");
    println!("╠════════════════════════════════════════════╣");
    println!("║  Rounds:           {:>6}                  ║", cfg.rounds);
    println!("║  Kill window:   {:>4}-{:<4}ms               ║", cfg.kill_delay_ms_min, cfg.kill_delay_ms_max);
    println!("╚════════════════════════════════════════════╝\n");

    clean_db(&cfg.db_path);

    let binary = std::env::current_exe().expect("cannot find current exe");
    let mut results: Vec<RoundResult> = Vec::new();

    for round in 1..=cfg.rounds {
        print!("Round {:>3}/{} — spawning writer... ", round, cfg.rounds);

        let committed_before = read_counter(&cfg.db_path);

        // Spawn child worker — pass db_path via env to avoid UTF-8 restriction on some platforms
        let mut child = std::process::Command::new(&binary)
            .env("CHAOS_WORKER", "1")
            .env("CHAOS_DB_PATH", &cfg.db_path)
            .spawn()
            .expect("failed to spawn worker");

        // Let it write for a random window then SIGKILL — no cleanup, no flush
        let kill_after = random_delay(cfg.kill_delay_ms_min, cfg.kill_delay_ms_max);
        std::thread::sleep(Duration::from_millis(kill_after));
        child.kill().ok();
        child.wait().ok();

        let committed_after = read_counter(&cfg.db_path);
        let written_this_round = committed_after.saturating_sub(committed_before);

        let start = Instant::now();
        let (recovered, lost) = match verify_database(&cfg.db_path, committed_after).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("\nverify failed on round {round}: {e}");
                std::process::exit(1);
            }
        };
        let duration = start.elapsed();

        println!(
            "killed after {:>4}ms | wrote {:>5} | committed {:>6} | recovered {:>6} | lost {}",
            kill_after, written_this_round, committed_after, recovered, lost
        );

        results.push(RoundResult { round, committed: committed_after, recovered, duration });
    }

    // ── Final report ────────────────────────────────────────────────────────
    // Use the highest committed/recovered seen — some rounds may show 0 if the
    // kill lands before DB init completes.
    let total_committed = results.iter().map(|r| r.committed).max().unwrap_or(0);
    let total_recovered = results.iter().map(|r| r.recovered).max().unwrap_or(0);
    // Compute loss from the final cumulative state, not by summing per-round
    // values (which would double-count any seq missing across consecutive rounds).
    let total_data_loss  = total_committed.saturating_sub(total_recovered);

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
    } else if total_committed > 0 {
        let loss_pct = total_data_loss as f64 / total_committed as f64 * 100.0;
        println!("║  ✗ FAIL — {:.4}% data loss detected       ║", loss_pct);
    } else {
        println!("║  — no commits observed across all rounds   ║");
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
        let db_path = std::env::var_os("CHAOS_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/aurora_chaos_db"));
        run_worker(db_path).await;
    } else {
        let cfg = parse_args();
        run_supervisor(cfg).await;
    }
}
