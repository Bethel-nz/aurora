/// Aurora Baseline Performance Benchmark
/// Tests memory usage and performance at different scales
/// Run before implementing DiskLocation optimization
use aurora_db::{
    Aurora,
    types::{AuroraConfig, FieldType, Value},
};
use std::fs::File;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = String::new();
    output.push_str("=".repeat(80).as_str());
    output.push_str("\nAURORA BASELINE PERFORMANCE BENCHMARK\n");
    output.push_str("Architecture: Primary Index with full Vec<u8> values\n");
    output.push_str(&format!(
        "Timestamp: {}\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    output.push_str("=".repeat(80).as_str());
    output.push_str("\n\n");

    println!("{}", output);

    let scales = vec![1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    // Shared data structure to collect metrics for CSV
    let mut benchmark_metrics = Vec::new();

    for scale in scales {
        println!("\n{}", "=".repeat(80));
        println!("Testing with {} documents", format_number(scale));
        println!("{}", "=".repeat(80));

        let (result_text, metrics) = benchmark_scale(scale).await?;
        output.push_str(&result_text);
        output.push_str("\n");
        benchmark_metrics.push(metrics);

        // Clean up between runs
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Write results to text file
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("aurora_baseline_{}.txt", timestamp);
    let mut file = File::create(&filename)?;
    file.write_all(output.as_bytes())?;

    // Save as CSV
    let csv_filename = format!("aurora_bench_{}.csv", timestamp);
    let mut csv_file = File::create(&csv_filename)?;
    writeln!(csv_file, "Scale,WriteThroughput,HotReadThroughput,MixedReadThroughput,RssMB,DiskMB")?;
    
    for m in benchmark_metrics {
        writeln!(csv_file, "{},{},{},{},{},{}", 
            m.scale, m.write_tput, m.hot_read_tput, m.mixed_read_tput, m.rss_mb, m.disk_mb)?;
    }
    
    println!("\n{}", "=".repeat(80));
    println!("Results saved to: {} and {}", filename, csv_filename);
    println!("{}", "=".repeat(80));

    Ok(())
}

struct ScaleMetrics {
    scale: usize,
    write_tput: f64,
    hot_read_tput: f64,
    mixed_read_tput: f64,
    rss_mb: i64,
    disk_mb: i64,
}

async fn benchmark_scale(num_docs: usize) -> Result<(String, ScaleMetrics), Box<dyn std::error::Error>> {
    let mut output = String::new();

    // Create temp directory
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join(format!("bench_{}.aurora", num_docs));

    let config = AuroraConfig {
        db_path,
        hot_cache_size_mb: 256,
        enable_write_buffering: true,
        enable_wal: true,
        ..Default::default()
    };

    let db = Arc::new(Aurora::with_config(config).await?);

    // Create collection
    db.new_collection(
        "bench",
        vec![
            ("id", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
            ("data", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
            ("index", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true }),
            ("timestamp", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
        ],
    )
    .await?;

    // Get initial process info
    let pid = std::process::id();
    let initial_rss = get_process_rss_mb(pid);

    output.push_str(&format!("Scale: {} documents\n", format_number(num_docs)));
    output.push_str(&format!("Initial RSS: {} MB\n", initial_rss));

    // WRITE TEST
    let write_start = Instant::now();

    for i in 0..num_docs {
        db.insert_into(
            "bench",
            vec![
                ("id", Value::String(format!("doc_{:08}", i))),
                (
                    "data",
                    Value::String(format!(
                        "Document {} with some sample text content to simulate realistic data size",
                        i
                    )),
                ),
                ("index", Value::Int(i as i64)),
                (
                    "timestamp",
                    Value::String(chrono::Local::now().to_rfc3339()),
                ),
            ],
        )
        .await?;

        // Progress indicator for large scales
        if num_docs >= 10_000 && i > 0 && i % (num_docs / 10) == 0 {
            println!("  Progress: {}%", (i * 100) / num_docs);
        }
    }

    let write_duration = write_start.elapsed();
    let write_tput = num_docs as f64 / write_duration.as_secs_f64();

    // Flush to ensure all data is written
    db.flush()?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let after_write_rss = get_process_rss_mb(pid);
    let write_memory_growth = after_write_rss - initial_rss;

    output.push_str(&format!(
        "After writes RSS: {} MB (growth: {} MB)\n",
        after_write_rss, write_memory_growth
    ));
    output.push_str(&format!(
        "Write time: {:.3}s\n",
        write_duration.as_secs_f64()
    ));
    output.push_str(&format!(
        "Write throughput: {:.0} docs/sec\n",
        write_tput
    ));

    // READ TEST - Hot Cache
    let hot_set_size = (num_docs / 10).max(10).min(5_000);
    for i in 0..hot_set_size {
        let key = format!("bench:doc_{:08}", i);
        let _ = db.get(&key)?;
    }

    let hot_read_count = hot_set_size;
    let hot_read_start = Instant::now();
    for i in 0..hot_read_count {
        let key = format!("bench:doc_{:08}", i);
        let _ = db.get(&key)?;
    }
    let hot_read_duration = hot_read_start.elapsed();
    let hot_read_tput = hot_read_count as f64 / hot_read_duration.as_secs_f64();

    // READ TEST - Mixed
    let mixed_read_count = num_docs.min(5_000);
    let mixed_read_start = Instant::now();
    for i in 0..mixed_read_count {
        let idx = if i % 10 < 7 { i % hot_set_size } else { i % num_docs };
        let key = format!("bench:doc_{:08}", idx);
        let _ = db.get(&key)?;
    }
    let mixed_read_duration = mixed_read_start.elapsed();
    let mixed_read_tput = mixed_read_count as f64 / mixed_read_duration.as_secs_f64();

    // Get database size on disk
    let db_size = get_dir_size_mb(temp_dir.path());

    let metrics = ScaleMetrics {
        scale: num_docs,
        write_tput,
        hot_read_tput,
        mixed_read_tput,
        rss_mb: after_write_rss,
        disk_mb: db_size,
    };

    output.push_str(&format!("Disk size: {} MB\n", db_size));
    output.push_str(&"-".repeat(80));
    output.push_str("\n");

    println!("{}", output);

    Ok((output, metrics))
}

fn get_process_rss_mb(pid: u32) -> i64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{}/status", pid)) {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<i64>() {
                            return kb / 1024;
                        }
                    }
                }
            }
        }
        return 0;
    }
    
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        let output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &pid.to_string()])
            .output();

        if let Ok(output) = output {
            let s = String::from_utf8_lossy(&output.stdout);
            if let Ok(kb) = s.trim().parse::<i64>() {
                return kb / 1024;
            }
        }
        return 0;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        0
    }
}

fn get_dir_size_mb(path: &std::path::Path) -> i64 {
    fn dir_size(path: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                } else if path.is_dir() {
                    total += dir_size(&path);
                }
            }
        }
        total
    }
    (dir_size(path) / (1024 * 1024)) as i64
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
