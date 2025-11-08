/// Aurora Baseline Performance Benchmark
/// Tests memory usage and performance at different scales
/// Run before implementing DiskLocation optimization

use aurora_db::{Aurora, types::{AuroraConfig, FieldType, Value}};
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
    output.push_str(&format!("Timestamp: {}\n", chrono::Local::now().format("%Y-%m-%d %H:%M:%S")));
    output.push_str("=".repeat(80).as_str());
    output.push_str("\n\n");

    println!("{}", output);

    let scales = vec![1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for scale in scales {
        println!("\n{}", "=".repeat(80));
        println!("Testing with {} documents", format_number(scale));
        println!("{}", "=".repeat(80));

        let result = benchmark_scale(scale).await?;
        output.push_str(&result);
        output.push_str("\n");

        // Clean up between runs
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Write results to file
    let filename = format!("aurora_baseline_{}.txt",
        chrono::Local::now().format("%Y%m%d_%H%M%S"));
    let mut file = File::create(&filename)?;
    file.write_all(output.as_bytes())?;

    println!("\n{}", "=".repeat(80));
    println!("Results saved to: {}", filename);
    println!("{}", "=".repeat(80));

    Ok(())
}

async fn benchmark_scale(num_docs: usize) -> Result<String, Box<dyn std::error::Error>> {
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

    let db = Arc::new(Aurora::with_config(config)?);

    // Create collection
    db.new_collection("bench", vec![
        ("id", FieldType::String, false),
        ("data", FieldType::String, false),
        ("index", FieldType::Int, false),
        ("timestamp", FieldType::String, false),
    ])?;

    // Get initial memory
    let pid = std::process::id();
    let initial_rss = get_process_rss_mb(pid);

    output.push_str(&format!("Scale: {} documents\n", format_number(num_docs)));
    output.push_str(&format!("Initial RSS: {} MB\n", initial_rss));

    // WRITE TEST
    let write_start = Instant::now();

    for i in 0..num_docs {
        db.insert_into("bench", vec![
            ("id", Value::String(format!("doc_{:08}", i))),
            ("data", Value::String(format!("Document {} with some sample text content to simulate realistic data size", i))),
            ("index", Value::Int(i as i64)),
            ("timestamp", Value::String(chrono::Local::now().to_rfc3339())),
        ]).await?;

        // Progress indicator for large scales
        if num_docs >= 10_000 && i > 0 && i % (num_docs / 10) == 0 {
            println!("  Progress: {}%", (i * 100) / num_docs);
        }
    }

    let write_duration = write_start.elapsed();

    // Flush to ensure all data is written
    db.flush()?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let after_write_rss = get_process_rss_mb(pid);
    let write_memory_growth = after_write_rss - initial_rss;

    // Calculate average document size
    let avg_doc_size_bytes = if num_docs > 0 {
        (write_memory_growth as f64 * 1024.0 * 1024.0) / num_docs as f64
    } else {
        0.0
    };

    output.push_str(&format!("After writes RSS: {} MB (growth: {} MB)\n",
        after_write_rss, write_memory_growth));
    output.push_str(&format!("Write time: {:.3}s\n", write_duration.as_secs_f64()));
    output.push_str(&format!("Write throughput: {:.0} docs/sec\n",
        num_docs as f64 / write_duration.as_secs_f64()));
    output.push_str(&format!("Avg memory per doc: {:.0} bytes\n", avg_doc_size_bytes));

    // READ TEST (sample reads)
    let read_count = num_docs.min(10_000);
    let read_start = Instant::now();

    for i in 0..read_count {
        let idx = (i * num_docs) / read_count; // Distributed sampling
        let key = format!("bench:doc_{:08}", idx);
        let _ = db.get(&key)?;
    }

    let read_duration = read_start.elapsed();
    let read_throughput = read_count as f64 / read_duration.as_secs_f64();

    output.push_str(&format!("Read test: {} samples in {:.3}s ({:.0} reads/sec)\n",
        format_number(read_count), read_duration.as_secs_f64(), read_throughput));

    // QUERY TEST
    let query_start = Instant::now();
    let results = db.query("bench")
        .filter(|f| f.gt("index", Value::Int(num_docs as i64 / 2)))
        .limit(100)
        .collect()
        .await?;
    let query_duration = query_start.elapsed();

    output.push_str(&format!("Query test: {} results in {:.3}s\n",
        results.len(), query_duration.as_secs_f64()));

    // Get database size on disk
    let db_size = get_dir_size_mb(temp_dir.path());
    let compression_ratio = if db_size > 0 {
        write_memory_growth as f64 / db_size as f64
    } else {
        0.0
    };

    output.push_str(&format!("Disk size: {} MB\n", db_size));
    output.push_str(&format!("Memory/Disk ratio: {:.2}x\n", compression_ratio));

    // Cache stats
    let cache_stats = db.get_cache_stats();
    output.push_str(&format!("Cache: {} items, {:.2} MB, {:.1}% hit rate\n",
        cache_stats.item_count,
        cache_stats.memory_usage as f64 / (1024.0 * 1024.0),
        cache_stats.hit_ratio * 100.0));

    output.push_str(&"-".repeat(80));
    output.push_str("\n");

    println!("{}", output);

    Ok(output)
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
    }
    0
}

fn get_dir_size_mb(path: &std::path::Path) -> i64 {
    let output = std::process::Command::new("du")
        .arg("-sm")
        .arg(path)
        .output();

    if let Ok(output) = output {
        let output_str = String::from_utf8_lossy(&output.stdout);
        if let Some(size_str) = output_str.split_whitespace().next() {
            return size_str.parse().unwrap_or(0);
        }
    }
    0
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
