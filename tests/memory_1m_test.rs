/// 1 Million Record Memory Test
///
/// Tests Aurora's memory usage with 1 million documents

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::Arc;
use std::time::Instant;

#[tokio::test]
async fn test_1_million_records_memory() {
    println!("\n=== 1 MILLION RECORDS MEMORY TEST ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("1m_test.aurora");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.enable_write_buffering = true;
    config.enable_wal = true;
    config.hot_cache_size_mb = 256; // Larger cache for 1M records

    let db = Arc::new(Aurora::with_config(config).unwrap());

    db.new_collection("million_test", vec![
        ("data", FieldType::String, false),
        ("index", FieldType::Int, false),
    ]).unwrap();

    // Get initial process info
    let pid = std::process::id();
    let initial_rss = get_process_rss_mb(pid);
    println!("Initial RSS: {} MB", initial_rss);
    println!("Starting 1 million record insert...\n");

    // Insert 1M documents in batches
    let batch_size = 50000;
    let num_batches = 20; // 1M total documents

    let overall_start = Instant::now();

    for batch in 0..num_batches {
        println!("Batch {}/{}:", batch + 1, num_batches);

        let start = Instant::now();

        // Insert batch
        for i in 0..batch_size {
            let idx = batch * batch_size + i;
            db.insert_into("million_test", vec![
                ("data", Value::String(format!("Document number {} with some text content to make it realistic", idx))),
                ("index", Value::Int(idx as i64)),
            ]).await.unwrap();
        }

        let duration = start.elapsed();
        let current_rss = get_process_rss_mb(pid);
        let growth = current_rss - initial_rss;

        println!("  Inserted: {} documents", (batch + 1) * batch_size);
        println!("  Time: {:.2?}", duration);
        println!("  Current RSS: {} MB", current_rss);
        println!("  Growth: {} MB", growth);
        println!("  Throughput: {:.2} docs/sec", batch_size as f64 / duration.as_secs_f64());
        println!("  Avg per 100k docs: {:.2} MB\n", growth as f64 / ((batch + 1) * batch_size / 100000) as f64);

        // Flush periodically
        if (batch + 1) % 5 == 0 {
            println!("  Flushing...");
            db.flush().unwrap();
        }
    }

    let total_duration = overall_start.elapsed();

    // Final flush
    println!("Final flush...");
    db.flush().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let final_rss = get_process_rss_mb(pid);
    let total_growth = final_rss - initial_rss;

    println!("\n=== SUMMARY ===");
    println!("Initial RSS: {} MB", initial_rss);
    println!("Final RSS: {} MB", final_rss);
    println!("Total growth: {} MB", total_growth);
    println!("Documents: 1,000,000");
    println!("Total time: {:.2?}", total_duration);
    println!("Average throughput: {:.2} docs/sec", 1_000_000.0 / total_duration.as_secs_f64());
    println!("Average per 100k docs: {:.2} MB", total_growth as f64 / 10.0);
    println!("Average per 1M docs: {} MB", total_growth);

    // Query sample to verify
    let query_start = Instant::now();
    let sample = db.query("million_test")
        .filter(|f| f.gt("index", Value::Int(900_000)))
        .limit(100)
        .collect()
        .await
        .unwrap();
    let query_time = query_start.elapsed();

    println!("\nQuery verification (last 100k): {} results in {:.2?}", sample.len(), query_time);

    // Get database size on disk
    let db_size = get_dir_size_mb(temp_dir.path());
    println!("Database size on disk: {} MB", db_size);
    println!("Compression ratio: {:.2}x", total_growth as f64 / db_size as f64);

    assert!(sample.len() > 0, "Should be able to query data");
}

// Helper function to get process RSS in MB
fn get_process_rss_mb(pid: u32) -> i64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{}/status", pid)) {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<i64>() {
                            return kb / 1024; // Convert to MB
                        }
                    }
                }
            }
        }
    }
    0
}

// Helper to get directory size
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
