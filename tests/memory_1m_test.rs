/// 1 Million Record Memory Test
///
/// Tests Aurora's memory usage with 1 million documents
use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::fs::File as StdFile;
use std::io::Write as IoWrite;
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

    let db = Arc::new(Aurora::with_config(config).await.unwrap());

    db.new_collection(
        "million_test",
        vec![
            (
                "data",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "index",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
        ],
    )
    .await
    .unwrap();

    // Get initial process info
    let pid = std::process::id();
    let initial_rss = get_process_rss_mb(pid);

    let mut report = String::new();
    report.push_str("==================================================\n");
    report.push_str("AURORA 1 MILLION RECORD MEMORY BENCHMARK\n");
    report.push_str(&format!(
        "Timestamp: {}\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    report.push_str("==================================================\n\n");
    report.push_str(&format!("Initial RSS: {} MB\n\n", initial_rss));

    println!("Initial RSS: {} MB", initial_rss);
    println!("Starting 1 million record insert...\n");

    // CSV Log Header
    let mut csv_log = String::from("Batch,TotalDocs,TimeSec,RSS_MB,GrowthMB,ThroughputDocsSec\n");

    // Insert 1M documents in batches
    let batch_size = 50000;
    let num_batches = 20; // 1M total documents

    let overall_start = Instant::now();

    for batch in 0..num_batches {
        let start = Instant::now();

        // Insert batch
        for i in 0..batch_size {
            let idx = batch * batch_size + i;
            db.insert_into(
                "million_test",
                vec![
                    (
                        "data",
                        Value::String(format!(
                            "Document number {} with some text content to make it realistic",
                            idx
                        )),
                    ),
                    ("index", Value::Int(idx as i64)),
                ],
            )
            .await
            .unwrap();
        }

        let duration = start.elapsed();
        let current_rss = get_process_rss_mb(pid);
        let growth = current_rss - initial_rss;
        let total_docs = (batch + 1) * batch_size;
        let throughput = batch_size as f64 / duration.as_secs_f64();

        let batch_info = format!(
            "Batch {:2}/{}: Inserted: {:7} | RSS: {:4} MB | Growth: {:4} MB | Tput: {:.0} docs/sec\n",
            batch + 1,
            num_batches,
            total_docs,
            current_rss,
            growth,
            throughput
        );

        report.push_str(&batch_info);
        print!("{}", batch_info);

        // Append to CSV log
        csv_log.push_str(&format!(
            "{},{},{:.3},{},{},{:.2}\n",
            batch + 1,
            total_docs,
            duration.as_secs_f64(),
            current_rss,
            growth,
            throughput
        ));

        // Flush periodically
        if (batch + 1) % 5 == 0 {
            db.flush().unwrap();
        }
    }

    let total_duration = overall_start.elapsed();

    // Final flush
    db.flush().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let final_rss = get_process_rss_mb(pid);
    let total_growth = final_rss - initial_rss;

    let summary = format!(
        "\n=== SUMMARY ===\n\
        Total Documents: 1,000,000\n\
        Total Time:      {:.2?}\n\
        Avg Throughput:  {:.0} docs/sec\n\
        Final RSS:       {} MB\n\
        Total Growth:    {} MB\n\
        Avg Growth/100k: {:.2} MB\n",
        total_duration,
        1_000_000.0 / total_duration.as_secs_f64(),
        final_rss,
        total_growth,
        total_growth as f64 / 10.0
    );

    report.push_str(&summary);
    println!("{}", summary);

    // Save Text Report
    let report_filename = "memory_benchmark_report.txt";
    let mut f_txt = StdFile::create(report_filename).unwrap();
    f_txt.write_all(report.as_bytes()).unwrap();

    // Save CSV log
    let log_filename = "memory_benchmark_batches.csv";
    let mut f_csv = StdFile::create(log_filename).unwrap();
    f_csv.write_all(csv_log.as_bytes()).unwrap();

    println!("Results saved to:");
    println!("  - TEXT: {}", report_filename);
    println!("  - CSV:  {}", log_filename);

    // Query sample to verify
    let sample = db
        .query("million_test")
        .filter(|f: &aurora_db::query::FilterBuilder| f.gt("index", Value::Int(900_000)))
        .limit(100)
        .collect()
        .await
        .unwrap();

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

    #[cfg(target_os = "windows")]
    {
        return 0;
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = pid;
        0
    }
}
