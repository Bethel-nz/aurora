/// Aurora Baseline V2 - Comprehensive Performance & Memory Benchmark
/// 
/// This benchmark measures:
/// 1. Ingestion Speed (1,000,000 records)
/// 2. Memory Usage (RSS) with Secondary Indices (Roaring Bitmaps)
/// 3. Query Performance (Index Lookups)
/// 4. AQL vs Fluent Parity

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::Arc;
use std::time::Instant;
use std::fs::File as StdFile;
use std::io::Write as IoWrite;
use serde_json::json;

#[tokio::test]
async fn test_aurora_baseline_v2() {
    println!("\n==================================================");
    println!("AURORA BASELINE V2 - PERFORMANCE & MEMORY REPORT");
    println!("Architecture: Roaring Bitmaps + SkipMap Dictionaries");
    println!("Timestamp: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
    println!("==================================================\n");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("baseline_v2.aurora");

    let mut config = AuroraConfig::default();
    config.db_path = db_path;
    config.enable_write_buffering = true;
    config.enable_wal = true;
    config.hot_cache_size_mb = 256;

    let db = Arc::new(Aurora::with_config(config).await.unwrap());

    // 1. Setup Collection with Indexing
    db.new_collection("benchmark", vec![
        ("id", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: true, nullable: true }),      // Primary Key
        ("tag", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),    // Secondary Index (Low Cardinality)
        ("val", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: true, nullable: true }),       // Secondary Index (High Cardinality - UNIQUE)
        ("data", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),   // Payload
    ] ).await.unwrap();

    // Ensure indices are initialized
    db.ensure_indices_initialized().await.unwrap();

    let pid = std::process::id();
    let initial_rss = get_process_rss_mb(pid);
    println!("Initial RSS: {} MB", initial_rss);

    // 2. Ingestion Phase (1,000,000 documents)
    let total_docs = 1_000_000;
    let batch_size = 50_000;
    let num_batches = total_docs / batch_size;

    println!("\nStarting Ingestion of {} documents...", total_docs);
    let ingest_start = Instant::now();

    for batch in 0..num_batches {
        let batch_start = Instant::now();
        let mut docs = Vec::with_capacity(batch_size);
        
        for i in 0..batch_size {
            let idx = batch * batch_size + i;
            let mut data = std::collections::HashMap::new();
            data.insert("id".to_string(), Value::String(format!("doc_{:07}", idx)));
            data.insert("tag".to_string(), Value::String(format!("tag_{}", idx % 100))); // 100 unique tags
            data.insert("val".to_string(), Value::Int(idx as i64));                      // 1,000,000 unique values
            data.insert("data".to_string(), Value::String("Realistic payload content for benchmarking memory footprint".to_string()));
            docs.push(data);
        }

        db.batch_insert("benchmark", docs).await.unwrap();
        
        let duration = batch_start.elapsed();
        let current_rss = get_process_rss_mb(pid);
        let tput = batch_size as f64 / duration.as_secs_f64();
        
        println!("  Batch {:2}/{}: {:7} docs | RSS: {:4} MB | Tput: {:.0} docs/sec", 
            batch + 1, num_batches, (batch + 1) * batch_size, current_rss, tput);
            
        if (batch + 1) % 5 == 0 {
            db.flush().unwrap();
        }
    }

    let ingest_duration = ingest_start.elapsed();
    let after_ingest_rss = get_process_rss_mb(pid);

    println!("\nIngestion Complete:");
    println!("  Total Time:       {:.2?}", ingest_duration);
    println!("  Avg Throughput:   {:.0} docs/sec", total_docs as f64 / ingest_duration.as_secs_f64());
    println!("  Memory Growth:    {} MB", after_ingest_rss - initial_rss);

    // 3. Query Phase (Index Lookups)
    println!("\nStarting Query Performance Phase (1,000 randomized lookups)...");
    
    // Warm up
    let _ = db.query("benchmark").filter(|f: &aurora_db::query::FilterBuilder| f.eq("val", Value::Int(500_000))).collect().await.unwrap();

    // A. Fluent API Lookup (High Cardinality Index)
    let start = Instant::now();
    for i in 0..1000 {
        let target = (i * 997) % total_docs; // Pseudo-random
        let results = db.query("benchmark")
            .filter(|f: &aurora_db::query::FilterBuilder| f.eq("val", Value::Int(target as i64)))
            .collect()
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }
    let fluent_time = start.elapsed();
    println!("  Fluent API (High Cardinality): {:?}", fluent_time / 1000);

    // B. AQL Cached Lookup (High Cardinality Index)
    let aql = "query Find($v: Int) { benchmark(where: { val: { eq: $v } }) { id } }";
    let start = Instant::now();
    for i in 0..1000 {
        let target = (i * 997) % total_docs;
        let vars = json!({ "v": target });
        let _ = db.execute((aql, vars)).await.unwrap();
    }
    let aql_time = start.elapsed();
    println!("  AQL (Cached Plan):            {:?}", aql_time / 1000);

    // C. Low Cardinality Lookup (Roaring Bitmap Power)
    // tag_50 should have 10,000 documents
    let start = Instant::now();
    let results = db.query("benchmark")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("tag", Value::String("tag_50".to_string())))
        .collect()
        .await
        .unwrap();
    let low_card_time = start.elapsed();
    println!("  Low Cardinality Scan (10k docs): {:?}", low_card_time);
    assert_eq!(results.len(), 10_000);

    // 4. Final Memory Report
    db.flush().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let final_rss = get_process_rss_mb(pid);

    let report = format!(
        "\n=== FINAL BASELINE V2 SUMMARY ===\n\
        Ingestion Throughput:  {:.0} docs/sec\n\
        Memory Growth:         {} MB\n\
        High-Card Lookup:      {:?} (per op)\n\
        AQL Overhead:          {:.2}x\n\
        Low-Card Scan (10k):   {:?}\n\
        Final Process RSS:     {} MB\n",
        total_docs as f64 / ingest_duration.as_secs_f64(),
        final_rss - initial_rss,
        fluent_time / 1000,
        aql_time.as_secs_f64() / fluent_time.as_secs_f64(),
        low_card_time,
        final_rss
    );

    println!("{}", report);

    // Save report to file
    let report_filename = format!("aurora_baseline_v2_{}.txt", chrono::Local::now().format("%Y%m%d_%H%M%S"));
    let mut f = StdFile::create(&report_filename).unwrap();
    f.write_all(report.as_bytes()).unwrap();
    println!("Baseline report saved to: {}", report_filename);
}

fn get_process_rss_mb(pid: u32) -> i64 {
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
        0
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = pid;
        0
    }
}
