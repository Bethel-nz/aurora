use aurora_db::{Aurora, FieldType, Value};
use std::time::Instant;
use std::fs::File as StdFile;
use std::io::Write as IoWrite;

#[tokio::test]
async fn test_reactive_latency_and_throughput() {
    println!("\n=== AURORA REACTIVE PERFORMANCE TEST ===");
    
    let temp_dir = tempfile::tempdir().unwrap();
    let db: &'static Aurora = Box::leak(Box::new(
        Aurora::open(temp_dir.path().join("reactive_perf.db")).await.unwrap()
    ));

    db.new_collection("market_data", vec![
        ("symbol", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ("price", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_FLOAT, unique: false, indexed: false, nullable: true, validations: vec![] }),
    ] ).await.unwrap();

    let symbol = "AUR";
    let target_price = 100.0;
    
    // Setup Watcher
    let mut watcher = db.query("market_data")
        .filter(move |f| f.eq("symbol", symbol) & f.gt("price", target_price))
        .watch()
        .await
        .unwrap();

    println!("Watcher established. Starting high-frequency writes...");

    let num_updates = 5_000;

    // BACKGROUND: Subscriber receiving updates
    let receiver_handle = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_update) = watcher.next().await {
            count += 1;
            if count >= num_updates { break; }
        }
        count
    });

    // MAIN: Hammering the DB with updates
    let start = Instant::now();
    for i in 0..num_updates {
        db.insert_into("market_data", vec![
            ("symbol", Value::String(symbol.to_string())),
            ("price", Value::Float(target_price + 1.0 + (i as f64))),
        ]).await.unwrap();
    }
    
    let write_duration = start.elapsed();
    let total_received = receiver_handle.await.unwrap();
    let total_duration = start.elapsed();

    let write_tput = num_updates as f64 / write_duration.as_secs_f64();
    let react_tput = total_received as f64 / total_duration.as_secs_f64();
    let avg_latency = total_duration / total_received as u32;

    let mut report = String::new();
    report.push_str("==================================================\n");
    report.push_str("AURORA REACTIVE PERFORMANCE REPORT\n");
    report.push_str(&format!("Timestamp: {}\n", chrono::Local::now().format("%Y-%m-%d %H:%M:%S")));
    report.push_str("==================================================\n\n");
    report.push_str(&format!("Total Reactive Events: {}\n", total_received));
    report.push_str(&format!("Overall Test Time:     {:.2?}\n", total_duration));
    report.push_str(&format!("Write Throughput:      {:.0} events/sec\n", write_tput));
    report.push_str(&format!("Reactive Throughput:   {:.0} matches/sec\n", react_tput));
    report.push_str(&format!("Avg Latency:           {:.2?} (End-to-End)\n", avg_latency));
    report.push_str("\nArchitectural Advantage: Push-based delta matching\n");

    println!("\n{}", report);

    // Save to file
    let report_filename = "reactive_benchmark_report.txt";
    let mut f = StdFile::create(report_filename).unwrap();
    f.write_all(report.as_bytes()).unwrap();
    println!("Report saved to: {}", report_filename);
    
    assert!(total_received > 0);
}
