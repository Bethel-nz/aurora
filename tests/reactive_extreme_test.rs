use aurora_db::{Aurora, FieldType, Value};
use std::time::Instant;
use std::fs::File;
use std::io::Write as IoWrite;

#[tokio::test]
async fn test_reactive_extreme_benchmark() {
    let mut output = String::new();
    output.push_str("=".repeat(80).as_str());
    output.push_str("\nAURORA REACTIVE PERFORMANCE BENCHMARK\n");
    output.push_str("Scenario: High-Frequency Data Ingestion + Live Query Watcher\n");
    output.push_str(&format!(
        "Timestamp: {}\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    output.push_str("=".repeat(80).as_str());
    output.push_str("\n\n");

    println!("{}", output);

    let scales = vec![1, 10, 100, 1000, 10_000, 100_000]; // 1M can be added but may take time
    let mut benchmark_metrics = Vec::new();

    for scale in scales {
        println!("\n{}", "=".repeat(80));
        println!("Testing Reactive Scale: {} updates", format_number(scale));
        println!("{}", "=".repeat(80));

        let (result_text, tput) = run_reactive_benchmark(scale).await;
        output.push_str(&result_text);
        output.push_str("\n");
        benchmark_metrics.push((scale, tput));

        // Clean up between runs
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Save Reports
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let txt_filename = format!("reactive_benchmark_{}.txt", timestamp);
    let csv_filename = format!("reactive_benchmark_{}.csv", timestamp);

    let mut txt_file = File::create(&txt_filename).unwrap();
    txt_file.write_all(output.as_bytes()).unwrap();

    let mut csv_file = File::create(&csv_filename).unwrap();
    writeln!(csv_file, "Scale,ReactiveThroughput").unwrap();
    for (s, t) in benchmark_metrics {
        writeln!(csv_file, "{},{}", s, t).unwrap();
    }

    println!("\n{}", "=".repeat(80));
    println!("Results saved to: {} and {}", txt_filename, csv_filename);
    println!("{}", "=".repeat(80));
}

async fn run_reactive_benchmark(num_updates: usize) -> (String, f64) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join(format!("reactive_bench_{}.db", num_updates));
    
    // Create a new DB instance for each scale to ensure isolation
    let db: &'static Aurora = Box::leak(Box::new(
        Aurora::open(db_path).await.unwrap()
    ));

    let collection = "live_data";
    db.new_collection(collection, vec![
        ("val", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
    ] ).await.unwrap();

    // Create a watcher that matches everything
    let mut watcher = db.query(collection)
        .filter(|f: &aurora_db::query::FilterBuilder| f.gt("val", -1))
        .watch()
        .await
        .unwrap();

    // BACKGROUND: Receiver
    let receiver_handle = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_update) = watcher.next().await {
            count += 1;
            if count >= num_updates { break; }
        }
        count
    });

    let start = Instant::now();
    
    // MAIN: Sender
    for i in 0..num_updates {
        db.insert_into(collection, vec![
            ("val", Value::Int(i as i64)),
        ]).await.unwrap();
        
        // Pacing for high scales
        if num_updates >= 1000 && i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }
    
    let total_received = receiver_handle.await.unwrap();
    let duration = start.elapsed();
    let tput = total_received as f64 / duration.as_secs_f64();

    let mut result = String::new();
    result.push_str(&format!("Scale: {} updates\n", format_number(num_updates)));
    result.push_str(&format!("Total matches processed: {}\n", total_received));
    result.push_str(&format!("Processing time: {:.2?}\n", duration));
    result.push_str(&format!("Reactive throughput: {:.0} matches/sec\n", tput));
    result.push_str("-".repeat(80).as_str());

    println!("{}", result);
    (result, tput)
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
