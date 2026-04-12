use aurora_db::{Aurora, FieldType, Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_pubsub_real_world_fanout() {
    println!("\n=== AURORA PUBSUB REAL-WORLD FAN-OUT TEST ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db: &'static Aurora = Box::leak(Box::new(
        Aurora::open(temp_dir.path().join("pubsub_fanout.db"))
            .await
            .unwrap(),
    ));

    db.new_collection(
        "chat_room",
        vec![
            (
                "user",
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
                "message",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
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

    let num_subscribers = 100; // 100 active chat users
    let messages_to_send = 1_000;

    let barrier = Arc::new(Barrier::new(num_subscribers + 1));
    let mut handles = Vec::new();

    println!("Spawning {} subscribers...", num_subscribers);

    for _ in 0..num_subscribers {
        let b = barrier.clone();
        let mut listener = db.listen("chat_room");

        handles.push(tokio::spawn(async move {
            b.wait().await; // Sync start
            let mut received = 0;
            while let Ok(_event) = listener.recv().await {
                received += 1;
                if received >= messages_to_send {
                    break;
                }
            }
            received
        }));
    }

    barrier.wait().await; // Start all at once
    let start = Instant::now();

    println!(
        "Sending {} messages to {} users...",
        messages_to_send, num_subscribers
    );

    for i in 0..messages_to_send {
        db.insert_into(
            "chat_room",
            vec![
                ("user", Value::String(format!("User_{}", i % 10))),
                ("message", Value::String("Hello everyone!".into())),
            ],
        )
        .await
        .unwrap();
    }

    let mut actual_deliveries = 0;
    for h in handles {
        actual_deliveries += h.await.unwrap();
    }

    let duration = start.elapsed();
    let tput = actual_deliveries as f64 / duration.as_secs_f64();

    println!("\n=== RESULTS ===");
    println!("Subscribers:         {}", num_subscribers);
    println!("Messages Sent:       {}", messages_to_send);
    println!("Total Deliveries:    {}", actual_deliveries);
    println!("Total Time:          {:.2?}", duration);
    println!("Delivery Throughput: {:.0} msgs/sec (Fan-out)", tput);

    let mut report = String::new();
    report.push_str("==================================================\n");
    report.push_str("AURORA PUBSUB FAN-OUT REPORT\n");
    report.push_str(&format!(
        "Timestamp: {}\n",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
    ));
    report.push_str("==================================================\n\n");
    report.push_str(&format!("Subscribers:         {}\n", num_subscribers));
    report.push_str(&format!("Messages Sent:       {}\n", messages_to_send));
    report.push_str(&format!("Total Deliveries:    {}\n", actual_deliveries));
    report.push_str(&format!("Delivery Throughput: {:.0} msgs/sec\n", tput));

    std::fs::write("pubsub_fanout_report.txt", report).unwrap();
}
