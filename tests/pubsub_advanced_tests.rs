/// Advanced PubSub Tests
///
/// These tests verify Aurora's PubSub system under stress, including:
/// - High fan-out scenarios (one event -> many subscribers)
/// - Slow consumer handling
/// - Backpressure behavior
/// - Subscriber churn (rapid subscribe/unsubscribe)
/// - Event ordering guarantees
/// - Message delivery under load

use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn create_test_db(path: std::path::PathBuf) -> Result<Aurora, aurora_db::AuroraError> {
    let mut config = AuroraConfig::default();
    config.db_path = path;
    config.enable_wal = false; // Not needed for PubSub tests
    config.enable_write_buffering = false;

    Aurora::with_config(config)
}

#[tokio::test]
async fn test_high_fanout_single_event() {
    println!("\n=== High Fan-Out Test: Single Event to Many Subscribers ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("fanout.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("events", vec![
        ("type", FieldType::String, false),
        ("data", FieldType::String, false),
    ]).unwrap();

    let num_subscribers = 100;
    let num_events = 10;

    println!("Creating {} subscribers for {} events", num_subscribers, num_events);

    let received_counts = Arc::new(AtomicUsize::new(0));
    let mut listener_tasks = vec![];

    // Spawn all subscribers
    for i in 0..num_subscribers {
        let mut listener = db.listen("events");
        let received_counts_clone = Arc::clone(&received_counts);

        listener_tasks.push(tokio::spawn(async move {
            let mut local_count = 0;
            let timeout = sleep(Duration::from_secs(5));
            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    result = listener.recv() => {
                        if result.is_ok() {
                            local_count += 1;
                            received_counts_clone.fetch_add(1, Ordering::Relaxed);
                            if local_count >= num_events {
                                break;
                            }
                        }
                    }
                    _ = &mut timeout => {
                        break;
                    }
                }
            }

            (i, local_count)
        }));
    }

    // Give subscribers time to set up
    sleep(Duration::from_millis(100)).await;

    // Publish events and measure fan-out latency
    let publish_start = Instant::now();
    for i in 0..num_events {
        db.insert_into("events", vec![
            ("type", Value::String("test_event".to_string())),
            ("data", Value::String(format!("event_{}", i))),
        ]).await.unwrap();
    }
    let publish_duration = publish_start.elapsed();

    println!("Published {} events in {:?}", num_events, publish_duration);

    // Wait for all subscribers to receive events
    sleep(Duration::from_millis(500)).await;

    let mut total_received = 0;
    let mut subscribers_with_all_events = 0;

    for task in listener_tasks {
        if let Ok((id, count)) = task.await {
            total_received += count;
            if count == num_events {
                subscribers_with_all_events += 1;
            }
            if count < num_events {
                println!("  Subscriber {} received only {}/{} events", id, count, num_events);
            }
        }
    }

    println!("\nResults:");
    println!("  Expected total: {}", num_subscribers * num_events);
    println!("  Actual received: {}", total_received);
    println!("  Subscribers receiving all events: {}/{}", subscribers_with_all_events, num_subscribers);
    println!("  Delivery rate: {:.1}%", (total_received as f64 / (num_subscribers * num_events) as f64) * 100.0);

    // Allow some message loss due to channel buffer limits, but should get most
    assert!(total_received >= (num_subscribers * num_events) * 8 / 10,
            "Should deliver at least 80% of messages (got {}/{})",
            total_received, num_subscribers * num_events);

    println!("✓ High fan-out test completed");
}

#[tokio::test]
async fn test_slow_consumer_backpressure() {
    println!("\n=== Slow Consumer Backpressure Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("slow_consumer.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("backpressure", vec![
        ("id", FieldType::String, false),
    ]).unwrap();

    // Create fast and slow consumers
    let mut fast_listener = db.listen("backpressure");
    let mut slow_listener = db.listen("backpressure");

    let fast_received = Arc::new(AtomicUsize::new(0));
    let slow_received = Arc::new(AtomicUsize::new(0));

    let fast_count = Arc::clone(&fast_received);
    let fast_task = tokio::spawn(async move {
        let timeout = sleep(Duration::from_secs(3));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                result = fast_listener.recv() => {
                    if result.is_ok() {
                        fast_count.fetch_add(1, Ordering::Relaxed);
                        // Fast consumer: no delay
                    }
                }
                _ = &mut timeout => break,
            }
        }
    });

    let slow_count = Arc::clone(&slow_received);
    let slow_task = tokio::spawn(async move {
        let timeout = sleep(Duration::from_secs(3));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                result = slow_listener.recv() => {
                    if result.is_ok() {
                        slow_count.fetch_add(1, Ordering::Relaxed);
                        // Slow consumer: add significant delay
                        sleep(Duration::from_millis(50)).await;
                    }
                }
                _ = &mut timeout => break,
            }
        }
    });

    // Give listeners time to set up
    sleep(Duration::from_millis(100)).await;

    // Rapidly publish many events
    let num_events = 100;
    println!("Publishing {} events rapidly...", num_events);

    let publish_start = Instant::now();
    for i in 0..num_events {
        db.insert_into("backpressure", vec![
            ("id", Value::String(format!("event_{}", i))),
        ]).await.unwrap();
    }
    let publish_duration = publish_start.elapsed();

    println!("Published in {:?}", publish_duration);

    // Wait for processing
    let _ = fast_task.await;
    let _ = slow_task.await;

    let fast_total = fast_received.load(Ordering::Relaxed);
    let slow_total = slow_received.load(Ordering::Relaxed);

    println!("\nResults:");
    println!("  Events published: {}", num_events);
    println!("  Fast consumer received: {}", fast_total);
    println!("  Slow consumer received: {}", slow_total);

    // Fast consumer should get most/all messages
    assert!(fast_total >= num_events * 8 / 10, "Fast consumer should receive most messages");

    // Slow consumer will likely miss messages due to backpressure
    println!("  Slow consumer received {:.1}% of messages", (slow_total as f64 / num_events as f64) * 100.0);

    println!("✓ Backpressure behavior observed");
}

#[tokio::test]
async fn test_subscriber_churn() {
    println!("\n=== Subscriber Churn Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("churn.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("churn_test", vec![
        ("counter", FieldType::Int, false),
    ]).unwrap();

    let events_published = Arc::new(AtomicUsize::new(0));
    let total_received = Arc::new(AtomicUsize::new(0));

    println!("Testing rapid subscribe/unsubscribe while publishing events");

    // Publisher task
    let db_pub = Arc::clone(&db);
    let events_count = Arc::clone(&events_published);
    let publisher = tokio::spawn(async move {
        for i in 0..100 {
            db_pub.insert_into("churn_test", vec![
                ("counter", Value::Int(i)),
            ]).await.unwrap();
            events_count.fetch_add(1, Ordering::Relaxed);
            sleep(Duration::from_millis(10)).await;
        }
    });

    // Subscriber churn task
    let mut churn_tasks = vec![];
    for _ in 0..20 {
        let db_sub = Arc::clone(&db);
        let received = Arc::clone(&total_received);

        churn_tasks.push(tokio::spawn(async move {
            let mut local_received = 0;

            // Subscribe and unsubscribe multiple times
            for _ in 0..3 {
                let mut listener = db_sub.listen("churn_test");

                // Listen for a short time
                let timeout = sleep(Duration::from_millis(200));
                tokio::pin!(timeout);

                loop {
                    tokio::select! {
                        result = listener.recv() => {
                            if result.is_ok() {
                                local_received += 1;
                            }
                        }
                        _ = &mut timeout => break,
                    }
                }

                // Drop listener (unsubscribe)
                drop(listener);

                // Brief pause before resubscribing
                sleep(Duration::from_millis(50)).await;
            }

            received.fetch_add(local_received, Ordering::Relaxed);
            local_received
        }));
    }

    // Wait for publisher
    publisher.await.unwrap();

    // Wait for all subscribers
    for task in churn_tasks {
        let _ = task.await;
    }

    let published = events_published.load(Ordering::Relaxed);
    let received = total_received.load(Ordering::Relaxed);

    println!("\nResults:");
    println!("  Events published: {}", published);
    println!("  Total messages received across all subscribers: {}", received);
    println!("  Listener count after churn: {}", db.listener_count("churn_test"));

    // System should handle churn without crashes
    assert_eq!(published, 100, "All events should be published");

    println!("✓ Subscriber churn handled correctly");
}

#[tokio::test]
async fn test_event_ordering() {
    println!("\n=== Event Ordering Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ordering.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("ordered", vec![
        ("seq", FieldType::Int, false),
    ]).unwrap();

    let mut listener = db.listen("ordered");

    // Spawn receiver task
    let receiver = tokio::spawn(async move {
        let mut received_sequence = vec![];
        let timeout = sleep(Duration::from_secs(2));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                result = listener.recv() => {
                    if let Ok(event) = result {
                        if let Some(doc) = event.document {
                            if let Some(Value::Int(seq)) = doc.data.get("seq") {
                                received_sequence.push(*seq);
                            }
                        }
                    }
                }
                _ = &mut timeout => break,
            }
        }

        received_sequence
    });

    // Give receiver time to set up
    sleep(Duration::from_millis(50)).await;

    // Publish events in order
    let num_events = 50;
    println!("Publishing {} events in sequence...", num_events);

    for i in 0..num_events {
        db.insert_into("ordered", vec![
            ("seq", Value::Int(i)),
        ]).await.unwrap();
    }

    // Wait for receiver
    let received = receiver.await.unwrap();

    println!("\nResults:");
    println!("  Published: {} events", num_events);
    println!("  Received: {} events", received.len());

    // Check for ordering
    let mut out_of_order = 0;
    for i in 1..received.len() {
        if received[i] < received[i - 1] {
            out_of_order += 1;
        }
    }

    println!("  Out of order events: {}", out_of_order);

    if out_of_order > 0 {
        println!("  First 20 received: {:?}", &received[..20.min(received.len())]);
    }

    // Events should be received in order
    assert_eq!(out_of_order, 0, "Events should be received in order");
    println!("✓ Event ordering preserved");
}

#[tokio::test]
async fn test_massive_fanout_under_load() {
    println!("\n=== Massive Fan-Out Under Load Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("massive_fanout.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    db.new_collection("massive", vec![
        ("id", FieldType::String, false),
    ]).unwrap();

    let num_subscribers = 200;
    let num_events = 50;

    println!("Creating {} subscribers...", num_subscribers);

    let received_counts = Arc::new(AtomicUsize::new(0));
    let mut tasks = vec![];

    for i in 0..num_subscribers {
        let mut listener = db.listen("massive");
        let counts = Arc::clone(&received_counts);

        tasks.push(tokio::spawn(async move {
            let mut local_count = 0;
            let timeout = sleep(Duration::from_secs(10));
            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    result = listener.recv() => {
                        if result.is_ok() {
                            local_count += 1;
                            counts.fetch_add(1, Ordering::Relaxed);

                            // Some subscribers are slow
                            if i % 10 == 0 {
                                sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }
                    _ = &mut timeout => break,
                }
            }

            local_count
        }));
    }

    sleep(Duration::from_millis(200)).await;

    println!("Publishing {} events to {} subscribers...", num_events, num_subscribers);

    let start = Instant::now();

    for i in 0..num_events {
        db.insert_into("massive", vec![
            ("id", Value::String(format!("msg_{}", i))),
        ]).await.unwrap();
    }

    let publish_duration = start.elapsed();
    println!("Published in {:?}", publish_duration);

    // Give time for message propagation
    sleep(Duration::from_millis(500)).await;

    // Collect results
    let mut total_received = 0;
    let mut min_received = usize::MAX;
    let mut max_received = 0;

    for task in tasks {
        if let Ok(count) = task.await {
            total_received += count;
            min_received = min_received.min(count);
            max_received = max_received.max(count);
        }
    }

    println!("\nResults:");
    println!("  Expected total: {}", num_subscribers * num_events);
    println!("  Actual received: {}", total_received);
    println!("  Min per subscriber: {}", min_received);
    println!("  Max per subscriber: {}", max_received);
    println!("  Average per subscriber: {:.1}", total_received as f64 / num_subscribers as f64);
    println!("  Delivery rate: {:.1}%", (total_received as f64 / (num_subscribers * num_events) as f64) * 100.0);

    // With massive fan-out and slow consumers, expect some message loss
    // but should still deliver a reasonable percentage
    let delivery_rate = (total_received as f64 / (num_subscribers * num_events) as f64) * 100.0;
    assert!(delivery_rate >= 60.0, "Should deliver at least 60% with massive fan-out (got {:.1}%)", delivery_rate);

    println!("✓ Massive fan-out test completed");
}

#[tokio::test]
async fn test_multiple_collections_pubsub() {
    println!("\n=== Multiple Collections PubSub Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("multi_collection.aurora");
    let db = Arc::new(create_test_db(db_path).unwrap());

    // Create multiple collections
    db.new_collection("users", vec![("name", FieldType::String, false)]).unwrap();
    db.new_collection("orders", vec![("order_id", FieldType::String, false)]).unwrap();
    db.new_collection("products", vec![("product_id", FieldType::String, false)]).unwrap();

    // Create listeners for each collection
    let mut user_listener = db.listen("users");
    let mut order_listener = db.listen("orders");
    let mut product_listener = db.listen("products");

    let user_count = Arc::new(AtomicUsize::new(0));
    let order_count = Arc::new(AtomicUsize::new(0));
    let product_count = Arc::new(AtomicUsize::new(0));

    // Spawn listener tasks
    let u_count = Arc::clone(&user_count);
    let user_task = tokio::spawn(async move {
        let timeout = sleep(Duration::from_secs(2));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                result = user_listener.recv() => {
                    if result.is_ok() {
                        u_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ = &mut timeout => break,
            }
        }
    });

    let o_count = Arc::clone(&order_count);
    let order_task = tokio::spawn(async move {
        let timeout = sleep(Duration::from_secs(2));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                result = order_listener.recv() => {
                    if result.is_ok() {
                        o_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ = &mut timeout => break,
            }
        }
    });

    let p_count = Arc::clone(&product_count);
    let product_task = tokio::spawn(async move {
        let timeout = sleep(Duration::from_secs(2));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                result = product_listener.recv() => {
                    if result.is_ok() {
                        p_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ = &mut timeout => break,
            }
        }
    });

    sleep(Duration::from_millis(50)).await;

    // Publish events to different collections
    println!("Publishing events to multiple collections...");

    for i in 0..20 {
        db.insert_into("users", vec![
            ("name", Value::String(format!("user_{}", i))),
        ]).await.unwrap();

        db.insert_into("orders", vec![
            ("order_id", Value::String(format!("order_{}", i))),
        ]).await.unwrap();

        db.insert_into("products", vec![
            ("product_id", Value::String(format!("product_{}", i))),
        ]).await.unwrap();
    }

    // Wait for processing
    user_task.await.unwrap();
    order_task.await.unwrap();
    product_task.await.unwrap();

    let users_received = user_count.load(Ordering::Relaxed);
    let orders_received = order_count.load(Ordering::Relaxed);
    let products_received = product_count.load(Ordering::Relaxed);

    println!("\nResults:");
    println!("  Users events: {}/20", users_received);
    println!("  Orders events: {}/20", orders_received);
    println!("  Products events: {}/20", products_received);

    // Each listener should receive events only for its collection
    assert!(users_received >= 18, "Should receive most user events");
    assert!(orders_received >= 18, "Should receive most order events");
    assert!(products_received >= 18, "Should receive most product events");

    println!("✓ Multiple collections PubSub working correctly");
}
