use aurora_db::{
    Aurora, Result,
    types::{AuroraConfig, FieldType, Value},
};
use std::time::Duration;

#[tokio::test]
async fn test_filtered_bulk_delete() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("bulk_delete.db");
    let config = AuroraConfig {
        db_path,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;

    // Create collection
    db.new_collection(
        "items",
        vec![
            ("status", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
            ("value", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ],
    )
    .await?;

    // Insert items
    for i in 0..10 {
        db.insert_into(
            "items",
            vec![
                (
                    "status",
                    Value::String(if i % 2 == 0 {
                        "active".into()
                    } else {
                        "inactive".into()
                    }),
                ),
                ("value", Value::Int(i)),
            ],
        )
        .await?;
    }

    // Verify count
    let count = db.query("items").count().await?;
    eprintln!("Initial count: {}", count);
    assert_eq!(count, 10);

    // Delete filtered (active only)
    let deleted = db
        .query("items")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("status", "active"))
        .delete() // This is the new method we added
        .await?;

    eprintln!("Deleted count: {}", deleted);

    // Verify remaining
    let remaining = db.query("items").count().await?;
    eprintln!("Remaining count: {}", remaining);

    // Debug: list remaining docs
    let all_remaining = db.query("items").collect().await?;
    for doc in &all_remaining {
        eprintln!("Remaining: {:?}", doc.data);
    }

    assert_eq!(deleted, 5);
    assert_eq!(remaining, 5);

    let docs = db.query("items").collect().await?;
    for doc in docs {
        assert_eq!(
            doc.data.get("status"),
            Some(&Value::String("inactive".into()))
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_watch_initial_set_and_debounce() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("watch_audit.db");
    let config = AuroraConfig {
        db_path,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;
    let db = Box::leak(Box::new(db)); // Leak to satisfy 'static requirement for watch()

    db.new_collection(
        "alerts",
        vec![
            ("level", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
            ("msg", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
            ("val", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ],
    )
    .await?;

    // Insert initial data
    db.insert_into(
        "alerts",
        vec![
            ("level", Value::String("critical".into())),
            ("msg", Value::String("Initial critical".into())),
            ("val", Value::Int(0)),
        ],
    )
    .await?;

    db.insert_into(
        "alerts",
        vec![
            ("level", Value::String("info".into())),
            ("msg", Value::String("Just info".into())),
            ("val", Value::Int(0)),
        ],
    )
    .await?;

    // 1. Verify Initial Set
    // We define a filtered query (critical only)
    let mut watcher = db
        .query("alerts")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("level", "critical"))
        .debounce(Duration::from_millis(50)) // Set debounce
        .watch()
        .await?;

    // Should receive initial snapshot (1 item) matching the filter
    // The implementation sends Added events for initial set
    if let Some(update) = watcher.next().await {
        match update {
            aurora_db::reactive::QueryUpdate::Added(doc) => {
                eprintln!("Initial Set Doc: {:?}", doc);
                assert_eq!(
                    doc.data.get("msg"),
                    Some(&Value::String("Initial critical".into()))
                );
            }
            _ => panic!("Expected Added event for initial set"),
        }
    } else {
        panic!("Using next() failed to get initial set");
    }

    // Ensure we DON'T get the 'info' document
    // We can use try_next() after a small sleep
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        watcher.try_next().is_none(),
        "Should not receive unmatched documents"
    );

    // 2. Verify Debounce
    // We send multiple updates quickly.
    // Since debounce is 50ms, if we send matches quickly, we should see throttling.

    let id = "debounced_doc";

    // Insert matching doc using upsert to control ID (and trigger events now!)
    db.upsert(
        "alerts",
        id,
        vec![
            ("level", Value::String("critical".into())),
            ("val", Value::Int(0)),
        ],
    )
    .await?;

    // Wait for the insert event (Added)
    eprintln!("Waiting for insert event...");
    let _ = watcher.next().await;
    eprintln!("Received insert event.");

    // Now rapid update
    eprintln!("Starting rapid updates...");
    for i in 1..=5 {
        db.upsert(
            "alerts",
            id,
            vec![
                ("level", Value::String("critical".into())),
                ("val", Value::Int(i)),
            ],
        )
        .await?;
    }

    // We expect to eventually see val=5.
    // We might see some intermediate values depending on timing, but definitely not all if it's very fast.
    // The key aspect of debounce/throttle is that we definitely get the LAST state.

    let mut received_final = false;
    let start = std::time::Instant::now();

    while start.elapsed() < Duration::from_secs(2) {
        if let Some(update) = watcher.try_next() {
            if let aurora_db::reactive::QueryUpdate::Modified { new, .. } = update {
                if let Some(Value::Int(v)) = new.data.get("val") {
                    if *v == 5 {
                        received_final = true;
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(received_final, "Should receive the final state (5)");

    Ok(())
}
