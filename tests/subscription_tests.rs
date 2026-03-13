use aurora_db::{Aurora, AuroraConfig};
use aurora_db::parser::executor::ExecutionResult;
use std::collections::HashMap;
use std::sync::Arc;
use serde_json::json;
use tokio::time::Duration;

#[tokio::test]
async fn test_subscription_flow() -> aurora_db::error::Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("subscription_test.db");
    let config = AuroraConfig {
        db_path,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;
    let db = Arc::new(db);

    // Create schema
    db.execute(r#"
        schema {
            define collection messages {
                content: String
            }
        }
    "#).await?;

    // Start subscription
    let sub_result = db.execute(r#"
        subscription {
            messages {
                content
            }
        }
    "#).await?;

    let mut stream = if let ExecutionResult::Subscription(res) = sub_result {
        res.stream.expect("Stream should be present")
    } else {
        panic!("Expected Subscription result");
    };

    // Spawn a listener task
    let handle = tokio::spawn(async move {
        // Wait for event
        let event = stream.recv().await.expect("Channel closed");
        event
    });

    // Give simpler time for listener to register (native broadcast might need a moment or just strict ordering)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger insert
    let mut vars = HashMap::new();
    vars.insert("content".to_string(), json!("Hello World"));
    
    db.execute((r#"
        mutation($content: String) {
            insertInto(collection: "messages", data: { content: $content }) {
                id
            }
        }
    "#, json!(vars))).await?;

    // Await listener
    let event = match tokio::time::timeout(Duration::from_secs(2), handle).await {
        Ok(res) => res.unwrap(),
        Err(_) => panic!("Timed out waiting for subscription event"),
    };

    assert_eq!(event.collection, "messages");
    assert!(event.document.is_some());
    assert_eq!(event.document.unwrap().data.get("content").unwrap(), &aurora_db::Value::String("Hello World".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_subscription_filter() -> aurora_db::error::Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("subscription_filter_test.db");
    let config = AuroraConfig {
        db_path,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;
    let db = Arc::new(db);

    // Create schema
    db.execute(r#"
        schema {
            define collection events {
                category: String
                score: Int
            }
        }
    "#).await?;

    // Start subscription with filter
    let sub_result = db.execute(r#"
        subscription {
            events(where: { score: { gt: 10 } }) {
                category
                score
            }
        }
    "#).await?;

    let mut stream = if let ExecutionResult::Subscription(res) = sub_result {
        res.stream.expect("Stream should be present")
    } else {
        panic!("Expected Subscription result");
    };

    // Spawn listener
    let handle = tokio::spawn(async move {
        let mut events = Vec::new();
        // Try to collect 2 events, or timeout
        // We expect only 1 event to match
        loop {
           match tokio::time::timeout(Duration::from_millis(500), stream.recv()).await {
               Ok(Ok(event)) => events.push(event),
               _ => break,
           }
        }
        events
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Insert non-matching doc
    db.execute((r#"mutation { insertInto(collection: "events", data: { category: "A", score: 5 }) { id } }"#, json!({}))).await?;

    // Insert matching doc
    db.execute((r#"mutation { insertInto(collection: "events", data: { category: "B", score: 15 }) { id } }"#, json!({}))).await?;

    // Insert another non-matching doc
    db.execute((r#"mutation { insertInto(collection: "events", data: { category: "C", score: 10 }) { id } }"#, json!({}))).await?;

    let events = handle.await.unwrap();
    
    // Should have exactly 1 event
    assert_eq!(events.len(), 1, "Expected exactly 1 matching event");
    let event = &events[0];
    assert_eq!(
        event.document.as_ref().unwrap().data.get("category"), 
        Some(&aurora_db::Value::String("B".to_string()))
    );

    Ok(())
}

#[tokio::test]
async fn test_subscription_string_filter() -> aurora_db::error::Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("subscription_string_filter_test.db");
    let config = AuroraConfig {
        db_path,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;
    let db = Arc::new(db);

    // Create schema (No commas!)
    db.execute(r#"
        schema {
            define collection logs {
                level: String
                msg: String
            }
        }
    "#).await?;

    // Start subscription with startsWith filter
    let sub_result = db.execute(r#"
        subscription {
            logs(where: { msg: { startsWith: "Error" } }) {
                level
                msg
            }
        }
    "#).await?;

    let mut stream = if let ExecutionResult::Subscription(res) = sub_result {
        res.stream.expect("Stream should be present")
    } else {
        panic!("Expected Subscription result");
    };

    // Spawn listener
    let handle = tokio::spawn(async move {
        let mut events = Vec::new();
        loop {
           match tokio::time::timeout(Duration::from_millis(500), stream.recv()).await {
               Ok(Ok(event)) => events.push(event),
               _ => break,
           }
        }
        events
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Insert non-matching doc
    db.execute((r#"mutation { insertInto(collection: "logs", data: { level: "INFO", msg: "Server started" }) { id } }"#, json!({}))).await?;

    // Insert matching doc
    db.execute((r#"mutation { insertInto(collection: "logs", data: { level: "ERROR", msg: "Error: db connection failed" }) { id } }"#, json!({}))).await?;

    // Insert another matching doc
    db.execute((r#"mutation { insertInto(collection: "logs", data: { level: "CRITICAL", msg: "Error: out of memory" }) { id } }"#, json!({}))).await?;

    let events = handle.await.unwrap();
    
    // Should have 2 events
    assert_eq!(events.len(), 2, "Expected exactly 2 matching events");
    assert_eq!(
        events[0].document.as_ref().unwrap().data.get("level"), 
        Some(&aurora_db::Value::String("ERROR".to_string()))
    );

    Ok(())
}
