use aurora_db::{Aurora, FieldType, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_demo().await
}

async fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aurora Reactive Queries Demo ===\n");

    let db = std::sync::Arc::new(Aurora::open("reactive_demo.db").await?);

    // Clean up any existing data
    let _ = db.delete_collection("tasks").await;

    // Create tasks collection
    db.new_collection(
        "tasks",
        vec![
            ("title", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
            ("completed", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_BOOL, unique: false, indexed: false, nullable: true }),
            ("priority", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true }),
        ],
    ).await?;

    println!("- Created 'tasks' collection\n");

    // Insert some initial tasks
    db.insert_into(
        "tasks",
        vec![
            ("title", Value::String("Buy groceries".into())),
            ("completed", Value::Bool(false)),
            ("priority", Value::Int(2)),
        ],
    )
    .await?;

    db.insert_into(
        "tasks",
        vec![
            ("title", Value::String("Write report".into())),
            ("completed", Value::Bool(false)),
            ("priority", Value::Int(1)),
        ],
    )
    .await?;

    println!("- Inserted initial tasks\n");

    // Spawn a background task that will modify data
    let writer_db = Arc::clone(&db);
    let writer_handle = tokio::spawn(async move {
        let db = writer_db;
        sleep(Duration::from_millis(500)).await;

        println!("[WRITER] Adding high priority task...");
        if let Err(e) = db.insert_into("tasks", vec![
            ("title", Value::String("Fix critical bug".into())),
            ("completed", Value::Bool(false)),
            ("priority", Value::Int(3)),
        ]).await { eprintln!("[WRITER] insert failed: {}", e); }

        sleep(Duration::from_millis(500)).await;

        println!("[WRITER] Adding another task...");
        if let Err(e) = db.insert_into("tasks", vec![
            ("title", Value::String("Review PR".into())),
            ("completed", Value::Bool(false)),
            ("priority", Value::Int(2)),
        ]).await { eprintln!("[WRITER] insert failed: {}", e); }

        sleep(Duration::from_millis(500)).await;

        println!("[WRITER] Adding low priority task...");
        if let Err(e) = db.insert_into("tasks", vec![
            ("title", Value::String("Update docs".into())),
            ("completed", Value::Bool(false)),
            ("priority", Value::Int(1)),
        ]).await { eprintln!("[WRITER] insert failed: {}", e); }

        sleep(Duration::from_millis(500)).await;
        println!("[WRITER] Done writing");
    });

    // Create a reactive query that watches for high-priority incomplete tasks
    println!(" Watching for high-priority tasks (priority >= 2)...\n");

    let mut watcher = db
        .query("tasks")
        .filter(|f: &aurora_db::query::FilterBuilder| f.gte("priority", 2) & f.eq("completed", false))
        .watch()
        .await?;

    // Spawn a task to receive updates
    let watcher_handle = tokio::spawn(async move {
        let mut update_count = 0;

        while let Some(update) = watcher.next().await {
            update_count += 1;

            match update {
                aurora_db::reactive::QueryUpdate::Added(doc) => {
                    let title = doc
                        .data
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    let priority = doc
                        .data
                        .get("priority")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    println!("   ADDED: '{}' (priority: {})", title, priority);
                }
                aurora_db::reactive::QueryUpdate::Removed(doc) => {
                    let title = doc
                        .data
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    println!("   REMOVED: '{}'", title);
                }
                aurora_db::reactive::QueryUpdate::Modified { old, new } => {
                    let old_title = old
                        .data
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    let new_title = new
                        .data
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown");
                    println!("   MODIFIED: '{}' -> '{}'", old_title, new_title);
                }
            }

            // Stop after receiving a few updates (for demo purposes)
            if update_count >= 3 {
                println!("\n Received {} updates, stopping watcher", update_count);
                break;
            }
        }
    });

    // Wait for writer to finish
    writer_handle.await?;

    // Wait a bit for watcher to process final events
    sleep(Duration::from_millis(500)).await;

    // Cleanup
    watcher_handle.abort();

    println!("\n Demo complete!");
    println!("\nKey takeaways:");
    println!("  * Reactive queries watch for real-time data changes");
    println!("  * Filters are applied automatically to incoming events");
    println!("  * Updates show Added/Removed/Modified changes");
    println!("  * Perfect for building real-time UIs and dashboards");

    Ok(())
}
