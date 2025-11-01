use aurora_db::{
    Aurora,
    pubsub::{ChangeType, EventFilter},
    types::{AuroraConfig, FieldType, Value},
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Aurora PubSub Demo - Real-time Change Notifications\n");

    let db = Aurora::with_config(AuroraConfig::realtime())?;

    // Create collection
    db.new_collection(
        "users",
        vec![
            ("id", FieldType::String, true),
            ("name", FieldType::String, false),
            ("email", FieldType::String, true),
            ("active", FieldType::Bool, false),
        ],
    )?;

    // ==========================================
    // Demo 1: Basic LISTEN/NOTIFY
    // ==========================================
    println!("📡 Demo 1: Basic LISTEN/NOTIFY Pattern");
    println!("----------------------------------------");

    // Start listening for changes on "users" collection
    let mut listener = db.listen("users");

    // Spawn a task to listen for events
    let listener_task = tokio::spawn(async move {
        println!("👂 Listener: Waiting for changes...");

        for i in 0..3 {
            if let Ok(event) = listener.recv().await {
                println!("  ✨ Listener received event #{}", i + 1);
                println!("     Collection: {}", event.collection);
                println!("     Type: {:?}", event.change_type);
                println!("     ID: {}", event.id);

                if let Some(doc) = &event.document {
                    if let Some(name) = doc.data.get("name") {
                        println!("     Name: {:?}", name);
                    }
                }
            }
        }

        println!("  Listener: Done receiving events\n");
    });

    // Give listener time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish some events by inserting documents
    println!("Publisher: Inserting users...");

    let id1 = db
        .insert_into(
            "users",
            vec![
                ("name", Value::String("Alice".into())),
                ("email", Value::String("alice@example.com".into())),
                ("active", Value::Bool(true)),
            ],
        )
        .await?;
    println!("  Inserted user: {}", id1);

    let id2 = db
        .insert_into(
            "users",
            vec![
                ("name", Value::String("Bob".into())),
                ("email", Value::String("bob@example.com".into())),
                ("active", Value::Bool(false)),
            ],
        )
        .await?;
    println!(" Inserted user: {}", id2);

    let id3 = db
        .insert_into(
            "users",
            vec![
                ("name", Value::String("Charlie".into())),
                ("email", Value::String("charlie@example.com".into())),
                ("active", Value::Bool(true)),
            ],
        )
        .await?;
    println!("  Inserted user: {}", id3);

    // Wait for listener to finish
    listener_task.await?;

    // ==========================================
    // Demo 2: Filtered Listeners
    // ==========================================
    println!("Demo 2: Filtered Event Listeners");
    println!("----------------------------------------");

    // Listen only for active users
    let mut active_listener = db.listen("users").filter(EventFilter::FieldEquals(
        "active".to_string(),
        Value::Bool(true),
    ));

    // Spawn filtered listener
    let filtered_task = tokio::spawn(async move {
        println!("👂 Active Users Listener: Waiting for active users only...");

        if let Ok(event) = active_listener.recv().await {
            println!("  Received active user event!");
            println!("     ID: {}", event.id);
            if let Some(doc) = &event.document {
                if let Some(name) = doc.data.get("name") {
                    println!("     Name: {:?}", name);
                }
            }
        }

        println!("  Active Users Listener: Done\n");
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Insert both active and inactive users
    println!("Publisher: Inserting mixed users...");

    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Inactive User".into())),
            ("email", Value::String("inactive@example.com".into())),
            ("active", Value::Bool(false)),
        ],
    )
    .await?;
    println!(" Inserted inactive user (listener won't see this)");

    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Active User".into())),
            ("email", Value::String("active@example.com".into())),
            ("active", Value::Bool(true)),
        ],
    )
    .await?;
    println!(" Inserted active user (listener will see this!)");

    filtered_task.await?;

    // ==========================================
    // Demo 3: Multiple Listeners
    // ==========================================
    println!(" Demo 3: Multiple Listeners on Same Collection");
    println!("----------------------------------------");

    let mut listener1 = db.listen("users");
    let mut listener2 = db.listen("users");

    println!(
        " Active listeners for 'users': {}",
        db.listener_count("users")
    );

    let task1 = tokio::spawn(async move {
        if let Ok(event) = listener1.recv().await {
            println!("  👂 Listener 1 received: {}", event.id);
        }
    });

    let task2 = tokio::spawn(async move {
        if let Ok(event) = listener2.recv().await {
            println!("  Listener 2 received: {}", event.id);
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    println!(" Publisher: Inserting one user...");
    let id = db
        .insert_into(
            "users",
            vec![
                ("name", Value::String("Broadcast Test".into())),
                ("email", Value::String("broadcast@example.com".into())),
                ("active", Value::Bool(true)),
            ],
        )
        .await?;
    println!("  Inserted user: {}", id);

    task1.await?;
    task2.await?;
    println!("  Both listeners received the same event!\n");

    // ==========================================
    // Demo 4: Global Listener (All Collections)
    // ==========================================
    println!("Demo 4: Global Listener (All Collections)");
    println!("----------------------------------------");

    // Create another collection
    db.new_collection(
        "posts",
        vec![
            ("id", FieldType::String, true),
            ("title", FieldType::String, false),
            ("user_id", FieldType::String, false),
        ],
    )?;

    let mut global_listener = db.listen_all();

    let global_task = tokio::spawn(async move {
        println!("👂 Global Listener: Waiting for changes from ANY collection...");

        for i in 0..2 {
            if let Ok(event) = global_listener.recv().await {
                println!(
                    "  Global event #{}: Collection = {}, ID = {}",
                    i + 1,
                    event.collection,
                    event.id
                );
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Publisher: Inserting into different collections...");

    db.insert_into(
        "users",
        vec![
            ("name", Value::String("User for global".into())),
            ("email", Value::String("global@example.com".into())),
            ("active", Value::Bool(true)),
        ],
    )
    .await?;
    println!("  Inserted into 'users'");

    db.insert_into(
        "posts",
        vec![
            ("title", Value::String("Hello World".into())),
            ("user_id", Value::String(id1.clone())),
        ],
    )
    .await?;
    println!("  Inserted into 'posts'");

    global_task.await?;
    println!("  Global listener saw both collections!\n");

    // ==========================================
    // Demo 5: Delete Events
    // ==========================================
    println!(" Demo 5: Delete Event Notifications");
    println!("----------------------------------------");

    let mut delete_listener = db
        .listen("users")
        .filter(EventFilter::ChangeType(ChangeType::Delete));

    let delete_task = tokio::spawn(async move {
        println!("Delete Listener: Waiting for delete events...");

        if let Ok(event) = delete_listener.recv().await {
            println!("  User deleted: {}", event.id);
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Publisher: Deleting a user...");
    db.delete(&format!("users:{}", id1)).await?;
    println!("  Deleted user: {}", id1);

    delete_task.await?;

    // ==========================================
    // Summary
    // ==========================================
    println!("\nPubSub Demo Complete!");
    println!("\nFinal Statistics:");
    println!(
        "   Total listeners across all collections: {}",
        db.total_listeners()
    );

    Ok(())
}
