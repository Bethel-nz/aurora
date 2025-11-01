use aurora_db::{Aurora, FieldType, Result, Value};
use std::path::Path;

// Timing macro for operations
macro_rules! time_operation {
    ($label:expr, $op:expr) => {{
        let start = std::time::Instant::now();
        let result = $op;
        let duration = start.elapsed();
        println!("{} in {:?}", $label, duration);
        result
    }};
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting Aurora database showcase...");

    let db_path = Path::new("showcase_db");
    // Clean up from previous runs
    if db_path.exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    let db = time_operation!("Database initialization", Aurora::open(db_path))?;

    // 1. Basic Key-Value Operations
    println!("\n=== 1. Basic Key-Value Operations ===");
    let key = "my_key";
    let value = b"my_value".to_vec();
    time_operation!("Set key", db.put(key.to_string(), value.clone(), None))?;
    let retrieved = time_operation!("Get key", db.get(key))?.unwrap();
    assert_eq!(retrieved, value);
    println!("Get/Set verification successful.");

    time_operation!("Delete key", db.delete(key).await)?;
    let retrieved_after_delete = time_operation!("Get key after delete", db.get(key))?;
    assert!(retrieved_after_delete.is_none());
    println!("Delete verification successful.");

    // 2. Collection and Document Operations
    println!("\n=== 2. Collection and Document Operations ===");
    let collection_name = "users";
    time_operation!(
        "Create collection",
        db.new_collection(
            collection_name,
            vec![
                ("name".to_string(), FieldType::String, false),
                ("email".to_string(), FieldType::String, true),
                ("age".to_string(), FieldType::Int, false),
                ("premium".to_string(), FieldType::Bool, false),
            ]
        )
    )?;
    println!("-> Created collection '{}'", collection_name);

    let user_id = time_operation!(
        "Insert user",
        db.insert_into(
            collection_name,
            vec![
                ("name", "Jane Doe".into()),
                ("email", "jane.doe@example.com".into()),
                ("age", Value::Int(32)),
                ("premium", true.into()),
            ],
        )
        .await
    )?;
    println!("-> Inserted user with ID: {}", user_id);

    let user_doc =
        time_operation!("Get document", db.get_document(collection_name, &user_id))?.unwrap();
    println!("Retrieved user: {}", user_doc);
    assert_eq!(
        user_doc.data.get("name"),
        Some(&Value::String("Jane Doe".to_string()))
    );

    // 3. Querying
    println!("\n=== 3. Querying Operations ===");
    println!("\n[Batch Inserting More Users...]");
    db.insert_into(
        collection_name,
        vec![
            ("name", "John Smith".into()),
            ("email", "john.smith@example.com".into()),
            ("age", Value::Int(45)),
            ("premium", false.into()),
        ],
    )
    .await?;
    db.insert_into(
        collection_name,
        vec![
            ("name", "Peter Jones".into()),
            ("email", "peter.jones@example.com".into()),
            ("age", Value::Int(28)),
            ("premium", true.into()),
        ],
    )
    .await?;
    println!("-> Batch insert complete.");

    println!("Querying for users older than 21...");
    let adult_users = time_operation!(
        "Query (age > 21)",
        db.query(collection_name)
            .filter(|f| f.gt("age", Value::Int(21)))
            .collect()
            .await
    )?;
    assert_eq!(adult_users.len(), 2);
    println!("Found {} adult user(s).", adult_users.len());
    for user in &adult_users {
        println!(" - Found user: {}", user);
    }

    // 4. Full-Text Search
    println!("\n=== 4. Full-Text Search ===");
    let articles_collection = "articles";
    db.new_collection(
        articles_collection,
        vec![
            ("title".to_string(), FieldType::String, false),
            ("content".to_string(), FieldType::String, false),
        ],
    )?;
    db.create_text_index(articles_collection, "content", true)?;

    println!("\n[Indexing Articles...]");
    db.insert_into(
        articles_collection,
        vec![
            ("title", "Aurora DB".into()),
            ("content", "Aurora is a fast, embedded database.".into()),
        ],
    )
    .await?;
    db.insert_into(
        articles_collection,
        vec![
            ("title", "Rust Programming".into()),
            (
                "content",
                "Rust is a systems programming language that runs blazingly fast.".into(),
            ),
        ],
    )
    .await?;
    println!("-> Articles indexed.");

    println!("Searching for articles about 'database'...");
    let search_results = time_operation!(
        "Search",
        db.search(articles_collection)
            .field("content")
            .matching("database")
            .collect()
            .await
    )?;
    assert_eq!(search_results.len(), 1);
    println!("Found {} search result(s).", search_results.len());
    for article in &search_results {
        println!(" - Found article: {}", article);
    }

    // Cleanup
    println!("\nCleaning up database file...");
    std::fs::remove_dir_all(db_path)?;
    println!("Showcase finished successfully.");

    Ok(())
}
