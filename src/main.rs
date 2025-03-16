use aurora_db::{Aurora, Result, FieldType, Value, DataInfo};
use uuid::Uuid;
use std::path::Path;

// Timing macro for operations
macro_rules! time_operation {
    ($op_name:expr, $op:expr) => {{
        let start = std::time::Instant::now();
        let result = $op;
        println!("Operation '{}' took: {:?}", $op_name, start.elapsed());
        result
    }};
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting Aurora database...");

    // Initialize database
    let db = time_operation!("Database initialization", 
        Aurora::open("mydb")?  // Will create mydb.db
    );

    // 1. Basic Key-Value Operations
    println!("\n=== Basic Key-Value Operations ===");
    time_operation!("Basic KV operations", {
        // Put
        db.put("user:1".to_string(), b"John Doe".to_vec(), None)?;
        
        // Get
        if let Some(value) = db.get("user:1")? {
            println!("Retrieved: {}", String::from_utf8_lossy(&value));
        }

        // Delete
        db.delete("user:1").await?;
        assert!(db.get("user:1")?.is_none());
    });

    // 2. Collection Operations
    println!("\n=== Collection Operations ===");
    time_operation!("Collection setup", {
        // Create collection
        db.new_collection("books", vec![
            ("title", FieldType::String, false),
            ("author", FieldType::String, false),
            ("year", FieldType::Int, false),
            ("tags", FieldType::String, false),
            ("id", FieldType::Uuid, true),
        ])?;

        // Insert sample books
        let books = vec![
            ("Fantasy Book", "Author 1", 2020, vec!["fantasy", "magic"]),
            ("Science Book", "Author 2", 2021, vec!["science", "education"]),
            ("Mystery Book", "Author 3", 2022, vec!["mystery", "thriller"]),
        ];

        for (title, author, year, tags) in books {
            db.insert_into("books", vec![
                ("title", Value::String(title.to_string())),
                ("author", Value::String(author.to_string())),
                ("year", Value::Int(year)),
                ("tags", Value::Array(tags.into_iter().map(|t| Value::String(t.to_string())).collect())),
                ("id", Value::Uuid(Uuid::new_v4())),
            ])?;
        }
    });

    // 3. Query Operations
    println!("\n=== Query Operations ===");
    
    // Author query
    let books = time_operation!("Query books by Author 1", 
        db.query("books")
            .filter(|f| f.eq("author", "Author 1"))
            .collect()
            .await?
    );
    println!("Books by Author 1: {:?}", books);

    // Year query
    let books = time_operation!("Query books after 2020",
        db.query("books")
            .filter(|f| f.gt("year", 2020))
            .order_by("year", true)
            .collect()
            .await?
    );
    println!("Books after 2020: {:?}", books);

    // Tag query
    let books = time_operation!("Query fantasy books",
        db.query("books")
            .filter(|f| f.contains("tags", "fantasy"))
            .collect()
            .await?
    );
    println!("Fantasy books: {:?}", books);

    // Combined query
    let books = time_operation!("Query science books after 2020",
        db.query("books")
            .filter(|f| f.gt("year", 2020) && f.contains("tags", "science"))
            .collect()
            .await?
    );
    println!("Science books after 2020: {:?}", books);

    // 4. Blob Operations
    println!("\n=== Blob Operations ===");
    time_operation!("Blob operations", {
        let image_path = Path::new("test_image.jpg");
        if image_path.exists() {
            match db.put_blob("images:profile".to_string(), &image_path).await {
                Ok(_) => println!("Image stored successfully"),
                Err(e) => println!("Failed to store image: {:?}", e),
            }
        }

        // List blobs
        let blobs = db.get_data_by_pattern("images:")?;
        for (key, info) in blobs {
            match info {
                DataInfo::Blob { size } => println!("Blob: {} ({} bytes)", key, size),
                _ => {}
            }
        }
    });

    // 5. Export Operations
    println!("\n=== Export Operations ===");
    time_operation!("Export operations", {
        db.export_as_json("books", "book_exports")?;
        db.export_as_csv("books", "book_exports")?;
    });

    // 6. Collection Management
    println!("\n=== Collection Management ===");
    time_operation!("Collection cleanup", {
        // Delete old books
        let old_books = db.query("books")
            .filter(|f| f.lt("year", 2021))
            .collect()
            .await?;
        
        for book in old_books {
            db.delete(&book.id).await?;
        }

        // Delete entire collection
        db.delete_collection("books").await?;
    });

    // Add this section to your existing main.rs examples
    println!("\n=== Database Statistics ===");
    time_operation!("Database statistics", {
        let stats = db.get_database_stats()?;
        println!("Total database size: {} MB", stats.estimated_size / (1024 * 1024));
        println!("Hot cache hit ratio: {:.2}%", stats.hot_stats.hit_ratio * 100.0);
        
        for (collection, cstats) in stats.collections {
            println!("Collection '{}': {} documents, {} KB total, {} bytes avg document size", 
                collection, cstats.count, cstats.size_bytes / 1024, cstats.avg_doc_size);
        }
    });

    // Test batch operations
    println!("\n=== Batch Operations ===");
    time_operation!("Batch write", {
        let batch = (0..100).map(|i| {
            (format!("batch:key:{}", i), format!("value-{}", i).into_bytes())
        }).collect();
        
        db.batch_write(batch)?;
    });

    // Test prefix scanning
    println!("\n=== Prefix Scan ===");
    time_operation!("Scan batch keys", {
        let keys: Vec<String> = db.scan_with_prefix("batch:key:")
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();
        
        println!("Found {} keys with prefix 'batch:key:'", keys.len());
    });

    // Test hot cache operations
    println!("\n=== Hot Cache Operations ===");
    time_operation!("Hot cache operations", {
        // Store a key that will be accessed frequently
        db.put("hot:key".to_string(), b"frequently accessed value".to_vec(), None)?;
        
        // Access it multiple times to keep it hot
        for _ in 0..10 {
            db.get("hot:key")?;
        }
        
        // Check if it's in the hot cache
        println!("Is 'hot:key' in hot cache: {}", db.is_in_hot_cache("hot:key"));
        
        // Clear the hot cache
        db.clear_hot_cache();
        
        // Check again
        println!("Is 'hot:key' in hot cache after clearing: {}", db.is_in_hot_cache("hot:key"));
    });

    // Add this section to your existing main.rs examples
    println!("\n=== Search Operations ===");
    time_operation!("Search and indexing", {
        // First create a collection with text data
        db.new_collection("articles", vec![
            ("title", FieldType::String, false),
            ("content", FieldType::String, false),
            ("author", FieldType::String, false),
            ("tags", FieldType::String, false),
        ])?;
        
        // Insert some test articles
        let articles = vec![
            ("Rust Programming", "Learn about Rust's memory safety and zero-cost abstractions", "Jane Smith", "programming,rust,learning"),
            ("Database Systems", "Modern database design principles and architecture", "John Doe", "databases,architecture,design"),
            ("Machine Learning", "Introduction to machine learning algorithms", "Alice Johnson", "ai,machine-learning,algorithms"),
        ];
        
        for (title, content, author, tags) in articles {
            db.insert_into("articles", vec![
                ("title", Value::String(title.to_string())),
                ("content", Value::String(content.to_string())),
                ("author", Value::String(author.to_string())),
                ("tags", Value::String(tags.to_string())),
            ])?;
        }
        
        // Create a full-text index on the content field
        db.create_text_index("articles", "content", true)?;
        
        // Perform a full-text search
        let results = db.full_text_search("articles", "content", "database")?;
        println!("Articles about databases: {:?}", results);
        
        // Search by exact value
        let author_value = Value::String("Jane Smith".to_string());
        let by_author = db.search_by_value("articles", "author", &author_value)?;
        println!("Articles by Jane Smith: {:?}", by_author);
    });

    Ok(())
}

