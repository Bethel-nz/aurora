use aurora::{Aurora, Result};
use aurora::types::{FieldType, Value};
use aurora::db::DataInfo;
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

    Ok(())
}

