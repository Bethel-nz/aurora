# Aurora DB

A hybrid embedded document database with key-value storage and document collections.

## Features

- **Hybrid Storage**: In-memory cache + persistent disk storage
- **Document Collections**: Schema-flexible JSON-like documents
- **Advanced Queries**: Rich filtering, sorting and indexing
- **Blob Storage**: Efficient handling of binary objects
- **Async API**: High-performance operations with async/await support
- **Durability**: Write-ahead logging for crash recovery

## Installation

Add to your Cargo.toml:

```toml
[dependencies]
aurora_db = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

## Basic Usage

```rust
use aurora_db::{Aurora, FieldType, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database
    let db = Aurora::open("my_database")?;
    
    // Create collection
    db.new_collection("users", vec![
        ("name", FieldType::String, false),
        ("email", FieldType::String, true), // unique field
        ("age", FieldType::Int, false),
    ])?;
    
    // Insert document
    db.insert_into("users", vec![
        ("name", Value::String("Jane Doe".to_string())),
        ("email", Value::String("jane@example.com".to_string())),
        ("age", Value::Int(28)),
    ])?;
    
    // Query documents
    let users = db.query("users")
        .filter(|f| f.gt("age", 25))
        .collect()
        .await?;
    
    println!("Found {} users", users.len());
    
    Ok(())
}
```

## Documentation

See the [docs directory](src/docs/) for detailed guides:

- [CRUD Operations](src/docs/crud.md)
- [Querying](src/docs/querying.md) 
- [Schema Management](src/docs/schema.md)

## Performance

Aurora DB delivers fast performance across various operations:

```
Operation 'Database initialization' took: 103.54ms
Operation 'Basic KV operations' took: 307.40µs
Operation 'Collection setup' took: 3.25ms
Operation 'Query books by Author' took: 759.70µs
```

## Status

Currently in beta. Core functionality is stable and actively developed.

## License

MIT License
