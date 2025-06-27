# Aurora DB

A hybrid embedded document database with key-value storage and document collections.

## Features

- **Hybrid Storage**: In-memory cache + persistent disk storage
- **Document Collections**: Schema-flexible JSON-like documents
- **Advanced Queries**: Rich filtering, sorting and indexing
- **Blob Storage**: Efficient handling of binary objects
- **Async API**: High-performance operations with async/await support
- **Durability**: Write-ahead logging for crash recovery
- **Network Support**: Optional HTTP JSON and high-performance binary servers

## Library Usage

To use Aurora DB as an embedded library in your Rust project, add it to your `Cargo.toml`:

```toml
[dependencies]
aurora_db = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

### Basic Example

```rust
use aurora_db::{Aurora, FieldType, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database in a temporary directory
    let db = Aurora::open("my_database")?;

    // Create a new collection for users
    db.new_collection("users", vec![
        ("name", FieldType::String, false),
        ("email", FieldType::String, true), // unique field
        ("age", FieldType::Int, false),
    ])?;

    // Insert a document
    db.insert_into("users", vec![
        ("name", Value::from("Jane Doe")),
        ("email", Value::from("jane@example.com")),
        ("age", Value::from(28)),
    ])?;

    // Query for users older than 25
    let users = db.query("users")
        .filter(|f| f.gt("age", 25))
        .collect()
        .await?;

    println!("Found {} user(s).", users.len());
    // -> Found 1 user(s).

    Ok(())
}
```

## Running as a Server

Aurora DB can also run as a standalone server, accessible over the network. The server is provided as a separate binary within the crate, enabled by feature flags.

### Server Features

- `http`: Enables the Actix-powered HTTP/JSON REST API.
- `binary`: Enables the high-performance Bincode-based TCP server.
- `full`: Enables both servers.

### Running the Server

To run the server binary, use the `--features` flag with Cargo:

```bash
# Run the HTTP server on port 7879
cargo run --bin aurora-db --features http

# Run the binary server on port 7878
cargo run --bin aurora-db --features binary

# Run both servers simultaneously
cargo run --bin aurora-db --features full
```

## Documentation

See the [docs directory](src/docs/) for detailed guides on CRUD operations, querying, and schema management.

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
