# Aurora DB

Aurora is a high-performance embedded key-value store written in Rust that combines in-memory and disk-based storage for optimal performance. It utilizes a two-tiered architecture with hot (in-memory) and cold (disk) storage.

## Features

- Hybrid storage architecture (memory + disk)
- Blob storage support
- Chunked storage for large objects
- Built-in object metadata handling
- Async support for large data operations
- it uses a WAL (write ahead log) to store data temporarily, incase it crashes

## Performance

-- old
```shell
Compiling aurora v0.1.0 (/home/kylo_ren/Documents/codes/aurora)
 Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.79s
  Running `target/debug/aurora .`
Operation 'put' took: 39.577216ms
Operation 'get' took: 29.474µs
Value: "world"
Operation 'put blob' took: 348.876095ms
Operation 'get blob' took: 14.715055ms
```

-- new
```shell

Starting Aurora database...
Creating database at mydb
Operation 'Database initialization' took: 103.54068ms

=== Basic Key-Value Operations ===
Retrieved: John Doe
Operation 'Basic KV operations' took: 307.403µs

=== Collection Operations ===
Operation 'Collection setup' took: 3.249967ms

=== Query Operations ===
Initializing indices...
Indices initialized
Operation 'Query books by Author 1' took: 759.699µs
Books by Author 1: [{ year: 2020, id: 89c4734a-f299-4cb7-a7c7-353b103031a4, tags: fantasy, magic, author: Author 1, title: Fantasy Book }]
Operation 'Query books after 2020' took: 117.262µs
Books after 2020: [{ year: 2021, tags: science, education, author: Author 2, title: Science Book, id: 387171eb-aad8-415b-be37-46eb51c8339a }, { author: Author 3, year: 2022, tags: mystery, thriller, id: f7005efb-2a02-4eb4-ad3a-ffa208a40bea, title: Mystery Book }]
Operation 'Query fantasy books' took: 167.876µs
Fantasy books: [{ id: 89c4734a-f299-4cb7-a7c7-353b103031a4, title: Fantasy Book, year: 2020, author: Author 1, tags: fantasy, magic }]
Operation 'Query science books after 2020' took: 162.139µs
Science books after 2020: [{ year: 2021, id: 387171eb-aad8-415b-be37-46eb51c8339a, author: Author 2, tags: science, education, title: Science Book }]

=== Blob Operations ===
Operation 'Blob operations' took: 29.717µs

=== Export Operations ===
Exported collection 'books' to book_exports.json
Exported collection 'books' to book_exports.csv
Operation 'Export operations' took: 2.916518ms

=== Collection Management ===
Operation 'Collection cleanup' took: 793.102µs
```

## Usage
```rust

let db = Aurora::open("mydb")?; // Will create mydb.db

// 1. Basic Key-Value Operations
println!("\n=== Basic Key-Value Operations ===");

// Put
db.put("user:1".to_string(), b"John Doe".to_vec(), None)?; // None is the time to live

// Get
if let Some(value) = db.get("user:1")? {
    println!("Retrieved: {}", String::from_utf8_lossy(&value));
}

// Delete
db.delete("user:1").await?;
assert!(db.get("user:1")?.is_none());

// 2. Collection Operations
println!("\n=== Collection Operations ===");

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

// 3. Query Operations
println!("\n=== Query Operations ===");

// Author query
let books = db.query("books")
    .filter(|f| f.eq("author", "Author 1"))
    .collect()
    .await?;
println!("Books by Author 1: {:?}", books);

// Year query
let books = db.query("books")
    .filter(|f| f.gt("year", 2020))
    .order_by("year", true)
    .collect()
    .await?;
println!("Books after 2020: {:?}", books);

// Tag query
let books = db.query("books")
    .filter(|f| f.contains("tags", "fantasy"))
    .collect()
    .await?;
println!("Fantasy books: {:?}", books);

// Combined query
let books = db.query("books")
    .filter(|f| f.gt("year", 2020) && f.contains("tags", "science"))
    .collect()
    .await?;
println!("Science books after 2020: {:?}", books);

// 4. Blob Operations
println!("\n=== Blob Operations ===");

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

// 5. Export Operations
println!("\n=== Export Operations ===");

db.export_as_json("books", "book_exports")?;
db.export_as_csv("books", "book_exports")?;

// 6. Collection Management
println!("\n=== Collection Management ===");

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

```

## Roadmap

[ x ] finish up on other operations (delete, list all data)

[ x ] Query system with indexing

[ x ] Additional data structures (lists, sets, collections) - had to fall to collections

[ ] Network layer for client-server operation
[ ] Lightweight ORM
[ x ] Extended querying capabilities

## Architecture

Aurora uses [sled](https://github.com/spacejam/sled) for persistent storage and [dashmap](https://github.com/xacrimon/dashmap) for in-memory caching, providing a balance between durability and performance.

## Status

Currently in active development. Core key-value functionality is stable-ish.
