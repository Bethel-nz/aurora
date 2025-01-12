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

For a 15MB file:
- Write: ~349ms
- Read: ~15ms

Standard key-value operations:
- Put: ~40ms
- Get: ~29μs

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

## Usage

```rust
use aurora::Aurora;

// Initialize the database
let db = Aurora::new("aurora.db")?;

// Basic operations
db.put("key".to_string(), b"value".to_vec())?;
let value = db.get("key")?;

// Blob storage
db.put_blob("image:profile".to_string(), Path::new("path/to/image.png"))?;
let image_data = db.get_blob("image:profile")?;
// write to fs the file
```

## Roadmap

- finish up on other operations (delete, list all data)
- Query system with indexing
- Additional data structures (lists, sets, collections)
- Network layer for client-server operation
- Lightweight ORM
- Extended querying capabilities

## Architecture

Aurora uses [sled](https://github.com/spacejam/sled) for persistent storage and [dashmap](https://github.com/xacrimon/dashmap) for in-memory caching, providing a balance between durability and performance.

## Status

Currently in active development. Core key-value functionality is stable-ish.
