# Aurora DB

> A lightweight, real-time embedded database designed for modern applications

[![Crates.io](https://img.shields.io/crates/v/aurora-db.svg)](https://crates.io/crates/aurora-db)
[![Documentation](https://docs.rs/aurora-db/badge.svg)](https://docs.rs/aurora-db)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why Aurora Exists

Most embedded databases force you to choose: either **simple key-value storage** with manual indexing and caching, or **heavy SQL databases** with complex setup. I wanted something different.

**Aurora was born from a simple need**: a database that **just works** for building real-time applications. No external services, no complex configuration, no choosing between twenty different storage engines. Just install it, open it, and start building.

### The Philosophy

**Real-time by default.** Data changes should propagate instantly. Your UI shouldn't poll—it should react. Background jobs should process reliably without external queues. This isn't optional; it's how modern applications work.

**Lightweight without compromise.** Embedded doesn't mean primitive. Aurora handles indexing, caching, pub/sub, reactive queries, and background workers—all built-in. You get production-grade features without the operational overhead.

**Performance through intelligence.** The hybrid hot/cold architecture wasn't an accident. Frequently accessed data lives in memory (200K ops/sec), everything else persists to disk (10K ops/sec). Schema-aware selective indexing means I only index what you actually query. Smart defaults, intelligent caching, zero configuration to get started.

### The Design

Every feature in Aurora solves a real problem:

- **PubSub** → Because polling databases is wasteful
- **Reactive Queries** → Because UIs should update automatically
- **Durable Workers** → Because background jobs shouldn't need Redis
- **Computed Fields** → Because derived data shouldn't duplicate storage
- **Write Buffering** → Because high-throughput writes shouldn't kill performance
- **Schema-Aware Indexing** → Because indexing every field is insane

This wasn't scope creep—it was intentional. Building real-time apps requires all these pieces, and they should work together seamlessly.

### What's Next

The current bottleneck is in-memory index management. I'm currently looking into how **DiceDB**, **Redis**, and **RocksDB** handle high-performance in-memory structures, exploring:

- Lock-free index structures inspired by DiceDB's concurrent patterns
- Memory-efficient data structures from Redis's designs
- RocksDB's LSM-tree approach for better write amplification
- Smarter cache eviction using access pattern analysis

---

## Table of Contents

### Getting Started

- [Schema Management](./src/docs/schema.md) - Define collections and field types
- [CRUD Operations](./src/docs/crud.md) - Create, read, update, and delete documents
- [Querying](./src/docs/querying.md) - Filter, sort, and retrieve data

### Advanced Features

- [PubSub System](./src/docs/pubsub.md) - Real-time change notifications
- [Reactive Queries](./src/docs/reactive.md) - Auto-updating query results
- [Durable Workers](./src/docs/workers.md) - Background job processing
- [Computed Fields](./src/docs/computed-fields.md) - Derived values from existing data

### Optimization

- [Performance Guide](./src/docs/performance.md) - Tuning, caching, and best practices

## Quick Start

### Installation

```toml
[dependencies]
aurora-db = "0.3.0"
```

### Basic Usage

```rust
use aurora_db::{Aurora, FieldType, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database
    let db = Aurora::open("myapp.db")?;

    // Create a collection
    db.new_collection("users", vec![
        ("name", FieldType::String, false),
        ("email", FieldType::String, true),  // unique
        ("age", FieldType::Int, false),
    ])?;

    // Insert a document
    let user_id = db.insert_into("users", vec![
        ("name", Value::String("Alice".to_string())),
        ("email", Value::String("alice@example.com".to_string())),
        ("age", Value::Int(30)),
    ]).await?;

    // Query documents
    let users = db.query("users")
        .filter(|f| f.gt("age", 25))
        .collect()
        .await?;

    println!("Found {} users over 25", users.len());

    Ok(())
}
```

## Feature Overview

### Collections & Schema

Define structured collections with type-safe fields and unique constraints.

```rust
db.new_collection("products", vec![
    ("sku", FieldType::String, true),      // unique
    ("name", FieldType::String, false),
    ("price", FieldType::Float, false),
    ("in_stock", FieldType::Bool, false),
])?;
```

[Learn more →](./src/docs/schema.md)

### Powerful Querying

Filter, sort, paginate, and search your data with an intuitive API.

```rust
let products = db.query("products")
    .filter(|f| f.eq("in_stock", true) && f.lt("price", 100.0))
    .order_by("price", true)
    .limit(10)
    .collect()
    .await?;
```

[Learn more →](./src/docs/querying.md)

### Real-time Updates

Subscribe to data changes with the PubSub system.

```rust
let mut subscription = db.subscribe("orders", None).await?;

while let Ok(event) = subscription.recv().await {
    println!("Order changed: {:?}", event);
}
```

[Learn more →](./src/docs/pubsub.md)

### ⚡ Reactive Queries

Automatically update query results when data changes.

```rust
let active_users = db.reactive_query("users")
    .filter(|f| f.eq("online", true))
    .build()
    .await?;

// Results automatically update when users go online/offline
let current_users = active_users.get_results().await?;
```

[Learn more →](./src/docs/reactive.md)

### Background Jobs

Process tasks asynchronously with automatic retries.

```rust
// Define a job handler
struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    async fn handle(&self, job: &Job) -> JobResult {
        send_email(&job.payload).await?;
        Ok(())
    }
}

// Start worker
let mut executor = WorkerExecutor::new(db.clone(), 4);
executor.register_handler("send_email", Box::new(EmailHandler));
executor.start().await?;

// Enqueue job
db.enqueue_job("send_email", payload, None, Priority::High).await?;
```

[Learn more →](./src/docs/workers.md)

### Computed Fields

Derive values automatically from document data.

```rust
let mut registry = ComputedFieldsRegistry::new();

// Full name from first + last
registry.register(
    "users",
    "full_name",
    Expression::Concat {
        fields: vec!["first_name".to_string(), "last_name".to_string()],
        separator: " ".to_string(),
    },
);

db.set_computed_fields(registry);
```

[Learn more →](./src/docs/computed-fields.md)

### High Performance

Optimized architecture with hot/cold storage and intelligent caching.

| Operation      | Throughput    |
| -------------- | ------------- |
| Hot Cache Read | ~200K ops/sec |
| Indexed Insert | ~17K ops/sec  |
| Indexed Query  | ~50K ops/sec  |

[Learn more →](./src/docs/performance.md)

## Architecture

Aurora uses a hybrid storage architecture:

```
┌─────────────────────────────────────────┐
│           Application Layer              │
├─────────────────────────────────────────┤
│  PubSub  │ Reactive │ Workers │ Computed │
├─────────────────────────────────────────┤
│            Query Engine                  │
├─────────────────────────────────────────┤
│   Hot Cache (In-Memory)  │   Indices    │
├──────────────────────────┴───────────────┤
│      Cold Storage (Sled - On Disk)      │
└─────────────────────────────────────────┘
```

### Storage Layers

- **Hot Cache**: In-memory cache for frequently accessed data (~200K ops/sec)
- **Cold Storage**: Persistent disk storage using Sled (~10K ops/sec)
- **Indices**: In-memory indices for fast lookups (O(1) complexity)

### Advanced Features

- **PubSub**: Real-time change notifications
- **Reactive Queries**: Auto-updating query results
- **Durable Workers**: Background job processing with retries
- **Computed Fields**: Virtual fields calculated at query time

## Configuration

Customize Aurora's behavior with `AuroraConfig`:

```rust
use aurora_db::{Aurora, AuroraConfig, EvictionPolicy};

let config = AuroraConfig {
    // Cache settings
    hot_cache_size_mb: 256,
    cold_cache_capacity_mb: 512,
    eviction_policy: EvictionPolicy::LRU,

    // Write buffering
    enable_write_buffering: true,
    write_buffer_size: 1000,
    write_buffer_flush_interval_ms: 100,

    // Index settings
    max_index_entries_per_field: 100000,

    // Cleanup intervals
    hot_cache_cleanup_interval_secs: 300,
    cold_flush_interval_ms: 1000,

    ..Default::default()
};

let db = Aurora::open_with_config("mydb.db", config)?;
```

[Learn more →](./src/docs/performance.md#configuration-tuning)

## Use Cases

### Real-time Applications

- Chat applications with live message updates
- Collaborative editing with reactive queries
- Live dashboards with auto-updating metrics
- Gaming leaderboards

### Background Processing

- Email delivery with retry logic
- Image processing pipelines
- Data exports and reports
- Webhook delivery

### Data-Intensive Apps

- E-commerce with inventory tracking
- Analytics with computed metrics
- Content management systems
- IoT data collection

## Best Practices

### 1. Index Strategy

```rust
//  DO: Index frequently queried fields
db.create_index("users", "email").await?;
db.create_index("orders", "customer_id").await?;

//  DON'T: Index every field
// Only index what you query
```

### 2. Query Optimization

```rust
//  DO: Use projections and limits
let users = db.query("users")
    .select(vec!["id", "name"])  // Only needed fields
    .limit(100)                   // Bounded result set
    .collect()
    .await?;

//  DON'T: Unbounded queries
let users = db.query("users").collect().await?;
```

### 3. Write Performance

```rust
//  DO: Use batch operations
db.batch_insert("logs", log_entries).await?;

//  DON'T: Individual inserts in loops
for entry in log_entries {
    db.insert_into("logs", entry).await?;
}
```

### 4. Real-time Updates

```rust
//  DO: Use reactive queries for UI state
let online_users = ReactiveState::new(
    db.clone(),
    "users",
    |f| f.eq("online", true)
).await?;

//  DON'T: Poll the database
loop {
    let users = db.query("users")
        .filter(|f| f.eq("online", true))
        .collect()
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

## Examples

Check out the `examples/` directory for complete examples:

- `reactive_demo.rs` - Reactive queries in action
- `contains_query_demo.rs` - Advanced querying
- `indexing_performance.rs` - Performance testing
- `index_demo.rs` - Index usage patterns

Run an example:

```bash
cargo run --example reactive_demo
```

## API Reference

For detailed API documentation:

```bash
cargo doc --open
```

## Community & Support

- **GitHub**: [github.com/bethel-nz/aurora](https://github.com/bethel-nz/aurora)
- **Issues**: [github.com/bethel-nz/aurora/issues](https://github.com/bethel-nz/aurora/issues)
- **Crates.io**: [crates.io/crates/aurora-db](https://crates.io/crates/aurora-db)

## Version History

### v0.3.0 (Current)

- Schema-aware selective indexing (3-5x faster inserts)
- Schema caching for reduced overhead
- Optimized benchmark suite
- Fixed compiler warnings
- Comprehensive documentation

### v0.2.1

- PubSub system for real-time updates
- Reactive queries
- Durable workers with retry logic
- Computed fields
- Write buffer optimization
- Multiple cache eviction policies

### v0.1.x

- Initial release
- Basic CRUD operations
- Collections and schema
- Query system
- Transactions

## License

MIT License - see LICENSE file for details

---

**Ready to build?** Start with the [Schema Management](./src/docs/schema.md) guide!
