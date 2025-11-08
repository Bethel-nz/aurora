# Aurora DB Performance Guide

This guide covers performance optimization techniques, caching strategies, and best practices for Aurora DB.

## Architecture Overview

Aurora uses a hybrid hot/cold storage architecture:

| Component | Type | Performance | Use Case |
|-----------|------|-------------|----------|
| **Hot Cache** | In-memory (DashMap) | ~200K ops/sec | Frequently accessed data |
| **Cold Storage** | Disk (Sled) | ~10K ops/sec | Persistent storage |
| **Indices** | In-memory (DashMap) | O(1) lookups | Fast queries |
| **Write Buffer** | Batched writes | 3-5x faster | High-volume writes |

## Indexing Strategy

### When to Create Indices

Indices dramatically improve query performance but add overhead to writes:

```rust
// Create index on frequently queried field
db.create_index("users", "email").await?;
db.create_index("orders", "customer_id").await?;
db.create_index("products", "sku").await?;
```

**Create indices for:**
- Fields used in `filter()` operations
- Unique fields (automatically indexed)
- Foreign key-like relationships
- Sort fields used frequently

**Don't index:**
- Fields that change frequently
- Fields rarely used in queries
- Large text fields (use full-text search instead)

### Index Performance

```rust
// Without index: O(n) - scans all documents
let user = db.query("users")
    .filter(|f| f.eq("email", "john@example.com"))
    .first_one()
    .await?;

// With index: O(1) - direct lookup
db.create_index("users", "email").await?;
let user = db.query("users")
    .filter(|f| f.eq("email", "john@example.com"))
    .first_one()
    .await?;
```

### Schema-Aware Indexing

As of version 0.3.0, Aurora only indexes fields marked in the schema:

```rust
// Only 'email' will be indexed (marked as unique)
db.new_collection("users", vec![
    ("name", FieldType::String, false),      // NOT indexed
    ("email", FieldType::String, true),      // INDEXED (unique)
    ("age", FieldType::Int, false),          // NOT indexed
])?;

// To explicitly index a non-unique field
db.create_index("users", "age").await?;
```

**Performance Impact:**
- Before selective indexing: ~5K ops/sec with many fields
- After selective indexing: **~17K ops/sec** (3.5x faster!)

## Caching Strategies

### Hot Cache Configuration

Configure hot cache size based on your working set:

```rust
use aurora_db::AuroraConfig;

let config = AuroraConfig {
    hot_cache_size_mb: 256,  // 256MB for hot cache
    cold_cache_capacity_mb: 512,  // 512MB for cold storage cache
    ..Default::default()
};

let db = Aurora::open_with_config("mydb.db", config)?;
```

**Sizing Guidelines:**
- Small app (< 1K active users): 64-128 MB
- Medium app (1K-10K users): 256-512 MB
- Large app (> 10K users): 1-2 GB
- Rule of thumb: 20-30% of your dataset

### Cache Eviction Policies

Choose the right eviction policy for your access patterns:

```rust
use aurora_db::EvictionPolicy;

// LRU: Evict least recently used (good for general use)
let config = AuroraConfig {
    eviction_policy: EvictionPolicy::LRU,
    ..Default::default()
};

// LFU: Evict least frequently used (good for hot data)
let config = AuroraConfig {
    eviction_policy: EvictionPolicy::LFU,
    ..Default::default()
};

// FIFO: Evict oldest entries (simple, predictable)
let config = AuroraConfig {
    eviction_policy: EvictionPolicy::FIFO,
    ..Default::default()
};
```

**Policy Comparison:**
- **LRU**: Best for time-sensitive data (recent = important)
- **LFU**: Best for popularity-based data (frequently accessed = important)
- **FIFO**: Simple, predictable, but less intelligent

### Cache Prewarming

Prewarm the cache with critical data at startup:

```rust
// Prewarm cache with active user sessions
let sessions = db.query("sessions")
    .filter(|f| f.eq("active", true))
    .collect()
    .await?;

// Prewarm cache with product catalog
db.prewarm_cache("products", None).await?;

// Prewarm specific keys
let important_keys = vec!["user:admin", "config:system", "cache:prices"];
db.prewarm_keys(&important_keys).await?;
```

## Write Performance

### Write Buffer

Enable write buffering for high-throughput writes:

```rust
let config = AuroraConfig {
    enable_write_buffering: true,
    write_buffer_size: 1000,           // Buffer up to 1000 writes
    write_buffer_flush_interval_ms: 100, // Flush every 100ms
    ..Default::default()
};

let db = Aurora::open_with_config("mydb.db", config)?;
```

**Benefits:**
- 3-5x faster writes
- Automatic batching
- Reduced disk I/O

**Trade-offs:**
- Up to 100ms latency before write is persisted
- Risk of data loss on crash (mitigated by Write-Ahead Log)

### Batch Operations

Batch writes when possible:

```rust
// Bad: Individual inserts
for user in users {
    db.insert_into("users", user).await?;
}

// Good: Batch insert
let mut batch = Vec::new();
for user in users {
    batch.push(user);
}
db.batch_insert("users", batch).await?;
```

### Transaction Performance

Use transactions for related writes:

```rust
// Start transaction
db.begin_transaction()?;

// Multiple writes
db.insert_into("orders", order_data).await?;
db.insert_into("order_items", item1_data).await?;
db.insert_into("order_items", item2_data).await?;

// Commit once
db.commit_transaction()?;
```

**Benefits:**
- ACID guarantees
- Single disk flush
- Better performance than individual writes

## Query Optimization

### Use Projections

Select only needed fields:

```rust
// Bad: Fetch entire document
let users = db.query("users")
    .collect()
    .await?;

// Good: Select only needed fields
let users = db.query("users")
    .select(vec!["id", "name", "email"])
    .collect()
    .await?;
```

### Limit Result Sets

Always limit large queries:

```rust
// Bad: Unbounded query
let logs = db.query("logs").collect().await?;

// Good: Limited query
let recent_logs = db.query("logs")
    .order_by("timestamp", false)
    .limit(100)
    .collect()
    .await?;
```

### Use Indices for Sorting

Index fields used in `order_by`:

```rust
// Create index for sorting
db.create_index("products", "price").await?;

// Sort uses index (fast)
let products = db.query("products")
    .order_by("price", true)
    .collect()
    .await?;
```

### Filter Before Sort

Apply filters first to reduce sort set:

```rust
// Good: Filter then sort
let products = db.query("products")
    .filter(|f| f.eq("category", "electronics"))  // Reduce set first
    .order_by("price", true)                       // Sort smaller set
    .limit(20)
    .collect()
    .await?;
```

## Monitoring Performance

### Track Operation Times

```rust
use std::time::Instant;

// Measure query time
let start = Instant::now();
let results = db.query("users")
    .filter(|f| f.eq("active", true))
    .collect()
    .await?;
let duration = start.elapsed();

println!("Query took {:?} for {} results", duration, results.len());
```

### Benchmarking

Aurora includes comprehensive benchmarks:

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench single_put

# Compare with baseline
cargo bench -- --save-baseline my_baseline
# ... make changes ...
cargo bench -- --baseline my_baseline
```

### Performance Testing Example

```rust
// Test insertion performance
let db = Aurora::open("bench.db")?;

db.new_collection("test", vec![
    ("id", FieldType::String, true),
    ("data", FieldType::String, false),
])?;

let start = Instant::now();
for i in 0..10000 {
    db.insert_into("test", vec![
        ("id", Value::String(format!("id-{}", i))),
        ("data", Value::String(format!("data-{}", i))),
    ]).await?;
}
let duration = start.elapsed();

let ops_per_sec = 10000.0 / duration.as_secs_f64();
println!("Throughput: {:.0} ops/sec", ops_per_sec);
```

## Configuration Tuning

### Optimal Configuration Template

```rust
// High-throughput application
let high_throughput_config = AuroraConfig {
    hot_cache_size_mb: 512,
    cold_cache_capacity_mb: 1024,
    enable_write_buffering: true,
    write_buffer_size: 1000,
    write_buffer_flush_interval_ms: 100,
    eviction_policy: EvictionPolicy::LRU,
    cold_mode: sled::Mode::HighThroughput,
    max_index_entries_per_field: 100000,
    ..Default::default()
};

// Low-latency application
let low_latency_config = AuroraConfig {
    hot_cache_size_mb: 1024,  // Large cache
    cold_cache_capacity_mb: 2048,
    enable_write_buffering: false,  // Direct writes
    eviction_policy: EvictionPolicy::LFU,
    cold_mode: sled::Mode::LowSpace,
    max_index_entries_per_field: 50000,
    ..Default::default()
};

// Memory-constrained application
let low_memory_config = AuroraConfig {
    hot_cache_size_mb: 64,   // Minimal cache
    cold_cache_capacity_mb: 128,
    enable_write_buffering: true,
    write_buffer_size: 100,
    eviction_policy: EvictionPolicy::LRU,
    cold_mode: sled::Mode::LowSpace,
    max_index_entries_per_field: 10000,
    ..Default::default()
};
```

## Best Practices Summary

### DO:
1. ✅ Create indices on frequently queried fields
2. ✅ Use projections to select only needed fields
3. ✅ Enable write buffering for high-volume writes
4. ✅ Batch related operations
5. ✅ Limit result sets with `limit()`
6. ✅ Use transactions for related writes
7. ✅ Configure appropriate cache sizes
8. ✅ Prewarm cache with critical data
9. ✅ Monitor query performance
10. ✅ Use the right eviction policy

### DON'T:
1. ❌ Create indices on every field
2. ❌ Run unbounded queries on large collections
3. ❌ Fetch entire documents when you need one field
4. ❌ Use individual writes for bulk operations
5. ❌ Ignore cache configuration
6. ❌ Sort before filtering
7. ❌ Use computed fields for expensive operations
8. ❌ Keep unused indices
9. ❌ Ignore write buffer settings
10. ❌ Run blocking operations in async contexts

## Performance Benchmarks

Aurora 0.3.0 performance (Intel i7, SSD):

| Operation | Throughput | Latency |
|-----------|------------|---------|
| **Hot Cache Read** | ~200K ops/sec | ~5 μs |
| **Cold Storage Read** | ~50K ops/sec | ~20 μs |
| **Indexed Insert (1 field)** | ~17K ops/sec | ~57 μs |
| **Indexed Insert (2 fields)** | ~14K ops/sec | ~71 μs |
| **Bulk Insert (100 docs)** | ~15K ops/sec | ~67 μs/doc |
| **Query (indexed)** | ~50K ops/sec | ~20 μs |
| **Query (full scan)** | ~10K ops/sec | ~100 μs |
| **Transaction (3 ops)** | ~8K ops/sec | ~125 μs |

*Benchmarks run on consumer hardware. Production servers typically 2-3x faster.*

## Profiling and Diagnostics

### Enable Detailed Logging

```rust
// In your Cargo.toml
[dependencies]
env_logger = "0.10"

// In your code
env_logger::init();

// Set log level
RUST_LOG=aurora_db=debug cargo run
```

### Database Statistics

```rust
// Get cache statistics
let stats = db.get_cache_stats()?;
println!("Hot cache size: {} MB", stats.hot_cache_mb);
println!("Cache hit rate: {:.2}%", stats.hit_rate * 100.0);
println!("Total queries: {}", stats.total_queries);

// Get collection statistics
let coll_stats = db.get_collection_stats("users")?;
println!("Document count: {}", coll_stats.document_count);
println!("Index count: {}", coll_stats.index_count);
println!("Total size: {} MB", coll_stats.size_mb);
```

## Scaling Strategies

### Vertical Scaling
- Increase cache sizes
- Add more memory
- Use faster storage (NVMe)
- Enable write buffering

### Horizontal Patterns
- Shard by collection
- Separate read replicas (manual)
- Use external cache (Redis)
- Offload search (Elasticsearch)

Aurora is designed as an embedded database. For distributed deployments, consider:
- Running multiple Aurora instances
- Using external coordination (etcd, consul)
- Implementing application-level sharding

## Further Reading

- [Querying Guide](./querying.md) - Efficient query patterns
- [CRUD Operations](./crud.md) - Optimal data access patterns
- [Schema Design](./schema.md) - Schema design for performance
- [PubSub System](./pubsub.md) - Real-time updates
- [Durable Workers](./workers.md) - Background processing
