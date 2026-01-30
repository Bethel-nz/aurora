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

Indices dramatically improve query performance but add overhead to writes. You define indices in your schema using directives.

```graphql
define collection users {
    id: Uuid! @primary
    email: Email! @unique      # Automatically indexed
    age: Int @indexed          # Explicitly indexed
    status: String
}
```

**Create indices for:**
- Fields used in `where` filters
- Unique fields (automatically indexed)
- Sort fields used frequently

**Don't index:**
- Fields that change frequently
- Fields rarely used in queries
- Large text fields (use search instead)

### Index Performance

```graphql
# Without index: O(n) - scans all documents
query {
    users(where: { status: { eq: "active" } }) {
        name
    }
}

# With index: O(1) - direct lookup
# (Assuming @indexed was added to 'status')
```

## Caching Strategies

### Hot Cache Configuration

Configure hot cache size in your server setup (Rust):

```rust
let config = AuroraConfig {
    hot_cache_size_mb: 256,
    cold_cache_capacity_mb: 512,
    eviction_policy: EvictionPolicy::LRU,
    ..Default::default()
};
```

**Sizing Guidelines:**
- Small app (< 1K active users): 64-128 MB
- Medium app (1K-10K users): 256-512 MB
- Large app (> 10K users): 1-2 GB
- Rule of thumb: 20-30% of your dataset

## Write Performance

### Batch Operations

Always use batch operations when inserting multiple documents to reduce overhead.

```graphql
# Bad: Multiple single inserts
mutation { insertInto(...) }
mutation { insertInto(...) }

# Good: Single batch insert
mutation {
    insertMany(
        collection: "users",
        data: [
            { name: "A", ... },
            { name: "B", ... }
        ]
    ) {
        count
    }
}
```

### Transactions

Use transactions to bundle operations. This ensures they are committed together to the Write-Ahead Log (WAL), which is more efficient than individual commits.

```graphql
mutation {
    transaction {
        insertInto(...)
        update(...)
    }
}
```

## Query Optimization

### Use Projections

Select only the fields you need. This reduces serialization overhead and network transfer.

```graphql
# Bad: Implicitly fetching everything (if API allowed it)
# Good: Explicit selection
query {
    users {
        id
        name # Only fetch these two fields
    }
}
```

### Limit Result Sets

Always use `limit` on queries that might return many results.

```graphql
query {
    logs(
        orderBy: { field: "timestamp", direction: DESC },
        limit: 100
    ) {
        message
        level
    }
}
```

### Filter Before Sort

Aurora's query planner attempts to optimize this, but it's good practice to filter down the dataset as much as possible before sorting.

```graphql
query {
    products(
        where: { category: { eq: "electronics" } }, # Reduce set first
        orderBy: { field: "price", direction: ASC },
        limit: 20
    ) {
        name
        price
    }
}
```

## Monitoring Performance (Server-Side)

### Enable Detailed Logging

```bash
RUST_LOG=aurora_db=debug cargo run
```

### Database Statistics

You can inspect internal statistics using Rust:

```rust
let stats = db.get_cache_stats()?;
println!("Hot cache size: {} MB", stats.hot_cache_mb);
println!("Cache hit rate: {:.2}%", stats.hit_rate * 100.0);
```

## Performance Benchmarks

Aurora 0.3.0 performance (Intel i7, SSD):

| Operation | Throughput | Latency |
|-----------|------------|---------|
| **Hot Cache Read** | ~200K ops/sec | ~5 μs |
| **Cold Storage Read** | ~50K ops/sec | ~20 μs |
| **Indexed Insert** | ~17K ops/sec | ~57 μs |
| **Indexed Query** | ~50K ops/sec | ~20 μs |
| **Full Scan Query** | ~10K ops/sec | ~100 μs |

*Benchmarks run on consumer hardware. Production servers typically 2-3x faster.*