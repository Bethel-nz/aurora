# Aurora DB Performance Optimization Guide

Aurora is designed for extreme performance in both read-heavy and write-heavy workloads. This guide provides an in-depth look at the engine's architecture and how to tune it for maximum efficiency.

## 1. Tiered Storage Architecture

Aurora uses a unique "Hot/Cold" storage system to balance speed and durability.

### The Hot Path (In-Memory)
- **DashMap/Moka**: Aurora keeps frequently accessed documents in a concurrent hash map or a high-performance LRU cache (`Moka`).
- **Performance**: Up to **1,000,000+ reads/sec**.
- **Optimization**: Ensure your `hot_cache_size_mb` is large enough to hold your most active "working set."

### The Cold Path (Persistent)
- **Sled/MMAP**: Persistent data lives in highly optimized B-Trees (`Sled`) or memory-mapped files.
- **Performance**: SSD-bound, typically **50,000+ reads/sec**.
- **Optimization**: Use high-performance NVMe drives for best results.

## 2. Advanced Indexing

Indexing is the single most important factor for query performance.

### Primary Index
- **Field**: `id` / `_sid`.
- **Performance**: Always O(1). Use direct ID lookups whenever possible.

### Secondary Indices (Roaring Bitmaps)
When you mark a field as `@indexed` or `@unique`, Aurora creates a compressed **Roaring Bitmap** index.
- **Bitwise Intersection**: When you query with multiple `where` filters (e.g., `age > 18 AND status == "active"`), Aurora performs a bitwise intersection of the two bitmaps. This is incredibly fast and completely avoids scanning the actual documents.
- **Memory Efficiency**: Roaring Bitmaps are highly compressed, allowing you to index millions of documents with minimal memory overhead.

### Full-Text Search Indices
- **Usage**: Use for keyword searching in large text fields.
- **Ranking**: Aurora uses a built-in relevance scoring algorithm based on term frequency and distance.

## 3. Query Optimization Best Practices

### Use `explain`
Before deploying a critical query, use `db.explain()` or the `@explain` directive in AQL to see how Aurora will execute it.
- **Look for**: `index_used: true`. If `false`, Aurora is performing a full collection scan (O(n)), which is slow for large collections.

### Selective Projections
Always select only the fields you need.
- **Why?**: Bypasses the overhead of serializing large documents.
- **Example**: `query { users { name } }` is much faster than fetching full user profiles.

### Use `@defer`
For fields containing large blobs or long text, use the `@defer` directive. This allows Aurora to return the primary document immediately while delaying the retrieval of the "heavy" fields.

## 4. Tuning the Engine (Rust)

```rust
let config = AuroraConfig {
    // Increase for larger working sets
    hot_cache_size_mb: 1024, 
    // Enable for 3-5x faster write throughput
    enable_write_buffering: true,
    // Adjust based on your durability requirements
    durability_mode: DurabilityMode::WAL,
    // Background worker threads for indexing/compaction
    worker_threads: 4,
    ..Default::default()
};
```

## 5. Write Performance

### Batching
The overhead of a mutation is largely in the Write-Ahead Log (WAL) and disk sync. 
- Use `insertMany` for bulk data.
- Use `transaction` blocks to group multiple updates.

### Write Buffering
Enabling `enable_write_buffering` allows Aurora to collect writes in memory and flush them to disk in large, contiguous chunks. This dramatically improves throughput on slow disks.

## 6. Benchmarks (Production-Ready)

| Operation | Performance (SSD + i7) | Complexity |
| --------- | ---------------------- | ---------- |
| **Point Lookup (ID)** | ~1,200,000 ops/sec | O(1) |
| **Indexed Filter** | ~450,000 ops/sec | O(log N) |
| **Bitwise AND Filter** | ~300,000 ops/sec | O(log N) |
| **Full Collection Scan** | ~15,000 docs/sec | O(N) |
| **Concurrent Writes** | ~80,000 ops/sec | O(1) |

*Note: These benchmarks are achieved using the Rust API with `doc!` and `object!` macros.*
