# Aurora DB

> A lightweight, real-time embedded database for modern Rust applications.

[![Crates.io](https://img.shields.io/crates/v/aurora-db.svg)](https://crates.io/crates/aurora-db)
[![Documentation](https://docs.rs/aurora-db/badge.svg)](https://docs.rs/aurora-db)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Why Aurora?

Storage, indexing, pub/sub, reactive queries, and a job queue — in a single embedded crate. No external services, no separate processes.

---

## Installation

```toml
[dependencies]
aurora-db = "0.5.1"
tokio = { version = "1", features = ["full"] }
```

Optional features:

```toml
# REST API via Actix-web
aurora-db = { version = "5.1.0", features = ["http"] }
```

---

## Opening a Database

```rust
use aurora_db::{Aurora, AuroraConfig};

// Simple open
let db = Aurora::open("myapp.db").await?;

// With full configuration
let db = Aurora::with_config(AuroraConfig {
    db_path: "myapp.db".into(),
    hot_cache_size_mb: 256,
    enable_wal: true,
    enable_write_buffering: true,
    write_buffer_size: 10_000,
    ..Default::default()
}).await?;
```

---

## Two Query APIs

### Option 1 — Fluent Rust API

Best for type-safe, direct integration in Rust code.

```rust
use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use aurora_db::types::FieldDefinition;

// Define schema
db.new_collection("users", vec![
    ("name",  FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: false }),
    ("email", FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: true,  indexed: true,  nullable: false }),
    ("age",   FieldDefinition { field_type: FieldType::SCALAR_INT,    unique: false, indexed: true,  nullable: true  }),
]).await?;

// Insert
let id = db.insert_into("users", vec![
    ("name",  Value::String("Alice".into())),
    ("email", Value::String("alice@example.com".into())),
    ("age",   Value::Int(30)),
]).await?;

// Query
let users = db.query("users")
    .filter(|f| f.gt("age", Value::Int(25)))
    .order_by("age", false)   // false = DESC
    .limit(10)
    .collect()
    .await?;

// First match
let user = db.query("users")
    .filter(|f| f.eq("email", Value::String("alice@example.com".into())))
    .first_one()
    .await?;

// Count
let total = db.query("users").count().await?;

// Delete matching
let deleted = db.query("users")
    .filter(|f| f.eq("active", Value::Bool(false)))
    .delete()
    .await?;
```

**Filter operators available on `FilterBuilder`:**
`eq` · `ne` · `gt` · `gte` · `lt` · `lte` · `in_values` · `contains` · `starts_with` · `between` · `And` · `Or`

---

### Option 2 — AQL (Aurora Query Language)

GraphQL-style syntax. Best for flexible queries, scripts, or network APIs.

```rust
use aurora_db::parser::executor::ExecutionResult;

let result = db.execute(r#"
    query {
        users(
            where: { age: { gt: 25 } },
            orderBy: { age: DESC },
            limit: 10
        ) {
            name
            email
            age
        }
    }
"#).await?;

if let ExecutionResult::Query(q) = result {
    for doc in &q.documents {
        println!("{:?}", doc.data);
    }
}
```

---

## AQL Reference

### Schema Definition

```graphql
schema {
    define collection users {
        username:  String  @unique
        email:     String  @unique
        age:       Int     @indexed
        active:    Boolean
        tags:      [String]
    }
}
```

**Field types:** `String` · `Int` · `Float` · `Boolean` · `Uuid` · `Object` · `[Type]` (arrays)
**Directives:** `@unique` enforces uniqueness · `@indexed` builds a secondary index for fast lookups

---

### Insert

```graphql
mutation {
    insertInto(collection: "users", data: {
        username: "alice",
        email:    "alice@example.com",
        age:      28,
        active:   true,
        tags:     ["rust", "databases"]
    }) { id }
}
```

---

### Query with Filters

```graphql
query {
    users(
        where: {
            and: [
                { role:   { eq:  "admin" } },
                { active: { eq:  true    } },
                { age:    { gte: 18      } }
            ]
        },
        orderBy: { age: DESC },
        limit:   20,
        offset:  0
    ) {
        username
        email
        age
    }
}
```

**Supported filter operators:**

| Operator | Description |
|---|---|
| `eq` / `ne` | Equal / not equal |
| `gt` / `gte` / `lt` / `lte` | Numeric comparisons |
| `in` | Value in list |
| `contains` | Substring match |
| `startsWith` / `endsWith` | Prefix / suffix match |
| `isNull` / `isNotNull` | Null checks |
| `and` / `or` | Logical combinators |

---

### Cursor Pagination

```graphql
query {
    users(first: 10, after: "<cursor-id>", orderBy: { age: ASC }) {
        edges {
            cursor
            node { username age }
        }
        pageInfo {
            hasNextPage
            endCursor
        }
    }
}
```

---

### Aggregations

```graphql
query {
    orders {
        aggregate {
            count
            sum(field: "amount")
            avg(field: "amount")
            min(field: "amount")
            max(field: "amount")
        }
    }
}
```

---

### Group By

```graphql
query {
    users {
        groupBy(field: "role") {
            key
            count
            aggregate {
                avg(field: "age")
            }
            nodes {
                username
                age
            }
        }
    }
}
```

---

### Update & Delete

```graphql
mutation {
    update(
        collection: "users",
        data:  { active: false },
        where: { email: { eq: "alice@example.com" } }
    ) { id username }
}

mutation {
    deleteFrom(
        collection: "orders",
        where: { status: { eq: "cancelled" } }
    ) { id }
}
```

### Enqueue a Background Job

```graphql
mutation {
    enqueueJob(
        jobType:    "send_email",
        payload:    { to: "alice@example.com", subject: "Welcome" },
        priority:   HIGH,
        maxRetries: 3
    ) { id }
}
```

**Priority values:** `LOW` · `NORMAL` · `HIGH` · `CRITICAL`

---

### Fragments

Reusable field selections — Aurora validates fragment cycles and unknown type conditions at parse time.

```graphql
fragment UserFields on users {
    id
    username
    email
}

query {
    users(where: { active: { eq: true } }) {
        ...UserFields
    }
}
```

---

## Migrations

Aurora tracks schema changes with versioned migration blocks. Each version is recorded in an internal `_sys_migration` store — running the same migration file multiple times is safe, already-applied versions are skipped automatically.

```graphql
migrate {
    "v1.1.0": {
        alter collection users {
            add age:  Int
            add role: String
        }
    }
    "v1.2.0": {
        alter collection users {
            add last_login: String
        }
        migrate data in users {
            set role = "member"
        }
    }
    "v1.3.0": {
        migrate data in users {
            set role = "admin" where { email: { endsWith: "@internal.com" } }
        }
    }
}
```

**Alter actions:** `add field: Type` · `drop field` · `rename old_name to new_name` · `modify field: NewType`

**`migrate data` expressions:**
- String literal: `"value"`
- Number: `42` / `3.14`
- Boolean: `true` / `false`
- Field reference: `other_field` (copies from that field on the same document)

**Result:**

```rust
if let ExecutionResult::Migration(m) = result {
    println!("Applied {} version(s), last: {}", m.steps_applied, m.version);
    // m.status → "applied" | "skipped"
}
```

---

## Reactive Queries

Watch a query live — fires instantly when matching documents are inserted, updated, or deleted.

```rust
use aurora_db::reactive::QueryUpdate;

let mut watcher = db.query("orders")
    .filter(|f| f.eq("status", Value::String("pending".into())))
    .debounce(std::time::Duration::from_millis(50))
    .watch()
    .await?;

tokio::spawn(async move {
    while let Some(update) = watcher.next().await {
        match update {
            QueryUpdate::Added(doc)              => println!("New order: {}", doc.id),
            QueryUpdate::Modified { old, new }   => println!("Updated:   {}", new.id),
            QueryUpdate::Removed(doc)            => println!("Removed:   {}", doc.id),
        }
    }
});
```

---

## PubSub

Low-level broadcast channel for document changes.

```rust
// Listen to a single collection
let mut listener = db.listen("orders");

// Listen to all collections
let mut listener = db.listen_all();

tokio::spawn(async move {
    while let Ok(event) = listener.recv().await {
        println!("{:?} on {} — id: {}", event.change_type, event.collection, event.id);
    }
});
```

### AQL Subscription

```rust
let mut stream = db.stream(r#"
    subscription {
        orders(where: { status: { eq: "pending" } }) {
            id
            user_id
            amount
        }
    }
"#).await?;

while let Ok(event) = stream.recv().await {
    println!("{}", event);
}
```

---

## Background Workers

Durable, prioritised job queue with persistent state.

```rust
use aurora_db::workers::{WorkerSystem, WorkerConfig, Job, JobPriority};

let workers = WorkerSystem::new(WorkerConfig {
    storage_path:             "workers.db".into(),
    concurrency:              4,
    poll_interval_ms:         50,
    cleanup_interval_seconds: 3600,
})?;

workers.register_handler("send_email", |job| async move {
    let to = job.payload.get("to").unwrap();
    println!("Sending email to {}", to);
    Ok(())
}).await;

workers.start().await?;

workers.enqueue(
    Job::new("send_email")
        .add_field("to", "alice@example.com")
        .with_priority(JobPriority::High)
).await?;

workers.stop().await?;
```

**Job priorities:** `Low` · `Normal` · `High` · `Critical`
**Job statuses:** `Pending` · `Running` · `Completed` · `Failed` · `DeadLetter`

---

## Configuration Reference

```rust
AuroraConfig {
    // Storage
    db_path:                        PathBuf,         // required
    hot_cache_size_mb:              usize,            // default: 256
    eviction_policy:                EvictionPolicy,   // default: LRU
    cold_cache_capacity_mb:         usize,            // default: 1024
    cold_mode:                      ColdStoreMode,    // HighThroughput | LowSpace

    // Write buffering
    enable_write_buffering:         bool,             // default: true
    write_buffer_size:              usize,            // default: 10_000
    write_buffer_flush_interval_ms: u64,              // default: 1_000

    // Durability / WAL
    enable_wal:                     bool,             // default: true
    durability_mode:                DurabilityMode,   // None | WAL | Strict | Synchronous
    checkpoint_interval_ms:         u64,              // default: 10_000

    // Maintenance
    auto_compact:                   bool,             // default: true
    compact_interval_mins:          u64,              // default: 60

    // Audit logging
    audit_log_path:                 Option<String>,   // writes JSONL entries when set
}
```

---

## Architecture

```
┌──────────────────────────────────────────────┐
│              Application Layer               │
├──────────────────────────────────────────────┤
│  PubSub  │  Reactive  │  Workers  │  Audit   │
├──────────────────────────────────────────────┤
│       AQL Engine      │  Fluent Rust API     │
├──────────────────────────────────────────────┤
│  Hot Cache (In-Memory LRU)  │   Indexes      │
├─────────────────────────────┴────────────────┤
│          WAL  +  Cold Storage (Sled)         │
└──────────────────────────────────────────────┘
```

| Layer | Role |
|---|---|
| **Hot Cache** | LRU in-memory store for recently accessed documents |
| **Cold Storage** | Sled-backed persistent store for all durable data |
| **WAL** | Write-ahead log; replayed on startup for crash recovery |
| **Write Buffer** | Batches writes in memory for high-throughput ingestion |
| **Indexes** | Bitmap + secondary indexes for fast filtered queries |
| **AQL Engine** | Parses and executes GraphQL-style queries with validation |
| **PubSub** | Broadcast channel for low-latency change events |
| **Reactive** | Query watchers that diff results on each mutation |
| **Workers** | Persistent priority job queue with durable state |

---

## License

MIT — see [LICENSE](./LICENSE) for details.
