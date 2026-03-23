# Aurora DB

[![Crates.io](https://img.shields.io/crates/v/aurora-db.svg)](https://crates.io/crates/aurora-db)
[![Documentation](https://docs.rs/aurora-db/badge.svg)](https://docs.rs/aurora-db)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An embedded database for Rust. Persistent storage, a GraphQL-style query language (AQL), pub/sub change events, reactive query watchers, and a background job queue — no external services required.

```toml
[dependencies]
aurora-db = "0.5.1"
tokio = { version = "1", features = ["full"] }
```

---

## Opening a database

```rust
use aurora_db::{Aurora, AuroraConfig};

let db = Aurora::open("myapp.db").await?;

// with config
let db = Aurora::with_config(AuroraConfig {
    db_path: "myapp.db".into(),
    hot_cache_size_mb: 256,
    enable_wal: true,
    ..Default::default()
}).await?;
```

---

## Queries

Two styles — pick whichever fits the context.

### Fluent API

```rust
use aurora_db::{Aurora, FieldType, Value};
use aurora_db::types::FieldDefinition;

db.new_collection("users", vec![
    ("name",  FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: false }),
    ("email", FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: true,  indexed: true,  nullable: false }),
    ("age",   FieldDefinition { field_type: FieldType::SCALAR_INT,    unique: false, indexed: true,  nullable: true  }),
]).await?;

let id = db.insert_into("users", vec![
    ("name",  Value::String("Alice".into())),
    ("email", Value::String("alice@example.com".into())),
    ("age",   Value::Int(30)),
]).await?;

let users = db.query("users")
    .filter(|f| f.gt("age", Value::Int(25)))
    .order_by("age", false)
    .limit(10)
    .collect()
    .await?;

let total = db.query("users").count().await?;

db.query("users")
    .filter(|f| f.eq("active", Value::Bool(false)))
    .delete()
    .await?;
```

Filter operators: `eq` `ne` `gt` `gte` `lt` `lte` `in_values` `contains` `starts_with` `between` `And` `Or`

### AQL (Aurora Query Language)

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

## AQL reference

### Schema

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

Field types: `String` `Int` `Float` `Boolean` `Uuid` `Object` `[Type]`
Directives: `@unique` enforces uniqueness · `@indexed` builds a secondary index

### Mutations

```graphql
mutation {
    insertInto(collection: "users", data: {
        username: "alice",
        email:    "alice@example.com",
        age:      28,
        active:   true
    }) { id }
}

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

### Filters

```graphql
query {
    users(
        where: {
            and: [
                { role:   { eq: "admin" } },
                { active: { eq: true   } },
                { age:    { gte: 18    } }
            ]
        },
        orderBy: { age: DESC },
        limit: 20,
        offset: 0
    ) {
        username email age
    }
}
```

| Operator | |
|---|---|
| `eq` / `ne` | equality |
| `gt` / `gte` / `lt` / `lte` | comparison |
| `in` | value in list |
| `contains` / `startsWith` / `endsWith` | string matching |
| `isNull` / `isNotNull` | null checks |
| `and` / `or` | combinators |

### Pagination

```graphql
query {
    users(first: 10, after: "<cursor>", orderBy: { age: ASC }) {
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

### Group By

```graphql
query {
    users {
        groupBy(field: "role") {
            key
            count
            aggregate { avg(field: "age") }
            nodes { username age }
        }
    }
}
```

### Fragments

```graphql
fragment UserFields on users {
    id username email
}

query {
    users(where: { active: { eq: true } }) {
        ...UserFields
    }
}
```

### Background jobs

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

Priority values: `LOW` `NORMAL` `HIGH` `CRITICAL`

---

## Migrations

Versioned migration blocks. Applied versions are tracked in `_sys_migration` — running the same file multiple times is safe.

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

Alter actions: `add field: Type` · `drop field` · `rename old to new` · `modify field: NewType`

```rust
if let ExecutionResult::Migration(m) = result {
    println!("applied {} version(s)", m.steps_applied);
    // m.status → "applied" | "skipped"
}
```

---

## Reactive queries

Watch a query live — fires on insert, update, or delete.

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
            QueryUpdate::Added(doc)            => println!("new:     {}", doc.id),
            QueryUpdate::Modified { old, new } => println!("updated: {}", new.id),
            QueryUpdate::Removed(doc)          => println!("removed: {}", doc.id),
        }
    }
});
```

---

## PubSub

```rust
// collection-scoped
let mut listener = db.listen("orders");

// all collections
let mut listener = db.listen_all();

while let Ok(event) = listener.recv().await {
    println!("{:?} on {} — {}", event.change_type, event.collection, event.id);
}
```

Watch specific fields — update events that don't touch those fields are skipped before filter evaluation:

```rust
let mut listener = db.listen("users")
    .watch_fields(["status", "role"]);
```

### AQL subscription

```rust
let mut stream = db.stream(r#"
    subscription {
        orders(where: { status: { eq: "pending" } }) {
            id user_id amount
        }
    }
"#).await?;

while let Ok(event) = stream.recv().await {
    println!("{}", event);
}
```

---

## Background workers

```rust
use aurora_db::workers::{WorkerSystem, WorkerConfig, Job, JobPriority};

let workers = WorkerSystem::new(WorkerConfig {
    storage_path:             "workers.db".into(),
    concurrency:              4,
    poll_interval_ms:         50,
    cleanup_interval_seconds: 3600,
})?;

workers.register_handler("send_email", |job| async move {
    println!("sending to {}", job.payload.get("to").unwrap());
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

Job statuses: `Pending` · `Running` · `Completed` · `Failed` · `DeadLetter`

---

## Configuration

```rust
AuroraConfig {
    db_path:                        PathBuf,         // required
    hot_cache_size_mb:              usize,            // default: 256
    eviction_policy:                EvictionPolicy,   // default: LRU
    cold_cache_capacity_mb:         usize,            // default: 1024
    cold_mode:                      ColdStoreMode,    // HighThroughput | LowSpace

    enable_write_buffering:         bool,             // default: true
    write_buffer_size:              usize,            // default: 10_000
    write_buffer_flush_interval_ms: u64,              // default: 1_000

    enable_wal:                     bool,             // default: true
    durability_mode:                DurabilityMode,   // None | WAL | Strict | Synchronous
    checkpoint_interval_ms:         u64,              // default: 10_000

    auto_compact:                   bool,             // default: true
    compact_interval_mins:          u64,              // default: 60

    audit_log_path:                 Option<String>,   // JSONL audit log when set
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
│  Hot Cache (LRU)  │  Indexes (Roaring Bitmap)│
├───────────────────┴──────────────────────────┤
│          WAL  +  Cold Storage (Sled)         │
└──────────────────────────────────────────────┘
```

| Layer | |
|---|---|
| Hot Cache | LRU in-memory store for recently accessed documents |
| Cold Storage | Sled-backed persistent store |
| WAL | Write-ahead log replayed on startup for crash recovery |
| Write Buffer | Batches writes in memory; flushes periodically |
| Indexes | Roaring bitmap secondary indexes for fast filtered queries |
| AQL Engine | Parses and executes GraphQL-style queries |
| PubSub | Broadcast channels with field-level change fingerprinting |
| Reactive | Query watchers that diff results on each mutation |
| Workers | Persistent priority job queue |

---

## License

MIT
