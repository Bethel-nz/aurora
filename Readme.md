# Aurora DB

> A lightweight, real-time embedded database designed for modern applications.

[![Crates.io](https://img.shields.io/crates/v/aurora-db.svg)](https://crates.io/crates/aurora-db)
[![Documentation](https://docs.rs/aurora-db/badge.svg)](https://docs.rs/aurora-db)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why Aurora Exists

Most embedded databases force you to choose: either **simple key-value storage** with manual indexing and caching, or **heavy SQL databases** with complex setup.

**Aurora** aims to fill the gap for building real-time applications without external services. It combines storage, indexing, caching, pub/sub, and background workers into a single embedded crate.

### The Philosophy

**Real-time by default.** Data changes can propagate instantly using built-in PubSub.
**Lightweight.** Embedded directly into your application binary.
**Hybrid Architecture.** Frequently accessed data lives in memory (Hot Cache), while the rest persists to disk (Cold Storage via Sled).

---

## Quick Start

### Installation

```toml
[dependencies]
aurora-db = "0.5.0"
```

### Basic Usage

You can interact with Aurora using either the **Native Rust API** (builder pattern) or **AQL** (Aurora Query Language).

#### Option 1: Native Rust API
Best for type safety and direct integration in Rust code.

```rust
use aurora_db::{Aurora, FieldType, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Aurora::open("myapp.db")?;

    // 1. Define Schema
    db.new_collection("users", vec![
        ("name", FieldType::String, false),
        ("email", FieldType::String, true), // unique
        ("age", FieldType::Int, false),
    ])?;

    // 2. Insert Data
    db.insert_into("users", vec![
        ("name", Value::String("Alice".to_string())),
        ("email", Value::String("alice@example.com".to_string())),
        ("age", Value::Int(30)),
    ]).await?;

    // 3. Query Data
    let users = db.query("users")
        .filter(|f| f.gt("age", 25))
        .collect()
        .await?;

    println!("Found {} users", users.len());
    Ok(())
}
```

#### Option 2: Aurora Query Language (AQL)
Best for flexible queries, scripts, or exposing an API.

```rust
use aurora_db::Aurora;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Aurora::open("myapp.db")?;

    // 1. Define Schema
    db.execute(r#"
        mutation {
            schema {
                define collection users {
                    name: String!
                    email: String! @unique
                    age: Int
                }
            }
        }
    "#).await?;

    // 2. Insert Data
    db.execute(r#"
        mutation {
            insertInto(collection: "users", data: {
                name: "Alice",
                email: "alice@example.com",
                age: 30
            })
        }
    "#).await?;

    // 3. Query Data
    let json_result = db.execute(r#"
        query {
            users(where: { age: { gt: 25 } }) {
                name
                email
            }
        }
    "#).await?;

    println!("{}", json_result);
    Ok(())
}
```

## Feature Overview

### ⚡ Reactive Subscriptions
Listen to changes in real-time.

**Rust API:**
```rust
let mut sub = db.subscribe("users", None).await?;
while let Ok(event) = sub.recv().await {
    println!("Change: {:?}", event);
}
```

**AQL:**
```graphql
subscription {
    users(where: { active: { eq: true } }) {
        mutation
        node { name }
    }
}
```

### 🔍 Computed Fields
Derive values on the fly without storing them.

**Rust API:**
```rust
registry.register("users", "full_name", Expression::Concat { ... });
```

**AQL:**
```graphql
query {
    users {
        # Template string interpolation
        display: "${first_name} ${last_name}"
        # Pipe syntax
        status_label: status | uppercase
    }
}
```

### 🛠️ Background Jobs
Enqueue durable background jobs.

**Rust API:**
```rust
db.enqueue_job("send_email", payload, None, Priority::Normal).await?;
```

**AQL:**
```graphql
mutation {
    enqueueJob(type: "send_email", payload: { ... }, priority: NORMAL)
}
```

## Architecture

Aurora uses a hybrid storage architecture:

```
┌─────────────────────────────────────────┐
│           Application Layer              │
├─────────────────────────────────────────┤
│  PubSub  │ Reactive │ Workers │ Computed │
├─────────────────────────────────────────┤
│    AQL Engine    │   Rust Builder API    │
├─────────────────────────────────────────┤
│   Hot Cache (In-Memory)  │   Indices    │
├──────────────────────────┴───────────────┤
│      Cold Storage (Sled - On Disk)      │
└─────────────────────────────────────────┘
```

## Documentation

- [Schema Management](./src/docs/schema.md)
- [CRUD Operations](./src/docs/crud.md)
- [Querying Guide](./src/docs/querying.md)
- [PubSub & Real-time](./src/docs/pubsub.md)
- [Reactive Queries](./src/docs/reactive.md)
- [Durable Workers](./src/docs/workers.md)
- [Computed Fields](./src/docs/computed-fields.md)
- [Performance](./src/docs/performance.md)

## License

MIT License - see LICENSE file for details
