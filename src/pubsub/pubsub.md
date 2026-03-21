# Aurora PubSub Module

Real-time event streaming for Aurora DB. This module provides a publish/subscribe system that notifies listeners whenever documents are inserted, updated, or deleted.

## Overview

- **`PubSubSystem`** — central hub; holds per-collection broadcast channels and a global catch-all channel
- **`ChangeChannel`** — thin wrapper around `tokio::sync::broadcast`
- **`ChangeEvent`** / **`ChangeType`** — event payload (Insert / Update / Delete)
- **`EventFilter`** — composable predicate tree applied on the listener side
- **`ChangeListener`** / **`ChangeStream`** — consumer types that drive async iteration

## Source: `channel.rs`

```rust
use super::events::ChangeEvent;
use tokio::sync::broadcast;

/// Wrapper around broadcast channel for change events
pub struct ChangeChannel {
    sender: broadcast::Sender<ChangeEvent>,
}

impl ChangeChannel {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer_size);
        Self { sender }
    }

    pub fn publish(&self, event: ChangeEvent) -> Result<(), broadcast::error::SendError<ChangeEvent>> {
        self.sender.send(event).map(|_| ())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}
```

## Source: `events.rs`

```rust
/// Type of change that occurred
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType { Insert, Update, Delete }

/// Change event containing information about a database modification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub collection: String,
    pub change_type: ChangeType,
    pub id: String,
    pub document: Option<Document>,
    pub old_document: Option<Document>,
}

/// Composable filter applied on the listener side — zero cost when unused
#[derive(Debug, Clone)]
pub enum EventFilter {
    All,
    ChangeType(ChangeType),
    HasField(String),
    FieldEquals(String, Value),
    FieldChanged(String),
    And(Vec<EventFilter>),
    Or(Vec<EventFilter>),
    Not(Box<EventFilter>),
    Gt(String, Value), Gte(String, Value),
    Lt(String, Value), Lte(String, Value),
    Ne(String, Value),
    In(String, Value), NotIn(String, Value),
    Contains(String, Value),
    StartsWith(String, Value),
    EndsWith(String, Value),
    IsNull(String), IsNotNull(String),
    /// Regex match — stores a *pre-compiled* `regex::Regex` to avoid per-event recompilation
    Matches(String, regex::Regex),
}
```

## Source: `listener.rs`

```rust
pub struct ChangeListener {
    collection: String,
    receiver: broadcast::Receiver<ChangeEvent>,
    filter: Option<EventFilter>,
}

impl ChangeListener {
    /// Add a filter — only events matching the predicate are delivered
    pub fn filter(mut self, filter: EventFilter) -> Self { ... }

    /// Async receive; skips non-matching events transparently
    pub async fn recv(&mut self) -> Result<ChangeEvent, broadcast::error::RecvError> { ... }

    /// Non-blocking receive
    pub fn try_recv(&mut self) -> Result<ChangeEvent, broadcast::error::TryRecvError> { ... }

    /// Convert into a stream for async-for iteration
    pub fn into_stream(self) -> ChangeStream { ... }
}
```

## Source: `mod.rs`

```rust
pub struct PubSubSystem {
    channels: Arc<DashMap<String, broadcast::Sender<ChangeEvent>>>,
    global_channel: broadcast::Sender<ChangeEvent>,
    buffer_size: usize,
}

impl PubSubSystem {
    pub fn new(buffer_size: usize) -> Self { ... }

    /// Publish to the collection channel + global channel
    pub fn publish(&self, event: ChangeEvent) -> Result<()> { ... }

    /// Subscribe to a specific collection
    pub fn listen(&self, collection: impl Into<String>) -> ChangeListener { ... }

    /// Subscribe to all collections
    pub fn listen_all(&self) -> ChangeListener { ... }
}
```

## Aurora entry points

Users never construct `PubSubSystem` directly — it lives inside `Aurora` and is exposed through three methods:

### `db.stream(aql)` — AQL subscription (async)

Subscribe via an AQL `subscription { ... }` query. Returns a `ChangeListener` filtered to the collection and fields named in the query.

```rust
let mut listener = db.stream(r#"
    subscription {
        products(where: { active: { eq: true } }) {
            id
            name
        }
    }
"#).await?;

while let Ok(event) = listener.recv().await {
    println!("Change: {:?} on {}", event.change_type, event.id);
}
```

### `db.listen(collection)` — Fluent API

Subscribe to a single collection. Chain `.filter()` to narrow events.

```rust
use aurora_db::pubsub::{EventFilter, ChangeType};

// All changes to "orders"
let mut listener = db.listen("orders");

// Only inserts where amount > 1000
let mut listener = db.listen("orders")
    .filter(EventFilter::And(vec![
        EventFilter::ChangeType(ChangeType::Insert),
        EventFilter::Gt("amount".into(), Value::Float(1000.0)),
    ]));

tokio::spawn(async move {
    while let Ok(event) = listener.recv().await {
        println!("[{}] {:?}", event.id, event.change_type);
    }
});
```

### `db.listen_all()` — Global listener

Receives every change across all collections. Useful for audit trails, replication, and CDC.

```rust
let mut listener = db.listen_all();

tokio::spawn(async move {
    while let Ok(event) = listener.recv().await {
        println!("Change in {}: {:?}", event.collection, event.change_type);
    }
});
```

## Internal usage

```rust
let pubsub = PubSubSystem::new(256);

// Filtered listener — only active-user inserts
let mut listener = pubsub
    .listen("users")
    .filter(EventFilter::And(vec![
        EventFilter::ChangeType(ChangeType::Insert),
        EventFilter::FieldEquals("active".into(), Value::Bool(true)),
    ]));

pubsub.publish(ChangeEvent::insert("users", "u1", doc))?;

let event = listener.recv().await?;
println!("Got: {:?}", event.change_type);
```
