# Aurora DB PubSub System

This guide covers Aurora's real-time change notification system using the built-in PubSub (Publish-Subscribe) mechanism.

## Overview

Aurora's PubSub system allows you to listen for changes to your data in real-time. When documents are inserted, updated, or deleted, change events are automatically published to subscribers.

## Basic Usage

### Subscribing to Collection Changes

Subscribe to all changes in a collection:

```rust
// Subscribe to all user changes
let mut subscription = db.subscribe("users", None).await?;

// Listen for changes
while let Ok(event) = subscription.recv().await {
    match event.operation {
        OperationType::Insert => println!("New user created: {:?}", event.document),
        OperationType::Update => println!("User updated: {:?}", event.document),
        OperationType::Delete => println!("User deleted: {}", event.document_id),
    }
}
```

### Filtered Subscriptions

Subscribe only to specific changes using filters:

```rust
use aurora_db::pubsub::{EventFilter, FieldFilter};

// Only listen for new premium users
let filter = EventFilter {
    operation: Some(OperationType::Insert),
    field_filters: vec![
        FieldFilter {
            field: "subscription".to_string(),
            value: Value::String("premium".to_string()),
        }
    ],
};

let mut subscription = db.subscribe("users", Some(filter)).await?;

while let Ok(event) = subscription.recv().await {
    println!("New premium user: {:?}", event.document);
}
```

## Event Types

### Change Events

Every change event contains:

```rust
pub struct ChangeEvent {
    pub collection: String,      // Collection name
    pub document_id: String,      // Document ID
    pub operation: OperationType, // Insert, Update, or Delete
    pub document: Option<Document>, // Full document (None for Delete)
    pub timestamp: DateTime<Utc>, // When the change occurred
}
```

### Operation Types

```rust
pub enum OperationType {
    Insert,  // New document created
    Update,  // Existing document modified
    Delete,  // Document removed
}
```

## Advanced Filtering

### Multiple Field Filters

Filter on multiple fields simultaneously:

```rust
let filter = EventFilter {
    operation: Some(OperationType::Update),
    field_filters: vec![
        FieldFilter {
            field: "status".to_string(),
            value: Value::String("active".to_string()),
        },
        FieldFilter {
            field: "age".to_string(),
            value: Value::Int(21),
        },
    ],
};

let mut subscription = db.subscribe("users", Some(filter)).await?;
```

### Operation-Only Filters

Listen for specific operations without field filters:

```rust
// Only listen for deletions
let filter = EventFilter {
    operation: Some(OperationType::Delete),
    field_filters: vec![],
};

let mut subscription = db.subscribe("users", Some(filter)).await?;
```

## Global Listeners

Subscribe to changes across all collections:

```rust
// Listen to all database changes
let mut global_subscription = db.subscribe_global(None).await?;

while let Ok(event) = global_subscription.recv().await {
    println!("Change in {}: {:?}", event.collection, event.operation);
}
```

## Multiple Subscribers

Multiple subscribers can listen to the same collection independently:

```rust
// Subscriber 1: Log all changes
let mut logger = db.subscribe("orders", None).await?;
tokio::spawn(async move {
    while let Ok(event) = logger.recv().await {
        log::info!("Order changed: {}", event.document_id);
    }
});

// Subscriber 2: Send notifications for new orders
let filter = EventFilter {
    operation: Some(OperationType::Insert),
    field_filters: vec![],
};
let mut notifier = db.subscribe("orders", Some(filter)).await?;
tokio::spawn(async move {
    while let Ok(event) = notifier.recv().await {
        send_notification(&event.document).await;
    }
});
```

## Use Cases

### 1. Real-time Dashboard

Update UI when data changes:

```rust
let mut subscription = db.subscribe("metrics", None).await?;

tokio::spawn(async move {
    while let Ok(event) = subscription.recv().await {
        // Update dashboard UI
        update_dashboard(event).await;
    }
});
```

### 2. Audit Logging

Track all changes for compliance:

```rust
let mut audit_log = db.subscribe_global(None).await?;

tokio::spawn(async move {
    while let Ok(event) = audit_log.recv().await {
        db.insert_into("audit_log", vec![
            ("collection", Value::String(event.collection)),
            ("operation", Value::String(format!("{:?}", event.operation))),
            ("document_id", Value::String(event.document_id)),
            ("timestamp", Value::String(event.timestamp.to_rfc3339())),
        ]).await.ok();
    }
});
```

### 3. Cache Invalidation

Invalidate cache when data changes:

```rust
let mut cache_subscription = db.subscribe("products", None).await?;

tokio::spawn(async move {
    while let Ok(event) = cache_subscription.recv().await {
        // Invalidate cache entry
        cache.remove(&event.document_id);
    }
});
```

### 4. Data Synchronization

Sync changes to external systems:

```rust
let mut sync_subscription = db.subscribe("users", None).await?;

tokio::spawn(async move {
    while let Ok(event) = sync_subscription.recv().await {
        match event.operation {
            OperationType::Insert | OperationType::Update => {
                sync_to_elasticsearch(&event.document).await;
            }
            OperationType::Delete => {
                remove_from_elasticsearch(&event.document_id).await;
            }
        }
    }
});
```

## Performance Considerations

1. **Event Buffer Size**: The PubSub system uses a 10,000 event buffer by default. If subscribers are slow, events may be dropped.

2. **Subscriber Backpressure**: Slow subscribers don't block the database - events are broadcast asynchronously.

3. **Memory Usage**: Each subscriber holds a tokio broadcast channel. Unsubscribe when done to free resources.

4. **Event Filtering**: Filtering happens after publication. For high-volume collections, consider application-level filtering.

## Best Practices

1. **Use Filters**: Always filter at subscription time to reduce unnecessary processing.

2. **Handle Errors**: Subscribers may miss events if they're too slow. Handle `RecvError::Lagged`.

3. **Spawn Tasks**: Process events in separate tokio tasks to avoid blocking.

4. **Unsubscribe**: Drop subscriptions when no longer needed to free resources.

```rust
// Good: Filtered subscription
let filter = EventFilter {
    operation: Some(OperationType::Insert),
    field_filters: vec![],
};
let subscription = db.subscribe("users", Some(filter)).await?;

// Good: Process in separate task
tokio::spawn(async move {
    while let Ok(event) = subscription.recv().await {
        process_event(event).await;
    }
});
```

## Limitations

- **No Persistence**: Events are in-memory only. Missed events are not recoverable.
- **No Replay**: Cannot replay historical events.
- **Single Database**: Events are local to the database instance.

For durable event processing, see the [Durable Workers](./workers.md) documentation.
