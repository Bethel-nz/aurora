# Aurora DB PubSub Guide

Aurora DB's PubSub (Publish-Subscribe) system is the engine behind its real-time capabilities. It provides a low-latency, asynchronous way to react to data changes across the entire database.

## 1. Core Concepts

-   **Topic**: In Aurora, topics are mapped to **Collection Names**.
-   **Events**: Every mutation (`insertInto`, `update`, `deleteFrom`, `upsert`) generates a `ChangeEvent`.
-   **Listeners**: Asynchronous consumers that receive events for specific collections or the entire database.

## 2. Using PubSub in Rust

The `Aurora` handle provides methods to create `ChangeListener` streams.

### Listening to a Specific Collection
```rust
let mut listener = db.listen("orders");

tokio::spawn(async move {
    while let Ok(event) = listener.recv().await {
        println!("Order {} was {}", event._sid, event.change_type);
    }
});
```

### Listening to All Changes
Perfect for global audit logs or system monitoring.
```rust
let mut global_listener = db.listen_all();
```

## 3. The `ChangeEvent` Structure

When a change occurs, listeners receive a `ChangeEvent` object:

| Field | Type | Description |
| ----- | ---- | ----------- |
| `collection` | `String` | The name of the affected collection. |
| `change_type` | `ChangeType` | `Insert`, `Update`, or `Delete`. |
| `_sid` | `String` | The internal system ID of the affected document. |
| `document` | `Option<Document>` | The updated document data (for Insert/Update). |
| `old_document` | `Option<Document>` | The previous version of the document (for Update). |

## 4. Advanced Filtering (`EventFilter`)

You can attach filters to a listener to reduce noise and improve performance. Filtering happens on the publisher side, so irrelevant events are never even sent to your listener's channel.

```rust
use aurora_db::pubsub::EventFilter;

let mut admin_listener = db.listen("users")
    .filter(EventFilter::FieldEquals("role".into(), Value::String("admin".into())));
```

### Available Filters
-   `ChangeType(type)`: Only listen for specific operations (e.g., `Insert`).
-   `FieldEquals(field, value)`: Only listen for documents where a field matches a specific value.
-   `FieldChanged(field)`: Only listen for updates that modified a specific field.

## 5. Subscriptions (AQL)

For network-based clients (like a WebSocket-connected frontend), use the AQL `subscription` operation.

```graphql
subscription {
    # Listen for new high-value orders
    orders(where: { total: { gt: 1000.0 } }) {
        mutation # "INSERT", "UPDATE", "DELETE"
        id
        node {
            total
            customer_name
        }
    }
}
```

## 6. Performance & Architecture

-   **Asynchronous Delivery**: Change events are published to an internal broadcast channel. This ensures that write operations (mutations) are not slowed down by slow listeners.
-   **Zero-Copy when possible**: Aurora uses `Arc` internally to share document data between multiple listeners without expensive cloning.
-   **Channel Capacity**: Each listener has a bounded internal buffer. If a listener is too slow and its buffer fills up, it will start missing events (lag detection).

## 7. Best Practices

1.  **Filter Early**: Always use `EventFilter` or AQL `where` clauses to minimize the number of events your application logic has to process.
2.  **Idempotent Handlers**: Design your event handlers to be idempotent. In rare crash scenarios, an event might be delivered more than once.
3.  **Non-Blocking**: Never perform heavy I/O or long-running synchronous tasks directly inside an event loop. Spawn a new task instead.
4.  **Use `listen_all` Sparingly**: Only use global listeners for truly cross-cutting concerns like auditing or replication.
