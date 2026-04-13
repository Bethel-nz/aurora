# Aurora DB Reactive Guide

Aurora DB features a first-class reactive system that allows your application to "watch" queries and receive updated result sets automatically whenever the underlying data changes.

## 1. The `QueryWatcher` (Rust API)

The `QueryWatcher` is the most powerful way to build reactive systems in Aurora. It automates the process of fetching initial data and subscribing to subsequent changes.

### Basic Usage
```rust
let mut watcher = db.query("users")
    .filter(|f| f.eq("active", true))
    .watch()
    .await?;

// Receive initial results
let initial_users = watcher.initial_results();
println!("Initial active users: {}", initial_users.len());

// Listen for updates
while let Some(updated_list) = watcher.next().await {
    println!("List updated! New count: {}", updated_list.len());
    // update_ui(updated_list);
}
```

## 2. How it Works Internally

1.  **Baseline**: When you call `.watch()`, Aurora executes the query and captures the initial result set.
2.  **Subscription**: It automatically opens a PubSub listener for the target collection.
3.  **Diffing**: Every time a mutation occurs in the collection, the watcher evaluates the change against your query filters.
4.  **Re-Evaluation**: If the change affects your result set (e.g., a new document matches, or an existing one is deleted), the watcher re-calculates the query and emits the entire new list.

## 3. Debouncing for Performance

In high-write environments, a query might change 100 times per second. To prevent overwhelming your UI, you can use `.debounce()` to group updates together.

```rust
let mut watcher = db.query("logs")
    .filter(|f| f.eq("level", "error"))
    .debounce(std::time::Duration::from_millis(500)) // Emit at most twice per second
    .watch()
    .await?;
```

## 4. AQL Subscriptions (Network/CLI)

If you are building a client that connects to Aurora over a network (e.g., a WebSocket), you can use the `subscription` operation directly.

```graphql
subscription {
    # Watch all active products
    products(where: { active: { eq: true } }) {
        mutation # "INSERT", "UPDATE", or "DELETE"
        id       # Document ID
        node {   # The full updated document
            name
            price
        }
    }
}
```

### Response Format
The subscription yields a stream of events:
```json
{
  "mutation": "UPDATE",
  "id": "p123",
  "node": { "name": "Laptop", "price": 999.0, "active": true }
}
```

## 5. Use Cases

- **Live Dashboards**: Watch "stats" or "logs" collections to update charts in real-time.
- **Collaborative Apps**: Use reactive queries to show a list of online users or active tasks.
- **Cache Invalidation**: Automatically clear local application caches when the database state changes.
- **Reactive UIs**: Perfect for frameworks like React or SwiftUI—simply pipe the `QueryWatcher` stream into your state management system.

## 6. Performance Considerations

- **Filter Complexity**: Keep reactive query filters simple. The more complex the filter, the more work the watcher has to do for every database mutation.
- **Index Usage**: Ensure fields used in reactive filters are **indexed**. This allows the watcher to discard irrelevant mutations instantly.
- **Result Set Size**: `QueryWatcher` emits the **entire list** on every change. If your result set contains thousands of documents, this can consume significant memory and CPU. For large lists, consider using `limit` or subscribing to individual document IDs.
