# Aurora DB Reactive Queries

This guide covers Aurora's reactive query system that automatically updates when underlying data changes.

## Overview

Reactive queries allow you to create queries that automatically re-execute when the data they depend on changes. This is perfect for building real-time applications where UI or downstream systems need to stay in sync with database changes.

## Basic Usage

### Creating a Reactive Query

```rust
use aurora_db::reactive::ReactiveQuery;

// Create a reactive query for active users
let reactive_query = db.reactive_query("users")
    .filter(|f| f.eq("active", true))
    .build()
    .await?;

// Get current results
let users = reactive_query.get_results().await?;
println!("Active users: {}", users.len());

// Subscribe to changes
let mut updates = reactive_query.subscribe();
while let Some(change) = updates.recv().await {
    match change {
        QueryUpdate::Added(doc) => println!("New user: {:?}", doc),
        QueryUpdate::Modified(doc) => println!("User updated: {:?}", doc),
        QueryUpdate::Removed(doc_id) => println!("User removed: {}", doc_id),
    }
}
```

## Query Update Types

Reactive queries emit three types of updates:

```rust
pub enum QueryUpdate {
    Added(Document),       // Document now matches query
    Modified(Document),    // Matching document was updated
    Removed(String),       // Document no longer matches query
}
```

### Added Events

Triggered when:
- A new document is inserted that matches the query
- An existing document is updated to match the query

### Modified Events

Triggered when:
- A document that already matches the query is updated
- The document still matches after the update

### Removed Events

Triggered when:
- A matching document is deleted
- A matching document is updated to no longer match the query

## Filtered Reactive Queries

### Single Field Filter

```rust
// Watch for premium users
let premium_users = db.reactive_query("users")
    .filter(|f| f.eq("subscription", "premium"))
    .build()
    .await?;

let mut updates = premium_users.subscribe();
while let Some(update) = updates.recv().await {
    match update {
        QueryUpdate::Added(user) => {
            // User upgraded to premium
            send_welcome_email(&user).await;
        }
        QueryUpdate::Removed(user_id) => {
            // User downgraded
            revoke_premium_features(&user_id).await;
        }
        _ => {}
    }
}
```

### Complex Filters

```rust
// Watch for high-value active orders
let important_orders = db.reactive_query("orders")
    .filter(|f|
        f.eq("status", "pending") &&
        f.gt("total", 1000.0)
    )
    .build()
    .await?;

let mut updates = important_orders.subscribe();
tokio::spawn(async move {
    while let Some(update) = updates.recv().await {
        if let QueryUpdate::Added(order) = update {
            notify_sales_team(&order).await;
        }
    }
});
```

## Reactive State Management

For simpler use cases, use `ReactiveState` which maintains an in-memory collection of matching documents:

```rust
use aurora_db::reactive::ReactiveState;

// Create reactive state for online users
let online_users = ReactiveState::new(
    db.clone(),
    "users",
    |f| f.eq("status", "online")
).await?;

// Get current online users
let users = online_users.get_all().await;
println!("Online users: {}", users.len());

// State automatically updates when users go online/offline
// Always has current data without manual polling
```

### Reactive State Operations

```rust
// Get all matching documents
let all = state.get_all().await;

// Get specific document (if it matches filter)
if let Some(user) = state.get(&user_id).await {
    println!("User is online: {:?}", user);
}

// Check if document exists in state
let is_online = state.contains(&user_id).await;
```

## Advanced Examples

### 1. Real-time Dashboard

Build a dashboard that updates automatically:

```rust
// Track key metrics in real-time
let active_sessions = ReactiveState::new(
    db.clone(),
    "sessions",
    |f| f.eq("active", true)
).await?;

let pending_orders = ReactiveState::new(
    db.clone(),
    "orders",
    |f| f.eq("status", "pending")
).await?;

// Dashboard always has current counts
loop {
    tokio::time::sleep(Duration::from_secs(1)).await;

    let session_count = active_sessions.get_all().await.len();
    let order_count = pending_orders.get_all().await.len();

    update_dashboard_ui(session_count, order_count);
}
```

### 2. Live Search Results

Update search results as data changes:

```rust
// User's search query
let search_term = "rust";

let search_results = db.reactive_query("articles")
    .filter(|f| f.contains("tags", search_term))
    .build()
    .await?;

// Initial results
let results = search_results.get_results().await?;
display_results(&results);

// Update UI when new matching articles are added
let mut updates = search_results.subscribe();
tokio::spawn(async move {
    while let Some(update) = updates.recv().await {
        match update {
            QueryUpdate::Added(article) => {
                add_result_to_ui(&article);
            }
            QueryUpdate::Removed(id) => {
                remove_result_from_ui(&id);
            }
            QueryUpdate::Modified(article) => {
                update_result_in_ui(&article);
            }
        }
    }
});
```

### 3. Inventory Monitoring

Monitor low stock items in real-time:

```rust
// Watch for low stock products
let low_stock = db.reactive_query("products")
    .filter(|f| f.lt("stock", 10))
    .build()
    .await?;

let mut updates = low_stock.subscribe();
tokio::spawn(async move {
    while let Some(update) = updates.recv().await {
        match update {
            QueryUpdate::Added(product) => {
                // Product stock fell below 10
                let product_name = product.data.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown");

                // Trigger reorder
                db.enqueue_job(
                    "reorder_product",
                    {
                        let mut payload = HashMap::new();
                        payload.insert("product_id".to_string(),
                            json!(product.id));
                        payload
                    },
                    None,
                    Priority::High,
                ).await.ok();

                // Notify team
                send_alert(&format!("Low stock: {}", product_name)).await;
            }
            QueryUpdate::Removed(product_id) => {
                // Stock replenished above 10
                log::info!("Stock restored for: {}", product_id);
            }
            _ => {}
        }
    }
});
```

### 4. User Presence System

Track which users are currently online:

```rust
use std::sync::Arc;
use tokio::sync::RwLock;

// Maintain online users list
let online_users = Arc::new(RwLock::new(HashSet::new()));

let presence = db.reactive_query("users")
    .filter(|f| f.eq("online", true))
    .build()
    .await?;

// Initialize with current online users
let initial_users = presence.get_results().await?;
for user in initial_users {
    online_users.write().await.insert(user.id);
}

// Track changes
let online_users_clone = Arc::clone(&online_users);
let mut updates = presence.subscribe();
tokio::spawn(async move {
    while let Some(update) = updates.recv().await {
        match update {
            QueryUpdate::Added(user) => {
                online_users_clone.write().await.insert(user.id.clone());
                broadcast_to_all(&format!("{} is now online", user.id)).await;
            }
            QueryUpdate::Removed(user_id) => {
                online_users_clone.write().await.remove(&user_id);
                broadcast_to_all(&format!("{} went offline", user_id)).await;
            }
            _ => {}
        }
    }
});

// Check if user is online
async fn is_user_online(user_id: &str, online_users: &RwLock<HashSet<String>>) -> bool {
    online_users.read().await.contains(user_id)
}
```

### 5. Leaderboard Updates

Real-time game leaderboard:

```rust
// Top 10 players by score
let leaderboard = db.reactive_query("players")
    .order_by("score", false)  // descending
    .limit(10)
    .build()
    .await?;

// Initial leaderboard
let top_players = leaderboard.get_results().await?;
display_leaderboard(&top_players);

// Update when rankings change
let mut updates = leaderboard.subscribe();
tokio::spawn(async move {
    while let Some(update) = updates.recv().await {
        // Refresh entire leaderboard when it changes
        let new_rankings = leaderboard.get_results().await.ok();
        if let Some(rankings) = new_rankings {
            update_leaderboard_ui(&rankings);
        }
    }
});
```

## Multiple Subscribers

Multiple parts of your application can subscribe to the same reactive query:

```rust
let query = db.reactive_query("orders")
    .filter(|f| f.eq("status", "pending"))
    .build()
    .await?;

// Subscriber 1: Update UI
let mut ui_updates = query.subscribe();
tokio::spawn(async move {
    while let Some(update) = ui_updates.recv().await {
        refresh_orders_ui().await;
    }
});

// Subscriber 2: Send notifications
let mut notification_updates = query.subscribe();
tokio::spawn(async move {
    while let Some(update) = notification_updates.recv().await {
        if let QueryUpdate::Added(order) = update {
            notify_warehouse(&order).await;
        }
    }
});

// Subscriber 3: Update analytics
let mut analytics_updates = query.subscribe();
tokio::spawn(async move {
    while let Some(update) = analytics_updates.recv().await {
        update_metrics(&update).await;
    }
});
```

## Performance Considerations

1. **Query Complexity**: Complex filters are evaluated on every change. Keep filters simple.

2. **Update Frequency**: High-frequency collections may generate many updates. Consider throttling in your subscriber.

3. **Memory Usage**: ReactiveState keeps all matching documents in memory. Limit result set size for large collections.

4. **Subscriber Count**: Each subscriber holds a broadcast channel. Clean up when done.

## Throttling Updates

For high-frequency updates, throttle in your subscriber:

```rust
use tokio::time::{interval, Duration};

let mut updates = reactive_query.subscribe();
let mut throttle = interval(Duration::from_millis(100));

tokio::spawn(async move {
    let mut pending_update = false;

    loop {
        tokio::select! {
            Some(_) = updates.recv() => {
                pending_update = true;
            }
            _ = throttle.tick() => {
                if pending_update {
                    refresh_ui().await;
                    pending_update = false;
                }
            }
        }
    }
});
```

## Best Practices

1. **Limit Result Sets**: Use filters to keep result sets small for ReactiveState.

2. **Cleanup Subscriptions**: Drop subscribers when no longer needed to free resources.

3. **Handle Missed Updates**: Reactive queries use broadcast channels which may lag. Handle `RecvError::Lagged`.

4. **Debounce UI Updates**: Group rapid updates to avoid excessive UI refreshes.

5. **Use ReactiveState for Simple Cases**: For maintaining a filtered collection, ReactiveState is simpler than manual subscription handling.

```rust
// Good: Limited, filtered query
let query = db.reactive_query("users")
    .filter(|f| f.eq("role", "admin"))  // Small result set
    .build()
    .await?;

// Bad: Unbounded query on large collection
let query = db.reactive_query("logs")  // Millions of documents
    .build()
    .await?;
```

## Comparison with PubSub

| Feature | Reactive Queries | PubSub |
|---------|------------------|--------|
| **Purpose** | Maintain query results | Listen to raw changes |
| **State** | Maintains current results | Stateless events |
| **Filtering** | Query-based | Event-based |
| **Use Case** | "Show all active users" | "Do X when Y happens" |
| **Overhead** | Higher (maintains state) | Lower (just events) |

**Use Reactive Queries** when you need to maintain a live view of query results.

**Use PubSub** when you need to react to individual change events without maintaining state.

See [PubSub System](./pubsub.md) for more on event-driven patterns.
