# Aurora Computed Fields

This guide covers how to use computed fields in Aurora Query Language (AQL) to derive values at query time.

## Overview

Computed fields allow you to transform data or calculate new values directly in your query selection. These values are calculated on-the-fly and are not stored in the database.

## Syntax

Computed fields are defined in the selection set using the `alias: expression` syntax.

```graphql
query {
    users {
        name
        email
        
        # Computed field: Template String
        display_label: "${name} <${email}>"
        
        # Computed field: Pipe Expression
        upper_name: name | uppercase
    }
}
```

## Expression Types

### 1. Template Strings

Use `${field}` syntax to interpolate field values into strings.

```graphql
query {
    users {
        full_name: "${first_name} ${last_name}"
        profile_url: "https://app.com/u/${username}"
    }
}
```

### 2. Pipe Expressions

Chain functions using the `|` operator for cleaner data transformations.

```graphql
query {
    products {
        name
        # Format price and ensure it's a string
        price_tag: price | toString | format("$%s")
        
        # Normalize tags
        clean_tags: tags | sort | join(", ")
    }
}
```

### 3. Function Calls

Use built-in functions for logic and math.

```graphql
query {
    orders {
        id
        
        # Logical conditional
        status_label: if(is_delivered, "Complete", "In Progress")
        
        # Math calculation
        total_value: multiply(quantity, price)
        
        # Date manipulation
        days_since: dateDiff(now(), created_at, "days")
    }
}
```

## Available Functions

Aurora supports a wide range of built-in functions:

*   **String:** `concat`, `uppercase`, `lowercase`, `trim`, `substring`, `replace`, `length`
*   **Math:** `add`, `subtract`, `multiply`, `divide`, `mod`, `round`, `floor`, `ceil`, `min`, `max`
*   **Logic:** `if`, `and`, `or`, `not`, `eq`, `gt`, `lt`, `isNull`
*   **Array:** `first`, `last`, `count`, `sum`, `avg`, `join`, `sort`
*   **Date:** `now`, `dateAdd`, `dateDiff`, `year`, `month`, `day`
*   **Type:** `toString`, `toInt`, `toFloat`, `toBoolean`

## Nested Computations

You can use computed fields as arguments to other functions within the same expression.

```graphql
query {
    products {
        # Calculate discount price
        final_price: subtract(price, multiply(price, discount_rate))
        
        # Complex logic
        stock_status: if(
            lt(stock, 10),
            "Low Stock",
            if(gt(stock, 100), "Plentiful", "Normal")
        )
    }
}
```# Aurora CRUD Operations Guide

This guide covers the basic Create, Read, Update, and Delete operations using the Aurora Query Language (AQL).

## Creating Documents

Use the `insertInto` mutation to add single documents.

```graphql
mutation {
    insertInto(
        collection: "users",
        data: {
            name: "John Doe",
            email: "john@example.com",
            age: 32,
            active: true
        }
    ) {
        id # Return the ID of the created document
    }
}
```

### Batch Inserts

For better performance, use `insertMany` to add multiple documents at once.

```graphql
mutation {
    insertMany(
        collection: "products",
        data: [
            { name: "Widget A", price: 10.0 },
            { name: "Widget B", price: 20.0 }
        ]
    ) {
        count # Return number of inserted documents
        ids   # Return list of generated IDs
    }
}
```

## Reading Documents

Retrieving documents is done via the `query` operation.

### Get Document by ID

```graphql
query {
    users(where: { id: { eq: "550e8400-e29b-41d4-a716-446655440000" } }) {
        id
        name
        email
    }
}
```

### Flexible Querying

```graphql
query {
    users(
        where: { active: { eq: true } },
        orderBy: { field: "age", direction: DESC },
        limit: 5
    ) {
        name
        age
    }
}
```

## Updating Documents

Use the `update` mutation to modify existing documents.

### Update by Filter

```graphql
mutation {
    update(
        collection: "products",
        where: { stock: { lt: 5 } },
        data: {
            status: "low_stock",
            needs_reorder: true
        }
    ) {
        affected # Returns the number of updated documents
    }
}
```

### Atomic Field Modifiers

You can use special operators to modify values atomically (e.g., incrementing counters).

```graphql
mutation {
    update(
        collection: "posts",
        where: { id: { eq: "123" } },
        data: {
            views: { increment: 1 },
            tags: { push: "trending" }
        }
    )
}
```

## Deleting Documents

Use the `deleteFrom` mutation to remove documents.

```graphql
mutation {
    deleteFrom(
        collection: "users",
        where: { id: { eq: "some-uuid" } }
    ) {
        affected
    }
}
```

## Upsert (Update or Insert)

The `upsert` operation tries to find a document matching the filter. If found, it updates it; otherwise, it inserts a new one.

```graphql
mutation {
    upsert(
        collection: "stats",
        where: { date: { eq: "2023-10-27" } },
        data: {
            date: "2023-10-27",
            daily_visits: 100
        }
    )
}
```

## Transactions

Aurora supports ACID transactions using the `transaction` block. All operations inside the block succeed or fail together.

```graphql
mutation {
    transaction {
        # Deduct balance
        debit: update(
            collection: "accounts",
            where: { id: { eq: "acc-1" } },
            data: { balance: { decrement: 50.0 } }
        )

        # Credit balance
        credit: update(
            collection: "accounts",
            where: { id: { eq: "acc-2" } },
            data: { balance: { increment: 50.0 } }
        )

        # Record transaction log
        log: insertInto(
            collection: "transfers",
            data: {
                from: "acc-1",
                to: "acc-2",
                amount: 50.0,
                timestamp: "2023-10-27T10:00:00Z"
            }
        )
    }
}
```
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

*Benchmarks run on consumer hardware. Production servers typically 2-3x faster.*# Aurora DB PubSub System

This guide covers Aurora's real-time change notification system using the AQL `subscription` operation.

## Overview

Aurora's PubSub system allows you to listen for changes to your data in real-time. When documents are inserted, updated, or deleted, change events are automatically pushed to connected clients.

## Basic Subscriptions

To listen for changes on a collection, use the `subscription` operation.

```graphql
subscription {
    users {
        mutation # The type of change: INSERT, UPDATE, DELETE
        id       # The ID of the document changed
        node {   # The actual document data (if available)
            name
            email
        }
    }
}
```

### Response Stream

The server will keep the connection open and stream results as they happen:

```json
// Event 1: User created
{
    "data": {
        "users": {
            "mutation": "INSERT",
            "id": "123",
            "node": { "name": "Alice", "email": "alice@ex.com" }
        }
    }
}

// Event 2: User updated
{
    "data": {
        "users": {
            "mutation": "UPDATE",
            "id": "123",
            "node": { "name": "Alice Cooper", "email": "alice@ex.com" }
        }
    }
}
```

## Filtered Subscriptions

You can subscribe to specific changes using the `where` argument. This reduces network traffic by only sending events you care about.

```graphql
subscription {
    # Only listen for changes to active admins
    users(where: {
        and: [
            { role: { eq: "admin" } },
            { active: { eq: true } }
        ]
    }) {
        mutation
        id
        node {
            name
            role
        }
    }
}
```

## Event Types

The `mutation` field (or `operation` field) indicates what happened:

*   `INSERT`: A new document was added.
*   `UPDATE`: An existing document was modified.
*   `DELETE`: A document was removed.

## Use Cases

### 1. Real-time Notifications

```graphql
subscription {
    notifications(where: { user_id: { eq: "current-user-id" } }) {
        mutation
        node {
            title
            message
            read
        }
    }
}
```

### 2. Live Chat

```graphql
subscription {
    messages(where: { room_id: { eq: "room-123" } }) {
        mutation
        node {
            sender
            text
            timestamp
        }
    }
}
```
# Aurora DB Query System

This guide explains Aurora DB's powerful query capabilities using the Aurora Query Language (AQL).

## Basic Queries

To retrieve data, use the `query` operation and select the fields you need:

```graphql
query {
    users {
        id
        name
        email
    }
}
```

This returns a JSON list of users with only the requested fields.

## Filtering

### Simple Filters

Use the `where` argument to filter results. The filter syntax uses operator objects (e.g., `eq`, `gt`, `contains`).

```graphql
query {
    # Equality
    active_users: users(where: { active: { eq: true } }) {
        name
    }

    # Greater than
    adults: users(where: { age: { gt: 18 } }) {
        name
        age
    }

    # Contains (Arrays or Strings)
    admins: users(where: { roles: { contains: "admin" } }) {
        name
    }
}
```

### Combining Filters

Combine conditions using `and`, `or`, and `not` operators.

```graphql
query {
    target_users: users(where: {
        and: [
            { age: { gt: 18 } },
            { age: { lt: 65 } },
            { or: [
                { subscription: { eq: "premium" } },
                { purchase_count: { gt: 5 } }
            ]}
        ]
    }) {
        name
        email
    }
}
```

## Sorting

Sort results using the `orderBy` argument.

```graphql
query {
    users(orderBy: { field: "age", direction: ASC }) {
        name
        age
    }
}
```

For multiple sort fields, pass a list:

```graphql
query {
    users(orderBy: [
        { field: "status", direction: DESC },
        { field: "name", direction: ASC }
    ]) {
        name
        status
    }
}
```

## Pagination

Use `limit` and `offset` for traditional pagination.

```graphql
query {
    # Get page 2 (items 11-20)
    users(limit: 10, offset: 10) {
        id
        name
    }
}
```

Aurora also supports cursor-based pagination via the `edges` selection (Relay-style).

## Full-Text Search

Perform full-text search using the `search` argument. This requires a text index on the target field.

```graphql
query {
    articles(search: {
        query: "quantum computing",
        fields: ["title", "content"],
        fuzzy: true
    }) {
        title
        snippet: content # You can alias fields
    }
}
```

## Aggregation

You can perform aggregations directly within your query.

```graphql
query {
    orders {
        # Get individual order data
        id
        amount
        
        # Get aggregate stats for this query result
        stats: aggregate {
            count
            total_revenue: sum(field: "amount")
            avg_order: avg(field: "amount")
        }
    }
}
```

## Group By

Group results by a specific field.

```graphql
query {
    orders {
        groupBy(field: "status") {
            key          # The status value (e.g., "shipped")
            count        # Number of orders in this group
            aggregate {  # Aggregates per group
                sum(field: "amount")
            }
        }
    }
}
```

## Computed Fields

You can define temporary computed fields in your query using pipe syntax or functions.

```graphql
query {
    users {
        name
        # Create a new field on the fly
        display_name: "${name} (${email})"
        
        # Transform existing data
        upper_role: role | uppercase
    }
}
```
# Aurora DB Reactive Queries

This guide covers how to build reactive, real-time user interfaces using Aurora's `subscription` system.

## Overview

A "Reactive Query" is a query that stays up-to-date automatically. Instead of polling the database, your application subscribes to changes and updates its local state instantly.

## The Pattern

To implement a reactive query:
1.  **Fetch Initial State**: Run a standard `query` to get the current list.
2.  **Subscribe to Updates**: Open a `subscription` with the *same filter* to hear about changes.
3.  **Merge Updates**: Apply the changes (Insert/Update/Delete) to your local list.

## Example: Active Users List

### Step 1: Initial Query

```graphql
query {
    users(where: { active: { eq: true } }) {
        id
        name
        status
    }
}
```

### Step 2: Subscription

```graphql
subscription {
    users(where: { active: { eq: true } }) {
        mutation # INSERT, UPDATE, DELETE
        id
        node {
            name
            status
        }
    }
}
```

### Step 3: Handling Updates (Client-Side Logic)

*   **On `INSERT`**: Add `node` to your list.
*   **On `UPDATE`**: Find item by `id` and replace it with `node`.
*   **On `DELETE`**: Remove item with `id` from your list.

## Live Search Example

For a search bar that updates in real-time:

```graphql
subscription {
    products(where: {
        name: { contains: "search_term" }
    }) {
        mutation
        id
        node {
            name
            price
        }
    }
}
```

## Benefits

*   **Zero Latency**: UI updates immediately when data changes.
*   **Efficiency**: No polling overhead; server pushes only what changed.
*   **Consistency**: Using the same filter for Query and Subscription ensures your UI is an exact reflection of the database state.# Aurora DB Schema Management

This guide covers how to define and manage collection schemas in Aurora DB using the Aurora Query Language (AQL).

## Collections Overview

Collections in Aurora DB are similar to tables in relational databases but with the flexibility of document stores. Each collection has a defined schema that enforces data integrity while allowing for rich types.

## Creating Collections

To create a new collection, use the `define collection` statement:

```graphql
mutation {
    schema {
        define collection users {
            id: Uuid! @primary
            name: String!
            email: Email! @unique
            age: Int
            active: Boolean = true
            preferences: JSON
            metadata: Any
        }
    }
}
```

The syntax includes:
1. Collection name (`users`)
2. Field definitions (`name: type`)
3. Type modifiers (`!` for required fields)
4. Default values (`= true`)
5. Directives (`@primary`, `@unique`)

## Field Types

Aurora DB supports a rich set of field types:

| Type          | Description                   | Example            |
| ------------- | ----------------------------- | ------------------ |
| `String`      | UTF-8 text data               | `"John Doe"`       |
| `Int`         | 64-bit integer                | `42`               |
| `Float`       | 64-bit floating point         | `3.14159`          |
| `Boolean`     | True/false value              | `true`             |
| `ID`          | Internal identifier           | `"user:123"`       |
| `Uuid`        | UUID v4 or v7                 | `550e8400...`      |
| `DateTime`    | ISO 8601 date-time            | `"2023-01-01T..."` |
| `Date`        | Calendar date                 | `"2023-01-01"`     |
| `Time`        | Time of day                   | `"12:00:00"`       |
| `JSON`        | Structured JSON object        | `{"x": 1}`         |
| `Email`       | Validated email address       | `"user@ex.com"`    |
| `URL`         | Validated URL                 | `"https://..."`    |
| `PhoneNumber` | Validated phone number        | `"+15550123"`      |
| `Any`         | Dynamic/Schemaless field      | *Anything*         |
| `[Type]`      | Array of values               | `["tag1", "tag2"]` |

### The `Any` Type
The `Any` type allows you to store mixed-type data or unstructured content within a specific field, effectively bypassing schema validation for that field only.

## Modifying Schemas

You can modify existing collections using the `alter collection` statement.

### Adding Fields

```graphql
mutation {
    schema {
        alter collection users {
            add last_login: DateTime
            add tags: [String]
        }
    }
}
```

### Removing Fields

```graphql
mutation {
    schema {
        alter collection users {
            drop temporary_field
        }
    }
}
```

### Renaming Fields

```graphql
mutation {
    schema {
        alter collection users {
            rename full_name to name
        }
    }
}
```

## Schema Directives

Directives allow you to add special behavior to fields:

*   `@primary`: Marks the field as the primary key.
*   `@unique`: Enforces a unique constraint across the collection.
*   `@indexed`: Creates an index for faster querying.
*   `@validate(min: 0, max: 100)`: Adds custom validation rules.

Example with validation:

```graphql
define collection products {
    sku: String! @primary
    price: Float! @validate(min: 0.0)
    stock: Int @validate(min: 0)
}
```

## Migrations

For complex schema changes that involve data transformation, use the `migrate` block:

```graphql
migration {
    "v1.0.1": {
        alter collection users {
            add status: String
        }
        migrate data in users {
            set status = "active" where { active: { eq: true } }
        }
    }
}
```
# Aurora DB Durable Workers

This guide covers Aurora's durable worker system for background job processing.

## Overview

Durable workers allow you to offload heavy tasks (email sending, image processing) to the background. Jobs are persisted, retried automatically on failure, and can be scheduled for the future.

## Enqueuing Jobs

You can enqueue jobs directly from AQL using the `enqueueJob` mutation.

```graphql
mutation {
    enqueueJob(
        type: "send_email",
        payload: {
            to: "user@example.com",
            subject: "Welcome!"
        },
        priority: HIGH
    ) {
        jobId
        status
    }
}
```

### Scheduled Jobs

Schedule a job to run in the future:

```graphql
mutation {
    enqueueJob(
        type: "cleanup_logs",
        payload: { days: 30 },
        runAt: "2023-12-31T23:59:59Z"
    ) {
        jobId
    }
}
```

### Options
*   `type`: The name of the job handler (must match a registered Rust handler).
*   `payload`: JSON object with job data.
*   `priority`: `LOW`, `NORMAL`, `HIGH`, `CRITICAL`.
*   `runAt`: ISO 8601 timestamp for delayed execution.
*   `retries`: Number of max retries (default: 3).

## Defining Handlers (Automation)

For simple automation, you can define handlers directly in AQL using `define handler`.

```graphql
# Automatically send a welcome email when a user is inserted
define handler "welcome_new_user" {
    on: "insert:users",
    action: {
        enqueueJob(
            type: "send_email",
            payload: {
                user_id: "${id}",
                template: "welcome"
            }
        )
    }
}
```

## Defining Workers (Rust Backend)

To process the jobs, you need to register Rust handlers in your application code.

```rust
use aurora_db::workers::{WorkerExecutor, JobHandler, Job, JobResult};

struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    async fn handle(&self, job: &Job) -> JobResult {
        let to = job.payload.get("to").unwrap();
        // Send email logic...
        Ok(())
    }
}

// In your main setup:
let mut executor = WorkerExecutor::new(db.clone(), 4);
executor.register_handler("send_email", Box::new(EmailHandler));
executor.start().await?;
```