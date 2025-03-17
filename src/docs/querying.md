# Aurora DB Query System

This guide explains Aurora DB's powerful query capabilities for filtering, sorting, and retrieving documents.

## Basic Queries

To start a query on a collection, use the `query` method:

```rust
let results = db.query("users")
    .collect()
    .await?;
```

This returns all documents in the collection.

## Filtering

### Simple Filters

Aurora supports various filter operations:

```rust
// Equality filter
let active_users = db.query("users")
    .filter(|f| f.eq("active", true))
    .collect()
    .await?;

// Greater than filter
let adult_users = db.query("users")
    .filter(|f| f.gt("age", 18))
    .collect()
    .await?;

// Less than filter
let young_users = db.query("users")
    .filter(|f| f.lt("age", 30))
    .collect()
    .await?;

// Contains filter (for strings and arrays)
let admin_users = db.query("users")
    .filter(|f| f.contains("roles", "admin"))
    .collect()
    .await?;
```

### Combining Filters

Combine multiple conditions with logical operators:

```rust
// AND: Find active adult users
let active_adults = db.query("users")
    .filter(|f| f.eq("active", true) && f.gt("age", 18))
    .collect()
    .await?;

// Complex conditions
let target_users = db.query("users")
    .filter(|f| 
        (f.gt("age", 18) && f.lt("age", 65)) && 
        (f.eq("subscription", "premium") || f.gt("purchase_count", 5))
    )
    .collect()
    .await?;
```

## Sorting

Sort results by one or more fields:

```rust
// Sort by age ascending
let users = db.query("users")
    .filter(|f| f.gt("age", 18))
    .order_by("age", true)  // true = ascending
    .collect()
    .await?;

// Sort by subscription status, then by name
let users = db.query("users")
    .order_by("subscription_status", false)  // false = descending
    .order_by("name", true)  // ascending
    .collect()
    .await?;
```

## Pagination

Limit and skip results for pagination:

```rust
// Get second page of users (10 per page)
let page = 2;
let page_size = 10;

let users = db.query("users")
    .order_by("name", true)
    .limit(page_size)
    .offset((page - 1) * page_size)
    .collect()
    .await?;
```

## Projection

Select only specific fields:

```rust
// Get only names and emails
let contacts = db.query("users")
    .select(vec!["name", "email"])
    .collect()
    .await?;
```

## Using Indices

Aurora automatically uses indices when available. Create indices on frequently queried fields:

```rust
// Create an index on the age field
db.create_index("users", "age").await?;

// Queries filtering on age will now use the index
let adult_users = db.query("users")
    .filter(|f| f.gt("age", 18))
    .collect()
    .await?;
```

## Full-Text Search

For text search capabilities:

```rust
// Create a full-text index
db.create_text_index("articles", "content", true)?;

// Perform full-text search
let results = db.search("articles")
    .field("content")
    .matching("quantum computing")
    .collect()
    .await?;
```

## Query Examples

### Finding Recent Orders

```rust
let recent_orders = db.query("orders")
    .filter(|f| f.gt("date", "2023-01-01") && f.eq("status", "completed"))
    .order_by("date", false)  // newest first
    .limit(10)
    .collect()
    .await?;
```

### Aggregation-Like Queries

While Aurora doesn't have built-in aggregations, you can implement them in code:

```rust
// Get all orders for a customer
let customer_orders = db.query("orders")
    .filter(|f| f.eq("customer_id", "cust-123"))
    .collect()
    .await?;

// Calculate total in application code
let total_spent = customer_orders.iter()
    .map(|order| match order.data.get("amount") {
        Some(Value::Float(amount)) => *amount,
        _ => 0.0,
    })
    .sum::<f64>();
```

## Performance Tips

1. **Always use indices** for frequently queried fields
2. **Be specific with filters** to allow index usage
3. **Limit results** when you don't need the entire collection
4. **Use projection** to reduce data transfer when only specific fields are needed 