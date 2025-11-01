# Aurora CRUD Operations Guide

This guide covers the basic Create, Read, Update, and Delete operations with Aurora.

## Creating Collections and Documents

### Define a Collection

Collections are like tables in SQL databases, but with a more flexible schema:

```rust
// Create a users collection
db.new_collection("users", vec![
    ("name", FieldType::String, false),
    ("email", FieldType::String, true),  // unique field
    ("age", FieldType::Int, false),
    ("active", FieldType::Bool, false),
])?;
```

### Insert Documents

```rust
// Insert a document with specific fields
let user_id = db.insert_into("users", vec![
    ("name", "John Doe"),
    ("email", "john@example.com"),
    ("age", 32),
    ("active", true),
])?;

// You can also use native Rust types
let complex_doc_id = db.insert_into("products", vec![
    ("name", Value::String("Widget Pro".to_string())),
    ("price", Value::Float(29.99)),
    ("tags", Value::Array(vec![
        Value::String("new".to_string()),
        Value::String("featured".to_string()),
    ])),
    ("metadata", Value::Object({
        let mut map = HashMap::new();
        map.insert("weight".to_string(), Value::Float(1.5));
        map.insert("color".to_string(), Value::String("blue".to_string()));
        map
    })),
])?;
```

## Reading Documents

### Get Document by ID

```rust
// Retrieve a specific document
if let Some(user) = db.get_document("users", &user_id)? {
    println!("Found user: {:?}", user);
} else {
    println!("User not found");
}
```

### Query Documents

Aurora's querying system is powerful and flexible:

```rust
// Simple query for all users
let all_users = db.query("users")
    .collect()
    .await?;

// Filter for active users over 21
let active_adult_users = db.query("users")
    .filter(|f| f.eq("active", true) && f.gt("age", 21))
    .collect()
    .await?;

// Sort by name
let sorted_users = db.query("users")
    .order_by("name", true)  // true = ascending
    .collect()
    .await?;

// Pagination
let page_2 = db.query("users")
    .limit(10)
    .offset(10)  // Skip first 10 results
    .collect()
    .await?;

// Get just specific fields
let names_only = db.query("users")
    .select(&["name", "email"])
    .collect()
    .await?;

// Get just the first match
let first_match = db.query("users")
    .filter(|f| f.eq("email", "john@example.com"))
    .first_one()
    .await?;

// Count results
let active_count = db.query("users")
    .filter(|f| f.eq("active", true))
    .count()
    .await?;
```

### Search Documents

For text search capabilities:

```rust
// Search for articles containing "quantum computing"
let articles = db.search("articles")
    .field("content")
    .matching("quantum computing")
    .collect()
    .await?;

// With fuzzy matching for typo tolerance
let search_results = db.search("products")
    .field("description")
    .matching("wireless headpones")  // typo in headphones
    .fuzzy(true)
    .collect()
    .await?;
```

## Updating Documents

### Update a Document by ID

```rust
// First get the document
if let Some(mut user) = db.get_document("users", &user_id)? {
    // Modify fields
    user.data.insert("age".to_string(), Value::Int(33));
    user.data.insert("last_login".to_string(), Value::String("2023-10-20".to_string()));

    // Save changes
    db.put(
        format!("users:{}", user.id),
        serde_json::to_vec(&user)?,
        None
    )?;
}
```

### Update Documents by Query

```rust
// Update all matching documents
let updated_count = db.query("products")
    .filter(|f| f.lt("stock", 5))
    .update([
        ("status", Value::String("low_stock".to_string())),
        ("needs_reorder", Value::Bool(true)),
    ].into_iter().collect())
    .await?;

println!("Updated {} products", updated_count);
```

## Deleting Documents

### Delete a Document by ID

```rust
// Delete a specific document
db.delete("users", &user_id)?;
```

### Delete Documents by Query

```rust
// Delete old logs
let deleted_count = db.delete_by_query("logs", |f|
    f.lt("timestamp", "2023-01-01")
).await?;

println!("Deleted {} old logs", deleted_count);
```

## Working with Transactions

Aurora supports ACID transactions:

```rust
// Begin a transaction
db.begin_transaction()?;

try {
    // Update account balance
    let account = db.get_document("accounts", &account_id)?.unwrap();
    let current_balance = match account.data.get("balance") {
        Some(Value::Float(bal)) => *bal,
        _ => 0.0,
    };

    // Create updated account with new balance
    let mut updated_account = account.clone();
    updated_account.data.insert("balance".to_string(), Value::Float(current_balance - 50.0));

    // Save the updated account
    db.put(
        format!("accounts:{}", account_id),
        serde_json::to_vec(&updated_account)?,
        None
    )?;

    // Record the transaction
    db.insert_into("transactions", vec![
        ("account_id", account_id),
        ("amount", Value::Float(-50.0)),
        ("timestamp", Value::String(chrono::Utc::now().to_rfc3339())),
    ])?;

    // Commit the transaction
    db.commit_transaction()?;
} catch (error) {
    // If any operation fails, roll back the transaction
    db.rollback_transaction()?;
    return Err(error);
}
```

## Import and Export

Aurora provides easy backup and restoration:

```rust
// Export a collection to JSON
db.export_as_json("users", "./backups/users.json")?;

// Import documents from JSON
let stats = db.import_from_json("users", "./backups/users.json").await?;
println!("Import completed: {} imported, {} skipped, {} failed",
    stats.imported, stats.skipped, stats.failed);
```

This guide covers the basic operations with Aurora. Refer to the API documentation for more advanced features and options.
