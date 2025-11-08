# Aurora DB Computed Fields

This guide covers Aurora's computed fields system for deriving values automatically from document data.

## Overview

Computed fields allow you to define expressions that automatically calculate derived values when documents are queried. These are similar to SQL computed columns or database views, but evaluated at query time.

## Basic Usage

### Defining Computed Fields

```rust
use aurora_db::computed::{ComputedFieldsRegistry, Expression};

// Create registry
let mut registry = ComputedFieldsRegistry::new();

// Register a simple computed field
registry.register(
    "users",
    "full_name",
    Expression::Concat {
        fields: vec!["first_name".to_string(), "last_name".to_string()],
        separator: " ".to_string(),
    },
);

// Apply to database
db.set_computed_fields(registry);
```

### Using Computed Fields

Once registered, computed fields appear automatically in query results:

```rust
// Insert user with separate name fields
db.insert_into("users", vec![
    ("first_name", Value::String("John".to_string())),
    ("last_name", Value::String("Doe".to_string())),
    ("age", Value::Int(30)),
]).await?;

// Query - full_name is automatically computed
let users = db.query("users").collect().await?;
for user in users {
    // full_name appears in the document automatically
    let full_name = user.data.get("full_name").unwrap();
    println!("User: {}", full_name); // "John Doe"
}
```

## Expression Types

### 1. Concat - String Concatenation

Combine multiple string fields:

```rust
// Full address from multiple fields
registry.register(
    "addresses",
    "full_address",
    Expression::Concat {
        fields: vec![
            "street".to_string(),
            "city".to_string(),
            "state".to_string(),
            "zip".to_string(),
        ],
        separator: ", ".to_string(),
    },
);

// Result: "123 Main St, Springfield, IL, 62701"
```

### 2. Sum - Numeric Addition

Sum multiple numeric fields:

```rust
// Total price from components
registry.register(
    "orders",
    "total",
    Expression::Sum {
        fields: vec![
            "subtotal".to_string(),
            "tax".to_string(),
            "shipping".to_string(),
        ],
    },
);

// If subtotal=100, tax=8, shipping=12, then total=120
```

### 3. Average - Numeric Average

Calculate average of fields:

```rust
// Average score from multiple tests
registry.register(
    "students",
    "average_score",
    Expression::Average {
        fields: vec![
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string(),
        ],
    },
);

// If test1=85, test2=90, test3=88, then average_score=87.67
```

### 4. Custom - JavaScript-like Expressions

For more complex logic, use custom expressions:

```rust
// Calculate age category
registry.register(
    "users",
    "age_category",
    Expression::Custom {
        code: r#"
            if (age < 18) return "minor";
            if (age < 65) return "adult";
            return "senior";
        "#.to_string(),
        dependencies: vec!["age".to_string()],
    },
);
```

## Advanced Examples

### 1. E-commerce Order Summary

```rust
let mut registry = ComputedFieldsRegistry::new();

// Subtotal from line items
registry.register(
    "orders",
    "subtotal",
    Expression::Sum {
        fields: vec!["item1_price".to_string(), "item2_price".to_string()],
    },
);

// Tax (10% of subtotal)
registry.register(
    "orders",
    "tax",
    Expression::Custom {
        code: "return subtotal * 0.10;".to_string(),
        dependencies: vec!["subtotal".to_string()],
    },
);

// Grand total
registry.register(
    "orders",
    "grand_total",
    Expression::Sum {
        fields: vec!["subtotal".to_string(), "tax".to_string(), "shipping".to_string()],
    },
);

// Discount label
registry.register(
    "orders",
    "discount_label",
    Expression::Custom {
        code: r#"
            if (grand_total > 100) return "FREE SHIPPING";
            if (grand_total > 50) return "10% OFF";
            return "No discount";
        "#.to_string(),
        dependencies: vec!["grand_total".to_string()],
    },
);

db.set_computed_fields(registry);
```

### 2. User Profile Enrichment

```rust
let mut registry = ComputedFieldsRegistry::new();

// Full name
registry.register(
    "users",
    "display_name",
    Expression::Concat {
        fields: vec!["title".to_string(), "first_name".to_string(), "last_name".to_string()],
        separator: " ".to_string(),
    },
);

// Account age in days
registry.register(
    "users",
    "account_age_days",
    Expression::Custom {
        code: r#"
            const now = Date.now();
            const created = new Date(created_at).getTime();
            return Math.floor((now - created) / (1000 * 60 * 60 * 24));
        "#.to_string(),
        dependencies: vec!["created_at".to_string()],
    },
);

// Membership tier
registry.register(
    "users",
    "tier",
    Expression::Custom {
        code: r#"
            if (total_purchases > 10000) return "platinum";
            if (total_purchases > 5000) return "gold";
            if (total_purchases > 1000) return "silver";
            return "bronze";
        "#.to_string(),
        dependencies: vec!["total_purchases".to_string()],
    },
);

db.set_computed_fields(registry);
```

### 3. Financial Calculations

```rust
let mut registry = ComputedFieldsRegistry::new();

// Net profit
registry.register(
    "transactions",
    "profit",
    Expression::Custom {
        code: "return revenue - cost;".to_string(),
        dependencies: vec!["revenue".to_string(), "cost".to_string()],
    },
);

// Profit margin percentage
registry.register(
    "transactions",
    "profit_margin",
    Expression::Custom {
        code: r#"
            if (revenue === 0) return 0;
            return ((revenue - cost) / revenue * 100).toFixed(2);
        "#.to_string(),
        dependencies: vec!["revenue".to_string(), "cost".to_string()],
    },
);

// Status indicator
registry.register(
    "transactions",
    "status",
    Expression::Custom {
        code: r#"
            const margin = parseFloat(profit_margin);
            if (margin < 0) return "LOSS";
            if (margin < 10) return "LOW_MARGIN";
            if (margin < 30) return "HEALTHY";
            return "EXCELLENT";
        "#.to_string(),
        dependencies: vec!["profit_margin".to_string()],
    },
);

db.set_computed_fields(registry);
```

### 4. Time-based Calculations

```rust
let mut registry = ComputedFieldsRegistry::new();

// Days until deadline
registry.register(
    "tasks",
    "days_remaining",
    Expression::Custom {
        code: r#"
            const deadline = new Date(due_date).getTime();
            const now = Date.now();
            const diff = deadline - now;
            return Math.ceil(diff / (1000 * 60 * 60 * 24));
        "#.to_string(),
        dependencies: vec!["due_date".to_string()],
    },
);

// Priority label based on deadline
registry.register(
    "tasks",
    "urgency",
    Expression::Custom {
        code: r#"
            if (days_remaining < 0) return "OVERDUE";
            if (days_remaining === 0) return "DUE_TODAY";
            if (days_remaining <= 2) return "URGENT";
            if (days_remaining <= 7) return "SOON";
            return "NORMAL";
        "#.to_string(),
        dependencies: vec!["days_remaining".to_string()],
    },
);

db.set_computed_fields(registry);
```

### 5. Aggregated Metrics

```rust
let mut registry = ComputedFieldsRegistry::new();

// Average rating
registry.register(
    "products",
    "avg_rating",
    Expression::Average {
        fields: vec![
            "rating_quality".to_string(),
            "rating_price".to_string(),
            "rating_service".to_string(),
        ],
    },
);

// Rating stars (convert to 5-star scale)
registry.register(
    "products",
    "stars",
    Expression::Custom {
        code: r#"
            const stars = Math.round(avg_rating / 20); // Convert 0-100 to 0-5
            return "★".repeat(stars) + "☆".repeat(5 - stars);
        "#.to_string(),
        dependencies: vec!["avg_rating".to_string()],
    },
);

// Recommendation label
registry.register(
    "products",
    "recommendation",
    Expression::Custom {
        code: r#"
            if (avg_rating >= 90) return "Highly Recommended";
            if (avg_rating >= 70) return "Recommended";
            if (avg_rating >= 50) return "Average";
            return "Not Recommended";
        "#.to_string(),
        dependencies: vec!["avg_rating".to_string()],
    },
);

db.set_computed_fields(registry);
```

## Chaining Computed Fields

Computed fields can depend on other computed fields:

```rust
let mut registry = ComputedFieldsRegistry::new();

// Step 1: Calculate base price
registry.register(
    "products",
    "base_total",
    Expression::Custom {
        code: "return price * quantity;".to_string(),
        dependencies: vec!["price".to_string(), "quantity".to_string()],
    },
);

// Step 2: Apply member discount (depends on base_total)
registry.register(
    "products",
    "discounted_total",
    Expression::Custom {
        code: r#"
            if (is_member) {
                return base_total * 0.9; // 10% discount
            }
            return base_total;
        "#.to_string(),
        dependencies: vec!["base_total".to_string(), "is_member".to_string()],
    },
);

// Step 3: Add tax (depends on discounted_total)
registry.register(
    "products",
    "final_total",
    Expression::Custom {
        code: "return discounted_total * 1.08;".to_string(), // 8% tax
        dependencies: vec!["discounted_total".to_string()],
    },
);

db.set_computed_fields(registry);
```

## Performance Considerations

1. **Evaluation Time**: Computed fields are calculated on every query. Keep expressions simple.

2. **Dependencies**: Clearly specify dependencies to avoid unnecessary recalculation.

3. **Caching**: Consider caching query results if the same data is accessed frequently.

4. **Materialized Fields**: For expensive calculations, consider storing the computed value as a regular field and updating it with triggers.

## Best Practices

1. **Simple Expressions**: Keep computed logic simple for better performance.

2. **Clear Names**: Use descriptive names like `total_price` not `tp`.

3. **Document Dependencies**: Always specify all field dependencies accurately.

4. **Error Handling**: Custom expressions should handle missing or invalid data gracefully.

5. **Testing**: Test computed fields with various input scenarios.

```rust
// Good: Simple, clear expression
registry.register(
    "orders",
    "total_price",
    Expression::Sum {
        fields: vec!["price".to_string(), "tax".to_string()],
    },
);

// Good: Error handling in custom expression
registry.register(
    "users",
    "age_group",
    Expression::Custom {
        code: r#"
            if (typeof age !== 'number' || age < 0) return "unknown";
            if (age < 18) return "minor";
            return "adult";
        "#.to_string(),
        dependencies: vec!["age".to_string()],
    },
);
```

## Limitations

- **No Side Effects**: Expressions cannot modify data or trigger external actions.
- **No Async Operations**: Expressions must be synchronous.
- **Limited JavaScript**: Custom expressions use a sandboxed JavaScript-like evaluator with limited APIs.
- **Query-Time Only**: Computed fields don't exist in storage, only in query results.

## When to Use Computed Fields

**Use computed fields when:**
- You need derived values that depend on existing fields
- The calculation is simple and fast
- You want to avoid storing redundant data
- Values need to be always current

**Don't use computed fields when:**
- Calculation is expensive or slow
- You need to index the computed value
- The value is used in filters (compute and store it instead)
- You need the value for external systems (store it explicitly)

For updating data based on changes, see [Durable Workers](./workers.md) or [PubSub System](./pubsub.md).
