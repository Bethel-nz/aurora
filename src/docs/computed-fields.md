# Aurora DB Computed Fields Guide

Computed fields allow you to derive new values from existing document data at retrieval time. This is perfect for formatting, aggregations, or business logic that doesn't need to be persisted but is frequently needed by the application.

## 1. Defining Computed Fields in AQL

The simplest way to use computed fields is directly within your query selection set.

### Template Strings
Ideal for basic string concatenation and interpolation.

```graphql
query {
    users {
        # Combine fields into a display label
        label: "${firstName} ${lastName} (${email})"
    }
}
```

### Pipe Expressions
Inspired by Unix pipes, this syntax allows you to chain transformations in a readable way.

```graphql
query {
    posts {
        title
        # Convert to uppercase and truncate for a preview
        header: title | uppercase | truncate(length: 20)
    }
}
```

### Logical & Math Functions
You can use standard functional syntax for more complex logic.

```graphql
query {
    products {
        name
        # Calculate final price after tax
        total: multiply(price, 1.15) | round(decimals: 2)
        # Conditional labels
        status: if(gt(stock, 0), "In Stock", "Out of Stock")
    }
}
```

## 2. Permanent Computed Fields (Retrieval-Time)

If you find yourself writing the same computed field in every query, you can register it permanently in the collection's schema. These fields will be automatically calculated and included in *every* query result for that collection.

### Registering via Rust API
```rust
use aurora_db::computed::ComputedExpression;

db.register_computed_field(
    "users", 
    "fullName", 
    ComputedExpression::TemplateString("${firstName} ${lastName}".to_string())
).await?;
```

### Benefits of Permanent Fields
- **Consistency**: The logic lives in one place (the database schema).
- **Simplicity**: Clients don't need to know the calculation logic; they just request the field name.
- **Performance**: Aurora optimizes the calculation of registered fields during the retrieval phase.

## 3. Available Built-in Functions

Aurora provides a rich library of functions for your expressions:

| Category | Functions |
| -------- | --------- |
| **String** | `uppercase`, `lowercase`, `trim`, `truncate`, `concat`, `replace` |
| **Math** | `add`, `subtract`, `multiply`, `divide`, `abs`, `round`, `floor`, `ceil` |
| **Logic** | `if`, `coalesce` (returns first non-null), `isNull`, `isNotNull` |
| **Date** | `now`, `formatDate`, `year`, `month`, `day` |
| **Type** | `toString`, `toInt`, `toFloat` |

## 4. Best Practices

1.  **Prefer Retrieval-Time for Logic**: If the logic might change (e.g., tax rates), keep it as a computed field rather than persisting the result.
2.  **Use `coalesce` for Defaults**: When interpolating optional fields, use `coalesce` to provide a fallback value.
    - Example: `display: coalesce(nickname, firstName, "User")`
3.  **Watch Selection Depth**: Computed fields that depend on `lookup` results can be expensive. Use them sparingly in large result sets.
4.  **Use Pipes for Readability**: `name | trim | uppercase` is much easier to read than `uppercase(trim(name))`.
