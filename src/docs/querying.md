# Aurora DB Querying Guide

This guide provides a comprehensive deep dive into the Aurora Query Language (AQL), exploring everything from basic retrieval to advanced aggregations and full-text search.

## 1. AQL Query Structure

AQL is inspired by GraphQL but optimized for document-database performance. A query consists of an operation name (`query`), a target collection, optional arguments (`where`, `limit`), and a **selection set** (the fields you want back).

```graphql
query {
    users(limit: 5) {
        name
        email
    }
}
```

## 2. Advanced Filtering (`where`)

Aurora supports rich, nested filtering logic using operator objects.

### Comparison Operators
| Operator | Description |
| -------- | ----------- |
| `eq` | Equal to |
| `ne` | Not equal to |
| `gt` / `gte` | Greater than / Greater than or equal to |
| `lt` / `lte` | Less than / Less than or equal to |
| `in` | Value exists in a provided list |
| `contains` | Substring match (strings) or element exists (arrays) |
| `startsWith` | Prefix match |

### Logical Operators (`and`, `or`, `not`)
```graphql
query {
    users(where: {
        or: [
            { and: [ { age: { gte: 18 } }, { status: { eq: "active" } } ] },
            { role: { eq: "admin" } }
        ]
    }) {
        name
    }
}
```

## 3. Projections & Aliases

You can rename fields in your result set using aliases.

```graphql
query {
    products {
        displayName: name
        current_price: price
    }
}
```

### The `@defer` Directive
If a field is computationally expensive or large (like a long `bio`), you can mark it with `@defer`. Aurora will exclude it from the primary document result and list it in the `deferred_fields` metadata.

```graphql
query {
    users {
        name
        bio @defer
    }
}
```

## 4. Sorting & Pagination

### Ordering
```graphql
query {
    posts(orderBy: { field: "created_at", direction: DESC }) {
        title
    }
}
```

### Offset Pagination
```graphql
query {
    products(limit: 10, offset: 20) {
        name
    }
}
```

## 5. Aggregation & Grouping

Aurora can perform calculations across entire collections or result sets.

### Global Aggregation
```graphql
query {
    sales {
        stats: aggregate {
            count
            total_revenue: sum(field: "amount")
            average_sale: avg(field: "amount")
            max_sale: max(field: "amount")
        }
    }
}
```

### Group By
Group documents by a field and perform aggregations per group.

```graphql
query {
    products {
        groupBy(field: "category") {
            key          # The category name
            count        # Number of products in this category
            stats: aggregate {
                avg_price: avg(field: "price")
            }
        }
    }
}
```

## 6. Manual Joins (`lookup`)

While Aurora is a document store, you can perform manual joins using the `lookup` selection. This is highly efficient when the join field is indexed.

```graphql
query {
    orders {
        id
        total
        # "Join" with the users collection
        user: lookup(collection: "users", localField: "user_id", foreignField: "id") {
            name
            email
        }
    }
}
```

## 7. Full-Text Search

Aurora features a built-in search engine. To use it, you must first create a text index on the target fields.

```graphql
query {
    articles(search: {
        query: "distributed systems",
        fields: ["title", "content"],
        fuzzy: 1  # Allows for 1-character typos
    }) {
        title
        score  # Aurora returns a relevance score for search results
    }
}
```

## 8. Computed Fields

You can generate new fields on the fly using **Templates** or **Pipes**.

### Template Strings
```graphql
query {
    users {
        # String interpolation
        fullName: "${firstName} ${lastName}"
    }
}
```

### Pipe Transformations
```graphql
query {
    users {
        # Transform data using built-in functions
        upperName: name | uppercase
        preview: bio | truncate(length: 50)
    }
}
```

## 9. Variables and Rust Integration

Never concatenate strings to build queries. Use **Variables** for safety and speed.

```graphql
query($id: Uuid!) {
    users(where: { id: { eq: $id } }) {
        name
    }
}
```

In Rust, use the `doc!` macro to bind values:
```rust
let id = "550e8400-e29b-41d4-a716-446655440000";
let result = db.execute(doc!(
    "query($id: Uuid!) { users(where: { id: { eq: $id } }) { name } }",
    { "id": id }
)).await?;
```
