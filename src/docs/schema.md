# Aurora DB Schema Design Guide

Aurora DB uses a strictly enforced but flexible schema system. This guide provides an in-depth look at how to design robust schemas that ensure data integrity while maintaining high performance.

## Collections & The Type System

In Aurora, every collection must have a schema. This schema acts as a contract for all documents stored within that collection.

### Fundamental Scalar Types

| Type          | Rust Mapping | Validation |
| ------------- | ------------ | ---------- |
| `String`      | `String`     | Valid UTF-8 |
| `Int`         | `i64`        | 64-bit integer range |
| `Float`       | `f64`        | 64-bit floating point |
| `Boolean`     | `bool`       | `true` or `false` |
| `Uuid`        | `uuid::Uuid` | Strict UUID format (v4 or v7) |
| `DateTime`    | `chrono::DateTime<Utc>` | ISO 8601 UTC format |
| `Date`        | `chrono::NaiveDate` | `YYYY-MM-DD` |
| `Time`        | `chrono::NaiveTime` | `HH:MM:SS` |

### Specialized & Advanced Types

*   **`ID`**: A specialized string type optimized for identifiers.
*   **`Email`**: Automatically validates that the value is a correctly formatted email address.
*   **`URL`**: Validates the value against standard URL specifications.
*   **`JSON`**: Stores a structured object but requires it to be a valid map or array.
*   **`Any`**: The "escape hatch." Any data type can be stored here. Useful for truly dynamic fields or during rapid prototyping.
*   **`[Type]`**: Defines an array of a specific type (e.g., `[String]`, `[Int]`).

## Defining a Collection

Use the `define collection` statement to initialize a new collection.

```graphql
mutation {
    schema {
        define collection users {
            id: Uuid! @primary
            username: String! @unique
            email: Email! @unique
            age: Int @validate(min: 13, max: 120)
            bio: String
            created_at: DateTime = "now"
            tags: [String]
            metadata: JSON
        }
    }
}
```

### Type Modifiers
*   **`!` (Required)**: By default, all fields are optional (nullable). Appending `!` makes the field mandatory.
*   **`= value` (Default)**: Provides a default value if the field is omitted during insertion. Use `"now"` for current timestamps.

## Directives & Constraints

Directives are decorators that modify field behavior or add validation logic.

### Structural Directives
*   **`@primary`**: Defines the application-level primary key. Only one field can be primary.
*   **`@unique`**: Ensures no two documents have the same value for this field. Aurora uses optimized O(1) indices for unique checks.
*   **`@indexed`**: Explicitly tells Aurora to create a secondary index (Roaring Bitmap) for this field to speed up queries.

### Validation Directives
*   **`@validate(min: N, max: M)`**: Enforces range constraints on `Int` and `Float` fields.
*   **`@validate(minLength: N, maxLength: M)`**: Enforces length constraints on `String` fields.
*   **`@validate(pattern: "regex")`**: Validates `String` fields against a Regular Expression.

## Relationships & Foreign Keys

Aurora supports typed relations between collections using the `@relation` directive.

```graphql
mutation {
    schema {
        define collection posts {
            id: Uuid! @primary
            title: String!
            content: String!
            # Link to the users collection
            author_id: Uuid! @relation(to: "users", key: "id")
        }
    }
}
```

**Why use `@relation`?**
1.  **Referential Integrity**: Aurora can optionally check if the referenced ID exists in the target collection.
2.  **Automatic Lookups**: Enables the `lookup` operation in AQL to perform manual joins efficiently.
3.  **Graph Capabilities**: Forms the basis for future graph traversal features.

## The Data Purity Philosophy

Aurora follows a **"Pure Data"** philosophy. 

### Internal `_sid` vs. App `id`
Every document has a private internal system ID called `_sid` (a time-ordered UUIDv7). 
*   **`_sid`**: Used by the engine for storage ordering, indexing, and efficient pagination. It is hidden from your query results by default.
*   **`id`**: Your application's primary key. Aurora treats your `id` as just another data field. It never overwrites your `id` with the internal `_sid`.

This ensures that your application logic is never coupled to the database's internal tracking mechanisms.

## Modifying Schemas (Alteration)

Aurora supports non-destructive schema changes via `alter collection`.

```graphql
mutation {
    schema {
        alter collection users {
            add status: String = "active"
            drop bio
            rename username to handle
        }
    }
}
```

### Best Practices for Schema Design
1.  **Index Strategic Fields**: Only index fields used frequently in `where` or `order_by`. Every index adds a small write overhead.
2.  **Use Specialized Types**: Prefer `Email` or `Uuid` over `String` for better validation and storage optimization.
3.  **Required Fields**: Use `!` liberally for critical data to catch application bugs early.
4.  **Relationships**: Always define `@relation` for foreign keys to enable optimized join performance.
