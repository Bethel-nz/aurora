# Aurora DB Schema Management

This guide covers how to define and manage collection schemas in Aurora DB.

## Collections Overview

Collections in Aurora DB are similar to tables in relational databases but with more flexibility. Each collection consists of documents that share a common schema structure.

## Creating Collections

To create a new collection, use the `new_collection` method:

```rust
db.new_collection("users", vec![
    ("name", FieldType::String, false),
    ("email", FieldType::String, true),  // unique field
    ("age", FieldType::Int, false),
    ("active", FieldType::Bool, false),
])?;
```

The method takes:

1. Collection name
2. Vector of field definitions, each containing:
   - Field name
   - Field type
   - Uniqueness flag (true = unique constraint)

## Field Types

Aurora DB supports the following field types:

| Type      | Description                   | Example            |
| --------- | ----------------------------- | ------------------ |
| `String`  | Text data                     | `"John Doe"`       |
| `Int`     | 64-bit integer                | `42`               |
| `Float`   | 64-bit floating point         | `3.14159`          |
| `Boolean` | True/false value              | `true`             |
| `Uuid`    | Universally unique identifier | `Uuid::new_v4()`   |
| `Array`   | List of values                | `["tag1", "tag2"]` |
| `Object`  | Nested document               | `{"x": 1, "y": 2}` |
| `Null`    | Absence of value              | `null`             |

## Primary Keys and Unique Constraints

Each collection can have one or more unique fields. When a field is marked as unique, Aurora DB enforces uniqueness across all documents in the collection.

```rust
// Create a collection with a unique email field
db.new_collection("users", vec![
    ("id", FieldType::Uuid, true),      // Primary key
    ("email", FieldType::String, true), // Unique constraint
    ("name", FieldType::String, false),
])?;
```

## Modifying Schemas

### Adding Fields

To add a new field to an existing collection:

```rust
db.add_field_to_collection("users", "last_login", FieldType::String, false)?;
```

### Removing Fields

To remove a field from a collection:

```rust
db.remove_field_from_collection("users", "temporary_field")?;
```

## Schema Validation

Aurora DB validates documents against the collection schema when inserting or updating:

```rust
// This will succeed
db.insert_into("users", vec![
    ("name", Value::String("John Doe".to_string())),
    ("email", Value::String("john@example.com".to_string())),
    ("age", Value::Int(30)),
])?;

// This will fail with a type mismatch error
db.insert_into("users", vec![
    ("name", Value::Int(123)),  // Error: expected String
    ("email", Value::String("john@example.com".to_string())),
])?;

// This will fail with a missing required field
db.insert_into("users", vec![
    ("name", Value::String("John Doe".to_string())),
    // Missing required email field
])?;
```

## Best Practices

1. **Define schemas explicitly**: Even though Aurora is schema-flexible, defining clear schemas improves data consistency.

2. **Use appropriate types**: Choose the most specific type for your data to enable proper indexing and querying.

3. **Consider indexing needs**: Fields that will be frequently queried should be considered for indexing.

4. **Limit unique constraints**: Use unique constraints only when necessary as they add overhead to write operations.

5. **Design for querying**: Structure your schemas based on how you'll query the data, not just how you'll store it.
