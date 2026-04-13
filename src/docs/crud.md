# Aurora DB CRUD Operations Guide

This guide details how to perform Create, Read, Update, and Delete operations using the Aurora Query Language (AQL) and the Rust API.

## 1. Creating Documents

### Single Insert
Use `insertInto` to add a single document. Aurora automatically validates the data against the collection schema.

```graphql
mutation {
    insertInto(
        collection: "users",
        data: {
            id: "u1",
            name: "Alice",
            email: "alice@example.com",
            tags: ["new-user", "beta-tester"]
        }
    ) {
        id
    }
}
```

### Batch Insert (`insertMany`)
For high-volume ingestion, use `insertMany`. This is significantly faster than multiple single inserts as it minimizes write-ahead log (WAL) overhead.

```graphql
mutation {
    insertMany(
        collection: "products",
        data: [
            { id: "p1", name: "Laptop", price: 1200.0 },
            { id: "p2", name: "Mouse", price: 25.0 }
        ]
    ) {
        affected # Number of documents inserted
    }
}
```

## 2. Reading Documents

Querying is performed via the `query` operation.

### Basic Retrieval
```graphql
query {
    users {
        name
        email
    }
}
```

### Filtering & Sorting
```graphql
query {
    products(
        where: { price: { lt: 50.0 } },
        orderBy: { field: "price", direction: ASC },
        limit: 10
    ) {
        name
        price
    }
}
```

## 3. Updating Documents

### Update by ID or Filter
The `update` mutation modifies existing documents that match a specific criteria.

```graphql
mutation {
    update(
        collection: "users",
        where: { id: { eq: "u1" } },
        data: {
            active: true,
            last_login: "now"
        }
    ) {
        affected
    }
}
```

### Atomic Modifiers
Aurora supports atomic field operations, preventing race conditions when multiple clients update the same document.

| Operator | Description |
| -------- | ----------- |
| `increment: N` | Adds N to a numeric field. |
| `decrement: N` | Subtracts N from a numeric field. |
| `push: val` | Appends a value to an array. |
| `pull: val` | Removes all instances of a value from an array. |

```graphql
mutation {
    update(
        collection: "posts",
        where: { id: { eq: "post-123" } },
        data: {
            views: { increment: 1 },
            tags: { push: "trending" }
        }
    )
}
```

## 4. Upsert (Update or Insert)

Upsert is an "idempotent" operation. If a document matching the `where` filter exists, it is updated. If not, a new document is created using the provided `data`.

```graphql
mutation {
    upsert(
        collection: "user_stats",
        where: { user_id: { eq: "u1" } },
        data: {
            user_id: "u1",
            login_count: { increment: 1 }
        }
    )
}
```

## 5. Deleting Documents

Use `deleteFrom` to permanently remove documents.

```graphql
mutation {
    deleteFrom(
        collection: "sessions",
        where: { expires_at: { lt: "now" } }
    ) {
        affected
    }
}
```

## 6. ACID Transactions

Transactions ensure that a group of operations are treated as a single atomic unit. If any operation fails, the entire transaction is rolled back.

### The `transaction` Block
```graphql
mutation {
    transaction {
        # Operation 1: Deduct from sender
        debit: update(
            collection: "accounts",
            where: { id: { eq: "acc-1" } },
            data: { balance: { decrement: 100.0 } }
        )

        # Operation 2: Add to receiver
        credit: update(
            collection: "accounts",
            where: { id: { eq: "acc-2" } },
            data: { balance: { increment: 100.0 } }
        )
    }
}
```

### Why use transactions?
- **Consistency**: Prevents partial updates (e.g., money leaving one account but not arriving in the other).
- **Isolation**: Changes made within a transaction are invisible to other clients until the transaction is committed.
- **Durability**: Aurora's WAL ensures that committed transactions survive system crashes.

## 7. Using the Rust API

The Rust API provides macros for ultra-efficient CRUD operations.

### Efficient Parametrized Mutation
```rust
use aurora_db::doc;

let user_id = "u1";
let new_name = "Alice Updated";

db.execute(doc!(
    "mutation($id: String, $name: String) {
        update(collection: \"users\", where: { id: { eq: $id } }, data: { name: $name }) {
            affected
        }
    }",
    { "id": user_id, "name": new_name }
)).await?;
```

For more details on macros, see [Variables and Macros](./querying.md#variables-and-macros).
