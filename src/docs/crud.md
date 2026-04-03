# Aurora CRUD Operations Guide

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
        id # Returns the ID provided in data (or generated internal _sid if missing)
    }
}
```

> **Note on IDs**: Aurora uses a decoupled internal System ID (`_sid`) for tracking. When you request `id` in a selection set, Aurora returns the value from your JSON data object. This ensures data purity. For more details, see [Schema Management](./schema.md#system-id--data-purity).

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