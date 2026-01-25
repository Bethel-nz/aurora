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
*   **Consistency**: Using the same filter for Query and Subscription ensures your UI is an exact reflection of the database state.