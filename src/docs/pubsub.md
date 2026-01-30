# Aurora DB PubSub System

This guide covers Aurora's real-time change notification system using the AQL `subscription` operation.

## Overview

Aurora's PubSub system allows you to listen for changes to your data in real-time. When documents are inserted, updated, or deleted, change events are automatically pushed to connected clients.

## Basic Subscriptions

To listen for changes on a collection, use the `subscription` operation.

```graphql
subscription {
    users {
        mutation # The type of change: INSERT, UPDATE, DELETE
        id       # The ID of the document changed
        node {   # The actual document data (if available)
            name
            email
        }
    }
}
```

### Response Stream

The server will keep the connection open and stream results as they happen:

```json
// Event 1: User created
{
    "data": {
        "users": {
            "mutation": "INSERT",
            "id": "123",
            "node": { "name": "Alice", "email": "alice@ex.com" }
        }
    }
}

// Event 2: User updated
{
    "data": {
        "users": {
            "mutation": "UPDATE",
            "id": "123",
            "node": { "name": "Alice Cooper", "email": "alice@ex.com" }
        }
    }
}
```

## Filtered Subscriptions

You can subscribe to specific changes using the `where` argument. This reduces network traffic by only sending events you care about.

```graphql
subscription {
    # Only listen for changes to active admins
    users(where: {
        and: [
            { role: { eq: "admin" } },
            { active: { eq: true } }
        ]
    }) {
        mutation
        id
        node {
            name
            role
        }
    }
}
```

## Event Types

The `mutation` field (or `operation` field) indicates what happened:

*   `INSERT`: A new document was added.
*   `UPDATE`: An existing document was modified.
*   `DELETE`: A document was removed.

## Use Cases

### 1. Real-time Notifications

```graphql
subscription {
    notifications(where: { user_id: { eq: "current-user-id" } }) {
        mutation
        node {
            title
            message
            read
        }
    }
}
```

### 2. Live Chat

```graphql
subscription {
    messages(where: { room_id: { eq: "room-123" } }) {
        mutation
        node {
            sender
            text
            timestamp
        }
    }
}
```