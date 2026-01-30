# Aurora DB Query System

This guide explains Aurora DB's powerful query capabilities using the Aurora Query Language (AQL).

## Basic Queries

To retrieve data, use the `query` operation and select the fields you need:

```graphql
query {
    users {
        id
        name
        email
    }
}
```

This returns a JSON list of users with only the requested fields.

## Filtering

### Simple Filters

Use the `where` argument to filter results. The filter syntax uses operator objects (e.g., `eq`, `gt`, `contains`).

```graphql
query {
    # Equality
    active_users: users(where: { active: { eq: true } }) {
        name
    }

    # Greater than
    adults: users(where: { age: { gt: 18 } }) {
        name
        age
    }

    # Contains (Arrays or Strings)
    admins: users(where: { roles: { contains: "admin" } }) {
        name
    }
}
```

### Combining Filters

Combine conditions using `and`, `or`, and `not` operators.

```graphql
query {
    target_users: users(where: {
        and: [
            { age: { gt: 18 } },
            { age: { lt: 65 } },
            { or: [
                { subscription: { eq: "premium" } },
                { purchase_count: { gt: 5 } }
            ]}
        ]
    }) {
        name
        email
    }
}
```

## Sorting

Sort results using the `orderBy` argument.

```graphql
query {
    users(orderBy: { field: "age", direction: ASC }) {
        name
        age
    }
}
```

For multiple sort fields, pass a list:

```graphql
query {
    users(orderBy: [
        { field: "status", direction: DESC },
        { field: "name", direction: ASC }
    ]) {
        name
        status
    }
}
```

## Pagination

Use `limit` and `offset` for traditional pagination.

```graphql
query {
    # Get page 2 (items 11-20)
    users(limit: 10, offset: 10) {
        id
        name
    }
}
```

Aurora also supports cursor-based pagination via the `edges` selection (Relay-style).

## Full-Text Search

Perform full-text search using the `search` argument. This requires a text index on the target field.

```graphql
query {
    articles(search: {
        query: "quantum computing",
        fields: ["title", "content"],
        fuzzy: true
    }) {
        title
        snippet: content # You can alias fields
    }
}
```

## Aggregation

You can perform aggregations directly within your query.

```graphql
query {
    orders {
        # Get individual order data
        id
        amount
        
        # Get aggregate stats for this query result
        stats: aggregate {
            count
            total_revenue: sum(field: "amount")
            avg_order: avg(field: "amount")
        }
    }
}
```

## Group By

Group results by a specific field.

```graphql
query {
    orders {
        groupBy(field: "status") {
            key          # The status value (e.g., "shipped")
            count        # Number of orders in this group
            aggregate {  # Aggregates per group
                sum(field: "amount")
            }
        }
    }
}
```

## Computed Fields

You can define temporary computed fields in your query using pipe syntax or functions.

```graphql
query {
    users {
        name
        # Create a new field on the fly
        display_name: "${name} (${email})"
        
        # Transform existing data
        upper_role: role | uppercase
    }
}
```