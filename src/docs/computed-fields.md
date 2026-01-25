# Aurora Computed Fields

This guide covers how to use computed fields in Aurora Query Language (AQL) to derive values at query time.

## Overview

Computed fields allow you to transform data or calculate new values directly in your query selection. These values are calculated on-the-fly and are not stored in the database.

## Syntax

Computed fields are defined in the selection set using the `alias: expression` syntax.

```graphql
query {
    users {
        name
        email
        
        # Computed field: Template String
        display_label: "${name} <${email}>"
        
        # Computed field: Pipe Expression
        upper_name: name | uppercase
    }
}
```

## Expression Types

### 1. Template Strings

Use `${field}` syntax to interpolate field values into strings.

```graphql
query {
    users {
        full_name: "${first_name} ${last_name}"
        profile_url: "https://app.com/u/${username}"
    }
}
```

### 2. Pipe Expressions

Chain functions using the `|` operator for cleaner data transformations.

```graphql
query {
    products {
        name
        # Format price and ensure it's a string
        price_tag: price | toString | format("$%s")
        
        # Normalize tags
        clean_tags: tags | sort | join(", ")
    }
}
```

### 3. Function Calls

Use built-in functions for logic and math.

```graphql
query {
    orders {
        id
        
        # Logical conditional
        status_label: if(is_delivered, "Complete", "In Progress")
        
        # Math calculation
        total_value: multiply(quantity, price)
        
        # Date manipulation
        days_since: dateDiff(now(), created_at, "days")
    }
}
```

## Available Functions

Aurora supports a wide range of built-in functions:

*   **String:** `concat`, `uppercase`, `lowercase`, `trim`, `substring`, `replace`, `length`
*   **Math:** `add`, `subtract`, `multiply`, `divide`, `mod`, `round`, `floor`, `ceil`, `min`, `max`
*   **Logic:** `if`, `and`, `or`, `not`, `eq`, `gt`, `lt`, `isNull`
*   **Array:** `first`, `last`, `count`, `sum`, `avg`, `join`, `sort`
*   **Date:** `now`, `dateAdd`, `dateDiff`, `year`, `month`, `day`
*   **Type:** `toString`, `toInt`, `toFloat`, `toBoolean`

## Nested Computations

You can use computed fields as arguments to other functions within the same expression.

```graphql
query {
    products {
        # Calculate discount price
        final_price: subtract(price, multiply(price, discount_rate))
        
        # Complex logic
        stock_status: if(
            lt(stock, 10),
            "Low Stock",
            if(gt(stock, 100), "Plentiful", "Normal")
        )
    }
}
```