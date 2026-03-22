use aurora_db::types::{DurabilityMode, FieldType};
use aurora_db::{Aurora, AuroraConfig};
use aurora_db::parser::executor::ExecutionResult;
use serde_json::json;
use tempfile::TempDir;

async fn setup_db() -> (Aurora, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = AuroraConfig {
        db_path: dir.path().join("test.db"),
        enable_write_buffering: false,
        durability_mode: DurabilityMode::Synchronous,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await.unwrap();
    (db, dir)
}

// ────────────────────────────────────────────────────────
// Test 1: Variable in query `where` filter
//   `eq: $name` parses because `eq_operator = { "eq" ~ ":" ~ value }`
//   and `value` includes `variable`.  The executor's `matches_filter`
//   then resolves the variable before comparing.
// ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_variable_filters_query_results() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "users",
        vec![
            ("name".to_string(), FieldType::SCALAR_STRING, false),
            ("role".to_string(), FieldType::SCALAR_STRING, false),
        ],
    )
    .await
    .unwrap();

    // Insert three users with different roles
    for (name, role) in [("Alice", "admin"), ("Bob", "editor"), ("Carol", "admin")] {
        db.execute((
            r#"mutation { insertInto(collection: "users", data: { name: $n, role: $r }) { id } }"#,
            json!({ "n": name, "r": role }),
        ))
        .await
        .unwrap();
    }

    // Query only admins using a variable in the where filter
    let aql = r#"
        query GetByRole($target: String!) {
            users(where: { role: { eq: $target } }) {
                name
                role
            }
        }
    "#;

    let result = db.execute((aql, json!({ "target": "admin" }))).await.unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(
            res.documents.len(), 2,
            "Expected 2 admins, got {}",
            res.documents.len()
        );
        for doc in &res.documents {
            assert_eq!(
                doc.data.get("role"),
                Some(&aurora_db::types::Value::String("admin".to_string())),
                "Every returned document should have role == admin"
            );
        }
    } else {
        panic!("Expected Query result");
    }

    // Same query with a different variable value should return only Bob
    let result = db
        .execute((aql, json!({ "target": "editor" })))
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 1);
        assert_eq!(
            res.documents[0].data.get("name"),
            Some(&aurora_db::types::Value::String("Bob".to_string()))
        );
    } else {
        panic!("Expected Query result");
    }
}

// ────────────────────────────────────────────────────────
// Test 2: Multiple variables in a single query
//   Verify that two independent `$var` references in the same
//   where clause both resolve to their respective bound values.
// ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_multiple_variables_in_where() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "products",
        vec![
            ("name".to_string(), FieldType::SCALAR_STRING, false),
            ("category".to_string(), FieldType::SCALAR_STRING, false),
            ("price".to_string(), FieldType::SCALAR_INT, false),
        ],
    )
    .await
    .unwrap();

    for (name, category, price) in [
        ("Laptop", "electronics", 999),
        ("Phone", "electronics", 499),
        ("Desk", "furniture", 300),
        ("Chair", "furniture", 150),
    ] {
        db.execute((
            r#"mutation {
                insertInto(collection: "products", data: {
                    name: $n, category: $c, price: $p
                }) { id }
            }"#,
            json!({ "n": name, "c": category, "p": price }),
        ))
        .await
        .unwrap();
    }

    // Filter by category AND a price ceiling — two variables
    let aql = r#"
        query FindAffordable($cat: String!, $maxPrice: Int!) {
            products(where: {
                category: { eq: $cat },
                price: { lte: $maxPrice }
            }) {
                name
                price
            }
        }
    "#;

    let result = db
        .execute((aql, json!({ "cat": "electronics", "maxPrice": 700 })))
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 1, "Only Phone should match");
        assert_eq!(
            res.documents[0].data.get("name"),
            Some(&aurora_db::types::Value::String("Phone".to_string()))
        );
    } else {
        panic!("Expected Query result");
    }
}

// ────────────────────────────────────────────────────────
// Test 3: Lookup `where` filter with a variable
//   The lookup grammar stores the filter as a round-tripped Value
//   (filter_object → value_to_filter → filter_to_value), preserving
//   Variable nodes.  `apply_projection_with_lookups` then calls
//   `resolve_ast_deep` to substitute them before compiling the filter.
// ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_variable_in_lookup_where_filter() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "users",
        vec![
            ("name".to_string(), FieldType::SCALAR_STRING, false),
            ("active".to_string(), FieldType::SCALAR_BOOL, false),
        ],
    )
    .await
    .unwrap();

    db.new_collection(
        "orders",
        vec![
            ("title".to_string(), FieldType::SCALAR_STRING, false),
            ("user_id".to_string(), FieldType::SCALAR_STRING, false),
        ],
    )
    .await
    .unwrap();

    // Insert users — one active, one inactive
    let alice_result = db
        .execute(r#"mutation {
            insertInto(collection: "users", data: { name: "Alice", active: true }) { id }
        }"#)
        .await
        .unwrap();

    let alice_id = if let ExecutionResult::Mutation(res) = alice_result {
        res.returned_documents[0].id.clone()
    } else {
        panic!("Expected mutation result");
    };

    let bob_result = db
        .execute(r#"mutation {
            insertInto(collection: "users", data: { name: "Bob", active: false }) { id }
        }"#)
        .await
        .unwrap();

    let bob_id = if let ExecutionResult::Mutation(res) = bob_result {
        res.returned_documents[0].id.clone()
    } else {
        panic!("Expected mutation result");
    };

    // Each user gets one order
    for (title, uid) in [("Order-A", alice_id.as_str()), ("Order-B", bob_id.as_str())] {
        db.execute((
            r#"mutation {
                insertInto(collection: "orders", data: { title: $t, user_id: $u }) { id }
            }"#,
            json!({ "t": title, "u": uid }),
        ))
        .await
        .unwrap();
    }

    // Query orders and lookup only ACTIVE users via a variable
    let aql = r#"
        query OrdersWithActiveUsers($onlyActive: Boolean!) {
            orders {
                title
                user: lookup(
                    collection: "users",
                    localField: "user_id",
                    foreignField: "id",
                    where: { active: { eq: $onlyActive } }
                ) {
                    name
                    active
                }
            }
        }
    "#;

    let result = db
        .execute((aql, json!({ "onlyActive": true })))
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 2, "Both orders returned");

        let mut active_user_count = 0usize;
        for doc in &res.documents {
            if let Some(aurora_db::types::Value::Array(users)) = doc.data.get("user") {
                active_user_count += users.len();
                for u in users {
                    if let aurora_db::types::Value::Object(obj) = u {
                        assert_eq!(
                            obj.get("active"),
                            Some(&aurora_db::types::Value::Bool(true)),
                            "Lookup where filter should only include active users"
                        );
                    }
                }
            }
        }
        // Only Alice is active, so exactly one order has a non-empty user array
        assert_eq!(active_user_count, 1, "Exactly one active user should appear across all lookups");
    } else {
        panic!("Expected Query result");
    }
}

// ────────────────────────────────────────────────────────
// Test 4: Variable passed to mutation data fields
//   Mutations accept variables in the `data:` object.
//   `resolve_value` (called inside `execute_mutation`) replaces
//   Variable nodes before the document is written.
// ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_variable_in_mutation_data() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "items",
        vec![
            ("label".to_string(), FieldType::SCALAR_STRING, false),
            ("qty".to_string(), FieldType::SCALAR_INT, false),
        ],
    )
    .await
    .unwrap();

    let aql = r#"
        mutation CreateItem($label: String!, $qty: Int!) {
            insertInto(collection: "items", data: { label: $label, qty: $qty }) {
                id
                label
                qty
            }
        }
    "#;

    let result = db
        .execute((aql, json!({ "label": "Widget", "qty": 42 })))
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.affected_count, 1);
    } else {
        panic!("Expected Mutation result");
    }

    // Read it back to verify the values were stored
    let read_result = db
        .execute(r#"query { items { label qty } }"#)
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = read_result {
        assert_eq!(res.documents.len(), 1);
        assert_eq!(
            res.documents[0].data.get("label"),
            Some(&aurora_db::types::Value::String("Widget".to_string()))
        );
        assert_eq!(
            res.documents[0].data.get("qty"),
            Some(&aurora_db::types::Value::Int(42))
        );
    } else {
        panic!("Expected Query result");
    }
}

// ────────────────────────────────────────────────────────
// Test 5: Variable reuse — same variable in multiple positions
//   A single `$term` used in both the `where` filter value
//   and (implicitly) confirming the returned field matches.
// ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_same_variable_reused_in_filter() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "tags",
        vec![("name".to_string(), FieldType::SCALAR_STRING, false)],
    )
    .await
    .unwrap();

    for tag in ["rust", "go", "python", "rust-async"] {
        db.execute((
            r#"mutation { insertInto(collection: "tags", data: { name: $n }) { id } }"#,
            json!({ "n": tag }),
        ))
        .await
        .unwrap();
    }

    let aql = r#"
        query ExactTag($t: String!) {
            tags(where: { name: { eq: $t } }) { name }
        }
    "#;

    // Should find exactly "rust" (not "rust-async")
    let result = db
        .execute((aql, json!({ "t": "rust" })))
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 1);
        assert_eq!(
            res.documents[0].data.get("name"),
            Some(&aurora_db::types::Value::String("rust".to_string()))
        );
    } else {
        panic!("Expected Query result");
    }
}

// ────────────────────────────────────────────────────────
// Test 6: Optional variable (`$limit: Int`) defaults to None
//   When an optional variable is omitted, the query still runs
//   and returns all results (no limit applied).
// ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_optional_variable_not_provided() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "nums",
        vec![("n".to_string(), FieldType::SCALAR_INT, false)],
    )
    .await
    .unwrap();

    for i in 1..=5i64 {
        db.execute((
            r#"mutation { insertInto(collection: "nums", data: { n: $v }) { id } }"#,
            json!({ "v": i }),
        ))
        .await
        .unwrap();
    }

    let aql = r#"
        query GetNums($limit: Int) {
            nums(limit: $limit) { n }
        }
    "#;

    // No variable provided — limit should be ignored/null
    let result = db.execute((aql, json!({}))).await.unwrap();

    if let ExecutionResult::Query(res) = result {
        // All 5 rows should come back
        assert_eq!(res.documents.len(), 5);
    } else {
        panic!("Expected Query result");
    }
}
