use aurora_db::types::{DurabilityMode, FieldType, Value};
use aurora_db::{Aurora, AuroraConfig};
use aurora_db::parser::executor::ExecutionResult;
use tempfile::TempDir;

async fn setup() -> (Aurora, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Aurora::with_config(AuroraConfig {
        db_path: dir.path().join("db"),
        enable_write_buffering: false,
        durability_mode: DurabilityMode::Synchronous,
        ..Default::default()
    })
    .await
    .unwrap();
    (db, dir)
}

// ─── containsAny ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_contains_any_filter() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "posts",
        vec![("tags".to_string(), FieldType::SCALAR_ARRAY, false)],
    )
    .await
    .unwrap();

    for (title, tags) in [
        ("Rust async", vec!["rust", "async"]),
        ("Go channels", vec!["go", "concurrency"]),
        ("Python ML", vec!["python", "ml"]),
    ] {
        let tag_vals: Vec<Value> = tags.iter().map(|t| Value::String(t.to_string())).collect();
        let mut data = std::collections::HashMap::new();
        data.insert("title".to_string(), Value::String(title.to_string()));
        data.insert("tags".to_string(), Value::Array(tag_vals));
        db.insert_map("posts", data).await.unwrap();
    }

    let result = db
        .execute(r#"query { posts(where: { tags: { containsAny: ["rust", "go"] } }) { title } }"#)
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 2, "Should match rust and go posts");
        let titles: Vec<_> = res.documents.iter()
            .filter_map(|d| if let Some(Value::String(s)) = d.data.get("title") { Some(s.as_str()) } else { None })
            .collect();
        assert!(titles.contains(&"Rust async"));
        assert!(titles.contains(&"Go channels"));
    } else {
        panic!("Expected Query result");
    }
}

// ─── containsAll ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_contains_all_filter() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "posts",
        vec![("tags".to_string(), FieldType::SCALAR_ARRAY, false)],
    )
    .await
    .unwrap();

    for (title, tags) in [
        ("Full stack", vec!["rust", "async", "web"]),
        ("Rust basics", vec!["rust", "beginner"]),
        ("Async patterns", vec!["async", "patterns"]),
    ] {
        let tag_vals: Vec<Value> = tags.iter().map(|t| Value::String(t.to_string())).collect();
        let mut data = std::collections::HashMap::new();
        data.insert("title".to_string(), Value::String(title.to_string()));
        data.insert("tags".to_string(), Value::Array(tag_vals));
        db.insert_map("posts", data).await.unwrap();
    }

    // Only "Full stack" has BOTH rust AND async
    let result = db
        .execute(r#"query { posts(where: { tags: { containsAll: ["rust", "async"] } }) { title } }"#)
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 1);
        assert_eq!(
            res.documents[0].data.get("title"),
            Some(&Value::String("Full stack".to_string()))
        );
    } else {
        panic!("Expected Query result");
    }
}

// ─── matches (regex) ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_matches_regex_filter() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "users",
        vec![("email".to_string(), FieldType::SCALAR_STRING, false)],
    )
    .await
    .unwrap();

    for email in ["alice@gmail.com", "bob@yahoo.com", "carol@gmail.com", "dan@hotmail.com"] {
        let mut data = std::collections::HashMap::new();
        data.insert("email".to_string(), Value::String(email.to_string()));
        db.insert_map("users", data).await.unwrap();
    }

    let result = db
        .execute(r#"query { users(where: { email: { matches: ".*@gmail\\.com$" } }) { email } }"#)
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 2, "Should match the two gmail addresses");
        for doc in &res.documents {
            if let Some(Value::String(e)) = doc.data.get("email") {
                assert!(e.ends_with("@gmail.com"));
            }
        }
    } else {
        panic!("Expected Query result");
    }
}

// ─── Update modifiers ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_increment_decrement_modifiers() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "counters",
        vec![
            ("name".to_string(), FieldType::SCALAR_STRING, false),
            ("value".to_string(), FieldType::SCALAR_INT, false),
        ],
    )
    .await
    .unwrap();

    db.execute(r#"mutation {
        insertInto(collection: "counters", data: { name: "hits", value: 10 }) { id }
    }"#)
    .await
    .unwrap();

    // increment
    db.execute(r#"mutation {
        update(collection: "counters",
               where: { name: { eq: "hits" } },
               set: { value: { increment: 5 } }) { id }
    }"#)
    .await
    .unwrap();

    let res = db.execute(r#"query { counters { name value } }"#).await.unwrap();
    if let ExecutionResult::Query(r) = res {
        assert_eq!(r.documents[0].data.get("value"), Some(&Value::Int(15)));
    }

    // decrement
    db.execute(r#"mutation {
        update(collection: "counters",
               where: { name: { eq: "hits" } },
               set: { value: { decrement: 3 } }) { id }
    }"#)
    .await
    .unwrap();

    let res = db.execute(r#"query { counters { value } }"#).await.unwrap();
    if let ExecutionResult::Query(r) = res {
        assert_eq!(r.documents[0].data.get("value"), Some(&Value::Int(12)));
    }
}

#[tokio::test]
async fn test_push_pull_add_to_set_modifiers() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "lists",
        vec![
            ("name".to_string(), FieldType::SCALAR_STRING, false),
            ("items".to_string(), FieldType::SCALAR_ARRAY, false),
        ],
    )
    .await
    .unwrap();

    db.execute(r#"mutation {
        insertInto(collection: "lists", data: { name: "todo", items: ["a", "b"] }) { id }
    }"#)
    .await
    .unwrap();

    // push
    db.execute(r#"mutation {
        update(collection: "lists",
               where: { name: { eq: "todo" } },
               set: { items: { push: "c" } }) { id }
    }"#)
    .await
    .unwrap();

    let res = db.execute(r#"query { lists { items } }"#).await.unwrap();
    if let ExecutionResult::Query(r) = res {
        if let Some(Value::Array(items)) = r.documents[0].data.get("items") {
            assert_eq!(items.len(), 3);
            assert!(items.contains(&Value::String("c".to_string())));
        }
    }

    // addToSet (no duplicate)
    db.execute(r#"mutation {
        update(collection: "lists",
               where: { name: { eq: "todo" } },
               set: { items: { addToSet: "c" } }) { id }
    }"#)
    .await
    .unwrap();

    let res = db.execute(r#"query { lists { items } }"#).await.unwrap();
    if let ExecutionResult::Query(r) = res {
        if let Some(Value::Array(items)) = r.documents[0].data.get("items") {
            assert_eq!(items.len(), 3, "addToSet should not duplicate");
        }
    }

    // pull
    db.execute(r#"mutation {
        update(collection: "lists",
               where: { name: { eq: "todo" } },
               set: { items: { pull: "b" } }) { id }
    }"#)
    .await
    .unwrap();

    let res = db.execute(r#"query { lists { items } }"#).await.unwrap();
    if let ExecutionResult::Query(r) = res {
        if let Some(Value::Array(items)) = r.documents[0].data.get("items") {
            assert_eq!(items.len(), 2);
            assert!(!items.contains(&Value::String("b".to_string())));
        }
    }
}

// ─── import / export ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_import_mutation() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "products",
        vec![
            ("name".to_string(), FieldType::SCALAR_STRING, false),
            ("price".to_string(), FieldType::SCALAR_INT, false),
        ],
    )
    .await
    .unwrap();

    let result = db
        .execute(r#"mutation {
            import(collection: "products", data: [
                { name: "Widget", price: 10 },
                { name: "Gadget", price: 20 },
                { name: "Doohickey", price: 30 }
            ]) { id }
        }"#)
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.affected_count, 3);
    } else {
        panic!("Expected Mutation result");
    }

    let read = db.execute(r#"query { products { name } }"#).await.unwrap();
    if let ExecutionResult::Query(res) = read {
        assert_eq!(res.documents.len(), 3);
    }
}

#[tokio::test]
async fn test_export_mutation() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "items",
        vec![("name".to_string(), FieldType::SCALAR_STRING, false)],
    )
    .await
    .unwrap();

    for name in ["Alpha", "Beta", "Gamma"] {
        let mut data = std::collections::HashMap::new();
        data.insert("name".to_string(), Value::String(name.to_string()));
        db.insert_map("items", data).await.unwrap();
    }

    let result = db
        .execute(r#"mutation {
            export(collection: "items", format: JSON) { id name }
        }"#)
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.affected_count, 3);
        assert_eq!(res.returned_documents.len(), 3);
    } else {
        panic!("Expected Mutation result");
    }
}

// ─── order_by formal syntax ─────────────────────────────────────────────────

#[tokio::test]
async fn test_order_by_formal_syntax() {
    let (db, _dir) = setup().await;
    db.new_collection(
        "nums",
        vec![("n".to_string(), FieldType::SCALAR_INT, false)],
    )
    .await
    .unwrap();

    for i in [3i64, 1, 4, 1, 5, 9, 2, 6] {
        let mut data = std::collections::HashMap::new();
        data.insert("n".to_string(), Value::Int(i));
        db.insert_map("nums", data).await.unwrap();
    }

    // Formal syntax: orderBy: { field: "n", direction: DESC }
    let result = db
        .execute(r#"query { nums(orderBy: { field: "n", direction: DESC }, limit: 3) { n } }"#)
        .await
        .unwrap();

    if let ExecutionResult::Query(res) = result {
        assert_eq!(res.documents.len(), 3);
        // Top 3 DESC: 9, 6, 5
        let vals: Vec<i64> = res.documents.iter()
            .filter_map(|d| if let Some(Value::Int(n)) = d.data.get("n") { Some(*n) } else { None })
            .collect();
        assert_eq!(vals[0], 9);
        assert_eq!(vals[1], 6);
        assert_eq!(vals[2], 5);
    } else {
        panic!("Expected Query result");
    }
}
