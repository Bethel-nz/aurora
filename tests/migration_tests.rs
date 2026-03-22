use aurora_db::parser::executor::ExecutionResult;
use aurora_db::{Aurora, AuroraConfig, Value};

async fn make_db() -> (Aurora, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let db = Aurora::with_config(AuroraConfig {
        db_path: tmp.path().join("migration_test.db"),
        ..Default::default()
    })
    .await
    .unwrap();
    (db, tmp)
}

// ---------------------------------------------------------------------------
// 1. Basic migration — alter collection (add fields) + data backfill
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_add_field_and_backfill() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection users {
                name:  String!
                email: String! @indexed
            }
        }
    "#).await.unwrap();

    db.insert_into("users", vec![
        ("name",  Value::String("Alice".into())),
        ("email", Value::String("alice@example.com".into())),
    ]).await.unwrap();

    db.insert_into("users", vec![
        ("name",  Value::String("Bob".into())),
        ("email", Value::String("bob@example.com".into())),
    ]).await.unwrap();

    let result = db.execute(r#"
        migrate {
            "v1.1.0": {
                alter collection users {
                    add role: String
                }
                migrate data in users {
                    set role = "member"
                }
            }
        }
    "#).await.unwrap();

    if let ExecutionResult::Migration(m) = result {
        assert_eq!(m.steps_applied, 1);
        assert_eq!(m.version, "v1.1.0");
        assert_eq!(m.status, "applied");
    } else {
        panic!("Expected Migration result");
    }

    let users = db.get_all_collection("users").await.unwrap();
    assert_eq!(users.len(), 2);
    for user in &users {
        assert_eq!(
            user.data.get("role"),
            Some(&Value::String("member".into())),
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Idempotency — running the same migration twice skips the second run
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_idempotent() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection items {
                title: String!
            }
        }
    "#).await.unwrap();

    let migration = r#"
        migrate {
            "v1.0.0": {
                alter collection items {
                    add status: String
                }
                migrate data in items {
                    set status = "active"
                }
            }
        }
    "#;

    let r1 = db.execute(migration).await.unwrap();
    if let ExecutionResult::Migration(m) = r1 {
        assert_eq!(m.steps_applied, 1);
        assert_eq!(m.status, "applied");
    } else { panic!("Expected Migration result"); }

    let r2 = db.execute(migration).await.unwrap();
    if let ExecutionResult::Migration(m) = r2 {
        assert_eq!(m.steps_applied, 0);
        assert_eq!(m.status, "skipped");
    } else { panic!("Expected Migration result"); }
}

// ---------------------------------------------------------------------------
// 3. Multiple versions — only unapplied versions run
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_multiple_versions_incremental() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection products {
                name: String!
            }
        }
    "#).await.unwrap();

    db.insert_into("products", vec![("name", Value::String("Widget".into()))]).await.unwrap();

    // Apply v1
    db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection products {
                    add price: Float
                }
            }
        }
    "#).await.unwrap();

    // Run v1 (already applied) + v2 (new)
    let result = db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection products {
                    add price: Float
                }
            }
            "v2.0.0": {
                alter collection products {
                    add category: String
                }
                migrate data in products {
                    set category = "general"
                }
            }
        }
    "#).await.unwrap();

    if let ExecutionResult::Migration(m) = result {
        assert_eq!(m.steps_applied, 1, "only v2 should be applied");
        assert_eq!(m.version, "v2.0.0");
        assert_eq!(m.status, "applied");
    } else { panic!("Expected Migration result"); }

    let products = db.get_all_collection("products").await.unwrap();
    for p in &products {
        assert_eq!(p.data.get("category"), Some(&Value::String("general".into())));
    }
}

// ---------------------------------------------------------------------------
// 4. Data migration with a WHERE filter
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_data_with_where_filter() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection orders {
                amount: Float!
                status: String!
                label:  String
            }
        }
    "#).await.unwrap();

    db.insert_into("orders", vec![
        ("amount", Value::Float(500.0)),
        ("status", Value::String("completed".into())),
    ]).await.unwrap();
    db.insert_into("orders", vec![
        ("amount", Value::Float(50.0)),
        ("status", Value::String("pending".into())),
    ]).await.unwrap();

    db.execute(r#"
        migrate {
            "v1.0.0": {
                migrate data in orders {
                    set label = "done" where { status: { eq: "completed" } }
                }
            }
        }
    "#).await.unwrap();

    let orders = db.get_all_collection("orders").await.unwrap();
    for order in &orders {
        let status = order.data.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let label = order.data.get("label");
        if status == "completed" {
            assert_eq!(label, Some(&Value::String("done".into())));
        } else {
            assert!(label.is_none() || label == Some(&Value::Null));
        }
    }
}

// ---------------------------------------------------------------------------
// 5. Drop field removes it from schema
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_drop_field() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection logs {
                message:    String!
                debug_info: String
            }
        }
    "#).await.unwrap();

    db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection logs {
                    drop debug_info
                }
            }
        }
    "#).await.unwrap();

    let col = db.get_collection_definition("logs").unwrap();
    assert!(!col.fields.contains_key("debug_info"));
}

// ---------------------------------------------------------------------------
// 6. Rename field — schema AND existing document data updated
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_rename_field() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection events {
                event_type: String!
                ts:         String
            }
        }
    "#).await.unwrap();

    db.insert_into("events", vec![
        ("event_type", Value::String("click".into())),
        ("ts",         Value::String("2026-01-01T00:00:00Z".into())),
    ]).await.unwrap();

    db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection events {
                    rename ts to timestamp
                }
            }
        }
    "#).await.unwrap();

    let col = db.get_collection_definition("events").unwrap();
    assert!(!col.fields.contains_key("ts"), "old field name should be gone from schema");
    assert!(col.fields.contains_key("timestamp"), "new field name should exist in schema");

    // Existing documents should also be updated
    let docs = db.get_all_collection("events").await.unwrap();
    for doc in &docs {
        assert!(!doc.data.contains_key("ts"), "old key should be gone from document");
        assert!(doc.data.contains_key("timestamp"), "new key should exist in document");
    }
}

// ---------------------------------------------------------------------------
// 7. Default value on add field — existing docs get the default
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_add_field_with_default() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection accounts {
                username: String!
            }
        }
    "#).await.unwrap();

    db.insert_into("accounts", vec![("username", Value::String("alice".into()))]).await.unwrap();
    db.insert_into("accounts", vec![("username", Value::String("bob".into()))]).await.unwrap();

    db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection accounts {
                    add plan: String = "free"
                }
            }
        }
    "#).await.unwrap();

    let accounts = db.get_all_collection("accounts").await.unwrap();
    for acc in &accounts {
        assert_eq!(
            acc.data.get("plan"),
            Some(&Value::String("free".into())),
            "existing doc should have default value"
        );
    }
}

// ---------------------------------------------------------------------------
// 8. Numeric version identifier
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_numeric_version() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection notes {
                body: String!
            }
        }
    "#).await.unwrap();

    let result = db.execute(r#"
        migrate {
            1: {
                alter collection notes {
                    add pinned: Boolean
                }
            }
            2: {
                alter collection notes {
                    add archived: Boolean
                }
            }
        }
    "#).await.unwrap();

    if let ExecutionResult::Migration(m) = result {
        assert_eq!(m.steps_applied, 2);
        assert_eq!(m.status, "applied");
    } else { panic!("Expected Migration result"); }

    let col = db.get_collection_definition("notes").unwrap();
    assert!(col.fields.contains_key("pinned"));
    assert!(col.fields.contains_key("archived"));
}

// ---------------------------------------------------------------------------
// 9. _migrations collection is queryable via AQL
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_migration_history_queryable() {
    let (db, _tmp) = make_db().await;

    db.execute(r#"
        schema {
            define collection things {
                name: String!
            }
        }
    "#).await.unwrap();

    db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection things {
                    add tag: String
                }
            }
        }
    "#).await.unwrap();

    // The _migrations collection should be visible and contain the applied version
    let result = db.execute(r#"
        query {
            _migrations {
                version
                status
                applied_at
            }
        }
    "#).await.unwrap();

    if let ExecutionResult::Query(q) = result {
        assert_eq!(q.documents.len(), 1);
        assert_eq!(
            q.documents[0].data.get("version"),
            Some(&Value::String("v1.0.0".into()))
        );
        assert_eq!(
            q.documents[0].data.get("status"),
            Some(&Value::String("applied".into()))
        );
    } else {
        panic!("Expected Query result");
    }
}
