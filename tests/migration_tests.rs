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

    // v1 schema
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

    // Migration v1.1.0: add 'role' field and backfill it
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

    // All users should now have role = "member"
    let users = db.get_all_collection("users").await.unwrap();
    assert_eq!(users.len(), 2);
    for user in &users {
        assert_eq!(
            user.data.get("role"),
            Some(&Value::String("member".into())),
            "user {:?} missing role",
            user.data.get("name")
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

    // First run — should apply
    let r1 = db.execute(migration).await.unwrap();
    if let ExecutionResult::Migration(m) = r1 {
        assert_eq!(m.steps_applied, 1);
        assert_eq!(m.status, "applied");
    } else {
        panic!("Expected Migration result");
    }

    // Second run — should skip
    let r2 = db.execute(migration).await.unwrap();
    if let ExecutionResult::Migration(m) = r2 {
        assert_eq!(m.steps_applied, 0);
        assert_eq!(m.status, "skipped");
    } else {
        panic!("Expected Migration result");
    }
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

    db.insert_into("products", vec![
        ("name", Value::String("Widget".into())),
    ]).await.unwrap();

    // Apply v1 first
    db.execute(r#"
        migrate {
            "v1.0.0": {
                alter collection products {
                    add price: Float
                }
            }
        }
    "#).await.unwrap();

    // Now run both v1 (already applied) and v2 (new)
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
    } else {
        panic!("Expected Migration result");
    }

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

    // Schema should no longer have debug_info
    let col = db.get_collection_definition("logs").unwrap();
    assert!(!col.fields.contains_key("debug_info"), "field should have been dropped");
}

// ---------------------------------------------------------------------------
// 6. Rename field updates the schema key
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
    assert!(!col.fields.contains_key("ts"), "old field name should be gone");
    assert!(col.fields.contains_key("timestamp"), "new field name should exist");
}
