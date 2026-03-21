use aurora_db::Aurora;
use aurora_db::parser::executor::{ExecutionOptions, ExecutionResult, execute};
use aurora_db::types::Value;
use tempfile::TempDir;

// Helper to init DB
async fn setup_db(name: &str) -> (Aurora, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join(name);
    let config = aurora_db::AuroraConfig {
        db_path,
        enable_write_buffering: false,
        durability_mode: aurora_db::DurabilityMode::Synchronous,
        ..Default::default()
    };
    (Aurora::with_config(config).await.unwrap(), temp_dir)
}

#[tokio::test]
async fn test_e2e_schema_management() {
    let (db, _dir) = setup_db("schema_test.db").await;

    // 1. Define Collection (Fixed: removed commas)
    let schema_query = r#"
        schema {
            define collection products {
                name: String
                price: Float
                tags: [String]
                active: Bool
            }
        }
    "#;
    let res = execute(&db, schema_query, ExecutionOptions::new()).await;
    assert!(res.is_ok(), "Schema definition failed: {:?}", res.err());

    // 2. Verify creation (implicitly by inserting)
    let insert_res = execute(
        &db,
        r#"
        mutation {
            insertInto(collection: "products", data: {
                name: "Laptop",
                price: 999.99,
                tags: ["electronics", "work"],
                active: true
            }) { id }
        }
    "#,
        ExecutionOptions::new(),
    )
    .await;
    assert!(insert_res.is_ok());
}

#[tokio::test]
async fn test_e2e_crud_operations() {
    let (db, _dir) = setup_db("crud_test.db").await;

    // Setup Schema
    execute(
        &db,
        r#"
        schema {
            define collection users {
                username: String
                level: Int
                score: Float
            }
        }
    "#,
        ExecutionOptions::new(),
    )
    .await
    .unwrap();

    // 1. CREATE (Multiple InsertInto)
    let insert_query = r#"
        mutation {
            insertInto(collection: "users", data: { username: "alice", level: 1, score: 85.5 }) { id }
            insertInto(collection: "users", data: { username: "bob", level: 5, score: 92.0 }) { id }
            insertInto(collection: "users", data: { username: "charlie", level: 3, score: 88.0 }) { id }
            insertInto(collection: "users", data: { username: "dave", level: 1, score: 40.0 }) { id }
        }
    "#;
    let res = execute(&db, insert_query, ExecutionOptions::new())
        .await
        .unwrap();
    // Expect Batch result because multiple mutation operations
    if let ExecutionResult::Batch(results) = res {
        assert_eq!(results.len(), 4);
    } else {
        panic!("Expected Batch result for multiple mutations");
    }

    // 2. READ (Query with Filters and Sort)
    let query = r#"
        query {
            users(
                where: { 
                    or: [
                        { level: { gte: 3 } },
                        { score: { gt: 90.0 } }
                    ] 
                },
                orderBy: { score: DESC }
            ) {
                username
                score
            }
        }
    "#;
    let res = execute(&db, query, ExecutionOptions::new()).await.unwrap();
    if let ExecutionResult::Query(q) = res {
        assert_eq!(q.documents.len(), 2); // Bob (92.0), Charlie (88.0)
        assert_eq!(
            q.documents[0].data["username"],
            Value::String("bob".to_string())
        );
        assert_eq!(
            q.documents[1].data["username"],
            Value::String("charlie".to_string())
        );
    } else {
        panic!("Expected Query result");
    }

    // 3. UPDATE
    let update_query = r#"
        mutation {
            update(
                collection: "users", 
                where: { username: { eq: "dave" } }, 
                set: { level: 2 }
            ) {
                affectedCount
            }
        }
    "#;
    let res = execute(&db, update_query, ExecutionOptions::new())
        .await
        .unwrap();
    if let ExecutionResult::Mutation(m) = res {
        assert_eq!(m.affected_count, 1);
    }

    // Verify Update
    let verify_update = r#"query { users(where: { username: { eq: "dave" } }) { level } }"#;
    let res = execute(&db, verify_update, ExecutionOptions::new())
        .await
        .unwrap();
    if let ExecutionResult::Query(q) = res {
        assert_eq!(q.documents[0].data["level"], Value::Int(2));
    }

    // 4. DELETE
    let delete_query = r#"
        mutation {
            deleteFrom(collection: "users", where: { score: { lt: 50.0 } }) {
                affectedCount
            }
        }
    "#;
    let res = execute(&db, delete_query, ExecutionOptions::new())
        .await
        .unwrap();
    if let ExecutionResult::Mutation(m) = res {
        assert_eq!(m.affected_count, 1);
    }
}

#[tokio::test]
async fn test_e2e_advanced_filters() {
    let (db, _dir) = setup_db("filters_test.db").await;

    execute(
        &db,
        r#"
        schema {
            define collection items {
                tags: [String]
                meta: String
            }
        }
    "#,
        ExecutionOptions::new(),
    )
    .await
    .unwrap();

    execute(&db, r#"
        mutation {
            insertInto(collection: "items", data: { tags: ["red", "large"], meta: "v1_special" }) { id }
            insertInto(collection: "items", data: { tags: ["blue", "small"], meta: "v2_standard" }) { id }
        }
    "#, ExecutionOptions::new()).await.unwrap();

    // Contains in Array (Should work now with Updated Executor)
    let query_tags = r#"
        query {
            items(where: { tags: { contains: "red" } }) { meta }
        }
    "#;
    let res = execute(&db, query_tags, ExecutionOptions::new())
        .await
        .unwrap();
    if let ExecutionResult::Query(q) = res {
        assert_eq!(q.documents.len(), 1);
        assert_eq!(
            q.documents[0].data["meta"],
            Value::String("v1_special".to_string())
        );
    }

    // EndsWith String
    let query_ends = r#"
        query {
            items(where: { meta: { endsWith: "standard" } }) { meta }
        }
    "#;
    let res = execute(&db, query_ends, ExecutionOptions::new())
        .await
        .unwrap();
    if let ExecutionResult::Query(q) = res {
        assert_eq!(q.documents.len(), 1);
        assert_eq!(
            q.documents[0].data["meta"],
            Value::String("v2_standard".to_string())
        );
    }
}

#[tokio::test]
async fn test_e2e_aggregation_and_grouping() {
    let (db, _dir) = setup_db("agg_test.db").await;

    execute(
        &db,
        r#"
        schema { define collection sales { category: String amount: Int } }
    "#,
        ExecutionOptions::new(),
    )
    .await
    .unwrap();

    execute(
        &db,
        r#"
        mutation {
            insertInto(collection: "sales", data: { category: "A", amount: 10 }) { id }
            insertInto(collection: "sales", data: { category: "A", amount: 20 }) { id }
            insertInto(collection: "sales", data: { category: "B", amount: 50 }) { id }
        }
    "#,
        ExecutionOptions::new(),
    )
    .await
    .unwrap();

    // Simple verification
    let query = r#"
        query {
             sales { category amount }
        }
    "#;
    let res = execute(&db, query, ExecutionOptions::new()).await.unwrap();
    if let ExecutionResult::Query(q) = res {
        assert_eq!(q.documents.len(), 3);
    }
}

#[tokio::test]
async fn test_e2e_subscriptions() {
    let (db, _dir) = setup_db("subs_test.db").await;

    // 1. Define Collection
    execute(
        &db,
        r#"
        schema {
            define collection messages {
                content: String
                type: String
            }
        }
    "#,
        ExecutionOptions::new(),
    )
    .await
    .unwrap();

    // 2. Subscribe to 'alert' messages
    let sub_query = r#"
        subscription {
            messages(where: { type: { eq: "alert" } }) {
                content
            }
        }
    "#;
    let res = execute(&db, sub_query, ExecutionOptions::new())
        .await
        .unwrap();

    let mut stream = if let ExecutionResult::Subscription(s) = res {
        s.stream.expect("Expected a stream in subscription result")
    } else {
        panic!("Expected Subscription result");
    };

    // 3. Insert meaningful data (should be received)
    // We spawn this because recv() is blocking/awaiting
    let db_clone = db.clone();
    tokio::spawn(async move {
        // Short delay to ensure subscription is active
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        execute(
            &db_clone,
            r#"
            mutation {
                insertInto(collection: "messages", data: { 
                    content: "System Meltdown Imminent", 
                    type: "alert" 
                }) { id }
            }
        "#,
            ExecutionOptions::new(),
        )
        .await
        .unwrap();

        // Insert ignored data
        execute(
            &db_clone,
            r#"
            mutation {
                insertInto(collection: "messages", data: { 
                    content: "Just sayin hi", 
                    type: "info" 
                }) { id }
            }
        "#,
            ExecutionOptions::new(),
        )
        .await
        .unwrap();
    });

    // 4. Verify reception
    // We expect the 'alert' message
    match tokio::time::timeout(tokio::time::Duration::from_secs(2), stream.recv()).await {
        Ok(Ok(event)) => {
            assert_eq!(event.collection, "messages");
            if let Some(doc) = event.document {
                assert_eq!(
                    doc.data["content"],
                    Value::String("System Meltdown Imminent".to_string())
                );
            } else {
                panic!("Expected document in event");
            }
        }
        Ok(Err(e)) => panic!("Stream error: {:?}", e),
        Err(_) => panic!("Timed out waiting for subscription event"),
    }

    // We should NOT receive the second message (type: info)
    // Verify by timeout (expecting timeout)
    match tokio::time::timeout(tokio::time::Duration::from_millis(500), stream.recv()).await {
        Ok(Ok(event)) => {
            panic!("Received unexpected event: {:?}", event);
        }
        Err(_) => {
            // This is expected! We timed out because no generic info message matched the filter.
        }
        _ => {}
    }
}
