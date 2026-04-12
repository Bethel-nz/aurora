use aurora_db::{Aurora, Result};
use serde_json::json;
use std::collections::HashMap;

#[tokio::test]
async fn test_execute_api() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("execute_api.db");
    let config = aurora_db::AuroraConfig {
        db_path,
        enable_write_buffering: false,
        durability_mode: aurora_db::DurabilityMode::Synchronous,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;

    // Create collection via execute
    let schema_res = db
        .execute(
            r#"
        schema {
            define collection users {
                name: String
                age: Int
            }
        }
    "#,
        )
        .await;

    // Check schema execution
    assert!(schema_res.is_ok());

    // Insert via execute_with_vars
    let mut vars = HashMap::new();
    vars.insert("name".to_string(), json!("Alice"));
    vars.insert("age".to_string(), json!(30));

    let _insert_res = db
        .execute((
            r#"
        mutation CreateUser($name: String, $age: Int) {
            insertInto(collection: "users", data: { name: $name, age: $age }) {
                id
            }
        }
    "#,
            json!(vars),
        ))
        .await?;

    // Query via execute (no vars)
    let result = db
        .execute(
            r#"
        query {
            users {
                name
                age
            }
        }
    "#,
        )
        .await?;

    if let aurora_db::parser::executor::ExecutionResult::Query(q_res) = result {
        assert_eq!(q_res.documents.len(), 1);
        assert_eq!(
            q_res.documents[0].data["name"],
            aurora_db::Value::String("Alice".to_string())
        );
    } else {
        panic!("Expected Query result");
    }

    // Explain
    let plan = db
        .explain(
            r#"
        query {
            users {
                name
            }
        }
    "#,
        )
        .await?;

    println!("Plan: {:?}", plan);
    assert!(plan.estimated_cost > 0.0);

    Ok(())
}
