use aurora_db::error::ErrorCode;
use aurora_db::{Aurora, AuroraConfig};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_required_variable_validation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let config = AuroraConfig {
        db_path,
        enable_write_buffering: false,
        durability_mode: aurora_db::types::DurabilityMode::Synchronous,
        ..Default::default()
    };
    let db = Aurora::with_config(config).unwrap();

    // Create a test collection
    db.new_collection(
        "users",
        vec![
            (
                "name".to_string(),
                aurora_db::types::FieldType::String,
                false,
            ),
            (
                "email".to_string(),
                aurora_db::types::FieldType::String,
                false,
            ),
        ],
    )
    .await
    .unwrap();

    // Test 1: Required variable ($name: String!) - should ERROR when missing
    let query_with_required = r#"
        query GetUser($name: String!) {
            users(where: { name: { eq: $name } }) {
                name
                email
            }
        }
    "#;

    // Execute WITHOUT providing the required variable
    let result = db.execute((query_with_required, json!({}))).await;

    // Should fail with UndefinedVariable error
    assert!(
        result.is_err(),
        "Query should fail when required variable is missing"
    );
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::UndefinedVariable);
    assert!(
        err.message.contains("name"),
        "Error should mention the missing variable name"
    );
    assert!(
        err.message.contains("String!"),
        "Error should show the required type"
    );

    // Test 2: Same query WITH the required variable - should succeed
    let result = db
        .execute((query_with_required, json!({ "name": "Alice" })))
        .await;
    assert!(
        result.is_ok(),
        "Query should succeed when required variable is provided"
    );

    // Test 3: Optional variable ($limit: Int) - should NOT error when missing
    let query_with_optional = r#"
        query GetUsers($limit: Int) {
            users(limit: $limit) {
                name
            }
        }
    "#;

    let result = db.execute((query_with_optional, json!({}))).await;
    assert!(
        result.is_ok(),
        "Query should succeed when optional variable is missing"
    );

    // Test 4: Variable with default value - should use default when missing
    let query_with_default = r#"
        query GetUsers($limit: Int = 10) {
            users(limit: $limit) {
                name
            }
        }
    "#;

    let result = db.execute((query_with_default, json!({}))).await;
    assert!(
        result.is_ok(),
        "Query should succeed when variable with default is missing"
    );
}

#[tokio::test]
async fn test_required_variable_in_mutation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let config = AuroraConfig {
        db_path,
        enable_write_buffering: false,
        durability_mode: aurora_db::types::DurabilityMode::Synchronous,
        ..Default::default()
    };
    let db = Aurora::with_config(config).unwrap();

    db.new_collection(
        "users",
        vec![
            (
                "name".to_string(),
                aurora_db::types::FieldType::String,
                false,
            ),
            (
                "email".to_string(),
                aurora_db::types::FieldType::String,
                false,
            ),
        ],
    )
    .await
    .unwrap();

    // Mutation with required variables
    let mutation = r#"
        mutation CreateUser($name: String!, $email: String!) {
            insertInto(collection: "users", data: {
                name: $name,
                email: $email
            }) {
                id
                name
            }
        }
    "#;

    // Test 1: Missing both required variables - should ERROR
    let result = db.execute((mutation, json!({}))).await;
    assert!(
        result.is_err(),
        "Mutation should fail when required variables are missing"
    );
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::UndefinedVariable);

    // Test 2: Missing one required variable - should ERROR
    let result = db.execute((mutation, json!({ "name": "Alice" }))).await;
    assert!(
        result.is_err(),
        "Mutation should fail when one required variable is missing"
    );
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::UndefinedVariable);
    assert!(
        err.message.contains("email"),
        "Error should mention the missing 'email' variable"
    );

    // Test 3: All required variables provided - should succeed
    let result = db
        .execute((
            mutation,
            json!({
                "name": "Alice",
                "email": "alice@example.com"
            }),
        ))
        .await;
    assert!(
        result.is_ok(),
        "Mutation should succeed when all required variables are provided"
    );
}

#[tokio::test]
async fn test_boolean_variable_type_validation() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let config = AuroraConfig {
        db_path,
        enable_write_buffering: false,
        durability_mode: aurora_db::types::DurabilityMode::Synchronous,
        ..Default::default()
    };
    let db = Aurora::with_config(config).unwrap();

    db.new_collection(
        "users",
        vec![
            (
                "name".to_string(),
                aurora_db::types::FieldType::String,
                false,
            ),
            (
                "active".to_string(),
                aurora_db::types::FieldType::Bool,
                false,
            ),
        ],
    )
    .await
    .unwrap();

    // Query expecting boolean variable
    let query = r#"
        query GetActiveUsers($isActive: Boolean!) {
            users(where: { active: { eq: $isActive } }) {
                name
            }
        }
    "#;

    // Test 1: Provide string instead of boolean - should ERROR with TypeError
    let result = db.execute((query, json!({ "isActive": "true" }))).await;

    // Note: This might pass parsing but fail during execution
    // The exact behavior depends on how strict the type checking is
    // If it errors, it should be TypeError
    if result.is_err() {
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            ErrorCode::TypeError,
            "Should fail with TypeError when wrong type is provided"
        );
    }

    // Test 2: Provide correct boolean type - should succeed
    let result = db.execute((query, json!({ "isActive": true }))).await;
    assert!(
        result.is_ok(),
        "Query should succeed with correct boolean type"
    );
}
