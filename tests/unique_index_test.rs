use aurora_db::Aurora;
use aurora_db::parser::executor::ExecutionResult;
use aurora_db::types::{FieldDefinition, FieldType};
use tempfile::tempdir;

#[tokio::test]
async fn test_unique_field_aql_equality() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test_unique.db");
    let db = Aurora::open(db_path.to_str().unwrap()).await.unwrap();

    // Create a collection with a unique field
    db.new_collection(
        "users",
        vec![
            (
                "email",
                FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: true,
                    indexed: true,
                    nullable: false,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "name",
                FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
        ],
    )
    .await
    .unwrap();

    // Insert a document using AQL
    let insert_query = r#"
        mutation {
            insertInto(collection: "users", data: { email: "test@example.com", name: "Alice" })
        }
    "#;
    let res = db.execute(insert_query).await.unwrap();
    if let ExecutionResult::Mutation(m) = res {
        assert_eq!(m.affected_count, 1, "Document should be inserted");
    } else {
        panic!("Expected Mutation result");
    }

    // Query the document using AQL equality on the unique field
    let select_query = r#"
        query {
            users(where: { email: { eq: "test@example.com" } }) {
                email
                name
            }
        }
    "#;
    let res = db.execute(select_query).await.unwrap();
    if let ExecutionResult::Query(q) = res {
        assert_eq!(
            q.documents.len(),
            1,
            "Expected 1 document to be returned via AQL for unique field lookup"
        );
        assert_eq!(
            q.documents[0].data.get("email").unwrap().as_str().unwrap(),
            "test@example.com"
        );
    } else {
        panic!("Expected Query result");
    }
}
