use aurora_db::Aurora;
use aurora_db::parser::executor::ExecutionResult;
use tempfile::tempdir;

#[tokio::test]
async fn test_unique_field_aql_schema_equality() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test_unique_schema.db");
    let db = Aurora::open(db_path.to_str().unwrap()).await.unwrap();

    // Create a collection using AQL Schema definition
    let schema_query = r#"
        schema {
            define collection User {
                email: String! @unique
                name: String
            }
        }
    "#;

    let res = db.execute(schema_query).await.unwrap();
    assert!(
        matches!(res, ExecutionResult::Schema(_)),
        "Schema should be created"
    );

    // Insert a document using AQL
    let insert_query = r#"
        mutation {
            insertInto(collection: "User", data: { email: "test@example.com", name: "Alice" })
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
            User(where: { email: { eq: "test@example.com" } }) {
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

    // Attempt to insert a second document with the same unique email, which should fail
    let duplicate_insert_query = r#"
        mutation {
            insertInto(collection: "User", data: { email: "test@example.com", name: "Bob" })
        }
    "#;
    let duplicate_res = db.execute(duplicate_insert_query).await;
    assert!(
        duplicate_res.is_err(),
        "Expected unique constraint violation error on duplicate insert"
    );
}
