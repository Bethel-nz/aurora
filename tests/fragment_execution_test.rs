use aurora_db::Aurora;
use aurora_db::types::{FieldType, Value};
use std::collections::HashMap;
use tempfile::tempdir;

async fn setup_test_db() -> (Aurora, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Aurora::open(dir.path().to_str().unwrap()).await.unwrap();

    db.new_collection(
        "users",
        vec![
            (
                "name",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "age",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "city",
                aurora_db::types::FieldDefinition {
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

    let mut user1 = HashMap::new();
    user1.insert("name".to_string(), Value::String("Alice".into()));
    user1.insert("age".to_string(), Value::Int(30));
    user1.insert("city".to_string(), Value::String("London".into()));
    db.insert_map("users", user1).await.unwrap();

    (db, dir)
}

#[tokio::test]
async fn test_fragment_execution() {
    let (db, _dir) = setup_test_db().await;

    let query = r#"
        fragment UserFields on users {
            name
            age
        }

        query {
            users {
                ...UserFields
                city
            }
        }
    "#;

    let result = db.execute(query).await.unwrap();
    let docs = if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        res.documents
    } else {
        panic!("Expected Query result");
    };

    assert_eq!(docs.len(), 1);
    let data = &docs[0].data;
    assert_eq!(data.get("name").unwrap().as_str().unwrap(), "Alice");
    assert_eq!(data.get("age").unwrap().as_i64().unwrap(), 30);
    assert_eq!(data.get("city").unwrap().as_str().unwrap(), "London");
}

#[tokio::test]
async fn test_nested_fragment_execution() {
    let (db, _dir) = setup_test_db().await;

    let query = r#"
        fragment BaseFields on users {
            name
        }

        fragment FullFields on users {
            ...BaseFields
            age
        }

        query {
            users {
                ...FullFields
            }
        }
    "#;

    let result = db.execute(query).await.unwrap();
    let docs = if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        res.documents
    } else {
        panic!("Expected Query result");
    };

    assert_eq!(docs.len(), 1);
    let data = &docs[0].data;
    assert_eq!(data.get("name").unwrap().as_str().unwrap(), "Alice");
    assert_eq!(data.get("age").unwrap().as_i64().unwrap(), 30);
}

#[tokio::test]
async fn test_fragment_in_groupby() {
    let (db, _dir) = setup_test_db().await;

    let query = r#"
        fragment NodeFields on users {
            name
        }

        query {
            users {
                groupBy(field: "city") {
                    key
                    nodes {
                        ...NodeFields
                    }
                }
            }
        }
    "#;

    let result = db.execute(query).await.unwrap();
    let docs = if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        res.documents
    } else {
        panic!("Expected Query result");
    };

    assert_eq!(docs.len(), 1);
    let group = &docs[0].data;
    assert_eq!(group.get("key").unwrap().as_str().unwrap(), "London");

    let nodes = group.get("nodes").unwrap().as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    let node_data = nodes[0].as_object().unwrap();
    assert_eq!(node_data.get("name").unwrap().as_str().unwrap(), "Alice");
    assert!(node_data.get("age").is_none()); // Should be excluded as it's not in the fragment
}

#[tokio::test]
async fn test_fragment_in_connection() {
    let (db, _dir) = setup_test_db().await;

    let query = r#"
        fragment NodeFields on users {
            name
        }

        query {
            users {
                edges {
                    node {
                        ...NodeFields
                    }
                }
            }
        }
    "#;

    let result = db.execute(query).await.unwrap();
    let docs = if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        res.documents
    } else {
        panic!("Expected Query result");
    };

    // Connection results return a virtual "connection" document
    assert_eq!(docs.len(), 1);
    let edges = docs[0].data.get("edges").unwrap().as_array().unwrap();
    assert_eq!(edges.len(), 1);

    let node = edges[0]
        .as_object()
        .unwrap()
        .get("node")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(node.get("name").unwrap().as_str().unwrap(), "Alice");
    assert!(node.get("age").is_none());
}
