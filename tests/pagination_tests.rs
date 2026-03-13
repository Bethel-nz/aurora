use aurora_db::Aurora;
use aurora_db::types::{AuroraConfig, Value, FieldType};
use std::collections::HashMap;
use tempfile::tempdir;

async fn setup_pagination_db() -> (Aurora, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_db_pagination");
    
    let config = AuroraConfig {
        db_path: path.clone(),
        enable_write_buffering: false,
        ..Default::default()
    };
    
    let db = Aurora::with_config(config).await.unwrap();

    // Create collection
    db.new_collection(
        "items",
        vec![
            ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
            ("value", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true }),
        ],
    )
    .await.unwrap();

    // Insert 5 items
    for i in 1..=5 {
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String(format!("Item {}", i)));
        data.insert("value".to_string(), Value::Int(i));
        db.insert_map("items", data).await.unwrap();
    }

    (db, dir)
}

#[tokio::test]
async fn test_pagination_first() {
    let (db, _dir) = setup_pagination_db().await;

    let query = r#"
        query {
            items(first: 2) {
                edges {
                    cursor
                    node {
                        name
                        value
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    "#;

    let result = db.execute(query).await.unwrap();
    if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        // Connection is returned as a single document wrapping edges and pageInfo
        assert_eq!(res.documents.len(), 1);
        let connection = &res.documents[0];
        
        let edges = match connection.data.get("edges").unwrap() {
            Value::Array(arr) => arr,
            _ => panic!("Expected edges array"),
        };
        assert_eq!(edges.len(), 2);
        
        let page_info = match connection.data.get("pageInfo").unwrap() {
            Value::Object(obj) => obj,
            _ => panic!("Expected pageInfo object"),
        };
        assert_eq!(page_info.get("hasNextPage"), Some(&Value::Bool(true)));
    } else {
        panic!("Expected Query result");
    }
}

#[tokio::test]
async fn test_pagination_next_page() {
    let (db, _dir) = setup_pagination_db().await;

    // 1. Get first 2 to get cursor
    let query1 = r#"
        query {
            items(first: 2) {
                pageInfo {
                    endCursor
                }
            }
        }
    "#;
    
    let result1 = db.execute(query1).await.unwrap();
    let cursor = if let aurora_db::parser::executor::ExecutionResult::Query(res) = result1 {
        assert_eq!(res.documents.len(), 1);
        let conn = &res.documents[0];
        let pi = match conn.data.get("pageInfo").unwrap() {
            Value::Object(o) => o,
            _ => panic!("Expected pageInfo object"),
        };
        match pi.get("endCursor").unwrap() {
            Value::String(s) => s.clone(),
            _ => panic!("Expected endCursor string"),
        }
    } else {
        panic!("Failed first query");
    };

    // 2. Get next 2 using after
    let query2 = format!(r#"
        query {{
            items(first: 2, after: "{}") {{
                edges {{
                    node {{
                        value
                    }}
                }}
                pageInfo {{
                    hasNextPage
                }}
            }}
        }}
    "#, cursor);

    let result2 = db.execute(query2.as_str()).await.unwrap();
    if let aurora_db::parser::executor::ExecutionResult::Query(res) = result2 {
        assert_eq!(res.documents.len(), 1);
        let conn = &res.documents[0];
        let edges = match conn.data.get("edges").unwrap() {
            Value::Array(arr) => arr,
            _ => panic!("Expected edges array"),
        };
        assert_eq!(edges.len(), 2);
        
        let node1 = match &edges[0] {
            Value::Object(edge) => match edge.get("node").unwrap() {
                Value::Object(node) => node,
                _ => panic!("Expected node object"),
            },
            _ => panic!("Expected edge object"),
        };
        
        // Items are inserted sequentially, and UUIDv7 preserves this order.
        // Item 1, 2 (Page 1)
        // Item 3, 4 (Page 2)
        assert_eq!(node1.get("value"), Some(&Value::Int(3)));
        
        let pi = match conn.data.get("pageInfo").unwrap() {
            Value::Object(o) => o,
            _ => panic!("Expected pageInfo object"),
        };
        assert_eq!(pi.get("hasNextPage"), Some(&Value::Bool(true))); 
    } else {
        panic!("Failed second query");
    }
}
