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
    
    let db = Aurora::with_config(config).unwrap();

    // Create collection
    db.new_collection(
        "items",
        vec![
            ("name", FieldType::String, false),
            ("value", FieldType::Int, false),
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
        // We expect a single "document" which wraps the connection result if it's treated as a single field
        // But likely implemented as: the 'items' field returns the connection object directly.
        // Wait, execute_collection_query returns QueryResult with `documents: Vec<Document>`.
        // If we switch to connection mode, `documents` will contain one Document representing the connection?
        // OR `documents` will be the edges? 
        // Standard GraphQL: `items` returns a Connection Object.
        // Aurora implementation plan says: "Return a wrapper Document { edges: [...], pageInfo: { ... } }"
        // So `res.documents` should have 1 element, which is the Connection object.
        
        assert_eq!(res.documents.len(), 1);
        let connection = &res.documents[0];
        
        // internal id check?
        
        let edges = if let Value::Array(arr) = connection.data.get("edges").unwrap() {
            arr
        } else {
            panic!("Expected edges array");
        };
        
        assert_eq!(edges.len(), 2);
        
        let page_info = if let Value::Object(obj) = connection.data.get("pageInfo").unwrap() {
            obj
        } else {
            panic!("Expected pageInfo object");
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
        let conn = &res.documents[0];
        let pi = if let Value::Object(o) = conn.data.get("pageInfo").unwrap() { o } else { panic!() };
        if let Value::String(s) = pi.get("endCursor").unwrap() { s.clone() } else { panic!() }
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
        let conn = &res.documents[0];
        let edges = if let Value::Array(arr) = conn.data.get("edges").unwrap() { arr } else { panic!() };
        assert_eq!(edges.len(), 2);
        
        // Should be item 3 and 4 (UUIDv7 preserves insertion order)
        let node1 = if let Value::Object(edge) = &edges[0] {
             if let Value::Object(node) = edge.get("node").unwrap() { node } else { panic!() }
        } else { panic!() };
        
        // With UUIDv7, documents are ordered by creation time
        // Page 1: items 1, 2
        // Page 2: items 3, 4
        assert_eq!(node1.get("value"), Some(&Value::Int(3)));
        
        let pi = if let Value::Object(o) = conn.data.get("pageInfo").unwrap() { o } else { panic!() };
        // hasNextPage should be true since there's still 1 item remaining after page 2
        assert_eq!(pi.get("hasNextPage"), Some(&Value::Bool(true))); // Item 5 remains
    } else {
        panic!("Failed second query");
    }
}
