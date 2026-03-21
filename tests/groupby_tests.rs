use aurora_db::Aurora;
use aurora_db::types::{AuroraConfig, Value, FieldType};
use std::collections::HashMap;
use tempfile::tempdir;

async fn setup_products_db() -> (Aurora, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_db_groupby");
    
    // Disable write buffering for immediate consistency in tests
    let config = AuroraConfig {
        db_path: path.clone(),
        enable_write_buffering: false,
        ..Default::default()
    };
    
    let db = Aurora::with_config(config).await.unwrap();

    // Create collection
    db.new_collection(
        "products",
        vec![
            ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
            ("category", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true }),
            ("price", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_FLOAT, unique: false, indexed: false, nullable: true }),
            ("stock", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true }),
        ],
    ).await
    .unwrap();

    // Insert test data
    // Categories: Electronics (2), OPTICS (1)
    let products = vec![
        ("Laptop", "Electronics", 1000.0, 10),
        ("Mouse", "Electronics", 50.0, 20),
        ("Telescope", "Optics", 500.0, 5),
    ];

    for (name, category, price, stock) in products {
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String(name.to_string()));
        data.insert("category".to_string(), Value::String(category.to_string()));
        data.insert("price".to_string(), Value::Float(price));
        data.insert("stock".to_string(), Value::Int(stock));

        db.insert_map("products", data).await.unwrap();
    }

    (db, dir)
}

#[tokio::test]
async fn test_groupby_simple_count() {
    let (db, _dir) = setup_products_db().await;

    let query = r#"
        query {
            products {
                groupBy(field: "category") {
                    key
                    count
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

    // Should have 2 groups: Electronics and Optics
    assert_eq!(docs.len(), 2);

    for doc in docs {
        let key = doc.data.get("key").unwrap().as_str().unwrap();
        let count = doc.data.get("count").unwrap().as_i64().unwrap();

        match key {
            "Electronics" => assert_eq!(count, 2),
            "Optics" => assert_eq!(count, 1),
            _ => panic!("Unexpected group key: {}", key),
        }
    }
}

#[tokio::test]
async fn test_groupby_aggregation() {
    let (db, _dir) = setup_products_db().await;

    let query = r#"
        query {
            products {
                groupBy(field: "category") {
                    key
                    stats: aggregate {
                        totalStock: sum(field: "stock")
                        avgPrice: avg(field: "price")
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

    for doc in docs {
        let key = doc.data.get("key").unwrap().as_str().unwrap();
        let stats = if let Value::Object(map) = doc.data.get("stats").unwrap() {
            map
        } else {
            panic!("Expected aggregated stats object");
        };

        match key {
            "Electronics" => {
                // Stock: 10 + 20 = 30
                assert_eq!(stats.get("totalStock").unwrap().as_f64().unwrap(), 30.0);
                // Price: (1000 + 50) / 2 = 525.0
                assert_eq!(stats.get("avgPrice").unwrap().as_f64().unwrap(), 525.0);
            },
            "Optics" => {
                // Stock: 5
                assert_eq!(stats.get("totalStock").unwrap().as_f64().unwrap(), 5.0);
                // Price: 500
                assert_eq!(stats.get("avgPrice").unwrap().as_f64().unwrap(), 500.0);
            },
            _ => panic!("Unexpected group key"),
        }
    }
}

#[tokio::test]
async fn test_groupby_nodes() {
    let (db, _dir) = setup_products_db().await;

    let query = r#"
        query {
            products {
                groupBy(field: "category") {
                    key
                    items: nodes {
                        name
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

    for doc in docs {
        let key = doc.data.get("key").unwrap().as_str().unwrap();
        let items = if let Value::Array(arr) = doc.data.get("items").unwrap() {
            arr
        } else {
            panic!("Expected items array");
        };

        match key {
            "Electronics" => {
                assert_eq!(items.len(), 2);
                let names: Vec<String> = items.iter().map(|v| {
                    if let Value::Object(o) = v {
                        o.get("name").unwrap().as_str().unwrap().to_string()
                    } else {
                        panic!("Expected object in nodes");
                    }
                }).collect();
                assert!(names.contains(&"Laptop".to_string()));
                assert!(names.contains(&"Mouse".to_string()));
            },
            "Optics" => {
                assert_eq!(items.len(), 1);
            },
            _ => panic!("Unexpected group key"),
        }
    }
}
