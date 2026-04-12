use aurora_db::Aurora;
use aurora_db::types::{AuroraConfig, Value};
use std::collections::HashMap;
use tempfile::tempdir;

async fn setup_products_db() -> (Aurora, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_db_agg");

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
            ("name", aurora_db::types::FieldType::SCALAR_STRING, false),
            (
                "category",
                aurora_db::types::FieldType::SCALAR_STRING,
                false,
            ),
            ("price", aurora_db::types::FieldType::SCALAR_FLOAT, false),
            ("stock", aurora_db::types::FieldType::SCALAR_INT, false),
        ] as Vec<(&str, aurora_db::types::FieldType, bool)>,
    )
    .await
    .unwrap();

    // Insert data
    let products = vec![
        ("Laptop", "Electronics", 1200.0, 50),
        ("Phone", "Electronics", 800.0, 100),
        ("Tablet", "Electronics", 500.0, 30),
        ("Chair", "Furniture", 150.0, 200),
        ("Desk", "Furniture", 300.0, 50),
        ("Monitor", "Electronics", 200.0, 75),
    ];

    for (name, cat, price, stock) in products {
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::from(name));
        data.insert("category".to_string(), Value::from(cat));
        data.insert("price".to_string(), Value::from(price));
        data.insert("stock".to_string(), Value::from(stock));
        db.insert_map("products", data).await.unwrap();
    }

    (db, dir)
}

#[tokio::test]
async fn test_aggregation_count() {
    let (db, _dir) = setup_products_db().await;

    // Total count
    let result = db
        .execute(
            r#"
        query {
            products {
                total: aggregate { count }
            }
        }
        "#,
        )
        .await
        .unwrap();

    let query_res = match result {
        aurora_db::parser::executor::ExecutionResult::Query(r) => r,
        _ => panic!("Expected Query result"),
    };

    let doc = &query_res.documents[0];
    let total = doc.data.get("total").unwrap().as_object().unwrap();
    let count = total.get("count").unwrap().as_i64().unwrap();

    // We inserted 6 products
    assert_eq!(count, 6);
}

#[tokio::test]
async fn test_aggregation_sum_avg() {
    let (db, _dir) = setup_products_db().await;

    let result = db
        .execute(
            r#"
        query {
            products {
                stats: aggregate {
                    totalStock: sum(field: "stock")
                    avgPrice: avg(field: "price")
                }
            }
        }
        "#,
        )
        .await
        .unwrap();

    let query_res = match result {
        aurora_db::parser::executor::ExecutionResult::Query(r) => r,
        _ => panic!("Expected Query result"),
    };

    let doc = &query_res.documents[0];
    let stats = doc.data.get("stats").unwrap().as_object().unwrap();

    // Sum stock: 50+100+30+200+50+75 = 505
    let total_stock = stats.get("totalStock").unwrap().as_f64().unwrap();
    assert_eq!(total_stock, 505.0); // sum returns float

    // Avg price: (1200+800+500+150+300+200) / 6 = 3150 / 6 = 525
    let avg_price = stats.get("avgPrice").unwrap().as_f64().unwrap();
    assert_eq!(avg_price, 525.0);
}

#[tokio::test]
async fn test_aggregation_min_max_filtered() {
    let (db, _dir) = setup_products_db().await;

    // Filter for Furniture only
    let result = db
        .execute(
            r#"
        query {
            products(where: { category: { eq: "Furniture" } }) {
                ranges: aggregate {
                    minPrice: min(field: "price")
                    maxPrice: max(field: "price")
                }
            }
        }
        "#,
        )
        .await
        .unwrap();

    let query_res = match result {
        aurora_db::parser::executor::ExecutionResult::Query(r) => r,
        _ => panic!("Expected Query result"),
    };

    let doc = &query_res.documents[0];
    let ranges = doc.data.get("ranges").unwrap().as_object().unwrap();

    // Furniture: Chair (150), Desk (300)
    let min_price = ranges.get("minPrice").unwrap().as_f64().unwrap();
    let max_price = ranges.get("maxPrice").unwrap().as_f64().unwrap();

    assert_eq!(min_price, 150.0);
    assert_eq!(max_price, 300.0);
}

#[tokio::test]
async fn test_aggregation_empty_result() {
    let (db, _dir) = setup_products_db().await;

    // Filter for non-existent category
    let result = db
        .execute(
            r#"
        query {
            products(where: { category: { eq: "Cars" } }) {
                agg: aggregate {
                    count
                    avgPrice: avg(field: "price")
                }
            }
        }
        "#,
        )
        .await
        .unwrap();

    let query_res = match result {
        aurora_db::parser::executor::ExecutionResult::Query(r) => r,
        _ => panic!("Expected Query result"),
    };

    let doc = &query_res.documents[0];
    let agg = doc.data.get("agg").unwrap().as_object().unwrap();

    let count = agg.get("count").unwrap().as_i64().unwrap();
    assert_eq!(count, 0);

    let avg_price = agg.get("avgPrice").unwrap();
    assert!(matches!(avg_price, Value::Null)); // Avg on empty set should be null
}
