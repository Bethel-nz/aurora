use aurora_db::{Aurora, Result};
use serde_json::json;
use std::collections::HashMap;

/// # AQL Showcase Test
///
/// This test file demonstrates the full power of the Aurora Query Language (AQL).
/// It serves as a living documentation and usage example.
///
/// Scenarios covered:
/// 1. Schema Definition (Defining collections)
/// 2. Data Insertion (Mutations with variables)
/// 3. Complex Querying (Filtering, Sorting, Limiting)
/// 4. Computed Fields (Dynamic values without storage)
/// 5. Aggregation (Grouping and Stats)

#[tokio::test]
async fn section_1_schema_definition() -> Result<()> {
    let db = setup_db().await?;

    // -------------------------------------------------------------------------
    // 1. Define Your Schema
    // -------------------------------------------------------------------------
    // Unlike SQL, you can define schema with a simple GraphQL-like syntax.
    // Notice "if not exists" checks.

    let schema_aql = r#"
        schema {
            define collection products if not exists {
                name: String!       @indexed
                category: String    @indexed
                price: Float        @indexed
                stock: Int
                tags: [String]
                active: Boolean
            }
            
            define collection orders {
                user_id: String!
                total: Float
                status: String      @indexed
                items: [JSON]
            }
        }
    "#;

    let result = db.execute(schema_aql).await?;
    println!("Schema Result: {:?}", result);
    assert!(format!("{:?}", result).contains("Schema"));

    Ok(())
}

#[tokio::test]
async fn section_2_data_mutation() -> Result<()> {
    let db = setup_db().await?;
    // (Assume schema is created)
    ensure_schema(&db).await?;

    // -------------------------------------------------------------------------
    // 2. Insert Data with Variables
    // -------------------------------------------------------------------------
    // Parameterized queries prevent injection and are cleaner.

    let mutation_aql = r#"
        mutation AddProduct($n: String, $c: String, $p: Float, $s: Int) {
            insertInto(collection: "products", data: { 
                name: $n, 
                category: $c, 
                price: $p, 
                stock: $s 
            }) {
                id
                name
                created_at  # Auto-generated
            }
        }
    "#;

    let mut vars = HashMap::new();
    vars.insert("n".to_string(), json!("Gaming Laptop"));
    vars.insert("c".to_string(), json!("Electronics"));
    vars.insert("p".to_string(), json!(1299.99));
    vars.insert("s".to_string(), json!(50));

    let result = db.execute((mutation_aql, json!(vars))).await?;
    println!("Mutation Result: {:?}", result);

    Ok(())
}

#[tokio::test]
async fn section_3_complex_querying() -> Result<()> {
    let db = setup_db().await?;
    ensure_schema(&db).await?;
    seed_data(&db).await?;

    // -------------------------------------------------------------------------
    // 3. Find specific data
    // -------------------------------------------------------------------------
    // "Find active Electronics under $1000, sorted by price (cheapest first)"

    let query_aql = r#"
        query {
            products(
                where: {
                    category: { eq: "Electronics" },
                    active: { eq: true },
                    price: { lt: 1000.0 }
                },
                orderBy: { field: "price", direction: ASC },
                limit: 5
            ) {
                name
                price
                stock
            }
        }
    "#;

    let result = db.execute(query_aql).await?;
    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        println!("Found {} cheap electronics:", q.documents.len());
        for doc in q.documents {
            println!("- {} (${})", doc.data["name"], doc.data["price"]);
        }
    }

    Ok(())
}

#[tokio::test]
async fn section_4_computed_fields() -> Result<()> {
    let db = setup_db().await?;
    ensure_schema(&db).await?;
    seed_data(&db).await?;

    // -------------------------------------------------------------------------
    // 4. Register Computed Field
    // -------------------------------------------------------------------------
    // Let's define "inventory_value" = price * stock
    // This field doesn't exist in the DB, but we can query it!

    db.register_computed_field(
        "products",
        "inventory_value",
        aurora_db::computed::ComputedExpression::Script("doc.price * doc.stock".to_string()),
    )
    .await?;

    // Query using the computed field
    let query_aql = r#"
        query {
            products(
                where: {
                    # Filter by computed value!
                    inventory_value: { gt: 10000.0 }
                }
            ) {
                name
                stock
                inventory_value # Request it in output
            }
        }
    "#;

    let result = db.execute(query_aql).await?;
    println!("Computed Field Query Result: {:?}", result);

    Ok(())
}

#[tokio::test]
async fn section_5_aggregation() -> Result<()> {
    let db = setup_db().await?;
    ensure_schema(&db).await?;
    seed_data(&db).await?;

    // -------------------------------------------------------------------------
    // 5. Analytics / Aggregation
    // -------------------------------------------------------------------------
    // "Group products by category and get the average price and total count"

    let agg_aql = r#"
        query {
            products {
                groupBy(field: "category") {
                    category: key
                    total_products: count
                    avg_price: aggregate {
                        avg(field: "price")
                    }
                }
            }
        }
    "#;

    let result = db.execute(agg_aql).await?;
    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        println!("Category Stats:");
        for doc in q.documents {
            println!(
                "Category: {}, Count: {}, Avg Price: {}",
                doc.data["category"],
                doc.data["total_products"],
                doc.data
                    .get("avg_price")
                    .unwrap_or(&aurora_db::types::Value::Null)
            );
        }
    }

    Ok(())
}

// --- DOMAIN HELPERS ---

async fn setup_db() -> Result<Aurora> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("showcase.db");
    let config = aurora_db::AuroraConfig {
        db_path,
        ..Default::default()
    };
    Aurora::with_config(config).await
}

async fn ensure_schema(db: &Aurora) -> Result<()> {
    db.execute(
        r#"
        schema {
            define collection products if not exists {
                name: String
                category: String
                price: Float
                stock: Int
                active: Boolean
            }
        }
    "#,
    )
    .await?;
    Ok(())
}

async fn seed_data(db: &Aurora) -> Result<()> {
    let products = vec![
        json!({"name": "Laptop", "category": "Electronics", "price": 1200.0, "stock": 50, "active": true}),
        json!({"name": "Mouse", "category": "Electronics", "price": 25.0, "stock": 200, "active": true}),
        json!({"name": "Keyboard", "category": "Electronics", "price": 80.0, "stock": 100, "active": true}),
        json!({"name": "Chair", "category": "Furniture", "price": 150.0, "stock": 20, "active": true}),
        json!({"name": "Desk", "category": "Furniture", "price": 300.0, "stock": 10, "active": false}), // Inactive
    ];

    for p in products {
        let aql = r#"
            mutation Seed($d: JSON) {
                insertInto(collection: "products", data: $d) { id }
            }
        "#;
        let mut vars = HashMap::new();
        vars.insert("d".to_string(), p);
        db.execute((aql, json!(vars))).await?;
    }
    Ok(())
}
