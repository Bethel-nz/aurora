//! # Aurora Full-Stack Example
//!
//!
//! Run with: `cargo run --example full_stack_showcase`

use aurora_db::types::Value;
use aurora_db::{Aurora, AuroraConfig, DurabilityMode};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> aurora_db::Result<()> {
    println!("                 AURORA DATABASE - FULL STACK DEMO                 ");

    // STEP 1: Initialize Aurora
    println!("🚀 [1/5] Initializing Aurora Database...");

    let temp_dir = tempfile::tempdir().unwrap();
    let config = AuroraConfig {
        db_path: temp_dir.path().join("demo.db"),
        enable_write_buffering: true,
        durability_mode: DurabilityMode::None, // Fastest for demo (no persistence guarantees)
        ..Default::default()
    };

    let db = Aurora::with_config(config)?;
    println!("   ✓ Database created at: {:?}\n", temp_dir.path());

    // STEP 2: Schema Migration (Define Collections via AQL)
    println!("📋 [2/5] Running Schema Migration...");

    let migration_aql = r#"
        schema {
            define collection products {
                name: String!       @indexed
                category: String    @indexed
                price: Float
                stock: Int
                active: Boolean
            }
            
            define collection orders {
                product_id: String!
                customer: String
                quantity: Int
                status: String      @indexed
            }
        }
    "#;

    db.execute(migration_aql).await?;
    println!("   ✓ Created 'products' and 'orders' collections\n");

    // STEP 3: Seed Data (Mutations)
    println!("📦 [3/5] Seeding Data...");

    let products = vec![
        ("MacBook Pro", "Electronics", 2499.0, 50, true),
        ("iPhone 15", "Electronics", 999.0, 200, true),
        ("AirPods Pro", "Electronics", 249.0, 500, true),
        ("Magic Keyboard", "Accessories", 99.0, 150, true),
        ("USB-C Cable", "Accessories", 19.0, 1000, true),
        ("Display Stand", "Furniture", 199.0, 30, false), // Inactive
    ];

    for (name, category, price, stock, active) in products {
        let mut vars = HashMap::new();
        vars.insert("n".to_string(), json!(name));
        vars.insert("c".to_string(), json!(category));
        vars.insert("p".to_string(), json!(price));
        vars.insert("s".to_string(), json!(stock));
        vars.insert("a".to_string(), json!(active));

        db.execute((
            r#"
            mutation AddProduct($n: String, $c: String, $p: Float, $s: Int, $a: Boolean) {
                insertInto(collection: "products", data: { 
                    name: $n, category: $c, price: $p, stock: $s, active: $a 
                }) { id }
            }
        "#,
            json!(vars),
        ))
        .await?;
    }
    println!("   ✓ Inserted {} products\n", 6);

    // STEP 4: Complex Queries

    println!("🔍 [4/5] Running Complex Queries...\n");

    // --- Query A: Filter + Sort + Limit ---
    println!("   📌 Query A: Active Electronics under $1000, sorted by price");
    let query_a = r#"
        query {
            products(
                where: {
                    category: { eq: "Electronics" },
                    active: { eq: true },
                    price: { lt: 1000.0 }
                },
                orderBy: { field: "price", direction: ASC },
                limit: 3
            ) {
                name
                price
            }
        }
    "#;

    if let aurora_db::parser::executor::ExecutionResult::Query(result) = db.execute(query_a).await?
    {
        for doc in &result.documents {
            println!(
                "      - {} (${:.2})",
                doc.data.get("name").unwrap_or(&Value::Null),
                match doc.data.get("price") {
                    Some(Value::Float(p)) => *p,
                    Some(Value::Int(p)) => *p as f64,
                    _ => 0.0,
                }
            );
        }
    }
    println!();

    // --- Query B: Computed Field ---
    println!("   📌 Query B: Computed Field (inventory_value = price * stock)");

    // Register computed field
    db.register_computed_field(
        "products",
        "inventory_value",
        aurora_db::computed::ComputedExpression::Script("doc.price * doc.stock".to_string()),
    )
    .await?;

    // Use the Fluent API (not AQL) for this example
    let high_value = db
        .query("products")
        .filter(|f| f.gt("inventory_value", 10000.0))
        .collect()
        .await?;

    for doc in &high_value {
        println!(
            "      - {} | Value: ${:.2}",
            doc.data.get("name").unwrap_or(&Value::Null),
            match doc.data.get("inventory_value") {
                Some(Value::Float(v)) => *v,
                Some(Value::Int(v)) => *v as f64,
                _ => 0.0,
            }
        );
    }
    println!();

    // --- Query C: Aggregation (GroupBy) ---
    println!("   📌 Query C: Aggregation - Products per Category");
    let query_c = r#"
        query {
            products {
                groupBy(field: "category") {
                    category: key
                    count
                }
            }
        }
    "#;

    if let aurora_db::parser::executor::ExecutionResult::Query(result) = db.execute(query_c).await?
    {
        for doc in &result.documents {
            println!(
                "      - {}: {} products",
                doc.data.get("category").unwrap_or(&Value::Null),
                doc.data.get("count").unwrap_or(&Value::Null)
            );
        }
    }
    println!();

    // STEP 5: Real-Time Subscriptions (via AQL `subscription` keyword)
    println!("📡 [5/5] Real-Time Subscription Demo (via AQL)...\n");

    // Subscribe using AQL syntax with the new ergonomic API!
    // No need to manually extract from ExecutionResult anymore.
    let subscription_aql = r#"
        subscription {
            products(where: { active: { eq: true } }) {
                id
                name
                category
            }
        }
    "#;

    // New clean API: db.stream() returns ChangeListener directly
    let mut listener = db.stream(subscription_aql).await?;
    println!("   📌 Streaming changes from 'products' collection (active products only)");

    // Spawn a task to listen for changes
    let db_clone = db.clone();
    let listener_handle = tokio::spawn(async move {
        println!("   👂 Listening for active product changes (will receive 2 events)...");
        let mut count = 0;
        while let Ok(event) = listener.recv().await {
            println!(
                "   🔔 Event: {:?} on document '{}'",
                event.change_type, event.id
            );
            count += 1;
            if count >= 2 {
                break;
            }
        }
    });

    // Give listener time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger some changes
    println!("   ✏️  Inserting new product...");
    db_clone
        .execute((
            r#"
        mutation {
            insertInto(collection: "products", data: { 
                name: "New Gadget", category: "Electronics", price: 599.0, stock: 25, active: true 
            }) { id }
        }
    "#,
            json!({}),
        ))
        .await?;

    println!("   ✏️  Updating existing product...");
    db_clone
        .execute(
            r#"
        mutation {
            update(
                collection: "products", 
                where: { name: { eq: "USB-C Cable" } },
                data: { stock: 1500 }
            ) { id }
        }
    "#,
        )
        .await?;

    // Wait for listener to receive events
    tokio::time::sleep(Duration::from_millis(200)).await;
    listener_handle.abort(); // Clean up

    println!("                         DEMO COMPLETE                             ");

    Ok(())
}
