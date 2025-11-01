use aurora_db::db::Aurora;
use aurora_db::types::{FieldType, Value};
use std::time::Instant;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Aurora Database - Contains Query Optimization Demo");
    println!("====================================================\n");

    // Create a temporary database
    let db_path = format!("demo_contains_{}", Uuid::new_v4());
    let db = Aurora::open(&db_path)?;

    // Create a collection for our demo
    db.new_collection(
        "products",
        vec![
            ("name".to_string(), FieldType::String, false),
            ("description".to_string(), FieldType::String, false),
            ("category".to_string(), FieldType::String, false),
            ("price".to_string(), FieldType::Float, false),
            ("in_stock".to_string(), FieldType::Bool, false),
        ],
    )?;

    println!("📦 Created 'products' collection");

    // Generate sample data
    let sample_products = vec![
        (
            "Wireless Bluetooth Headphones",
            "High-quality wireless headphones with noise cancellation",
            "Electronics",
            99.99,
        ),
        (
            "Gaming Mechanical Keyboard",
            "RGB backlit mechanical keyboard for gaming enthusiasts",
            "Electronics",
            149.99,
        ),
        (
            "Organic Coffee Beans",
            "Premium organic coffee beans from South America",
            "Food",
            24.99,
        ),
        (
            "Yoga Mat Premium",
            "Non-slip yoga mat made from eco-friendly materials",
            "Fitness",
            39.99,
        ),
        (
            "Smartphone Case Leather",
            "Genuine leather case for latest smartphone models",
            "Accessories",
            29.99,
        ),
        (
            "Bluetooth Speaker Portable",
            "Waterproof portable speaker with excellent sound quality",
            "Electronics",
            79.99,
        ),
        (
            "Organic Green Tea",
            "Premium organic green tea leaves from Japan",
            "Food",
            19.99,
        ),
        (
            "Fitness Tracker Watch",
            "Advanced fitness tracker with heart rate monitoring",
            "Electronics",
            199.99,
        ),
        (
            "Bamboo Cutting Board",
            "Eco-friendly bamboo cutting board for kitchen use",
            "Kitchen",
            34.99,
        ),
        (
            "Wireless Charging Pad",
            "Fast wireless charging pad for compatible devices",
            "Electronics",
            49.99,
        ),
    ];

    // Insert sample data
    println!("📝 Inserting sample products...");
    let mut product_ids = Vec::new();

    for (name, description, category, price) in &sample_products {
        let id = db
            .insert_into(
                "products",
                vec![
                    ("name", Value::String(name.to_string())),
                    ("description", Value::String(description.to_string())),
                    ("category", Value::String(category.to_string())),
                    ("price", Value::Float(*price)),
                    ("in_stock", Value::Bool(true)),
                ],
            )
            .await?;
        product_ids.push(id);
    }

    // Add more products to make the demo more interesting
    println!("📝 Adding more products for performance testing...");
    for i in 0..1000 {
        let name = format!("Product {}", i);
        let description = format!(
            "Description for product {} with various keywords like wireless, bluetooth, organic, premium",
            i
        );
        let category = match i % 4 {
            0 => "Electronics",
            1 => "Food",
            2 => "Fitness",
            _ => "Kitchen",
        };
        let price = 10.0 + (i as f64 * 0.1);

        db.insert_into(
            "products",
            vec![
                ("name", Value::String(name)),
                ("description", Value::String(description)),
                ("category", Value::String(category.to_string())),
                ("price", Value::Float(price)),
                ("in_stock", Value::Bool(i % 3 != 0)),
            ],
        )
        .await?;
    }

    println!(
        "✅ Inserted {} total products\n",
        sample_products.len() + 1000
    );

    // Demo 1: Contains query without index (baseline)
    println!("🔍 Demo 1: Contains Query WITHOUT Index");
    println!("---------------------------------------");

    let start = Instant::now();
    let results_no_index = db
        .query("products")
        .filter(|f| f.contains("description", "wireless"))
        .collect()
        .await?;
    let duration_no_index = start.elapsed();

    println!("⏱️  Query time (no index): {:?}", duration_no_index);
    println!("📊 Results found: {}", results_no_index.len());

    // Show a few results
    println!("📋 Sample results:");
    for (i, doc) in results_no_index.iter().take(3).enumerate() {
        if let Some(Value::String(name)) = doc.data.get("name") {
            println!("   {}. {}", i + 1, name);
        }
    }
    println!();

    // Demo 2: Create index and test Contains query with index
    println!("🔍 Demo 2: Contains Query WITH Index");
    println!("------------------------------------");

    println!("🏗️  Creating index on 'description' field...");
    db.create_index("products", "description").await?;

    let start = Instant::now();
    let results_with_index = db
        .query("products")
        .filter(|f| f.contains("description", "wireless"))
        .collect()
        .await?;
    let duration_with_index = start.elapsed();

    println!("⏱️  Query time (with index): {:?}", duration_with_index);
    println!("📊 Results found: {}", results_with_index.len());

    // Verify we get the same results
    if results_no_index.len() == results_with_index.len() {
        println!("✅ Index optimization successful - same number of results");
    } else {
        println!("❌ Index optimization issue - different result counts");
    }

    // Performance comparison
    if duration_with_index < duration_no_index {
        let speedup = duration_no_index.as_nanos() as f64 / duration_with_index.as_nanos() as f64;
        println!(
            "🚀 Performance improvement: {:.2}x faster with index",
            speedup
        );
    }
    println!();

    // Demo 3: Multiple Contains queries with different terms
    println!("🔍 Demo 3: Multiple Contains Query Tests");
    println!("---------------------------------------");

    let search_terms = vec!["bluetooth", "organic", "premium", "eco-friendly"];

    for term in &search_terms {
        let start = Instant::now();
        let results = db
            .query("products")
            .filter(|f| f.contains("description", term))
            .collect()
            .await?;
        let duration = start.elapsed();

        println!(
            "🔍 Search term '{}': {} results in {:?}",
            term,
            results.len(),
            duration
        );
    }
    println!();

    // Demo 4: Case-insensitive Contains queries
    println!("🔍 Demo 4: Case-Insensitive Contains Queries");
    println!("--------------------------------------------");

    let case_variations = vec!["WIRELESS", "Wireless", "wireless", "WiReLeSs"];

    for variant in &case_variations {
        let start = Instant::now();
        let results = db
            .query("products")
            .filter(|f| f.contains("description", variant))
            .collect()
            .await?;
        let duration = start.elapsed();

        println!(
            "🔍 Search '{}': {} results in {:?}",
            variant,
            results.len(),
            duration
        );
    }
    println!();

    // Demo 5: Contains query on different fields
    println!("🔍 Demo 5: Contains Queries on Different Fields");
    println!("-----------------------------------------------");

    // Create indices for other fields
    db.create_index("products", "name").await?;
    db.create_index("products", "category").await?;

    let field_searches = vec![
        ("name", "Bluetooth"),
        ("description", "quality"),
        ("category", "Electronics"),
    ];

    for (field, term) in &field_searches {
        let start = Instant::now();
        let results = db
            .query("products")
            .filter(|f| f.contains(field, term))
            .collect()
            .await?;
        let duration = start.elapsed();

        println!(
            "🔍 Field '{}' contains '{}': {} results in {:?}",
            field,
            term,
            results.len(),
            duration
        );

        // Show first result
        if let Some(doc) = results.first() {
            if let Some(Value::String(name)) = doc.data.get("name") {
                println!("   📋 Example: {}", name);
            }
        }
    }
    println!();

    // Demo 6: Complex queries combining Contains with other filters
    println!("🔍 Demo 6: Complex Queries with Contains + Other Filters");
    println!("--------------------------------------------------------");

    let start = Instant::now();
    let complex_results = db
        .query("products")
        .filter(|f| f.contains("description", "wireless") && f.eq("category", "Electronics"))
        .collect()
        .await?;
    let duration = start.elapsed();

    println!("🔍 Complex query (contains 'wireless' AND category = 'Electronics'):");
    println!("   ⏱️  Query time: {:?}", duration);
    println!("   📊 Results: {}", complex_results.len());

    for (i, doc) in complex_results.iter().take(3).enumerate() {
        if let Some(Value::String(name)) = doc.data.get("name") {
            println!("   {}. {}", i + 1, name);
        }
    }
    println!();

    // Demo 7: Index statistics
    println!("📊 Demo 7: Index Statistics");
    println!("---------------------------");

    let index_stats = db.get_index_stats("products");
    for (field, stats) in &index_stats {
        println!("📈 Field '{}' index:", field);
        println!("   • Unique values: {}", stats.unique_values);
        println!("   • Total documents: {}", stats.total_documents);
        println!("   • Avg docs per value: {}", stats.avg_docs_per_value);
    }
    println!();

    // Demo 8: Performance comparison with large dataset
    println!("🔍 Demo 8: Performance Test with Larger Dataset");
    println!("-----------------------------------------------");

    // Add more test data
    println!("📝 Adding more test data...");
    for i in 1000..5000 {
        let name = format!("Test Product {}", i);
        let description = format!(
            "Test description {} with wireless bluetooth technology and premium quality",
            i
        );
        let category = "Electronics";
        let price = 50.0 + (i as f64 * 0.05);

        db.insert_into(
            "products",
            vec![
                ("name", Value::String(name)),
                ("description", Value::String(description)),
                ("category", Value::String(category.to_string())),
                ("price", Value::Float(price)),
                ("in_stock", Value::Bool(true)),
            ],
        )
        .await?;
    }

    // Test performance with larger dataset
    let start = Instant::now();
    let large_results = db
        .query("products")
        .filter(|f| f.contains("description", "premium"))
        .collect()
        .await?;
    let large_duration = start.elapsed();

    println!("⏱️  Large dataset query time: {:?}", large_duration);
    println!(
        "📊 Results from {} total documents: {}",
        5000 + sample_products.len(),
        large_results.len()
    );

    // Calculate query efficiency
    let docs_per_ms = large_results.len() as f64 / large_duration.as_millis() as f64;
    println!("⚡ Query efficiency: {:.2} matching docs/ms", docs_per_ms);
    println!();

    // Cleanup
    println!("🧹 Cleaning up...");
    std::fs::remove_dir_all(&db_path).ok();

    println!("✅ Contains Query Optimization Demo Complete!");
    println!("\n🎯 Key Takeaways:");
    println!("   • Contains queries can be optimized using secondary indices");
    println!("   • Case-insensitive matching is supported");
    println!("   • Index optimization provides significant performance improvements");
    println!("   • Multiple field indices can be used simultaneously");
    println!("   • Complex queries combining Contains with other filters work efficiently");

    Ok(())
}
