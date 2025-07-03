use aurora_db::{Aurora, FieldType, Value};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a database
    let db = Aurora::open("range_query_demo.db")?;

    // Create a collection for products
    db.new_collection(
        "products",
        vec![
            ("id".to_string(), FieldType::String, true),
            ("name".to_string(), FieldType::String, false),
            ("price".to_string(), FieldType::Float, false),
            ("category".to_string(), FieldType::String, false),
            ("rating".to_string(), FieldType::Float, false),
            ("stock_quantity".to_string(), FieldType::Int, false),
        ],
    )?;

    println!("🏗️  Inserting 50,000 test products...");
    let start = Instant::now();

    // Insert 50,000 test products with varied data
    let categories = ["Electronics", "Books", "Clothing", "Home", "Sports", "Toys"];
    for i in 0..50000 {
        db.insert_into(
            "products",
            vec![
                ("id", Value::String(format!("prod_{}", i))),
                ("name", Value::String(format!("Product {}", i))),
                ("price", Value::Float(10.0 + (i as f64 * 0.5))), // Prices from $10 to $25,010
                (
                    "category",
                    Value::String(categories[i as usize % 6].to_string()),
                ),
                ("rating", Value::Float(1.0 + ((i % 50) as f64) / 10.0)), // Ratings 1.0 to 5.9
                ("stock_quantity", Value::Int(i % 1000)),                 // Stock 0 to 999
            ],
        )?;
    }

    let insert_time = start.elapsed();
    println!("✅ Inserted 50,000 products in {:?}", insert_time);

    // Demo 1: Range query WITHOUT index (slow)
    println!("\n🐌 Range query WITHOUT index...");
    let start = Instant::now();

    let results = db
        .query("products")
        .filter(|f| f.between("price", Value::Float(1000.0), Value::Float(2000.0)))
        .collect()
        .await?;

    let slow_query_time = start.elapsed();
    println!(
        "Found {} products in price range $1000-$2000 in {:?} (full scan)",
        results.len(),
        slow_query_time
    );

    // Create index on price field
    println!("\n🚀 Creating index on price field...");
    let start = Instant::now();
    db.create_index("products", "price").await?;
    let index_creation_time = start.elapsed();
    println!("✅ Price index created in {:?}", index_creation_time);

    // Demo 2: Range query WITH index (fast)
    println!("\n⚡ Range query WITH index...");
    let start = Instant::now();

    let results = db
        .query("products")
        .filter(|f| f.between("price", Value::Float(1000.0), Value::Float(2000.0)))
        .collect()
        .await?;

    let fast_query_time = start.elapsed();
    println!(
        "Found {} products in price range $1000-$2000 in {:?} (index lookup)",
        results.len(),
        fast_query_time
    );

    // Show performance improvement
    if slow_query_time.as_nanos() > 0 {
        let speedup = slow_query_time.as_nanos() as f64 / fast_query_time.as_nanos() as f64;
        println!(
            "\n📊 Range query speedup: {:.1}x faster with index!",
            speedup
        );
    }

    // Create more indices for complex queries
    println!("\n🔧 Creating additional indices...");
    db.create_indices("products", &["rating", "stock_quantity", "category"])
        .await?;

    // Demo 3: Complex multi-condition range query
    println!("\n🔍 Complex range query with multiple conditions...");
    let start = Instant::now();

    // Find high-rated, well-stocked products in a specific price range
    let results = db
        .query("products")
        .filter(|f| {
            f.between("price", Value::Float(500.0), Value::Float(1500.0))
                && f.gte("rating", Value::Float(4.0))
                && f.gt("stock_quantity", Value::Int(100))
        })
        .order_by("rating", false) // Best rated first
        .limit(20)
        .collect()
        .await?;

    let complex_query_time = start.elapsed();
    println!(
        "Found {} premium products in {:?}",
        results.len(),
        complex_query_time
    );

    // Demo 4: Different range query types
    println!("\n🎯 Testing different range query operators...");

    // Greater than
    let start = Instant::now();
    let expensive_products = db
        .query("products")
        .filter(|f| f.gt("price", Value::Float(20000.0)))
        .collect()
        .await?;
    let gt_time = start.elapsed();
    println!(
        "Products > $20,000: {} found in {:?}",
        expensive_products.len(),
        gt_time
    );

    // Less than or equal
    let start = Instant::now();
    let budget_products = db
        .query("products")
        .filter(|f| f.lte("price", Value::Float(50.0)))
        .collect()
        .await?;
    let lte_time = start.elapsed();
    println!(
        "Products ≤ $50: {} found in {:?}",
        budget_products.len(),
        lte_time
    );

    // High stock items
    let start = Instant::now();
    let high_stock = db
        .query("products")
        .filter(|f| f.gte("stock_quantity", Value::Int(900)))
        .collect()
        .await?;
    let gte_time = start.elapsed();
    println!(
        "High stock items (≥900): {} found in {:?}",
        high_stock.len(),
        gte_time
    );

    // Show index statistics
    let stats = db.get_index_stats("products");
    println!("\n📈 Index Statistics:");
    for (field, stat) in stats {
        println!(
            "  {}: {} unique values, {} total docs, avg {:.1} docs/value",
            field, stat.unique_values, stat.total_documents, stat.avg_docs_per_value as f64
        );
    }

    // Show some sample results
    println!("\n🏆 Sample premium products:");
    for (i, product) in results.iter().take(5).enumerate() {
        let name = product.data.get("name").unwrap();
        let price = product.data.get("price").unwrap();
        let rating = product.data.get("rating").unwrap();
        let stock = product.data.get("stock_quantity").unwrap();

        println!(
            "  {}. {} - ${:.0} (★{:.1}, {} in stock)",
            i + 1,
            name,
            match price {
                Value::Float(p) => *p,
                _ => 0.0,
            },
            match rating {
                Value::Float(r) => *r,
                _ => 0.0,
            },
            match stock {
                Value::Int(s) => *s,
                _ => 0,
            }
        );
    }

    println!("\n🎉 Range query demo completed!");
    Ok(())
}
