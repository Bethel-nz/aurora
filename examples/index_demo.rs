use aurora_db::{Aurora, FieldType, Value};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a database
    let db = Aurora::open("index_demo.db")?;

    // Create a collection for users
    db.new_collection(
        "users",
        vec![
            ("id".to_string(), FieldType::String, true),
            ("name".to_string(), FieldType::String, false),
            ("email".to_string(), FieldType::String, true),
            ("age".to_string(), FieldType::Int, false),
            ("department".to_string(), FieldType::String, false),
            ("salary".to_string(), FieldType::Float, false),
        ],
    )?;

    println!("🏗️  Inserting 10,000 test users...");
    let start = Instant::now();

    // Insert 10,000 test users
    let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
    for i in 0..10000 {
        db.insert_into(
            "users",
            vec![
                ("id", Value::String(format!("user_{}", i))),
                ("name", Value::String(format!("User {}", i))),
                ("email", Value::String(format!("user{}@company.com", i))),
                ("age", Value::Int(25 + (i % 40))),
                (
                    "department",
                    Value::String(departments[i as usize % 5].to_string()),
                ),
                ("salary", Value::Float(50000.0 + (i as f64 * 1000.0))),
            ],
        )?;
    }

    let insert_time = start.elapsed();
    println!("✅ Inserted 10,000 users in {:?}", insert_time);

    // Query without index (slow)
    println!("\n🐌 Querying without index...");
    let start = Instant::now();

    let results = db
        .query("users")
        .filter(|f| f.eq("department", "Engineering"))
        .collect()
        .await?;

    let slow_query_time = start.elapsed();
    println!(
        "Found {} Engineering users in {:?} (full scan)",
        results.len(),
        slow_query_time
    );

    // Create index on department field
    println!("\n🚀 Creating index on department field...");
    let start = Instant::now();
    db.create_index("users", "department").await?;
    let index_creation_time = start.elapsed();
    println!("✅ Index created in {:?}", index_creation_time);

    // Query with index (fast)
    println!("\n⚡ Querying with index...");
    let start = Instant::now();

    let results = db
        .query("users")
        .filter(|f| f.eq("department", "Engineering"))
        .collect()
        .await?;

    let fast_query_time = start.elapsed();
    println!(
        "Found {} Engineering users in {:?} (index lookup)",
        results.len(),
        fast_query_time
    );

    // Show the performance improvement
    if slow_query_time.as_nanos() > 0 {
        let speedup = slow_query_time.as_nanos() as f64 / fast_query_time.as_nanos() as f64;
        println!(
            "\n📊 Performance improvement: {:.1}x faster with index!",
            speedup
        );
    }

    // Create more indices for demonstration
    println!("\n🔧 Creating additional indices...");
    db.create_indices("users", &["email", "age", "salary"])
        .await?;

    // Show index statistics
    let stats = db.get_index_stats("users");
    println!("\n📈 Index Statistics:");
    for (field, stat) in stats {
        println!(
            "  {}: {} unique values, {} total docs, avg {:.1} docs/value",
            field, stat.unique_values, stat.total_documents, stat.avg_docs_per_value as f64
        );
    }

    // Demonstrate complex query with multiple indices
    println!("\n🔍 Complex query using multiple indices...");
    let start = Instant::now();

    let results = db
        .query("users")
        .filter(|f| f.eq("department", "Sales") && f.gt("salary", Value::Float(75000.0)))
        .order_by("salary", false)
        .limit(10)
        .collect()
        .await?;

    let complex_query_time = start.elapsed();
    println!(
        "Found {} high-earning Sales users in {:?}",
        results.len(),
        complex_query_time
    );

    // Show some results
    println!("\n👥 Top Sales performers:");
    for (i, user) in results.iter().take(5).enumerate() {
        let name = user.data.get("name").unwrap();
        let salary = user.data.get("salary").unwrap();
        println!(
            "  {}. {} - ${:.0}",
            i + 1,
            name,
            match salary {
                Value::Float(s) => *s,
                _ => 0.0,
            }
        );
    }

    println!("\n🎉 Index demo completed!");
    Ok(())
}
