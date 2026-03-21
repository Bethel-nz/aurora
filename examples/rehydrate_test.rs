use aurora_db::{Aurora, AuroraConfig};
use std::time::Instant;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = PathBuf::from("stress_test.db");
    
    if !db_path.exists() {
        println!("Database not found! Run stress_test_1m first.");
        return Ok(());
    }

    println!("==================================================");
    println!("AURORA RE-HYDRATION & COMPLEX QUERY TEST");
    println!("==================================================");

    // 1. Measure Open & Re-hydration Time
    let start_open = Instant::now();
    let config = AuroraConfig {
        db_path: db_path.clone(),
        hot_cache_size_mb: 256,
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;
    let open_duration = start_open.elapsed();
    println!("Database Opened in: {:?}", open_duration);

    let start_rehydrate = Instant::now();
    db.ensure_indices_initialized().await?;
    let rehydrate_duration = start_rehydrate.elapsed();
    println!("Indices Re-hydrated in: {:?}", rehydrate_duration);
    println!("(Index checkpoint memory-mapped; secondary indices populated from cold storage on demand)");

    // 2. Complex Query Test
    println!("\nExecuting Fluent Query (3 Equality conditions)...");
    let start_fluent = Instant::now();
    let fluent_results = db.query("users")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("city", "Lagos"))
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("age", 42))
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("active", true))
        .collect()
        .await?;
    let fluent_duration = start_fluent.elapsed();

    println!("Fluent Result Count: {}", fluent_results.len());
    println!("Fluent Execution Time: {:?}", fluent_duration);

    if !fluent_results.is_empty() {
        println!("\nSample Match:");
        println!("  ID: {}", fluent_results[0].id);
        println!("  Name: {:?}", fluent_results[0].data.get("name"));
    }

    // 3. AQL Query
    let aql_query = r#"
        query {
            users(where: { 
                city: { eq: "London" }, 
                age: { gt: 95 }
            }) { 
                id 
                name 
            }
        }
    "#;

    println!("\nExecuting AQL Query...");
    let start_aql = Instant::now();
    let result = db.execute(aql_query).await?;
    let aql_duration = start_aql.elapsed();

    if let aurora_db::parser::executor::ExecutionResult::Query(res) = result {
        println!("AQL Result Count: {}", res.documents.len());
        println!("AQL Execution Time: {:?}", aql_duration);
    }

    println!("==================================================");
    Ok(())
}
