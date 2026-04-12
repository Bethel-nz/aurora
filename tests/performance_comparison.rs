use aurora_db::{Aurora, FieldType, Value};
use serde_json::json;
use std::time::Instant;
use tempfile::TempDir;

#[tokio::test]
async fn test_fluent_vs_aql_optimized() {
    let temp_dir = TempDir::new().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    println!("\n=== OPTIMIZED PERFORMANCE: Fluent API vs AQL (Variable Caching) ===");

    // 1. Ingestion Performance (1,000 Inserts)
    db.new_collection(
        "perf",
        vec![
            (
                "name",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "val",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
                    unique: false,
                    indexed: true,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ), // Indexed
        ],
    )
    .await
    .unwrap();

    println!("\nIngestion (1,000 Inserts):");

    // Fluent Timing
    let start = Instant::now();
    for i in 0..1000 {
        db.insert_into(
            "perf",
            vec![
                ("name", Value::String(format!("User {}", i))),
                ("val", Value::Int(i)),
            ],
        )
        .await
        .unwrap();
    }
    let fluent_ingest_time = start.elapsed();
    println!("  Fluent API:  {:?}", fluent_ingest_time);

    // AQL Timing (Using Variables to hit AST cache)
    let aql_mutation = "mutation Create($n: String, $v: Int) { insertInto(collection: \"perf\", data: { name: $n, val: $v }) { id } }";
    let start = Instant::now();
    for i in 0..1000 {
        let vars = json!({ "n": format!("User {}", i + 1000), "v": i + 1000 });
        db.execute((aql_mutation, vars)).await.unwrap();
    }
    let aql_ingest_time = start.elapsed();
    println!("  AQL (Cached): {:?}", aql_ingest_time);

    // 2. Query Performance (1,000 Index Lookups)
    println!("\nQuery (1,000 Index Lookups):");

    // Fluent Timing (Should benefit from Object Cache)
    let start = Instant::now();
    for i in 0..1000 {
        let target = (i % 1000) as i64;
        let _ = db
            .query("perf")
            .filter(|f: &aurora_db::query::FilterBuilder| f.eq("val", target))
            .collect()
            .await
            .unwrap();
    }
    let fluent_query_time = start.elapsed();
    println!("  Fluent API:  {:?}", fluent_query_time);

    // AQL Timing (Using Variables + Object Cache)
    let aql_query = "query Find($v: Int) { perf(where: { val: { eq: $v } }) { name } }";
    let start = Instant::now();
    for i in 0..1000 {
        let target = (i % 1000) as i64;
        let vars = json!({ "v": target });
        let _ = db.execute((aql_query, vars)).await.unwrap();
    }
    let aql_query_time = start.elapsed();
    println!("  AQL (Cached): {:?}", aql_query_time);

    println!("\nSummary:");
    println!(
        "  AQL overhead for Ingestion: {:.1}x (was 4.0x)",
        aql_ingest_time.as_secs_f64() / fluent_ingest_time.as_secs_f64()
    );
    println!(
        "  AQL overhead for Query:     {:.1}x (was 1.1x)",
        aql_query_time.as_secs_f64() / fluent_query_time.as_secs_f64()
    );
    println!("==========================================================\n");
}
