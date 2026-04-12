use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::time::Instant;
use tempfile::TempDir;

#[tokio::test]
async fn test_deep_pagination_and_projections() {
    println!("\n=== STRESS TEST: Deep Pagination & Projections (1M Docs) ===");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("pagination.db");

    // Use high-performance config
    let config = AuroraConfig {
        db_path: db_path.clone(),
        enable_write_buffering: true,
        ..Default::default()
    };

    let db = Aurora::with_config(config).await.unwrap();
    let collection = "large_data";

    // 1. Schema
    db.new_collection(
        collection,
        vec![
            (
                "index",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
                    unique: false,
                    indexed: true,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "tag",
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
                "payload",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
        ],
    )
    .await
    .unwrap();

    // 2. Ingestion (1M documents)
    let num_docs = 1_000_000;
    println!("Ingesting {} documents...", format_number(num_docs));
    let start = Instant::now();

    let batch_size = 10_000;
    for i in 0..(num_docs / batch_size) {
        let mut batch = Vec::with_capacity(batch_size);
        for j in 0..batch_size {
            let idx = (i * batch_size + j) as i64;
            let mut data = std::collections::HashMap::new();
            data.insert("index".to_string(), Value::Int(idx));
            data.insert(
                "tag".to_string(),
                Value::String(format!("tag_{}", idx % 100)),
            );
            data.insert("payload".to_string(), Value::String("A".repeat(100))); // 100 bytes of data
            batch.push(data);
        }
        db.batch_insert(collection, batch).await.unwrap();

        if (i + 1) % 10 == 0 {
            println!(
                "  Processed {} docs...",
                format_number((i + 1) * batch_size)
            );
        }
    }
    println!("Ingestion finished in {:?}", start.elapsed());

    // 3. Deep Pagination Test
    // Goal: Fetch the very last 10 records.
    let offset = 999_990;
    let limit = 10;
    println!(
        "\nTesting Deep Pagination (Offset: {}, Limit: {})...",
        format_number(offset),
        limit
    );

    let start = Instant::now();
    let aql = format!(
        r#"
        query {{
            {}(offset: {}, limit: {}, orderBy: {{ index: ASC }}) {{
                index
            }}
        }}
    "#,
        collection, offset, limit
    );

    let result = db.execute(aql.as_str()).await.unwrap();
    let duration = start.elapsed();

    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        assert_eq!(q.documents.len(), limit);
        assert_eq!(
            q.documents[0].data.get("index"),
            Some(&Value::Int(offset as i64))
        );
        println!("  Deep Offset Query took: {:?}", duration);
    }

    // 3b. Cursor Pagination Test (at 1M scale)
    println!("\nTesting Cursor Pagination (After 500,000)...");
    // Find the 500,000th document ID to use as a cursor
    let mid_offset = 500_000;
    let mid_docs = db
        .query(collection)
        .limit(1)
        .offset(mid_offset)
        .collect()
        .await
        .unwrap();
    let cursor_id = mid_docs[0]._sid.clone();

    let start = Instant::now();
    let aql = format!(
        r#"
        query {{
            {}(first: 10, after: "{}") {{
                edges {{
                    node {{
                        index
                    }}
                }}
                pageInfo {{
                    hasNextPage
                }}
            }}
        }}
    "#,
        collection, cursor_id
    );

    let result = db.execute(aql.as_str()).await.unwrap();
    let duration = start.elapsed();
    println!("  Cursor Query (Jump to 500k) took: {:?}", duration);

    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        assert_eq!(q.documents.len(), 1); // Connection wrapper
    }

    // 4. Projection Test
    println!("\nTesting Projections...");
    let aql = format!(
        r#"
        query {{
            {}(limit: 1) {{
                id
                index
                tag
            }}
        }}
    "#,
        collection
    );

    let result = db.execute(aql.as_str()).await.unwrap();
    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        let doc = &q.documents[0];
        println!("  Document keys: {:?}", doc.data.keys().collect::<Vec<_>>());
        assert!(doc.data.contains_key("id"), "Should contain id");
        assert!(doc.data.contains_key("index"), "Should contain 'index'");
        assert!(doc.data.contains_key("tag"), "Should contain 'tag'");
        assert!(
            !doc.data.contains_key("payload"),
            "Should NOT contain excluded field 'payload'"
        );

        println!("  Projection: SUCCESS");
    }

    // 5. Empty Projection Test (Return All)
    println!("Testing Empty Projection (Return All)...");
    let aql = format!(
        r#"
        query {{
            {} (limit: 1)
        }}
    "#,
        collection
    );

    let result = db.execute(aql.as_str()).await.unwrap();
    if let aurora_db::parser::executor::ExecutionResult::Query(q) = result {
        let doc = &q.documents[0];
        assert!(doc.data.len() >= 3, "Should contain all fields");
        assert!(doc.data.contains_key("index"));
        assert!(doc.data.contains_key("tag"));
        assert!(doc.data.contains_key("payload"));
        println!("  Empty Projection: SUCCESS");
    }

    println!("\n=== ALL PAGINATION AND PROJECTION TESTS PASSED ===\n");
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;
    for c in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
        count += 1;
    }
    result.chars().rev().collect()
}
