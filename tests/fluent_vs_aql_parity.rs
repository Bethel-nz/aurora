use aurora_db::{Aurora, FieldType, Value, parser::executor::ExecutionResult};
use tempfile::TempDir;

#[tokio::test]
async fn test_fluent_vs_aql_parity() {
    let temp_dir = TempDir::new().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    println!("\n=== Testing Parity: Fluent API vs AQL ===");

    // 1. Schema Creation Parity
    // Fluent
    db.new_collection(
        "fluent_coll",
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
                "age",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
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

    // AQL
    db.execute(
        r#"
        schema {
            define collection aql_coll {
                name: String
                age: Int
            }
        }
    "#,
    )
    .await
    .unwrap();

    // Verify both exist
    assert!(db.get_collection_definition("fluent_coll").is_ok());
    assert!(db.get_collection_definition("aql_coll").is_ok());

    // 2. Data Ingestion Parity
    let data = vec![("Alice", 30), ("Bob", 25), ("Charlie", 35), ("David", 20)];

    for (name, age) in &data {
        // Fluent insert
        db.insert_into(
            "fluent_coll",
            vec![
                ("name", Value::String(name.to_string())),
                ("age", Value::Int(*age as i64)),
            ],
        )
        .await
        .unwrap();

        // AQL insert
        let aql_mutation = format!(
            "mutation {{ insertInto(collection: \"aql_coll\", data: {{ name: \"{}\", age: {} }}) {{ id }} }}",
            name, age
        );
        db.execute(aql_mutation.as_str()).await.unwrap();
    }

    // 3. Query Results Parity (Simple Filter)
    println!("Checking Simple Filter Parity (age > 25)...");

    // Fluent result
    let fluent_results = db
        .query("fluent_coll")
        .filter(|f: &aurora_db::query::FilterBuilder| f.gt("age", 25))
        .order_by("name", true)
        .collect()
        .await
        .unwrap();

    // AQL result
    let aql_query = r#"
        query {
            aql_coll(where: { age: { gt: 25 } }, orderBy: { name: ASC }) {
                name
                age
            }
        }
    "#;
    let aql_exec_res = db.execute(aql_query).await.unwrap();
    let aql_results = if let ExecutionResult::Query(q) = aql_exec_res {
        q.documents
    } else {
        panic!("Expected Query result");
    };

    // Compare
    assert_eq!(
        fluent_results.len(),
        aql_results.len(),
        "Result count mismatch"
    );
    for i in 0..fluent_results.len() {
        assert_eq!(fluent_results[i].data["name"], aql_results[i].data["name"]);
        assert_eq!(fluent_results[i].data["age"], aql_results[i].data["age"]);
    }
    println!("SUCCESS: Simple Filter parity verified.");

    // 4. Query Results Parity (Complex Logic)
    println!("Checking Complex Logic Parity (age < 25 OR age > 30)...");

    // Fluent result
    let fluent_complex = db
        .query("fluent_coll")
        .filter(|f: &aurora_db::query::FilterBuilder| f.lt("age", 25) | f.gt("age", 30))
        .order_by("age", true)
        .collect()
        .await
        .unwrap();

    // AQL result
    let aql_complex_query = r#"
        query {
            aql_coll(
                where: {
                    or: [
                        { age: { lt: 25 } },
                        { age: { gt: 30 } }
                    ]
                },
                orderBy: { age: ASC }
            ) {
                name
                age
            }
        }
    "#;
    let aql_complex_exec = db.execute(aql_complex_query).await.unwrap();
    let aql_complex_results = if let ExecutionResult::Query(q) = aql_complex_exec {
        q.documents
    } else {
        panic!("Expected Query result");
    };

    assert_eq!(fluent_complex.len(), aql_complex_results.len());
    for i in 0..fluent_complex.len() {
        assert_eq!(
            fluent_complex[i].data["name"],
            aql_complex_results[i].data["name"]
        );
    }
    println!("SUCCESS: Complex Logic parity verified.");

    // 5. Update Parity
    println!("Checking Update Parity...");

    // Fluent Update
    db.update_document(
        "fluent_coll",
        &fluent_results[0]._sid,
        vec![("age", Value::Int(100))],
    )
    .await
    .unwrap();

    // AQL Update
    let aql_update = format!(
        "mutation {{ update(collection: \"aql_coll\", where: {{ name: {{ eq: \"{}\" }} }}, set: {{ age: 100 }}) {{ affectedCount }} }}",
        aql_results[0].data["name"].as_str().unwrap()
    );
    db.execute(aql_update.as_str()).await.unwrap();

    // Verify both updated to 100
    let f_updated = db
        .query("fluent_coll")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("age", 100))
        .first_one()
        .await
        .unwrap()
        .unwrap();
    let a_updated_res = db
        .execute("query { aql_coll(where: { age: { eq: 100 } }) { name age } }")
        .await
        .unwrap();
    let a_updated = if let ExecutionResult::Query(q) = a_updated_res {
        q.documents[0].clone()
    } else {
        panic!("Expected Query result");
    };

    assert_eq!(f_updated.data["name"], a_updated.data["name"]);
    println!("SUCCESS: Update parity verified.");

    println!("=== ALL PARITY TESTS PASSED ===\n");
}
