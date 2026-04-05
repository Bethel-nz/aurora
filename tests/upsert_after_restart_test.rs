use aurora_db::{Aurora, AuroraConfig};
use aurora_db::types::{FieldDefinition, FieldType, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_upsert_after_restart_no_unique_error() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Session 1: Insert 100 documents
    {
        let db = Aurora::with_config(AuroraConfig {
            db_path: db_path.clone(),
            enable_wal: true,
            ..Default::default()
        }).await.unwrap();

        db.new_collection("items", vec![
            ("unique_key", FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: true, indexed: true, nullable: false, validations: vec![] }),
            ("counter", FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: false, validations: vec![] }),
        ]).await.unwrap();

        for i in 0..100 {
            let key = format!("key_{}", i);
            let aql = format!(r#"
                mutation {{
                    insertInto(collection: "items", data: {{
                        unique_key: "{}",
                        counter: {}
                    }}) {{ id }}
                }}
            "#, key, i);
            
            db.execute(aql).await.expect("failed to insert");
        }
        
        // Ensure everything is flushed before shutdown
        db.flush().unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Session 2: Restart and perform upserts
    {
        let db = Aurora::with_config(AuroraConfig {
            db_path: db_path.clone(),
            enable_wal: true,
            ..Default::default()
        }).await.unwrap();

        // Upsert existing documents
        for i in 0..100 {
            let key = format!("key_{}", i);
            // Updating the 'counter' field while keeping the 'unique_key' same
            let aql = format!(r#"
                mutation {{
                    upsert(
                        collection: "items",
                        where: {{ unique_key: {{ eq: "{}" }} }},
                        data: {{ unique_key: "{}", counter: {} }}
                    ) {{ id }}
                }}
            "#, key, key, i + 100);

            // This should not fail with a unique constraint violation
            db.execute(aql).await.unwrap_or_else(|e| panic!("upsert after restart failed for {}: {}", key, e));
        }
        
        // Verify the update worked (e.g. for item 50, counter should be 150)
        let result = db.find_by_field("items", "unique_key", Value::String("key_50".to_string())).await.unwrap();
        assert_eq!(result.len(), 1);
        
        if let Some(counter) = result[0].data.get("counter") {
            match counter {
                Value::Int(v) => assert_eq!(v, &150),
                Value::Float(v) => assert_eq!(*v as i64, 150),
                _ => panic!("Expected Int or Float, got {:?}", counter),
            }
        } else {
            panic!("counter field missing");
        }
    }
}
