use aurora_db::Aurora;
use aurora_db::types::{FieldType, Value};
use std::collections::HashMap;
use tempfile::tempdir;

#[tokio::test]
async fn test_any_type_and_nested_query() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_db");
    let db = Aurora::open(path).unwrap();

    // 1. Test schema validation for Any type
    // Should not allow unique (and by extension, indexed) Any fields
    let res = db
        .new_collection(
            "test_collection",
            vec![("meta", FieldType::Any, true)], // true for unique
        )
        .await;
    assert!(res.is_err(), "Should not allow unique 'Any' fields");

    // Create a valid collection
    db.new_collection(
        "test_collection",
        vec![
            ("name", FieldType::String, false),
            ("meta", FieldType::Any, false),
        ],
    )
    .await
    .unwrap();

    // Should not allow creating an index on an Any field
    let res = db.create_index("test_collection", "meta").await;
    assert!(
        res.is_err(),
        "Should not allow creating an index on 'Any' field"
    );

    // 2. Insert a document with a nested object in the 'Any' field
    let mut nested_obj = HashMap::new();
    nested_obj.insert("author".to_string(), Value::from("John Doe"));
    nested_obj.insert("visits".to_string(), Value::from(150));

    let mut data = HashMap::new();
    data.insert("name".to_string(), Value::from("My Document"));
    data.insert("meta".to_string(), Value::Object(nested_obj));

    db.insert_map("test_collection", data).await.unwrap();

    // 3. Perform nested queries
    // Test nested eq
    let results = db
        .query("test_collection")
        .filter(|f| f.eq("meta.author", "John Doe"))
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].data.get("name").unwrap().as_str(),
        Some("My Document")
    );

    // Test nested gt
    let results = db
        .query("test_collection")
        .filter(|f| f.gt("meta.visits", 100))
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    // Test nested lt (for a non-matching case)
    let results = db
        .query("test_collection")
        .filter(|f| f.lt("meta.visits", 100))
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 0);

    // Test query on non-Any field to ensure that still works
    let results = db
        .query("test_collection")
        .filter(|f| f.eq("name", "My Document"))
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_any_field_caching() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_db_caching");
    let db = Aurora::open(path).unwrap();

    // 1. Collection with an 'Any' field
    db.new_collection(
        "any_collection",
        vec![
            ("name", FieldType::String, false),
            ("data", FieldType::Any, false),
        ],
    )
    .await
    .unwrap();

    let doc_id_any = db
        .insert_into(
            "any_collection",
            vec![
                ("name", Value::from("any-doc")),
                ("data", Value::from("some data")),
            ],
        )
        .await
        .unwrap();

    // After insert, it should NOT be in the hot cache
    let key_any = format!("any_collection:{}", doc_id_any);
    assert!(
        !db.is_in_hot_cache(&key_any),
        "Document with 'Any' field should not be in hot cache after insert"
    );

    // Get the document, which should read from cold storage
    let _ = db.get_document("any_collection", &doc_id_any).unwrap();

    // It should still NOT be in the hot cache
    assert!(
        !db.is_in_hot_cache(&key_any),
        "Document with 'Any' field should not be in hot cache after get"
    );

    // 2. Collection without an 'Any' field
    db.new_collection(
        "normal_collection",
        vec![("name", FieldType::String, false)],
    )
    .await
    .unwrap();

    let doc_id_normal = db
        .insert_into(
            "normal_collection",
            vec![("name", Value::from("normal-doc"))],
        )
        .await
        .unwrap();

    // After insert, it SHOULD be in the hot cache
    let key_normal = format!("normal_collection:{}", doc_id_normal);
    assert!(
        db.is_in_hot_cache(&key_normal),
        "Document without 'Any' field should be in hot cache after insert"
    );

    // Flush write buffer to cold storage before testing the cold->hot cache path
    db.flush().unwrap();

    // To be extra sure, clear cache, get, and check again
    db.clear_hot_cache();
    assert!(
        !db.is_in_hot_cache(&key_normal),
        "Document should not be in cache after clearing"
    );

    let _ = db
        .get_document("normal_collection", &doc_id_normal)
        .unwrap();

    assert!(
        db.is_in_hot_cache(&key_normal),
        "Document without 'Any' field should be in hot cache after get"
    );
}
