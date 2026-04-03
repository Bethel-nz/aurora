use aurora_db::Aurora;
use aurora_db::types::{FieldType, Value};

#[tokio::test]
async fn test_rich_query_operators() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap()).await.unwrap();

    // Setup collection
    db.new_collection("users", vec![
        ("name", FieldType::Scalar(aurora_db::types::ScalarType::String), false),
        ("role", FieldType::Scalar(aurora_db::types::ScalarType::String), false),
        ("age", FieldType::Scalar(aurora_db::types::ScalarType::Int), false),
    ]).await.unwrap();

    // Insert test data
    let alice_id = db.insert_into("users", vec![
        ("name", Value::String("Alice".into())),
        ("role", Value::String("admin".into())),
        ("age", Value::Int(30)),
    ]).await.unwrap();

    let _bob_id = db.insert_into("users", vec![
        ("name", Value::String("Bob".into())),
        ("role", Value::String("moderator".into())),
        ("age", Value::Int(25)),
    ]).await.unwrap();

    let charlie_id = db.insert_into("users", vec![
        ("name", Value::String("Charlie".into())),
        ("role", Value::String("user".into())),
        ("age", Value::Int(20)),
    ]).await.unwrap();

    // Test 1: Multi-ID Lookup (IN operator via FilterBuilder)
    let ids = vec![alice_id.clone(), charlie_id.clone()];
    let results = db.query("users")
        .filter(move |f: &aurora_db::query::FilterBuilder| f.in_values("_sid", &ids))
        .collect()
        .await
        .unwrap();
    
    assert_eq!(results.len(), 2);
    let returned_names: Vec<_> = results.iter().filter_map(|d| {
        if let Some(Value::String(s)) = d.data.get("name") { Some(s.clone()) } else { None }
    }).collect();
    assert!(returned_names.contains(&"Alice".to_string()));
    assert!(returned_names.contains(&"Charlie".to_string()));

    // Test 2: Complex Boolean Logic (OR / AND via Filter overloading)
    let results = db.query("users")
        .filter(|f: &aurora_db::query::FilterBuilder| {
            (f.eq("role", "admin") | f.eq("role", "moderator")) & f.gt("age", 20)
        })
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);

    // Test 3: Starts With
    let results = db.query("users")
        .filter(|f: &aurora_db::query::FilterBuilder| f.starts_with("name", "Ch"))
        .collect()
        .await
        .unwrap();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data.get("name").unwrap(), &Value::String("Charlie".into()));
    
    println!("SUCCESS: All rich query operator tests passed!");
}
