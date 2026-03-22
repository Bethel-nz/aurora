
use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::path::PathBuf;

async fn create_test_db(path: PathBuf) -> Aurora {
    let mut config = AuroraConfig::default();
    config.db_path = path;
    config.enable_wal = true;
    
    Aurora::with_config(config).await.unwrap()
}

#[tokio::test]
async fn test_transaction_rollback_works() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_tx.aurora");
    let db = create_test_db(db_path).await;

    db.new_collection("users", vec![
        ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
        ("age", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
    ]).await.unwrap();

    println!("Starting transaction...");
    let tx_id = db.begin_transaction().await;

    println!("Inserting document during transaction scope...");
    let doc_id = aurora_db::transaction::ACTIVE_TRANSACTION_ID.scope(tx_id, async {
        db.insert_into("users", vec![
            ("name", Value::String("Alice".to_string())),
            ("age", Value::Int(30)),
        ]).await.unwrap()
    }).await;

    // Verify document does NOT exist in main storage yet (it's in buffer)
    let doc = db.get_document("users", &doc_id).unwrap();
    assert!(doc.is_none(), "Document should not exist in storage before commit");
    
    println!("Rolling back transaction...");
    db.rollback_transaction(tx_id).await.unwrap();

    // Verify document status AFTER rollback
    let doc = db.get_document("users", &doc_id).unwrap();
    
    if doc.is_some() {
        println!("BUG STILL PRESENT: Document still exists after rollback!");
    } else {
        println!("SUCCESS: Document rolled back correctly.");
    }
    
    assert!(doc.is_none(), "Document should have been removed (or never committed) after rollback");
}

#[tokio::test]
async fn test_aql_transaction_rollback_works() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_aql_tx.aurora");
    let db = create_test_db(db_path).await;

    db.new_collection("accounts", vec![
        ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: true, nullable: true, validations: vec![] }),
        ("balance", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_INT, unique: false, indexed: false, nullable: true, validations: vec![] }),
    ]).await.unwrap();

    // Initial data
    db.insert_into("accounts", vec![
        ("name", Value::String("Alice".to_string())),
        ("balance", Value::Int(100)),
    ]).await.unwrap();

    println!("Executing AQL transaction...");
    
    let tx_id = db.aql_begin_transaction().await.unwrap();
    
    let _ = aurora_db::transaction::ACTIVE_TRANSACTION_ID.scope(tx_id, async {
        db.execute(r#"mutation { update(collection: "accounts", where: { name: { eq: "Alice" } }, data: { balance: 50 }) { id } }"#).await.unwrap();
    }).await;
    
    println!("Rolling back AQL transaction...");
    db.aql_rollback_transaction(tx_id).await.unwrap();
    
    // Alice's balance should be 100 after rollback
    let alice = db.query("accounts").filter(|f: &aurora_db::query::FilterBuilder| f.eq("name", "Alice")).collect().await.unwrap();
    assert_eq!(alice[0].data.get("balance").unwrap(), &Value::Int(100), "Balance should be 100 after rollback");
    println!("SUCCESS: AQL Transaction rolled back correctly.");
}

#[tokio::test]
async fn test_transaction_commit_works() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_commit.aurora");
    let db = create_test_db(db_path).await;

    db.new_collection("users", vec![
        ("name", aurora_db::types::FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: true, validations: vec![] }),
    ]).await.unwrap();

    let tx_id = db.begin_transaction().await;

    let doc_id = aurora_db::transaction::ACTIVE_TRANSACTION_ID.scope(tx_id, async {
        db.insert_into("users", vec![
            ("name", Value::String("Bob".to_string())),
        ]).await.unwrap()
    }).await;

    // Verify document does NOT exist in main storage yet
    let doc = db.get_document("users", &doc_id).unwrap();
    assert!(doc.is_none());

    // Commit
    db.commit_transaction(tx_id).await.unwrap();

    // Verify document EXISTS after commit
    let doc = db.get_document("users", &doc_id).unwrap();
    assert!(doc.is_some());
    assert_eq!(doc.unwrap().data.get("name").unwrap(), &Value::String("Bob".to_string()));
    println!("SUCCESS: Transaction commit works.");
}
