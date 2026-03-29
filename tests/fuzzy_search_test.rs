use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use aurora_db::types::FieldDefinition;

fn field(ft: FieldType) -> FieldDefinition {
    FieldDefinition { field_type: ft, unique: false, indexed: false, nullable: true, validations: vec![] }
}

async fn open_db(path: std::path::PathBuf) -> Aurora {
    Aurora::with_config(AuroraConfig {
        db_path: path,
        enable_wal: false,
        enable_write_buffering: false,
        ..Default::default()
    })
    .await
    .unwrap()
}

// ── exact fuzzy match ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_fuzzy_returns_close_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path().join("db")).await;

    db.new_collection("products", vec![
        ("name", field(FieldType::SCALAR_STRING)),
    ]).await.unwrap();

    db.insert_into("products", vec![("name", Value::String("wireless headphones".into()))]).await.unwrap();
    db.insert_into("products", vec![("name", Value::String("bluetooth speaker".into()))]).await.unwrap();
    db.insert_into("products", vec![("name", Value::String("laptop stand".into()))]).await.unwrap();

    // "wireles" is 1 edit away from "wireless" — should match
    let results = db.search("products").query("wireles").fuzzy(1).collect().await.unwrap();
    assert!(!results.is_empty(), "expected at least one fuzzy match for 'wireles'");
    assert!(
        results.iter().any(|d| matches!(d.data.get("name"), Some(Value::String(s)) if s.contains("wireless"))),
        "expected 'wireless headphones' in results"
    );
}

// ── zero-score docs excluded ─────────────────────────────────────────────────

#[tokio::test]
async fn test_fuzzy_excludes_zero_score_docs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path().join("db")).await;

    db.new_collection("items", vec![
        ("name", field(FieldType::SCALAR_STRING)),
    ]).await.unwrap();

    db.insert_into("items", vec![("name", Value::String("keyboard".into()))]).await.unwrap();
    db.insert_into("items", vec![("name", Value::String("monitor".into()))]).await.unwrap();
    db.insert_into("items", vec![("name", Value::String("mouse pad".into()))]).await.unwrap();

    // "keybord" is 1 edit from "keyboard" — only that doc should come back
    let results = db.search("items").query("keybord").fuzzy(1).collect().await.unwrap();
    assert_eq!(results.len(), 1, "only the close match should be returned, got: {:?}", results.iter().map(|d| &d.data).collect::<Vec<_>>());
    assert!(matches!(results[0].data.get("name"), Some(Value::String(s)) if s == "keyboard"));
}

// ── sorted by relevance ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_fuzzy_results_sorted_by_score() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path().join("db")).await;

    db.new_collection("docs", vec![
        ("text", field(FieldType::SCALAR_STRING)),
    ]).await.unwrap();

    // Exact match should score higher than a 1-edit match
    db.insert_into("docs", vec![("text", Value::String("rust programming".into()))]).await.unwrap();
    db.insert_into("docs", vec![("text", Value::String("rast programming".into()))]).await.unwrap(); // 1 edit from "rust"

    let results = db.search("docs").query("rust").fuzzy(1).collect().await.unwrap();
    assert_eq!(results.len(), 2);
    // First result must be the exact match
    assert!(matches!(results[0].data.get("text"), Some(Value::String(s)) if s.contains("rust")));
}

// ── no match beyond distance ─────────────────────────────────────────────────

#[tokio::test]
async fn test_fuzzy_respects_distance_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path().join("db")).await;

    db.new_collection("words", vec![
        ("word", field(FieldType::SCALAR_STRING)),
    ]).await.unwrap();

    db.insert_into("words", vec![("word", Value::String("elephant".into()))]).await.unwrap();

    // "lphant" is 2 edits from "elephant" — should not match with distance=1
    let results = db.search("words").query("lphant").fuzzy(1).collect().await.unwrap();
    assert!(results.is_empty(), "should not return doc 2 edits away when distance=1");

    // But should match with distance=2
    let results = db.search("words").query("lphant").fuzzy(2).collect().await.unwrap();
    assert_eq!(results.len(), 1);
}
