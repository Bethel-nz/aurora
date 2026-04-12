use aurora_db::Aurora;
use aurora_db::types::{FieldType, Value};
use tempfile::TempDir;

#[tokio::test]
async fn test_platform_style_query_chain() {
    let temp_dir = TempDir::new().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Create collection schema first (required by Aurora's design)
    db.new_collection(
        "users",
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
                "role",
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
                "tier",
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

    // Data Setup: "Users" simulation
    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Alice".into())),
            ("role", Value::String("admin".into())),
            ("tier", Value::String("gold".into())),
        ],
    )
    .await
    .unwrap();

    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Bob".into())),
            ("role", Value::String("user".into())),
            ("tier", Value::String("silver".into())),
        ],
    )
    .await
    .unwrap();

    // Store ID for verification
    let id_charlie = db
        .insert_into(
            "users",
            vec![
                ("name", Value::String("Charlie".into())),
                ("role", Value::String("user".into())),
                ("tier", Value::String("gold".into())),
            ],
        )
        .await
        .unwrap();

    // "Platform" Query: Complex filter logic
    // "Find all Gold Tier users who are not Admins"
    let target_users = db
        .query("users")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("tier", "gold"))
        .filter(|f: &aurora_db::query::FilterBuilder| !f.eq("role", "admin")) // Negation logic
        .collect()
        .await
        .unwrap();

    assert_eq!(target_users.len(), 1);
    assert_eq!(target_users[0]._sid, id_charlie);
}

#[tokio::test]
async fn test_computed_fields_integration() {
    let temp_dir = TempDir::new().unwrap();
    let db = Aurora::open(temp_dir.path().to_str().unwrap())
        .await
        .unwrap();

    // Create collection schema first (required by Aurora's design)
    db.new_collection(
        "cart_items",
        vec![
            (
                "item",
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
                "price",
                aurora_db::types::FieldDefinition {
                    field_type: FieldType::SCALAR_INT,
                    unique: false,
                    indexed: false,
                    nullable: true,
                    validations: vec![],
                    relation: None,
                },
            ),
            (
                "qty",
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

    // Setup: Computed Field "total" = price * qty
    db.register_computed_field(
        "cart_items",
        "total",
        aurora_db::computed::ComputedExpression::Script("doc.price * doc.qty".to_string()),
    )
    .await
    .unwrap();

    // Insert Data
    db.insert_into(
        "cart_items",
        vec![
            ("item", Value::String("Apple".into())),
            ("price", Value::Int(10)),
            ("qty", Value::Int(5)),
        ],
    )
    .await
    .unwrap();

    // Retrieve and verify computed field
    let item = db
        .query("cart_items")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("item", "Apple"))
        .collect()
        .await
        .unwrap();

    assert_eq!(item.len(), 1);
    let total = item[0].data.get("total").unwrap();
    assert_eq!(total, &Value::Int(50));
}
