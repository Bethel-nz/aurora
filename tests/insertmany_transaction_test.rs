use aurora_db::Aurora;
use aurora_db::types::FieldType;
use tempfile::tempdir;

#[tokio::test]
async fn test_insert_many_in_transaction() {
    let dir = tempdir().unwrap();
    let db = Aurora::open(dir.path().join("test.aurora").to_str().unwrap())
        .await
        .unwrap();

    db.new_collection(
        "items",
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
                "value",
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

    let result = db
        .execute(
            r#"
            mutation {
                transaction {
                    insertMany(collection: "items", data: [
                        { name: "alpha", value: 1 },
                        { name: "beta",  value: 2 },
                        { name: "gamma", value: 3 }
                    ])
                }
            }
        "#,
        )
        .await
        .unwrap();

    if let aurora_db::parser::executor::ExecutionResult::Mutation(res) = result {
        assert_eq!(res.operation, "transaction");
        assert_eq!(res.affected_count, 3);
    } else {
        panic!("expected Mutation result");
    }

    // let all = db.scan_and_filter("items", |_| true, None).unwrap();
    // assert_eq!(all.len(), 3);
}
