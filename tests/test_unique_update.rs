use aurora_db::Aurora;
use aurora_db::parser::executor::ExecutionResult;
use tempfile::tempdir;

#[tokio::test]
async fn test_unique_update_reinsert() {
    let temp_dir = tempdir().unwrap();
    let db = Aurora::open(temp_dir.path().join("test.db").to_str().unwrap())
        .await
        .unwrap();

    let _ = db
        .execute(
            r#"
        schema {
            define collection User {
                email: String! @unique
            }
        }
    "#,
        )
        .await
        .unwrap();

    let res = db
        .execute(
            r#"
        mutation {
            insertInto(collection: "User", data: { email: "test1@example.com" }) { _sid }
        }
    "#,
        )
        .await
        .unwrap();

    let doc_id = if let ExecutionResult::Mutation(m) = res {
        m.returned_documents[0]
            .data
            .get("_sid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    } else {
        panic!("")
    };

    // Update email to test2
    let update_query = format!(
        r#"
        mutation {{
            update(collection: "User", id: "{}", data: {{ email: "test2@example.com" }})
        }}
    "#,
        doc_id
    );
    let _ = db.execute(update_query).await.unwrap();

    // Now insert test1 again
    let res = db
        .execute(
            r#"
        mutation {
            insertInto(collection: "User", data: { email: "test1@example.com" })
        }
    "#,
        )
        .await;

    assert!(
        res.is_ok(),
        "Should be able to reuse test1@example.com after updating the only document that had it. Error: {:?}",
        res.err()
    );
}
