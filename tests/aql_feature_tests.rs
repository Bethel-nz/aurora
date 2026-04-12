use aurora_db::{Aurora, AuroraConfig, doc, object};

async fn get_temp_db() -> Aurora {
    let mut config = AuroraConfig::default();
    let random_id: u64 = rand::random();
    config.db_path = format!("/tmp/aurora_test_{}", random_id).into();
    Aurora::with_config(config).await.unwrap()
}

#[tokio::test]
async fn test_computed_field_execution() {
    let db = get_temp_db().await;

    // Setup schema
    db.execute(
        "schema { define collection users { id: Uuid! @primary first: String last: String } }",
    )
    .await
    .unwrap();

    // Insert data
    db.execute(doc!("mutation($data: [Object]!) { insertMany(collection: \"users\", data: $data) { id } }", {
        "data": [
            object!({ "id": "u1", "first": "John", "last": "Doe" }),
            object!({ "id": "u2", "first": "Jane", "last": "Smith" })
        ]
    })).await.unwrap();

    // Query with computed field (Template String)
    let query = r#"
        query {
            users {
                id
                fullName: "${first} ${last}"
            }
        }
    "#;

    let res = db.execute(query).await.unwrap();
    let result = res.into_query().unwrap();
    assert_eq!(result.documents.len(), 2);

    let doc1 = result
        .documents
        .iter()
        .find(|d| d.data.get("id").and_then(|v| v.as_str()) == Some("u1"))
        .unwrap();
    assert_eq!(
        doc1.data.get("fullName").unwrap().as_str().unwrap(),
        "John Doe"
    );

    let doc2 = result
        .documents
        .iter()
        .find(|d| d.data.get("id").and_then(|v| v.as_str()) == Some("u2"))
        .unwrap();
    assert_eq!(
        doc2.data.get("fullName").unwrap().as_str().unwrap(),
        "Jane Smith"
    );
}

#[tokio::test]
async fn test_group_by_parsing_and_execution() {
    let db = get_temp_db().await;

    db.execute(
        "schema { define collection sales { id: Uuid! @primary category: String amount: Int } }",
    )
    .await
    .unwrap();

    db.execute(doc!("mutation($data: [Object]!) { insertMany(collection: \"sales\", data: $data) { id } }", {
        "data": [
            object!({ "id": "s1", "category": "A", "amount": 10 }),
            object!({ "id": "s2", "category": "A", "amount": 20 }),
            object!({ "id": "s3", "category": "B", "amount": 30 })
        ]
    })).await.unwrap();

    // Test GroupBy
    let query = r#"
        query {
            sales {
                groupBy(field: "category") {
                    key
                    count
                }
            }
        }
    "#;

    let res = db.execute(query).await.unwrap();
    let result = res.into_query().unwrap();
    assert_eq!(result.documents.len(), 2);

    let group_a = result
        .documents
        .iter()
        .find(|d| d.data.get("key").and_then(|v| v.as_str()) == Some("A"))
        .unwrap();
    assert_eq!(group_a.data.get("count").unwrap().as_i64().unwrap(), 2);

    let group_b = result
        .documents
        .iter()
        .find(|d| d.data.get("key").and_then(|v| v.as_str()) == Some("B"))
        .unwrap();
    assert_eq!(group_b.data.get("count").unwrap().as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_deep_lookup_with_computed_fields() {
    let db = get_temp_db().await;

    db.execute(
        "schema {
        define collection orders { id: Uuid! @primary user_id: String total: Int }
        define collection users { id: Uuid! @primary name: String }
    }",
    )
    .await
    .unwrap();

    db.execute(doc!(
        "mutation {
        insertInto(collection: \"users\", data: { id: \"u1\", name: \"Alice\" })
        insertInto(collection: \"orders\", data: { id: \"o1\", user_id: \"u1\", total: 100 })
    }"
    ))
    .await
    .unwrap();

    // Query with nested lookup AND computed field inside lookup
    let query = r#"
        query {
            orders {
                id
                total
                user: lookup(collection: "users", localField: "user_id", foreignField: "id") {
                    id
                    upperName: "ALICE"
                }
            }
        }
    "#;

    let res = db.execute(query).await.unwrap();
    let result = res.into_query().unwrap();
    let order = &result.documents[0];
    println!("{}", order);
    let user_list = order.data.get("user").unwrap().as_array().unwrap();
    let user = user_list[0].as_object().unwrap();
    println!("{:?}", user);
    assert_eq!(user.get("id").unwrap().as_str().unwrap(), "u1");
    assert!(user.contains_key("upperName"));
}

#[tokio::test]
async fn test_upsert_projection() {
    let db = get_temp_db().await;
    db.execute("schema { define collection counters { id: String! @primary val: Int } }")
        .await
        .unwrap();

    // Upsert and request specific fields back
    let (query, options) = doc!(r#"
        mutation($id: String!, $val: Int!) {
            upsert(collection: "counters", where: { id: { eq: $id } }, data: { id: $id, val: $val }) {
                val
            }
        }
    "#, {
        "id": "c1",
        "val": 42
    });

    let res = db.execute((query, options)).await.unwrap();
    let result = res.into_mutation().unwrap();
    let doc = &result.returned_documents[0];

    assert!(doc.data.contains_key("val"));
    assert_eq!(doc.data.get("val").unwrap().as_i64().unwrap(), 42);
    assert!(!doc.data.contains_key("id"));
}

#[derive(serde::Deserialize, Debug, PartialEq)]
struct UserStruct {
    id: String,
    first: String,
    last: String,
}

#[tokio::test]
async fn test_result_binding() {
    let db = get_temp_db().await;

    db.execute(
        "schema { define collection users { id: String! @primary first: String last: String } }",
    )
    .await
    .unwrap();

    db.execute(doc!(
        "mutation {
        insertInto(collection: \"users\", data: { id: \"u1\", first: \"John\", last: \"Doe\" })
    }"
    ))
    .await
    .unwrap();

    let res = db
        .execute("query { users { id first last } }")
        .await
        .unwrap();

    // Bind to Vec<UserStruct>
    let users: Vec<UserStruct> = res.bind().unwrap();

    assert_eq!(users.len(), 1);
    assert_eq!(
        users[0],
        UserStruct {
            id: "u1".to_string(),
            first: "John".to_string(),
            last: "Doe".to_string(),
        }
    );
}

#[tokio::test]
async fn test_bind_first() {
    let db = get_temp_db().await;
    db.execute("schema { define collection users { id: String! @primary name: String } }")
        .await
        .unwrap();
    db.execute(doc!(
        "mutation { insertInto(collection: \"users\", data: { id: \"u1\", name: \"Alice\" }) }"
    ))
    .await
    .unwrap();

    let res = db
        .execute("query { users { id _sid name } }")
        .await
        .unwrap();

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct MiniUser {
        id: String,
        _sid: String,
        name: String,
    }

    let user: MiniUser = res.bind_first().unwrap();
    assert_eq!(user.name, "Alice");
    assert_eq!(user.id, "u1");
    assert_eq!(user._sid, "u1");
}

#[tokio::test]
async fn test_automatic_relation_lookup() {
    let db = get_temp_db().await;

    db.execute("schema {
        define collection users { id: String! @primary name: String }
        define collection posts { id: String! @primary title: String author_id: String @relation(to: \"users\", key: \"id\") }
    }").await.unwrap();

    db.execute(doc!("mutation {
        insertInto(collection: \"users\", data: { id: \"u1\", name: \"Bob\" })
        insertInto(collection: \"posts\", data: { id: \"p1\", title: \"Hello Aurora\", author_id: \"u1\" })
    }")).await.unwrap();

    // Query posts and automatically follow the author_id relation
    let query = r#"
        query {
            posts {
                title
                author_id {
                    name
                }
            }
        }
    "#;

    let res = db.execute(query).await.unwrap();
    let result = res.into_query().unwrap();

    let post = &result.documents[0];
    assert_eq!(
        post.data.get("title").unwrap().as_str().unwrap(),
        "Hello Aurora"
    );

    let author_list = post.data.get("author_id").unwrap().as_array().unwrap();
    let author = author_list[0].as_object().unwrap();
    assert_eq!(author.get("name").unwrap().as_str().unwrap(), "Bob");
}
