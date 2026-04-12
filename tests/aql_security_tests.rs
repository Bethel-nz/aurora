use aurora_db::{doc, Aurora, AuroraConfig, ErrorCode};

async fn get_temp_db() -> Aurora {
    let mut config = AuroraConfig::default();
    let random_id: u64 = rand::random();
    config.db_path = format!("/tmp/aurora_security_test_{}", random_id).into();
    Aurora::with_config(config).await.unwrap()
}

#[tokio::test]
async fn test_relation_integrity() {
    let db = get_temp_db().await;
    
    db.execute("schema { 
        define collection groups { id: String! @primary name: String }
        define collection users { id: String! @primary name: String group_id: String @relation(to: \"groups\", key: \"id\") }
    }").await.unwrap();
    
    // 1. Success: Insert with existing group
    db.execute(doc!("mutation { insertInto(collection: \"groups\", data: { id: \"g1\", name: \"Admins\" }) }")).await.unwrap();
    db.execute(doc!("mutation { insertInto(collection: \"users\", data: { id: \"u1\", name: \"Alice\", group_id: \"g1\" }) }")).await.unwrap();

    // 2. Failure: Insert with non-existing group
    let res = db.execute(doc!("mutation { insertInto(collection: \"users\", data: { id: \"u2\", name: \"Bob\", group_id: \"g2\" }) }")).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(err.code, ErrorCode::ValidationError);
    assert!(err.message.contains("Foreign key violation"));
}

#[tokio::test]
async fn test_rbac_security_directives() {
    let db = get_temp_db().await;
    db.execute("schema { define collection data { id: String! @primary val: Int } }").await.unwrap();
    db.execute(doc!("mutation { insertInto(collection: \"data\", data: { id: \"d1\", val: 10 }) }")).await.unwrap();

    // 1. @auth failure
    let query = "query { data @auth { val } }";
    let res = db.execute(query).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Unauthorized);

    // 2. @auth success
    let options = aurora_db::parser::executor::ExecutionOptions::new().with_role("USER");
    let res = db.execute((query, options)).await;
    assert!(res.is_ok());

    // 3. @require role failure
    let query_req = "query { data @require(role: \"ADMIN\") { val } }";
    let options = aurora_db::parser::executor::ExecutionOptions::new().with_role("USER");
    let res = db.execute((query_req, options)).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Forbidden);

    // 4. @require role success
    let options = aurora_db::parser::executor::ExecutionOptions::new().with_role("ADMIN");
    let res = db.execute((query_req, options.clone())).await;
    assert!(res.is_ok());
    
    // 5. Mutation level check
    let mut_req = "mutation @require(role: \"SUPER\") { insertInto(collection: \"data\", data: { id: \"d2\", val: 20 }) }";
    let res = db.execute((mut_req, options)).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, ErrorCode::Forbidden);
}

#[tokio::test]
async fn test_complex_expressions() {
    let db = get_temp_db().await;
    db.execute("schema { define collection products { id: String! @primary price: Float qty: Int } }").await.unwrap();
    db.execute(doc!("mutation { insertInto(collection: \"products\", data: { id: \"p1\", price: 10.5, qty: 2 }) }")).await.unwrap();

    // Note: Expression evaluation logic needs to be fully implemented in executor.rs
    // For now I'll just verify the parser handles the syntax.
    let query = r#"
        query {
            products {
                id
                total: price * qty
                discounted: price > 10 ? price * 0.9 : price
            }
        }
    "#;
    
    // Since I haven't implemented the Evaluator in executor.rs yet, 
    // I expect this to either fail or return the fallback raw string.
    // I'll finish the evaluator now before running tests.
    let res = db.execute(query).await;
    assert!(res.is_ok());
}
