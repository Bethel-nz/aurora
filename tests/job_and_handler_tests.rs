use aurora_db::parser::executor::ExecutionResult;
use aurora_db::types::{FieldDefinition, FieldType, Value};
use aurora_db::workers::Job;
/// Tests for AQL enqueueJob mutations and define handler event-driven actions.
///
/// Covers:
/// - enqueueJob via AQL: returns pending job metadata
/// - Worker handler picks up and processes job
/// - define handler: fires on insert / update / delete
/// - Handler variables (_id, _fieldname) accessible in action
/// - insertMany, upsert, transaction mutations (implemented alongside enqueueJob)
use aurora_db::{Aurora, AuroraConfig};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn setup_db() -> (Aurora, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Aurora::with_config(AuroraConfig {
        db_path: dir.path().join("test.aurora"),
        enable_write_buffering: false,
        enable_wal: false,
        workers_enabled: true,
        ..Default::default()
    })
    .await
    .unwrap();
    (db, dir)
}

fn fd(field_type: FieldType) -> FieldDefinition {
    FieldDefinition {
        field_type,
        nullable: true,
        unique: false,
        indexed: false,
        ..Default::default()
    }
}

// Poll until `check` returns true or timeout expires.
async fn wait_until<F>(check: F, timeout_ms: u64) -> bool
where
    F: Fn() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if check() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(20)).await;
    }
}

// ── enqueueJob ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_enqueue_job_via_aql_returns_metadata() {
    let (db, _dir) = setup_db().await;

    let result = db
        .execute(r#"
            mutation {
                enqueueJob(jobType: "send_email", payload: { to: "alice@example.com", subject: "Hello" }, priority: HIGH, maxRetries: 3)
            }
        "#)
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.operation, "enqueueJob");
        assert_eq!(res.affected_count, 1);
        let doc = &res.returned_documents[0];
        assert_eq!(
            doc.data.get("job_type"),
            Some(&Value::String("send_email".into()))
        );
        assert_eq!(
            doc.data.get("status"),
            Some(&Value::String("pending".into()))
        );
        assert!(doc.data.contains_key("job_id"), "should return job_id");
    } else {
        panic!("Expected Mutation result");
    }
}

#[tokio::test]
async fn test_enqueue_job_handler_executes() {
    let (db, _dir) = setup_db().await;

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    // Register a handler that increments the counter
    db.workers
        .as_ref()
        .unwrap()
        .register_handler("count_job", move |_job: Job| {
            let c = Arc::clone(&counter_clone);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;

    db.workers.as_ref().unwrap().start().await.unwrap();

    // Enqueue via AQL
    db.execute(
        r#"
        mutation {
            enqueueJob(jobType: "count_job", payload: { msg: "test" }, priority: NORMAL)
        }
    "#,
    )
    .await
    .unwrap();

    // Wait for handler to run
    let processed = wait_until(|| counter.load(Ordering::SeqCst) == 1, 2000).await;
    assert!(processed, "handler should have processed the job within 2s");

    db.workers.as_ref().unwrap().stop().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_job_payload_available_in_handler() {
    let (db, _dir) = setup_db().await;

    let received_to: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let received_clone = Arc::clone(&received_to);

    db.workers
        .as_ref()
        .unwrap()
        .register_handler("send_email", move |job: Job| {
            let slot = Arc::clone(&received_clone);
            async move {
                let to = job
                    .payload
                    .get("to")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                *slot.lock().unwrap() = Some(to);
                Ok(())
            }
        })
        .await;

    db.workers.as_ref().unwrap().start().await.unwrap();

    db.execute(r#"
        mutation {
            enqueueJob(jobType: "send_email", payload: { to: "bob@example.com", subject: "Welcome" }, priority: HIGH)
        }
    "#)
    .await
    .unwrap();

    let received = wait_until(|| received_to.lock().unwrap().is_some(), 2000).await;
    assert!(received, "handler should have run");
    assert_eq!(
        received_to.lock().unwrap().as_deref(),
        Some("bob@example.com")
    );

    db.workers.as_ref().unwrap().stop().await.unwrap();
}

#[tokio::test]
async fn test_enqueue_job_priority_low() {
    let (db, _dir) = setup_db().await;

    let counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&counter);

    db.workers
        .as_ref()
        .unwrap()
        .register_handler("low_job", move |_job: Job| {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;

    db.workers.as_ref().unwrap().start().await.unwrap();

    db.execute(
        r#"
        mutation {
            enqueueJob(jobType: "low_job", payload: { x: 1 }, priority: LOW)
        }
    "#,
    )
    .await
    .unwrap();

    let done = wait_until(|| counter.load(Ordering::SeqCst) == 1, 3000).await;
    assert!(done, "LOW priority job should still be processed");

    db.workers.as_ref().unwrap().stop().await.unwrap();
}

// ── define handler ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_define_handler_on_insert_fires() {
    let (db, _dir) = setup_db().await;

    // Collection we'll insert into
    db.new_collection(
        "users",
        vec![
            ("name", fd(FieldType::SCALAR_STRING)),
            ("email", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    // Collection the handler writes into (audit log)
    db.new_collection(
        "audit_log",
        vec![
            ("action", fd(FieldType::SCALAR_STRING)),
            ("user_id", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    // Register handler: on every user insert, write to audit_log
    // NOTE: comma between `on:` and `action:` is required by the grammar
    let res = db
        .execute(r#"
            define handler "user_audit" {
                on: "insert users",
                action: {
                    insertInto(collection: "audit_log", data: { action: "user_created", user_id: $_id })
                }
            }
        "#)
        .await
        .unwrap();

    // Registration should succeed
    if let ExecutionResult::Query(r) = res {
        assert_eq!(
            r.documents[0].data.get("status"),
            Some(&Value::String("registered".into()))
        );
    } else {
        panic!("expected Query result from define handler");
    }

    // Now insert a user — this should fire the handler
    let insert_res = db
        .execute(
            r#"
            mutation {
                insertInto(collection: "users", data: { name: "Alice", email: "alice@example.com" })
            }
        "#,
        )
        .await
        .unwrap();

    let user_id = if let ExecutionResult::Mutation(m) = insert_res {
        m.returned_documents[0]._sid.clone()
    } else {
        panic!("expected mutation result");
    };

    // Wait for the tokio-spawned handler task to write the audit row
    let db_ref = &db;
    let uid = user_id.clone();
    let audit_written = wait_until(
        || {
            db_ref
                .scan_and_filter(
                    "audit_log",
                    |doc| doc.data.get("user_id") == Some(&Value::String(uid.clone())),
                    None,
                )
                .map(|v| !v.is_empty())
                .unwrap_or(false)
        },
        2000,
    )
    .await;

    assert!(
        audit_written,
        "audit_log should have a row for the new user"
    );

    let audit_docs = db.scan_and_filter("audit_log", |_| true, None).unwrap();
    assert_eq!(audit_docs.len(), 1);
    assert_eq!(
        audit_docs[0].data.get("action"),
        Some(&Value::String("user_created".into()))
    );
    assert_eq!(
        audit_docs[0].data.get("user_id"),
        Some(&Value::String(user_id))
    );
}

#[tokio::test]
async fn test_define_handler_on_update_fires() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "products",
        vec![
            ("name", fd(FieldType::SCALAR_STRING)),
            ("price", fd(FieldType::SCALAR_INT)),
        ],
    )
    .await
    .unwrap();

    db.new_collection(
        "price_history",
        vec![
            ("product_id", fd(FieldType::SCALAR_STRING)),
            ("event", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    // Register handler on update
    db.execute(r#"
        define handler "price_change_log" {
            on: "update products",
            action: {
                insertInto(collection: "price_history", data: { product_id: $_id, event: "price_updated" })
            }
        }
    "#)
    .await
    .unwrap();

    // Insert a product first
    let insert_res = db
        .execute(
            r#"
            mutation {
                insertInto(collection: "products", data: { name: "Laptop", price: 999 })
            }
        "#,
        )
        .await
        .unwrap();

    let product_id = if let ExecutionResult::Mutation(m) = insert_res {
        m.returned_documents[0]._sid.clone()
    } else {
        panic!();
    };

    // Update it by name — this should fire the handler
    db.execute(
        r#"
        mutation {
            update(collection: "products", where: { name: { eq: "Laptop" } }, set: { price: 899 })
        }
    "#,
    )
    .await
    .unwrap();

    let pid = product_id.clone();
    let db_ref = &db;
    let logged = wait_until(
        || {
            db_ref
                .scan_and_filter(
                    "price_history",
                    |doc| doc.data.get("product_id") == Some(&Value::String(pid.clone())),
                    None,
                )
                .map(|v| !v.is_empty())
                .unwrap_or(false)
        },
        2000,
    )
    .await;

    assert!(logged, "price_history should have logged the update");
}

#[tokio::test]
async fn test_define_handler_on_delete_fires() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "sessions",
        vec![
            ("user_id", fd(FieldType::SCALAR_STRING)),
            ("token", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    db.new_collection(
        "revoked_tokens",
        vec![("session_id", fd(FieldType::SCALAR_STRING))],
    )
    .await
    .unwrap();

    // On session delete → record in revoked_tokens
    db.execute(
        r#"
        define handler "revoke_on_delete" {
            on: "delete sessions",
            action: {
                insertInto(collection: "revoked_tokens", data: { session_id: $_id })
            }
        }
    "#,
    )
    .await
    .unwrap();

    // Insert a session
    let insert_res = db
        .execute(
            r#"
            mutation {
                insertInto(collection: "sessions", data: { user_id: "u1", token: "tok-abc" })
            }
        "#,
        )
        .await
        .unwrap();

    let session_id = if let ExecutionResult::Mutation(m) = insert_res {
        m.returned_documents[0]._sid.clone()
    } else {
        panic!();
    };

    // Delete the session by token (filter by data field, not id)
    db.execute(
        r#"
        mutation {
            deleteFrom(collection: "sessions", where: { token: { eq: "tok-abc" } })
        }
    "#,
    )
    .await
    .unwrap();

    let sid = session_id.clone();
    let db_ref = &db;
    let revoked = wait_until(
        || {
            db_ref
                .scan_and_filter(
                    "revoked_tokens",
                    |doc| doc.data.get("session_id") == Some(&Value::String(sid.clone())),
                    None,
                )
                .map(|v| !v.is_empty())
                .unwrap_or(false)
        },
        2000,
    )
    .await;

    assert!(
        revoked,
        "revoked_tokens should contain the deleted session id"
    );
}

#[tokio::test]
async fn test_define_handler_field_variables_propagated() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "orders",
        vec![
            ("amount", fd(FieldType::SCALAR_INT)),
            ("customer_id", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    db.new_collection(
        "notifications",
        vec![
            ("customer_id", fd(FieldType::SCALAR_STRING)),
            ("message", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    // Handler uses $_customer_id variable from the inserted doc
    db.execute(r#"
        define handler "order_notify" {
            on: "insert orders",
            action: {
                insertInto(collection: "notifications", data: { customer_id: $_customer_id, message: "order_placed" })
            }
        }
    "#)
    .await
    .unwrap();

    db.execute(
        r#"
        mutation {
            insertInto(collection: "orders", data: { amount: 250, customer_id: "cust-99" })
        }
    "#,
    )
    .await
    .unwrap();

    let db_ref = &db;
    let notified = wait_until(
        || {
            db_ref
                .scan_and_filter(
                    "notifications",
                    |doc| doc.data.get("customer_id") == Some(&Value::String("cust-99".into())),
                    None,
                )
                .map(|v| !v.is_empty())
                .unwrap_or(false)
        },
        2000,
    )
    .await;

    assert!(
        notified,
        "notification should contain customer_id from the inserted order"
    );

    let notifs = db.scan_and_filter("notifications", |_| true, None).unwrap();
    assert_eq!(
        notifs[0].data.get("message"),
        Some(&Value::String("order_placed".into()))
    );
}

// ── insertMany / upsert / transaction ────────────────────────────────────────

#[tokio::test]
async fn test_insert_many_mutation() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "items",
        vec![
            ("name", fd(FieldType::SCALAR_STRING)),
            ("value", fd(FieldType::SCALAR_INT)),
        ],
    )
    .await
    .unwrap();

    let result = db
        .execute(
            r#"
            mutation {
                insertMany(collection: "items", data: [
                    { name: "alpha", value: 1 },
                    { name: "beta",  value: 2 },
                    { name: "gamma", value: 3 }
                ])
            }
        "#,
        )
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.operation, "insertMany");
        assert_eq!(res.affected_count, 3);
        assert_eq!(res.returned_documents.len(), 3);
    } else {
        panic!("expected Mutation result");
    }

    let all = db.scan_and_filter("items", |_| true, None).unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn test_upsert_inserts_when_not_found() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "configs",
        vec![
            ("key", fd(FieldType::SCALAR_STRING)),
            ("value", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    let result = db
        .execute(r#"
            mutation {
                upsert(collection: "configs", where: { key: { eq: "theme" } }, data: { key: "theme", value: "dark" })
            }
        "#)
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.operation, "upsert");
        assert_eq!(res.affected_count, 1);
    } else {
        panic!();
    }

    let docs = db.scan_and_filter("configs", |_| true, None).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0].data.get("value"),
        Some(&Value::String("dark".into()))
    );
}

#[tokio::test]
async fn test_upsert_updates_when_found() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "configs",
        vec![
            ("key", fd(FieldType::SCALAR_STRING)),
            ("value", fd(FieldType::SCALAR_STRING)),
        ],
    )
    .await
    .unwrap();

    // Pre-insert using correct AQL syntax
    db.execute(
        r#"
        mutation {
            insertInto(collection: "configs", data: { key: "theme", value: "light" })
        }
    "#,
    )
    .await
    .unwrap();

    // Upsert should update, not create a new doc
    db.execute(r#"
        mutation {
            upsert(collection: "configs", where: { key: { eq: "theme" } }, data: { key: "theme", value: "dark" })
        }
    "#)
    .await
    .unwrap();

    let docs = db.scan_and_filter("configs", |_| true, None).unwrap();
    assert_eq!(docs.len(), 1, "upsert should not create a duplicate");
    assert_eq!(
        docs[0].data.get("value"),
        Some(&Value::String("dark".into()))
    );
}

#[tokio::test]
async fn test_transaction_executes_all_ops() {
    let (db, _dir) = setup_db().await;

    db.new_collection(
        "accounts",
        vec![
            ("owner", fd(FieldType::SCALAR_STRING)),
            ("balance", fd(FieldType::SCALAR_INT)),
        ],
    )
    .await
    .unwrap();
    db.new_collection("tx_log", vec![("note", fd(FieldType::SCALAR_STRING))])
        .await
        .unwrap();

    let result = db
        .execute(
            r#"
            mutation {
                transaction {
                    insertInto(collection: "accounts", data: { owner: "Alice", balance: 1000 })
                    insertInto(collection: "accounts", data: { owner: "Bob",   balance: 500  })
                    insertInto(collection: "tx_log",   data: { note: "initial_funding"       })
                }
            }
        "#,
        )
        .await
        .unwrap();

    if let ExecutionResult::Mutation(res) = result {
        assert_eq!(res.operation, "transaction");
        assert_eq!(res.affected_count, 3);
    } else {
        panic!("expected Mutation");
    }

    let accounts = db.scan_and_filter("accounts", |_| true, None).unwrap();
    assert_eq!(accounts.len(), 2);

    let logs = db.scan_and_filter("tx_log", |_| true, None).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(
        logs[0].data.get("note"),
        Some(&Value::String("initial_funding".into()))
    );
}
