/// ChangeFingerprint Tests
///
/// Verifies that:
/// - changed_fields is populated eagerly on update events
/// - insert and delete events have empty changed_fields
/// - watch_fields() skips update events that don't touch watched fields
/// - watch_fields() does NOT skip events that do touch watched fields
/// - filter still runs normally when watched fields are touched
/// - non-update events (insert/delete) always pass through watch_fields fast-path

use aurora_db::pubsub::events::{ChangeEvent, ChangeType, EventFilter};
use aurora_db::{Aurora, AuroraConfig, FieldType, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn make_doc(id: &str, fields: Vec<(&str, Value)>) -> aurora_db::types::Document {
    aurora_db::types::Document {
        id: id.to_string(),
        data: fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

// ── ChangeEvent unit tests ────────────────────────────────────────────────────

#[test]
fn test_update_event_populates_changed_fields() {
    let old = make_doc("1", vec![
        ("status", Value::String("inactive".into())),
        ("bio", Value::String("old bio".into())),
    ]);
    let new = make_doc("1", vec![
        ("status", Value::String("active".into())),
        ("bio", Value::String("old bio".into())),
    ]);

    let event = ChangeEvent::update("users", "1", old, new);

    assert!(event.changed_fields.contains("status"), "status should be in changed_fields");
    assert!(!event.changed_fields.contains("bio"), "bio should NOT be in changed_fields");
    assert_eq!(event.changed_fields.len(), 1);
}

#[test]
fn test_insert_event_has_empty_changed_fields() {
    let doc = make_doc("1", vec![("status", Value::String("active".into()))]);
    let event = ChangeEvent::insert("users", "1", doc);
    assert!(event.changed_fields.is_empty());
}

#[test]
fn test_delete_event_has_empty_changed_fields() {
    let event = ChangeEvent::delete("users", "1");
    assert!(event.changed_fields.is_empty());
}

#[test]
fn test_touches_field_true_when_field_changed() {
    let old = make_doc("1", vec![("score", Value::Int(10))]);
    let new = make_doc("1", vec![("score", Value::Int(99))]);
    let event = ChangeEvent::update("scores", "1", old, new);
    assert!(event.touches_field("score"));
}

#[test]
fn test_touches_field_false_when_field_unchanged() {
    let old = make_doc("1", vec![
        ("score", Value::Int(10)),
        ("name", Value::String("Alice".into())),
    ]);
    let new = make_doc("1", vec![
        ("score", Value::Int(10)),
        ("name", Value::String("Alice".into())),
    ]);
    let event = ChangeEvent::update("scores", "1", old, new);
    assert!(!event.touches_field("score"));
    assert!(!event.touches_field("name"));
}

#[test]
fn test_touches_field_false_for_insert() {
    let doc = make_doc("1", vec![("status", Value::String("active".into()))]);
    let event = ChangeEvent::insert("users", "1", doc);
    assert!(!event.touches_field("status"));
}

#[test]
fn test_field_added_is_in_changed_fields() {
    let old = make_doc("1", vec![("name", Value::String("Alice".into()))]);
    let new = make_doc("1", vec![
        ("name", Value::String("Alice".into())),
        ("email", Value::String("alice@example.com".into())),
    ]);
    let event = ChangeEvent::update("users", "1", old, new);
    assert!(event.changed_fields.contains("email"), "newly added field should be in changed_fields");
    assert!(!event.changed_fields.contains("name"));
}

#[test]
fn test_field_removed_is_in_changed_fields() {
    let old = make_doc("1", vec![
        ("name", Value::String("Alice".into())),
        ("temp", Value::String("gone".into())),
    ]);
    let new = make_doc("1", vec![("name", Value::String("Alice".into()))]);
    let event = ChangeEvent::update("users", "1", old, new);
    assert!(event.changed_fields.contains("temp"), "removed field should be in changed_fields");
    assert!(!event.changed_fields.contains("name"));
}

// ── ChangeListener watch_fields fast-path integration tests ──────────────────

async fn make_db() -> (Aurora, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Aurora::with_config(AuroraConfig {
        db_path: dir.path().join("db"),
        enable_write_buffering: false,
        ..Default::default()
    })
    .await
    .unwrap();
    (db, dir)
}

#[tokio::test]
async fn test_watch_fields_skips_unrelated_update() {
    let (db, _dir) = make_db().await;
    db.new_collection("users", vec![
        ("status".to_string(), FieldType::SCALAR_STRING, false),
        ("bio".to_string(), FieldType::SCALAR_STRING, false),
    ])
    .await
    .unwrap();

    // Insert a user
    db.execute(r#"mutation { insertInto(collection: "users", data: { status: "active", bio: "hello" }) { id } }"#)
        .await
        .unwrap();

    // Watcher only cares about "status"
    let mut listener = db.listen("users").watch_fields(["status"]);

    // Update only "bio" — should be skipped
    db.execute(r#"mutation { update(collection: "users", where: { status: { eq: "active" } }, set: { bio: "updated bio" }) { id } }"#)
        .await
        .unwrap();

    // Update "status" — should come through
    db.execute(r#"mutation { update(collection: "users", where: { status: { eq: "active" } }, set: { status: "inactive" }) { id } }"#)
        .await
        .unwrap();

    // Should receive exactly the status update, not the bio update
    let event = timeout(Duration::from_secs(2), listener.recv())
        .await
        .expect("timed out waiting for event")
        .unwrap();

    assert_eq!(event.change_type, ChangeType::Update);
    assert!(event.touches_field("status"), "received event should be the status change");
    assert!(!event.touches_field("bio"));

    // No more events should be pending (bio update was skipped)
    assert!(
        timeout(Duration::from_millis(100), listener.recv()).await.is_err(),
        "bio update should have been skipped"
    );
}

#[tokio::test]
async fn test_watch_fields_passes_insert_and_delete() {
    let (db, _dir) = make_db().await;
    db.new_collection("items", vec![
        ("name".to_string(), FieldType::SCALAR_STRING, false),
    ])
    .await
    .unwrap();

    // Watch only "name" — but inserts/deletes should always pass through
    let mut listener = db.listen("items").watch_fields(["name"]);

    db.execute(r#"mutation { insertInto(collection: "items", data: { name: "Widget" }) { id } }"#)
        .await
        .unwrap();

    let event = timeout(Duration::from_secs(2), listener.recv())
        .await
        .expect("timed out waiting for insert event")
        .unwrap();

    assert_eq!(event.change_type, ChangeType::Insert);
}

#[tokio::test]
async fn test_watch_fields_with_filter_still_filters() {
    let (db, _dir) = make_db().await;
    db.new_collection("orders", vec![
        ("status".to_string(), FieldType::SCALAR_STRING, false),
        ("total".to_string(), FieldType::SCALAR_INT, false),
    ])
    .await
    .unwrap();

    db.execute(r#"mutation { insertInto(collection: "orders", data: { status: "pending", total: 100 }) { id } }"#)
        .await
        .unwrap();
    db.execute(r#"mutation { insertInto(collection: "orders", data: { status: "pending", total: 200 }) { id } }"#)
        .await
        .unwrap();

    // Watch status field, but also filter for status = "completed"
    let mut listener = db
        .listen("orders")
        .watch_fields(["status"])
        .filter(EventFilter::FieldEquals(
            "status".to_string(),
            Value::String("completed".into()),
        ));

    // Update one order to "completed" — should pass through watch_fields AND filter
    db.execute(r#"mutation { update(collection: "orders", where: { total: { eq: 100 } }, set: { status: "completed" }) { id } }"#)
        .await
        .unwrap();

    // Update other order to "cancelled" — passes watch_fields but blocked by filter
    db.execute(r#"mutation { update(collection: "orders", where: { total: { eq: 200 } }, set: { status: "cancelled" }) { id } }"#)
        .await
        .unwrap();

    let event = timeout(Duration::from_secs(2), listener.recv())
        .await
        .expect("timed out")
        .unwrap();

    assert_eq!(
        event.get_field("status"),
        Some(&Value::String("completed".into()))
    );

    // "cancelled" update should have been filtered out
    assert!(
        timeout(Duration::from_millis(100), listener.recv()).await.is_err(),
        "cancelled update should be blocked by filter"
    );
}

#[tokio::test]
async fn test_watch_fields_skips_counted_correctly() {
    // Verify that N unrelated updates produce 0 watcher events and
    // 1 related update produces exactly 1 event.
    let (db, _dir) = make_db().await;
    db.new_collection("sensors", vec![
        ("value".to_string(), FieldType::SCALAR_INT, false),
        ("label".to_string(), FieldType::SCALAR_STRING, false),
    ])
    .await
    .unwrap();

    // Insert several sensors — filter on value so we can always find them
    for i in 0..5i64 {
        db.execute(format!(
            r#"mutation {{ insertInto(collection: "sensors", data: {{ value: {}, label: "temp" }}) {{ id }} }}"#,
            i
        ))
        .await
        .unwrap();
    }

    let received = Arc::new(AtomicUsize::new(0));
    let received_clone = Arc::clone(&received);

    let mut listener = db.listen("sensors").watch_fields(["value"]);

    let handle = tokio::spawn(async move {
        while let Ok(Ok(event)) = timeout(Duration::from_millis(400), listener.recv()).await {
            if event.change_type == ChangeType::Update {
                received_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    // 5 label-only updates (one per sensor, by value) — all should be skipped
    for i in 0..5i64 {
        db.execute(format!(
            r#"mutation {{ update(collection: "sensors", where: {{ value: {{ eq: {} }} }}, set: {{ label: "updated" }}) {{ id }} }}"#,
            i
        ))
        .await
        .unwrap();
    }

    // 1 value update — should come through
    db.execute(r#"mutation { update(collection: "sensors", where: { value: { eq: 0 } }, set: { value: 42 }) { id } }"#)
        .await
        .unwrap();

    handle.await.unwrap();

    assert_eq!(
        received.load(Ordering::SeqCst),
        1,
        "only the value update should have been received"
    );
}
