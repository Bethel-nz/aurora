//! # AQL Showcase
//!
//! A runnable end-to-end example of every AQL operation Aurora supports:
//!   schema · insert · query (filters, orderBy, pagination) ·
//!   aggregate · groupBy · cursor pagination · update · delete · subscription

use aurora_db::parser::executor::ExecutionResult;
use aurora_db::types::Value;
use aurora_db::{Aurora, AuroraConfig};
use std::time::Duration;
use tokio::time::sleep;

fn val(opt: Option<&Value>) -> String {
    opt.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aurora AQL Showcase ===\n");

    let tmp = tempfile::tempdir()?;
    let db = Aurora::with_config(AuroraConfig {
        db_path: tmp.path().join("aurora_showcase"),
        ..Default::default()
    }).await?;

    // ─────────────────────────────────────────────────────────────────────────
    // 1. SCHEMA
    //    Field modifiers: @unique enforces uniqueness, @indexed builds a
    //    secondary index for fast equality lookups.
    // ─────────────────────────────────────────────────────────────────────────
    println!("1. Defining schemas...");

    db.execute(r#"
        schema {
            define collection users {
                username:   String  @unique
                email:      String  @unique
                age:        Int     @indexed
                active:     Boolean
                role:       String  @indexed
            }
        }
    "#).await?;

    db.execute(r#"
        schema {
            define collection posts {
                user_id:    String  @indexed
                title:      String
                content:    String
                views:      Int     @indexed
                published:  Boolean @indexed
                tags:       [String]
            }
        }
    "#).await?;

    db.execute(r#"
        schema {
            define collection orders {
                user_id:    String  @indexed
                amount:     Float   @indexed
                status:     String  @indexed
                items:      Int
            }
        }
    "#).await?;

    // ─────────────────────────────────────────────────────────────────────────
    // 2. REACTIVE WATCHER
    //    Aurora.clone() is cheap (Arc under the hood). Move the clone into
    //    the spawned task — no unsafe Box::leak required.
    // ─────────────────────────────────────────────────────────────────────────
    println!("2. Setting up reactive watcher...");

    let (watcher_ready_tx, watcher_ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    let watch_db = db.clone();
    tokio::spawn(async move {
        let mut watcher = match watch_db
            .query("posts")
            .filter(|f: &aurora_db::query::FilterBuilder| f.eq("published", true))
            .watch()
            .await
        {
            Ok(w) => w,
            Err(e) => {
                let _ = watcher_ready_tx.send(Err(format!("watcher setup failed: {e}")));
                return;
            }
        };

        let _ = watcher_ready_tx.send(Ok(()));

        while let Some(update) = watcher.next().await {
            match update {
                aurora_db::reactive::QueryUpdate::Added(doc) => {
                    println!("  [watcher] new post: {}", val(doc.data.get("title")));
                }
                aurora_db::reactive::QueryUpdate::Modified { new, .. } => {
                    println!("  [watcher] post updated: {}", val(new.data.get("title")));
                }
                aurora_db::reactive::QueryUpdate::Removed(doc) => {
                    println!("  [watcher] post removed: {}", val(doc.data.get("title")));
                }
            }
        }
    });

    match watcher_ready_rx.await {
        Ok(Ok(())) => {}
        Ok(Err(msg)) => return Err(msg.into()),
        Err(_) => return Err("watcher task exited before signaling readiness".into()),
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. INSERTS
    //    mutation { insertInto(collection: "...", data: {...}) { <fields> } }
    // ─────────────────────────────────────────────────────────────────────────
    println!("3. Inserting data...");

    let users = [
        ("alice_wonder",  "alice@example.com",   28, true,  "admin"),
        ("bob_builder",   "bob@example.com",      35, true,  "user"),
        ("charlie_code",  "charlie@example.com",  42, true,  "moderator"),
        ("diana_design",  "diana@example.com",    31, false, "user"),
        ("eve_engineer",  "eve@example.com",      29, true,  "admin"),
    ];

    for (username, email, age, active, role) in &users {
        db.execute(format!(r#"
            mutation {{
                insertInto(collection: "users", data: {{
                    username: "{username}",
                    email:    "{email}",
                    age:      {age},
                    active:   {active},
                    role:     "{role}"
                }}) {{ id }}
            }}
        "#)).await?;
    }

    let posts = [
        ("alice_wonder", "Welcome to Aurora",    150, true,  r#"["announcement","welcome"]"#),
        ("alice_wonder", "Rust Performance",      89, true,  r#"["rust","performance"]"#),
        ("bob_builder",  "Building Systems",      45, true,  r#"["architecture"]"#),
        ("charlie_code", "Code Reviews",          23, false, r#"["best-practices"]"#),
        ("eve_engineer", "Database Design",       78, true,  r#"["database","design"]"#),
    ];

    for (user_id, title, views, published, tags) in &posts {
        db.execute(format!(r#"
            mutation {{
                insertInto(collection: "posts", data: {{
                    user_id:   "{user_id}",
                    title:     "{title}",
                    content:   "Content for {title}",
                    views:     {views},
                    published: {published},
                    tags:      {tags}
                }}) {{ id }}
            }}
        "#)).await?;
    }

    let orders = [
        ("alice_wonder", 299.99, "completed", 3),
        ("bob_builder",   49.99, "pending",   1),
        ("charlie_code", 149.99, "shipped",   2),
        ("eve_engineer",  599.99, "processing", 5),
        ("alice_wonder",  79.99, "completed",  1),
        ("bob_builder",  199.99, "cancelled",  2),
        ("charlie_code",  34.99, "completed",  1),
        ("eve_engineer",  899.99, "pending",   7),
    ];

    for (user_id, amount, status, items) in &orders {
        db.execute(format!(r#"
            mutation {{
                insertInto(collection: "orders", data: {{
                    user_id: "{user_id}",
                    amount:  {amount},
                    status:  "{status}",
                    items:   {items}
                }}) {{ id }}
            }}
        "#)).await?;
    }

    println!("  Inserted {} users, {} posts, {} orders",
        users.len(), posts.len(), orders.len());

    // ─────────────────────────────────────────────────────────────────────────
    // 4. QUERIES — filters, orderBy, limit/offset
    //    orderBy accepts { fieldName: ASC } or { fieldName: DESC }
    //    where supports: eq, ne, gt, gte, lt, lte, in, contains, startsWith,
    //                    isNull, isNotNull, and: [...], or: [...]
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n4. Queries...");

    // Active admins older than 25, sorted by age descending
    let result = db.execute(r#"
        query {
            users(
                where: {
                    and: [
                        { role:   { eq: "admin" } },
                        { active: { eq: true   } },
                        { age:    { gt: 25     } }
                    ]
                },
                orderBy: { age: DESC }
            ) {
                username
                email
                age
            }
        }
    "#).await?;
    if let ExecutionResult::Query(q) = result {
        println!("  Active admins >25: {} found", q.documents.len());
        for doc in &q.documents {
            println!("    {}", val(doc.data.get("username")));
        }
    }

    // Published posts, top 3 by views
    let result = db.execute(r#"
        query {
            posts(
                where:   { published: { eq: true } },
                orderBy: { views: DESC },
                limit:   3
            ) {
                title
                views
            }
        }
    "#).await?;
    if let ExecutionResult::Query(q) = result {
        println!("  Top 3 published posts by views:");
        for doc in &q.documents {
            println!("    {}  views={}", val(doc.data.get("title")), val(doc.data.get("views")));
        }
    }

    // Offset pagination — page 2 (skip 2, take 2)
    let result = db.execute(r#"
        query {
            users(orderBy: { age: ASC }, limit: 2, offset: 2) {
                username
                age
            }
        }
    "#).await?;
    if let ExecutionResult::Query(q) = result {
        println!("  Users page 2 (offset 2, limit 2):");
        for doc in &q.documents {
            println!("    {}", val(doc.data.get("username")));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. AGGREGATE
    //    Nest `aggregate` inside a collection query.
    //    Supported: count, sum(field:"..."), avg(field:"..."),
    //               min(field:"..."), max(field:"...")
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n5. Aggregations...");

    let result = db.execute(r#"
        query {
            orders {
                aggregate {
                    count
                    sum(field: "amount")
                    avg(field: "amount")
                    max(field: "amount")
                    min(field: "amount")
                }
            }
        }
    "#).await?;
    if let ExecutionResult::Query(q) = result {
        if let Some(doc) = q.documents.first() {
            if let Some(aurora_db::types::Value::Object(agg)) = doc.data.get("aggregate") {
                println!("  Order stats:");
                println!("    count={}", val(agg.get("count")));
                println!("    total={}", val(agg.get("sum")));
                println!("    avg  ={}", val(agg.get("avg")));
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. GROUP BY
    //    groupBy(field: "...") { key count aggregate { ... } nodes { ... } }
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n6. GroupBy...");

    let result = db.execute(r#"
        query {
            users {
                groupBy(field: "role") {
                    key
                    count
                    aggregate {
                        avg(field: "age")
                    }
                    nodes {
                        username
                        age
                    }
                }
            }
        }
    "#).await?;
    if let ExecutionResult::Query(q) = result {
        println!("  Users by role ({} groups):", q.documents.len());
        for doc in &q.documents {
            println!("    role={}  count={}  avg_age={}",
                val(doc.data.get("key")),
                val(doc.data.get("count")),
                val(doc.data.get("aggregate")
                    .and_then(|v| if let Value::Object(m) = v { m.get("avg") } else { None }))
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. CURSOR / CONNECTION PAGINATION
    //    first: N, after: "<document-id>" returns edges + pageInfo.
    //    Useful for infinite-scroll / relay-style pagination.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n7. Cursor pagination...");

    // Grab the ID of the first user to use as a cursor
    let seed = db.execute(r#"
        query { users(limit: 1, orderBy: { age: ASC }) { id age } }
    "#).await?;

    if let ExecutionResult::Query(q) = seed {
        if let Some(cursor_doc) = q.documents.first() {
            let cursor_id = &cursor_doc._sid;
            let aql = format!(r#"
                query {{
                    users(first: 3, after: "{cursor_id}", orderBy: {{ age: ASC }}) {{
                        edges {{
                            node {{ username age }}
                        }}
                        pageInfo {{
                            hasNextPage
                        }}
                    }}
                }}
            "#);
            let result = db.execute(aql).await?;
            if let ExecutionResult::Query(q) = result {
                println!("  Connection query returned {} wrapper doc(s)", q.documents.len());
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 8. UPDATE
    //    update(collection, data, where) patches matching documents in-place.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n8. Updates...");

    let result = db.execute(r#"
        mutation {
            update(
                collection: "posts",
                data:  { views: 999 },
                where: { title: { eq: "Welcome to Aurora" } }
            ) {
                id
                title
                views
            }
        }
    "#).await?;
    if let ExecutionResult::Mutation(m) = result {
        println!("  Updated {} post(s)", m.affected_count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 9. DELETE
    //    deleteFrom(collection, where) removes all matching documents.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n9. Deletes...");

    let result = db.execute(r#"
        mutation {
            deleteFrom(
                collection: "orders",
                where: { status: { eq: "cancelled" } }
            ) { id status }
        }
    "#).await?;
    if let ExecutionResult::Mutation(m) = result {
        println!("  Deleted {} cancelled order(s)", m.affected_count);
    }

    // Deactivate inactive users
    let result = db.execute(r#"
        mutation {
            deleteFrom(
                collection: "users",
                where: { active: { eq: false } }
            ) { id username }
        }
    "#).await?;
    if let ExecutionResult::Mutation(m) = result {
        println!("  Removed {} inactive user(s)", m.affected_count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 10. SUBSCRIPTION
    //     Listens on a single collection. Returns a channel that receives
    //     events as documents are mutated.
    //     Note: only the *first* collection in the block is subscribed.
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n10. Subscription...");

    let sub_result = db.execute(r#"
        subscription {
            orders(where: { status: { eq: "pending" } }) {
                user_id
                amount
                status
            }
        }
    "#).await?;

    if let ExecutionResult::Subscription(sub) = sub_result {
        println!("  Subscription id: {}", sub.subscription_id);

        if let Some(mut stream) = sub.stream {
            // Trigger an event so the stream has something to receive
            db.execute(r#"
                mutation {
                    insertInto(collection: "orders", data: {
                        user_id: "alice_wonder",
                        amount:  49.00,
                        status:  "pending",
                        items:   1
                    }) { id }
                }
            "#).await?;

            match tokio::time::timeout(Duration::from_secs(2), stream.recv()).await {
                Ok(Ok(event)) => println!("  Received event:\n{}", event),
                Ok(Err(_)) => println!("  Subscription channel closed"),
                Err(_) => println!("  No event within timeout (2s)"),
            }
        }
    }

    // Allow reactive watcher to fire
    sleep(Duration::from_millis(200)).await;

    println!("\n=== Showcase complete ===");
    Ok(())
}
