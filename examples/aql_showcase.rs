use aurora_db::{Aurora, AuroraConfig};
use aurora_db::parser::executor::ExecutionResult;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Aurora AQL Complete Showcase");
    println!("=================================");

    // Initialize with real-time config (showcasing your optimizations)
    let db = Aurora::with_config(AuroraConfig::realtime())?;

    // ==========================================
    // 1. SCHEMA DEFINITION
    // ==========================================
    println!("\n📝 1. Defining Schemas...");

    db.execute(
        r#"
        schema {
            define collection users {
                username: String
                email: String
                age: Int
                active: Boolean
                role: String
                created_at: DateTime
            }
        }
    "#,
    )
    .await?;

    db.execute(
        r#"
        schema {
            define collection posts {
                user_id: String
                title: String
                content: String
                views: Int
                published: Boolean
                tags: [String]
                created_at: DateTime
            }
        }
    "#,
    )
    .await?;

    db.execute(
        r#"
        schema {
            define collection orders {
                user_id: String
                amount: Float
                status: String
                items: Int
                created_at: DateTime
            }
        }
    "#,
    )
    .await?;

    // ==========================================
    // 2. REACTIVE QUERIES (Your New Feature!)
    // ==========================================
    println!("\n📡 2. Setting up Reactive Queries...");

    // Watch for new posts
    let db_clone = db.clone();
    let static_db: &'static Aurora = Box::leak(Box::new(db_clone));
    tokio::spawn(async move {
        let mut post_watcher = match static_db
            .query("posts")
            .filter(|f| f.eq("published", true))
            .watch()
            .await
        {
            Ok(w) => w,
            Err(e) => {
                println!("   ❌ Failed to setup post watcher: {}", e);
                return;
            }
        };

        println!("   👀 Watching for published posts...");
        while let Some(update) = post_watcher.next().await {
            match update {
                aurora_db::reactive::QueryUpdate::Added(doc) => {
                    println!(
                        "   🆕 New published post: {}",
                        doc.data
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Untitled")
                    );
                }
                aurora_db::reactive::QueryUpdate::Modified { old: _, new } => {
                    println!(
                        "   ✏️  Post updated: {}",
                        new.data
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Untitled")
                    );
                }
                aurora_db::reactive::QueryUpdate::Removed(doc) => {
                    println!(
                        "   🗑️  Post removed: {}",
                        doc.data
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Untitled")
                    );
                }
            }
        }
    });

    // Watch for high-value orders
    let db_clone2 = db.clone();
    let static_db2: &'static Aurora = Box::leak(Box::new(db_clone2));
    tokio::spawn(async move {
        let mut order_watcher = match static_db2
            .query("orders")
            .filter(|f| f.gt("amount", 100.0))
            .watch()
            .await
        {
            Ok(w) => w,
            Err(e) => {
                println!("   ❌ Failed to setup order watcher: {}", e);
                return;
            }
        };

        println!("   💰 Watching for high-value orders...");
        while let Some(update) = order_watcher.next().await {
            if let aurora_db::reactive::QueryUpdate::Added(order) = update {
                println!(
                    "   💸 High-value order: ${}",
                    order
                        .data
                        .get("amount")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0)
                );
            }
        }
    });

    sleep(Duration::from_millis(100)).await;

    // ==========================================
    // 3. DATA SEEDING WITH BULK OPERATIONS
    // ==========================================
    println!("\n💾 3. Seeding Data...");

    // Insert users
    let users = vec![
        ("alice_wonder", "alice@example.com", 28, true, "admin"),
        ("bob_builder", "bob@example.com", 35, true, "user"),
        ("charlie_code", "charlie@example.com", 42, true, "moderator"),
        ("diana_design", "diana@example.com", 31, false, "user"),
        ("eve_engineer", "eve@example.com", 29, true, "admin"),
    ];

    for (username, email, age, active, role) in &users {
        db.execute(format!(
            r#"
            mutation {{
                insertInto(collection: "users", data: {{
                    username: "{}",
                    email: "{}",
                    age: {},
                    active: {},
                    role: "{}",
                    created_at: "{}"
                }}) {{
                    id
                }}
            }}
        "#,
            username,
            email,
            age,
            active,
            role,
            chrono::Utc::now().to_rfc3339()
        ))
        .await?;
    }
    println!("   ✅ Inserted {} users", users.len());

    // Insert posts
    let posts = vec![
        (
            "alice_id",
            "Welcome to Aurora",
            "This database is blazing fast!",
            150,
            true,
            vec!["announcement", "welcome"],
        ),
        (
            "alice_id",
            "Rust Performance",
            "Zero-cost abstractions at work.",
            89,
            true,
            vec!["rust", "performance"],
        ),
        (
            "bob_id",
            "Building Systems",
            "Distributed systems are complex but rewarding.",
            45,
            true,
            vec!["architecture", "distributed"],
        ),
        (
            "charlie_id",
            "Code Reviews",
            "Why code reviews matter for quality.",
            23,
            false,
            vec!["best-practices"],
        ),
        (
            "eve_id",
            "Database Design",
            "Lessons learned from database design.",
            78,
            true,
            vec!["database", "design"],
        ),
    ];

    for (user_id, title, content, views, published, tags) in &posts {
        let tags_str = tags
            .iter()
            .map(|t| format!("\"{}\"", t))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(format!(
            r#"
            mutation {{
                insertInto(collection: "posts", data: {{
                    user_id: "{}",
                    title: "{}",
                    content: "{}",
                    views: {},
                    published: {},
                    tags: [{}],
                    created_at: "{}"
                }}) {{
                    id
                }}
            }}
        "#,
            user_id,
            title,
            content,
            views,
            published,
            tags_str,
            chrono::Utc::now().to_rfc3339()
        ))
        .await?;
    }
    println!("   ✅ Inserted {} posts", posts.len());

    // Insert orders
    let orders = vec![
        ("alice_id", 299.99, "completed", 3),
        ("bob_id", 49.99, "pending", 1),
        ("charlie_id", 149.99, "shipped", 2),
        ("eve_id", 599.99, "processing", 5),
        ("alice_id", 79.99, "completed", 1),
        ("bob_id", 199.99, "cancelled", 2),
        ("charlie_id", 34.99, "completed", 1),
        ("eve_id", 899.99, "pending", 7),
    ];

    for (user_id, amount, status, items) in &orders {
        db.execute(format!(
            r#"
            mutation {{
                insertInto(collection: "orders", data: {{
                    user_id: "{}",
                    amount: {},
                    status: "{}",
                    items: {},
                    created_at: "{}"
                }}) {{
                    id
                }}
            }}
        "#,
            user_id,
            amount,
            status,
            items,
            chrono::Utc::now().to_rfc3339()
        ))
        .await?;
    }
    println!("   ✅ Inserted {} orders", orders.len());

    // ==========================================
    // 4. COMPLEX QUERIES
    // ==========================================
    println!("\n🔍 4. Complex Queries...");

    // Find active admins over 25
    println!("   > Active admins over 25:");
    let admins = db
        .execute(
            r#"
        query {
            users(where: {
                and: [
                    { role: { eq: "admin" } },
                    { active: { eq: true } },
                    { age: { gt: 25 } }
                ]
            }) {
                username
                email
                age
            }
        }
    "#,
        )
        .await?;
    println!("     Result: {:?}", admins);

    // Posts by tag with pagination
    println!("   > Published posts with 'rust' tag:");
    let rust_posts = db
        .execute(
            r#"
        query {
            posts(
                where: {
                    and: [
                        { published: { eq: true } },
                        { tags: { contains: "rust" } }
                    ]
                },
                orderBy: { field: "views", direction: DESC },
                first: 5
            ) {
                title
                views
                tags
            }
        }
    "#,
        )
        .await?;
    println!("     Result: {:?}", rust_posts);

    // ==========================================
    // 5. AGGREGATION QUERIES (Your Fix!)
    // ==========================================
    println!("\n📊 5. Aggregation Queries...");

    // Count users by role
    println!("   > User count by role:");
    let user_stats = db
        .execute(
            r#"
        query {
            userStats: aggregate {
                count
            }
            usersByRole: aggregate {
                groupBy: ["role"]
                count
            }
        }
    "#,
        )
        .await?;
    println!("     Result: {:?}", user_stats);

    // Order statistics
    println!("   > Order statistics:");
    let order_stats = db
        .execute(
            r#"
        query {
            totalRevenue: aggregate {
                sum(field: "amount")
            }
            avgOrderValue: aggregate {
                avg(field: "amount")
            }
            orderCount: aggregate {
                count
            }
            maxOrder: aggregate {
                max(field: "amount")
            }
        }
    "#,
        )
        .await?;
    println!("     Result: {:?}", order_stats);

    // ==========================================
    // 6. WORKERS/JOBS (Your New Feature!)
    // ==========================================
    println!("\n⚙️  6. Background Jobs...");

    // Email notification job
    let job_result = db
        .execute(
            r#"
        mutation {
            enqueueJob(type: "email_notification", payload: {
                to: "admin@example.com",
                subject: "Daily Report",
                body: "System is running smoothly"
            }, priority: HIGH) {
                jobId
                status
            }
        }
    "#,
        )
        .await?;
    println!("   📧 Email job enqueued: {:?}", job_result);

    // Bulk data processing job
    let bulk_job = db
        .execute(
            r#"
        mutation {
            enqueueJob(type: "bulk_export", payload: {
                collection: "posts",
                format: "json",
                filter: { published: { eq: true } }
            }, priority: MEDIUM) {
                jobId
                status
            }
        }
    "#,
        )
        .await?;
    println!("   📦 Bulk export job enqueued: {:?}", bulk_job);

    // ==========================================
    // 7. REAL-TIME DEMONSTRATION
    // ==========================================
    println!("\n🔴 7. Real-time Updates Demo...");

    // Trigger some updates to show reactive queries in action
    println!("   Adding a new post...");
    db.execute(
        r#"
        mutation {
            insertInto(collection: "posts", data: {
                user_id: "eve_id",
                title: "Real-time Aurora",
                content: "This post should trigger watchers!",
                views: 5,
                published: true,
                tags: ["realtime", "demo"],
                created_at: "2024-01-15T10:00:00Z"
            }) {
                id
            }
        }
    "#,
    )
    .await?;

    println!("   Adding a high-value order...");
    db.execute(
        r#"
        mutation {
            insertInto(collection: "orders", data: {
                user_id: "alice_id",
                amount: 999.99,
                status: "pending",
                items: 10,
                created_at: "2024-01-15T10:01:00Z"
            }) {
                id
            }
        }
    "#,
    )
    .await?;

    // ==========================================
    // 8. UPDATES AND DELETES
    // ==========================================
    println!("\n✏️  8. Updates & Deletes...");

    // Update post views
    let update_result = db
        .execute(
            r#"
        mutation {
            update(
                collection: "posts",
                data: { views: 200 },
                where: { title: { eq: "Welcome to Aurora" } }
            ) {
                id
                views
            }
        }
    "#,
        )
        .await?;
    println!("   ✅ Updated post views: {:?}", update_result);

    // Delete cancelled orders
    let delete_result = db
        .execute(
            r#"
        mutation {
            deleteFrom(
                collection: "orders",
                where: { status: { eq: "cancelled" } }
            ) {
                id
                status
            }
        }
    "#,
        )
        .await?;
    println!("   🗑️  Deleted cancelled orders: {:?}", delete_result);

    // ==========================================
    // 9. SUBSCRIPTION DEMO (AQL Style)
    // ==========================================
    println!("\n📡 9. AQL Subscriptions...");

    let subscription_result = db
        .execute(
            r#"
        subscription {
            activeUsers: users(where: { active: { eq: true } }) {
                username
                role
            }
            pendingOrders: orders(where: { status: { eq: "pending" } }) {
                user_id
                amount
            }
        }
    "#,
        )
        .await?;

    if let ExecutionResult::Subscription(sub) = subscription_result {
        if let Some(mut stream) = sub.stream {
            println!("   👂 Listening for subscription events...");

            // Listen for a few events
            for i in 0..3 {
                match tokio::time::timeout(Duration::from_secs(2), stream.recv()).await {
                    Ok(Ok(event)) => {
                        println!("   🔔 Event {}: {:?}", i + 1, event);
                    }
                    Ok(Err(_)) => break,
                    Err(_) => {
                        println!("   ⏰ No more events within timeout");
                        break;
                    }
                }
            }
        }
    }

    // Wait for background tasks
    sleep(Duration::from_secs(2)).await;

    println!("\n✅ Aurora AQL Showcase Complete!");
    println!("=================================");
    println!("🚀 Your optimized storage + reactive features = Amazing performance!");

    Ok(())
}
