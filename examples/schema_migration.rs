//! Schema Migration Example
//!
//! Demonstrates how to evolve your Aurora DB schema over time:
//! - Adding new fields
//! - Adding indexes
//! - Backward compatibility with existing data
//!
//! Run with: `cargo run --example schema_migration`

use aurora_db::{Aurora, AuroraConfig, Value};

#[tokio::main]
async fn main() -> aurora_db::Result<()> {
    println!("===================================================================");
    println!("              AURORA DB - SCHEMA MIGRATION EXAMPLE                 ");
    println!("===================================================================\n");

    let temp_dir = tempfile::tempdir()?;
    let config = AuroraConfig {
        db_path: temp_dir.path().join("migration.db"),
        ..Default::default()
    };
    let db = Aurora::with_config(config).await?;

    // =========================================================================
    // VERSION 1: Initial Schema
    // =========================================================================
    println!(" VERSION 1: Initial Schema");
    println!("-----------------------------------------------------------------\n");

    // Create initial users collection with basic fields
    db.execute(
        r#"
        schema {
            define collection users {
                name: String!
                email: String! @indexed
            }
        }
    "#,
    )
    .await?;
    println!("   - Created 'users' collection with: name, email\n");

    // Insert some v1 data
    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Alice".to_string())),
            ("email", Value::String("alice@example.com".to_string())),
        ],
    )
    .await?;

    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Bob".to_string())),
            ("email", Value::String("bob@example.com".to_string())),
        ],
    )
    .await?;

    println!("   - Inserted 2 users with v1 schema\n");

    // =========================================================================
    // VERSION 2: Add new optional fields
    // =========================================================================
    println!(" VERSION 2: Adding New Fields");
    println!("-----------------------------------------------------------------\n");

    // Extend schema with new fields (age, role) via versioned migration.
    // Existing documents will have null for new fields.
    db.execute(
        r#"
        migrate {
            "v1.1.0": {
                alter collection users {
                    add age: Int
                    add role: String
                }
            }
        }
    "#,
    )
    .await?;
    println!("   - Extended 'users' with: age, role (nullable)\n");

    // Insert v2 data with new fields
    db.insert_into(
        "users",
        vec![
            ("name", Value::String("Charlie".to_string())),
            ("email", Value::String("charlie@example.com".to_string())),
            ("age", Value::Int(28)),
            ("role", Value::String("admin".to_string())),
        ],
    )
    .await?;
    println!("   - Inserted v2 user with all fields\n");

    // Query all users - v1 users will have null for age/role
    println!("    All users (v1 + v2 data coexisting):");
    let all_users = db.get_all_collection("users").await?;
    for user in &all_users {
        let name = user.data.get("name").unwrap_or(&Value::Null);
        let age = user.data.get("age").unwrap_or(&Value::Null);
        let role = user.data.get("role").unwrap_or(&Value::Null);
        println!("      - {} | age: {:?} | role: {:?}", name, age, role);
    }
    println!();

    // =========================================================================
    // VERSION 3: Add indexes for performance
    // =========================================================================
    println!(" VERSION 3: Adding Indexes");
    println!("-----------------------------------------------------------------\n");

    // Add index on role for faster filtering
    db.create_index("users", "role").await?;
    db.create_index("users", "age").await?;
    println!("   - Created indexes on: role, age\n");

    // Now queries on role/age will use indexes
    let admins = db
        .query("users")
        .filter(|f: &aurora_db::query::FilterBuilder| f.eq("role", Value::String("admin".to_string())))
        .collect()
        .await?;

    println!("    Indexed query - admins only:");
    for user in &admins {
        println!("      - {}", user.data.get("name").unwrap_or(&Value::Null));
    }
    println!();

    // =========================================================================
    // VERSION 4: Migrate existing data (backfill)
    // =========================================================================
    println!(" VERSION 4: Data Migration (Backfill)");
    println!("-----------------------------------------------------------------\n");

    // Backfill: set default role for users without one via migration.
    // Running this again later is safe — the version is already recorded.
    db.execute(
        r#"
        migrate {
            "v1.2.0": {
                migrate data in users {
                    set role = "member" where { role: { isNull: true } }
                }
            }
        }
    "#,
    )
    .await?;
    println!("   - Backfilled role='member' for users without role\n");

    // Verify backfill
    println!("    After backfill:");
    let updated_users = db.get_all_collection("users").await?;
    for user in &updated_users {
        let name = user.data.get("name").unwrap_or(&Value::Null);
        let role = user.data.get("role").unwrap_or(&Value::Null);
        println!("      - {} | role: {:?}", name, role);
    }
    println!();

    // =========================================================================
    // Summary
    // =========================================================================
    println!("===================================================================");
    println!("                         MIGRATION SUMMARY                          ");
    println!("===================================================================");
    println!();
    println!("   v1 -> v2: Added optional fields (backward compatible)");
    println!("   v2 -> v3: Added indexes (no data change)");
    println!("   v3 -> v4: Backfilled defaults (data migration)");
    println!();
    println!("   Key takeaways:");
    println!("   * New nullable fields are backward compatible");
    println!("   * Existing data coexists with new schema");
    println!("   * Indexes can be added without touching data");
    println!("   * Backfill mutations update old documents");
    println!();

    Ok(())
}
