use aurora_db::{Aurora, FieldType, Value};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Indexing Performance Test ===\n");

    // Test 1: Collection with ONLY unique field indexed
    {
        let db = Aurora::open("indexing_test1.db").await?;
        let _ = db.delete_collection("users");

        db.new_collection(
            "users",
            vec![
                (
                    "id",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: true,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // indexed
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
                ), // NOT indexed
                (
                    "email",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // NOT indexed
                (
                    "age",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_INT,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // NOT indexed
                (
                    "address",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // NOT indexed
            ],
        )
        .await?;

        let start = Instant::now();
        for i in 0..1000 {
            db.insert_into(
                "users",
                vec![
                    ("id", Value::String(format!("user-{}", i))),
                    ("name", Value::String(format!("User {}", i))),
                    ("email", Value::String(format!("user{}@test.com", i))),
                    ("age", Value::Int((i % 50 + 20) as i64)),
                    ("address", Value::String(format!("{} Main St", i))),
                ],
            )
            .await?;
        }
        let duration = start.elapsed();

        println!("1 indexed field (id only):");
        println!("   1000 inserts in {:?}", duration);
        println!("   {:.2} inserts/sec", 1000.0 / duration.as_secs_f64());
        println!(
            "   {:.2}ms per insert\n",
            duration.as_millis() as f64 / 1000.0
        );
    }

    // Test 2: Collection with MULTIPLE fields indexed
    {
        let db = Aurora::open("indexing_test2.db").await?;
        let _ = db.delete_collection("products");

        db.new_collection(
            "products",
            vec![
                (
                    "id",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: true,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // indexed
                (
                    "sku",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: true,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // indexed
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
                ), // NOT indexed
                (
                    "price",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_FLOAT,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // NOT indexed
                (
                    "category",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // NOT indexed
                (
                    "stock",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_INT,
                        unique: false,
                        indexed: false,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // NOT indexed
            ],
        )
        .await?;

        let start = Instant::now();
        for i in 0..1000 {
            db.insert_into(
                "products",
                vec![
                    ("id", Value::String(format!("prod-{}", i))),
                    ("sku", Value::String(format!("SKU-{:06}", i))),
                    ("name", Value::String(format!("Product {}", i))),
                    ("price", Value::Float((i as f64) * 9.99)),
                    ("category", Value::String(format!("Category {}", i % 10))),
                    ("stock", Value::Int((i % 100) as i64)),
                ],
            )
            .await?;
        }
        let duration = start.elapsed();

        println!("2 indexed fields (id + sku):");
        println!("   1000 inserts in {:?}", duration);
        println!("   {:.2} inserts/sec", 1000.0 / duration.as_secs_f64());
        println!(
            "   {:.2}ms per insert\n",
            duration.as_millis() as f64 / 1000.0
        );
    }

    // Test 3: Many fields, none indexed (except auto-indexed id)
    {
        let db = Aurora::open("indexing_test3.db").await?;
        let _ = db.delete_collection("logs");

        db.new_collection(
            "logs",
            vec![
                (
                    "id",
                    aurora_db::types::FieldDefinition {
                        field_type: FieldType::SCALAR_STRING,
                        unique: false,
                        indexed: true,
                        nullable: true,
                        validations: vec![],
                        relation: None,
                    },
                ), // auto-indexed
                (
                    "timestamp",
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
                    "level",
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
                    "message",
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
                    "source",
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
                    "user",
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
                    "ip",
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
                    "duration_ms",
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
                    "status",
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
                    "metadata",
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
        .await?;

        let start = Instant::now();
        for i in 0..1000 {
            db.insert_into(
                "logs",
                vec![
                    ("id", Value::String(format!("log-{}", i))),
                    ("timestamp", Value::String("2024-01-01T00:00:00Z".into())),
                    ("level", Value::String("INFO".into())),
                    ("message", Value::String(format!("Log message {}", i))),
                    ("source", Value::String("app".into())),
                    ("user", Value::String(format!("user{}", i % 100))),
                    ("ip", Value::String(format!("192.168.1.{}", i % 255))),
                    ("duration_ms", Value::Int((i % 1000) as i64)),
                    ("status", Value::Int(200)),
                    ("metadata", Value::String("{}".into())),
                ],
            )
            .await?;
        }
        let duration = start.elapsed();

        println!("10 fields, 1 indexed (id only):");
        println!("   1000 inserts in {:?}", duration);
        println!("   {:.2} inserts/sec", 1000.0 / duration.as_secs_f64());
        println!(
            "   {:.2}ms per insert\n",
            duration.as_millis() as f64 / 1000.0
        );
    }

    println!("ANALYSIS:");
    println!("   Before fix: Indexed ALL fields regardless of schema");
    println!("   After fix: Only index fields marked as unique/indexed");
    println!("   Expected speedup: 5-10x for documents with many fields\n");

    Ok(())
}
