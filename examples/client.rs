use aurora_db::client::Client;
use aurora_db::types::{FieldType, Value};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to Aurora server...");
    let mut client = Client::connect("127.0.0.1:7878").await?;
    println!("Connected!");

    // 1. Create a new collection
    // new_collection expects Vec<(String, FieldType, bool)> where bool = indexed
    println!("\n1. Creating 'products' collection...");
    client
        .new_collection(
            "products",
            vec![
                ("name".to_string(), FieldType::SCALAR_STRING, false),
                ("price".to_string(), FieldType::SCALAR_FLOAT, false),
                ("in_stock".to_string(), FieldType::SCALAR_BOOL, false),
            ],
        )
        .await?;
    println!("   -> Collection created successfully.");

    // 2. Insert documents
    println!("\n2. Inserting products...");
    let mut product_data = HashMap::new();
    product_data.insert("name".to_string(), Value::String("Laptop".into()));
    product_data.insert("price".to_string(), Value::Float(1200.50));
    product_data.insert("in_stock".to_string(), Value::Bool(true));
    let doc_id = client.insert("products", product_data).await?;
    println!("   -> Inserted Laptop with ID: {}", doc_id);

    let mut keyboard_data = HashMap::new();
    keyboard_data.insert("name".to_string(), Value::String("Keyboard".into()));
    keyboard_data.insert("price".to_string(), Value::Float(75.50));
    keyboard_data.insert("in_stock".to_string(), Value::Bool(true));
    client.insert("products", keyboard_data).await?;
    println!("   -> Inserted Keyboard.");

    // 3. Retrieve a document
    println!("\n3. Retrieving product '{}'...", &doc_id);
    let doc = client.get_document("products", &doc_id).await?;
    if let Some(doc) = doc {
        println!("   -> Found document: {:?}", doc);
    } else {
        println!("   -> Document not found");
    }

    // 4. Transactions
    println!("\n4. Testing a transaction...");
    client.begin_transaction().await?;
    println!("   -> Beginning transaction...");

    let mut tx_product = HashMap::new();
    tx_product.insert(
        "name".to_string(),
        Value::String("Transactional Item".into()),
    );
    tx_product.insert("price".to_string(), Value::Float(99.99));
    tx_product.insert("in_stock".to_string(), Value::Bool(true));

    let tx_id = client.insert("products", tx_product).await?;
    println!("   -> Inserting item '{}' within transaction...", tx_id);

    client.commit_transaction().await?;
    println!("   -> Transaction committed.");

    // 5. Deleting a document
    println!("\n5. Deleting product '{}'...", &doc_id);
    client.delete(&format!("products:{}", doc_id)).await?;
    println!("   -> Document deleted successfully.");

    println!("\nClient finished.");
    Ok(())
}
