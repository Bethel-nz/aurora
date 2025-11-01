use aurora_db::client::Client;
use aurora_db::query::SimpleQueryBuilder;
use aurora_db::types::{FieldType, Value};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to Aurora server...");
    let mut client = Client::connect("127.0.0.1:7878").await?;
    println!("Connected!");

    // 1. Create a new collection
    println!("\n1. Creating 'products' collection...");
    client
        .new_collection(
            "products",
            vec![
                ("name".to_string(), FieldType::String, false),
                ("price".to_string(), FieldType::Float, false),
                ("in_stock".to_string(), FieldType::Bool, false),
            ],
        )
        .await?;
    println!("   -> Collection created successfully.");

    // 2. Insert documents
    println!("\n2. Inserting products...");
    let mut product_data = HashMap::new();
    product_data.insert("name".to_string(), "Laptop".into());
    product_data.insert("price".to_string(), 1200.50.into());
    product_data.insert("in_stock".to_string(), true.into());
    let doc_id = client.insert("products", product_data).await?;
    println!("   -> Inserted Laptop with ID: {}", doc_id);

    let mut keyboard_data = HashMap::new();
    keyboard_data.insert("name".to_string(), "Keyboard".into());
    keyboard_data.insert("price".to_string(), 75.50.into());
    keyboard_data.insert("in_stock".to_string(), Value::Bool(true));
    client.insert("products", keyboard_data).await?;
    println!("   -> Inserted Keyboard.");

    // 3. Retrieve a document
    println!("\n3. Retrieving product '{}'...", &doc_id);
    let doc = client.get_document("products", &doc_id).await?;
    println!("   -> Found document: {:?}", doc.unwrap());

    // 4. Query for documents
    println!("\n4. Querying for in-stock products cheaper than $100...");
    let query_builder = SimpleQueryBuilder::new("products".to_string())
        .lt("price", 100.0.into())
        .eq("in_stock", true.into());

    let docs = client.query(query_builder).await?;
    println!("   -> Found {} documents:", docs.len());
    for doc in docs {
        println!("      - {:?}", doc);
    }

    // 5. Transactions
    println!("\n5. Testing a transaction...");
    client.begin_transaction().await?;
    println!("   -> Beginning transaction...");

    let mut tx_product = HashMap::new();
    tx_product.insert("name".to_string(), "Transactional Item".into());
    tx_product.insert("price".to_string(), 99.99.into());
    tx_product.insert("in_stock".to_string(), true.into());

    let tx_id = client.insert("products", tx_product).await?;
    println!("   -> Inserting item '{}' within transaction...", tx_id);

    client.commit_transaction().await?;
    println!("   -> Transaction committed.");

    // 6. Deleting a document
    println!("\n6. Deleting product '{}'...", &doc_id);
    client.delete(&format!("products:{}", doc_id)).await?;
    println!("   -> Document deleted successfully.");

    println!("\nClient finished.");
    Ok(())
}
