use std::path::Path;

use aurora::{Aurora, Result};

macro_rules! time_operation {
    ($op_name:expr, $op:expr) => {{
        let start = std::time::Instant::now();
        let result = $op;
        println!("Operation '{}' took: {:?}", $op_name, start.elapsed());
        result
    }};
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the database
    let db = Aurora::new("aurora.db")?;

    // Set a value with ttl set to None
    time_operation!("put", db.put("hello".to_string(), b"world".to_vec(), None))?;

    // Get a value
    match time_operation!("get", db.get("hello"))? {
        Some(value) => println!("Value: {:?}", String::from_utf8_lossy(&value)),
        None => println!("Key not found"),
    }

    time_operation!(
        "put blob",
        db.put_blob(
            "image:profile".to_string(),
            Path::new("/home/kylo_ren/Pictures/wallpapers/Study-table.png"),
        )?
    );

    if let Some(image_data) = time_operation!("get blob", db.get_blob("image:profile")?) {
        // Save it back to disk
        std::fs::write("retrieved_image.png", image_data)?;
    }
    Ok(())
}

