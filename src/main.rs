#[cfg(any(feature = "http", feature = "binary"))]
fn setup_database() -> std::io::Result<Arc<Aurora>> {
    let db_path = "aurora_data";
    if std::path::Path::new(db_path).exists() {
        println!("🗑️ Removing old database directory for a clean run...");
        std::fs::remove_dir_all(db_path)?;
    }
    let db = Arc::new(
        Aurora::open(db_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?,
    );
    println!("💽 Database opened at '{}'", db_path);
    Ok(db)
}

#[cfg(feature = "http")]
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db = setup_database()?;
    println!("🚀 Running in HTTP mode.");

    #[cfg(feature = "binary")]
    {
        use aurora_db::network::server::BincodeServer;
        let bincode_db = db.clone();
        tokio::spawn(async move {
            let server = BincodeServer::new(bincode_db, "127.0.0.1:7878");
            println!("⚡ Starting Bincode server at 127.0.0.1:7878");
            if let Err(e) = server.run().await {
                eprintln!("Bincode server failed: {}", e);
            }
        });
    }

    use aurora_db::network::http_server::run_http_server;
    println!("🌐 Starting HTTP server at http://127.0.0.1:7879");
    run_http_server(db, "127.0.0.1:7879").await
}

#[cfg(not(feature = "http"))]
#[tokio::main]
async fn main() -> std::io::Result<()> {
    #[cfg(feature = "binary")]
    {
        let db = setup_database()?;
        println!("🚀 Running in Binary-only mode.");

        use aurora_db::network::server::BincodeServer;
        let server = BincodeServer::new(db, "127.0.0.1:7878");
        println!("⚡ Starting Bincode server at 127.0.0.1:7878");

        if let Err(e) = server.run().await {
            eprintln!("Bincode server failed: {}", e);
        }
    }

    #[cfg(not(any(feature = "http", feature = "binary")))]
    {
        println!("No server features enabled.");
        println!("To run a server, build with a feature flag, e.g.:");
        println!("  cargo run --bin aurora-db --features http");
        println!("  cargo run --bin aurora-db --features binary");
        println!("  cargo run --bin aurora-db --features full");
    }

    Ok(())
}
