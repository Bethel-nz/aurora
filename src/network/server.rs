use crate::db::Aurora;
use crate::error::{AuroraError, Result};
use crate::network::protocol::Request;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct BincodeServer {
    db: Arc<Aurora>,
    addr: String,
}

impl BincodeServer {
    pub fn new(db: Arc<Aurora>, addr: &str) -> Self {
        Self {
            db,
            addr: addr.to_string(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("Bincode server listening on {}", self.addr);

        loop {
            let (stream, _) = listener.accept().await?;
            let db_clone = self.db.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_bincode_connection(stream, db_clone).await {
                    eprintln!("Error handling bincode connection: {}", e);
                }
            });
        }
    }

    async fn handle_bincode_connection(mut stream: TcpStream, db: Arc<Aurora>) -> Result<()> {
        loop {
            let mut len_bytes = [0u8; 4];
            match stream.read_exact(&mut len_bytes).await {
                Ok(_) => (),
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Client disconnected
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut buffer = vec![0u8; len];
            stream.read_exact(&mut buffer).await?;

            let request: Request =
                bincode::deserialize(&buffer).map_err(AuroraError::Bincode)?;

            let response = db.process_network_request(request).await;

            let response_bytes = bincode::serialize(&response).map_err(AuroraError::Bincode)?;
            let len_bytes = (response_bytes.len() as u32).to_le_bytes();

            stream.write_all(&len_bytes).await?;
            stream.write_all(&response_bytes).await?;
        }
        Ok(())
    }
}
