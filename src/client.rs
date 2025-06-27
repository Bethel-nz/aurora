use crate::error::{AuroraError, Result};
use crate::network::protocol::{Request, Response};
use crate::query::SimpleQueryBuilder;
use crate::types::{Document, FieldType, Value};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connect to an Aurora server at the given address.
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// Sends a request to the server and awaits a response.
    async fn send_request(&mut self, request: Request) -> Result<Response> {
        let request_bytes = bincode::serialize(&request).map_err(AuroraError::Bincode)?;
        let len_bytes = (request_bytes.len() as u32).to_le_bytes();

        self.stream.write_all(&len_bytes).await?;
        self.stream.write_all(&request_bytes).await?;

        let mut len_bytes = [0u8; 4];
        self.stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buffer = vec![0u8; len];
        self.stream.read_exact(&mut buffer).await?;

        let response: Response = bincode::deserialize(&buffer).map_err(AuroraError::Bincode)?;

        Ok(response)
    }

    pub async fn new_collection(
        &mut self,
        name: &str,
        fields: Vec<(String, FieldType, bool)>,
    ) -> Result<()> {
        let req = Request::NewCollection {
            name: name.to_string(),
            fields,
        };
        match self.send_request(req).await? {
            Response::Done => Ok(()),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }

    pub async fn insert(
        &mut self,
        collection: &str,
        data: HashMap<String, Value>,
    ) -> Result<String> {
        let req = Request::Insert {
            collection: collection.to_string(),
            data,
        };
        match self.send_request(req).await? {
            Response::Message(id) => Ok(id),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }

    pub async fn get_document(&mut self, collection: &str, id: &str) -> Result<Option<Document>> {
        let req = Request::GetDocument {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        match self.send_request(req).await? {
            Response::Document(doc) => Ok(doc),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }

    pub async fn query(&mut self, builder: SimpleQueryBuilder) -> Result<Vec<Document>> {
        let req = Request::Query(builder);
        match self.send_request(req).await? {
            Response::Documents(docs) => Ok(docs),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }

    pub async fn begin_transaction(&mut self) -> Result<()> {
        match self.send_request(Request::BeginTransaction).await? {
            Response::Done => Ok(()),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }

    pub async fn commit_transaction(&mut self) -> Result<()> {
        match self.send_request(Request::CommitTransaction).await? {
            Response::Done => Ok(()),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        match self.send_request(Request::Delete(key.to_string())).await? {
            Response::Done => Ok(()),
            Response::Error(e) => Err(AuroraError::Protocol(e)),
            _ => Err(AuroraError::Protocol("Unexpected response".into())),
        }
    }
}
