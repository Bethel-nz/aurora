use crate::query::SimpleQueryBuilder;
use crate::types::{Document, FieldType, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A module to handle Base64 encoding for Vec<u8> in JSON.
mod base64_serde {
    use base64::{engine::general_purpose, Engine as _};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let base64 = general_purpose::STANDARD.encode(bytes);
            serializer.serialize_str(&base64)
        } else {
            serializer.serialize_bytes(bytes)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let base64 = <String>::deserialize(deserializer)?;
            general_purpose::STANDARD
                .decode(&base64)
                .map_err(serde::de::Error::custom)
        } else {
            <Vec<u8>>::deserialize(deserializer)
        }
    }
}

/// Represents a request sent from a client to the server.
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Get a value by key.
    Get(String),
    /// Put a value by key.
    Put(String, #[serde(with = "base64_serde")] Vec<u8>),
    /// Delete a key.
    Delete(String),
    /// Create a new collection.
    NewCollection {
        name: String,
        fields: Vec<(String, FieldType, bool)>,
    },
    /// Insert a document into a collection.
    Insert {
        collection: String,
        data: HashMap<String, Value>,
    },
    /// Get a document from a collection by ID.
    GetDocument { collection: String, id: String },
    /// Query a collection.
    Query(SimpleQueryBuilder),
    /// Begin a transaction.
    BeginTransaction,
    /// Commit a transaction.
    CommitTransaction(u64),
    /// Roll back a transaction.
    RollbackTransaction(u64),
}

/// Represents a response sent from the server to a client.
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// A successful operation with an optional value.
    Success(#[serde(with = "base64_serde_option")] Option<Vec<u8>>),
    /// A successful operation with a Document.
    Document(Option<Document>),
    /// A successful operation with a list of Documents.
    Documents(Vec<Document>),
    /// A successful operation with a string response (e.g., a document ID).
    Message(String),
    /// Transaction ID returned from BeginTransaction.
    TransactionId(u64),
    /// A successful operation with no return value.
    Done,
    /// An error occurred.
    Error(String),
}

/// A module for optional Base64 encoding.
mod base64_serde_option {
    use base64::{engine::general_purpose, Engine as _};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(b) => super::base64_serde::serialize(b, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<Vec<u8>> = if deserializer.is_human_readable() {
            let base64: Option<String> = Option::deserialize(deserializer)?;
            match base64 {
                Some(s) => Some(
                    general_purpose::STANDARD
                        .decode(&s)
                        .map_err(serde::de::Error::custom)?,
                ),
                None => None,
            }
        } else {
            Option::deserialize(deserializer)?
        };
        Ok(opt)
    }
}
