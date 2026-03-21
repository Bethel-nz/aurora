//! # Aurora Database
//!
//! Aurora is an embedded document database with tiered storage architecture.
//! It provides document storage, querying, indexing, and search capabilities
//! while optimizing for both performance and durability.
//!
//! ## Key Features
//!
//! * **Tiered Storage**: Hot in-memory cache + persistent cold storage
//! * **Document Model**: Schema-flexible JSON-like document storage
//! * **Querying**: Rich query capabilities with filtering and sorting
//! * **Full-text Search**: Built-in search engine with relevance ranking
//! * **Transactions**: ACID-compliant transaction support
//!
//! ## Quick Start
//!

//! use aurora_db::{Aurora, Value, FieldType};
//!
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open a database
//! let db = Aurora::open("my_app.db")?;
//!
//! // Create a collection
//! db.new_collection("users", vec![
//!     ("name", FieldType::String, false),
//!     ("email", FieldType::String, true),  // unique field
//!     ("age", FieldType::Int, false),
//! ]).await?;
//!
//! // Insert data
//! let user_id = db.insert_into("users", vec![
//!     ("name", Value::String("Jane Doe".to_string())),
//!     ("email", Value::String("jane@example.com".to_string())),
//!     ("age", Value::Int(28)),
//! ]).await?;
//!
//! // Query data
//! let users = db.query("users")
//!     .filter(|f| f.gt("age", 21))
//!     .collect()
//!     .await?;
//!
//! # Ok(())
//! # }
//! ```

// Re-export primary types and modules
pub use crate::db::Aurora;
pub use crate::error::{AqlError, Result};
pub use crate::query::{QueryBuilder, SimpleQueryBuilder, SearchBuilder};
pub use types::{
    AuroraConfig, ColdStoreMode, Collection, Document, DurabilityMode, FieldDefinition, FieldType,
    Value,
};

pub use crate::parser::validator::{
    ErrorCode, InMemorySchema, SchemaProvider, ValidationError, ValidationResult, validate_document,
};

// Re-export commonly used storage types
pub use storage::{EvictionPolicy, HotStore};

// Re-export PubSub types for convenience
pub use pubsub::{ChangeEvent, ChangeListener, ChangeType};

// Re-export Workers types for convenience
pub use workers::{Job, JobPriority, JobStatus};

// Re-export Transaction types for convenience
pub use transaction::{TransactionBuffer, TransactionId};

// Module declarations
pub mod audit;
pub mod client;
pub mod computed;
pub mod db;
pub mod error;
pub mod index;
pub mod network;
pub mod parser; // AQL parser module
pub mod pubsub;
pub mod query;
pub mod reactive;
pub mod search;
pub mod storage;
pub mod transaction;
pub mod types;
pub mod wal;
pub mod workers;
