//! # Aurora Database
//!
//! Aurora is a high-performance, embedded document database designed for speed, 
//! durability, and developer ergonomics. It features a tiered storage architecture 
//! combining a blazing-fast in-memory cache with reliable, persistent cold storage.
//!
//! ## Core Architecture
//! - **Tiered Storage**: Automatically manages data between a "hot" cache (DashMap/Moka) 
//!   and "cold" storage (Sled/MMAP) for optimal performance.
//! - **AQL (Aurora Query Language)**: A GraphQL-inspired query language for powerful 
//!   data retrieval, manipulation, and real-time subscriptions.
//! - **Roaring Bitmaps**: Uses Roaring Bitmaps for high-performance secondary indexing 
//!   and bitwise query optimization.
//! - **Write-Ahead Logging (WAL)**: Ensures crash-consistency and data durability.
//!
//! ## Quick Start
//!
//! ```rust
//! use aurora_db::{Aurora, doc, object, FieldType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Open or create a database
//!     let db = Aurora::open("my_database.db").await?;
//!
//!     // Define a collection with schema
//!     db.new_collection("users", vec![
//!         ("name", FieldType::SCALAR_STRING, false),
//!         ("email", FieldType::SCALAR_STRING, true), // Unique constraint
//!         ("age", FieldType::SCALAR_INT, false),
//!     ]).await?;
//!
//!     // Insert a document using the object! macro
//!     let user_id = db.insert_map("users", object!({
//!         "name": "Jane Doe",
//!         "email": "jane@example.com",
//!         "age": 28
//!     }).as_object().unwrap().clone()).await?;
//!
//!     // Run a parametrized AQL query using the doc! macro
//!     let result = db.execute(doc!(
//!         "query($minAge: Int) {
//!             users(where: { age: { gte: $minAge } }) {
//!                 name
//!                 email
//!             }
//!         }",
//!         { "minAge": 21 }
//!     )).await?;
//!
//!     // Bind the results back to a Rust struct
//!     #[derive(serde::Deserialize, Debug)]
//!     struct User { name: String, email: String }
//!     let users: Vec<User> = result.bind()?;
//!
//!     Ok(())
//! }
//! ```

// Re-export primary types and modules
pub use crate::db::Aurora;
pub use crate::error::{AqlError, ErrorCode, Result};
pub use crate::query::{QueryBuilder, SearchBuilder, SimpleQueryBuilder};
pub use types::{
    AuroraConfig, ColdStoreMode, Collection, Document, DurabilityMode, FieldDefinition, FieldType,
    Value,
};

pub use crate::parser::validator::{
    InMemorySchema, SchemaProvider, ValidationError, ValidationResult, validate_document,
};

// Re-export AQL execution types for convenience
pub use crate::parser::executor::{
    ExecutionResult, MigrationResult, MutationResult, QueryResult, SchemaResult, SubscriptionResult,
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
#[macro_use]
pub mod macros;
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
