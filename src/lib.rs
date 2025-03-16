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
//! ```
//! use aurora_db::{Aurora, Value, FieldType};
//!
//! // Open a database
//! let db = Aurora::open("my_app.db")?;
//!
//! // Create a collection
//! db.new_collection("users", vec![
//!     ("name", FieldType::String, false),
//!     ("email", FieldType::String, true),  // unique field
//!     ("age", FieldType::Int, false),
//! ])?;
//!
//! // Insert data
//! let user_id = db.insert_into("users", vec![
//!     ("name", "Jane Doe"),
//!     ("email", "jane@example.com"),
//!     ("age", 28),
//! ])?;
//!
//! // Query data
//! let users = db.query("users")
//!     .filter(|f| f.gt("age", 21))
//!     .collect()
//!     .await?;
//! ```

// Re-export primary types and modules
pub use crate::db::Aurora;
pub use crate::types::{
    Document, Value, Collection, FieldType, FieldDefinition, InsertData,
    AuroraConfig, ColdStoreMode
};
pub use crate::error::{AuroraError, Result};
pub use crate::query::{QueryBuilder, FilterBuilder, SearchBuilder};
pub use crate::db::DataInfo;

// Re-export query module for direct access to query API
pub mod query;

// Module declarations
mod db;
mod error;
mod index;
mod search;
mod storage;
mod types;
