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
//! * **Blob Storage**: Efficient storage for large binary objects
//!
//! ## Usage Example
//!
//! ```rust
//! use aurora::Aurora;
//!
//! // Open a database
//! let db = Aurora::open("my_database.db")?;
//!
//! // Create a collection with schema
//! db.new_collection("users", vec![
//!     ("name", FieldType::String, false),
//!     ("email", FieldType::String, true),  // unique field
//!     ("age", FieldType::Int, false),
//! ])?;
//!
//! // Insert a document
//! let user_id = db.insert_into("users", vec![
//!     ("name", Value::String("Jane Doe".to_string())),
//!     ("email", Value::String("jane@example.com".to_string())),
//!     ("age", Value::Int(28)),
//! ])?;
//!
//! // Query for documents
//! let adult_users = db.query("users")
//!     .filter(|f| f.gt("age", 18))
//!     .order_by("name", true)
//!     .collect()
//!     .await?;
//! ```

use crate::error::{AuroraError, Result};
use crate::index::{Index, IndexDefinition, IndexType};
use crate::network::http_models::{
    Filter as HttpFilter, FilterOperator, QueryPayload, json_to_value,
};
use crate::query::{Filter, FilterBuilder, QueryBuilder, SearchBuilder, SimpleQueryBuilder};
use crate::storage::{ColdStore, HotStore, WriteBuffer};
use crate::types::{
    AuroraConfig, Collection, Document, DurabilityMode, FieldDefinition, FieldType, Value,
};
use crate::wal::{Operation, WriteAheadLog};
use dashmap::DashMap;
use serde_json::Value as JsonValue;
use serde_json::from_str;
use std::collections::HashMap;
use std::fmt;
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::fs::File;
use tokio::fs::read_to_string;
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;
use uuid::Uuid;

// Disk location metadata for primary index
// Instead of storing full Vec<u8> values, we store minimal metadata
#[derive(Debug, Clone, Copy)]
struct DiskLocation {
    size: u32,  // Size in bytes (useful for statistics)
}

impl DiskLocation {
    fn new(size: usize) -> Self {
        Self {
            size: size as u32,
        }
    }
}

// Index types for faster lookups
type PrimaryIndex = DashMap<String, DiskLocation>;
type SecondaryIndex = DashMap<String, Vec<String>>;

// Move DataInfo enum outside impl block
#[derive(Debug)]
pub enum DataInfo {
    Data { size: usize, preview: String },
    Blob { size: usize },
    Compressed { size: usize },
}

impl DataInfo {
    pub fn size(&self) -> usize {
        match self {
            DataInfo::Data { size, .. } => *size,
            DataInfo::Blob { size } => *size,
            DataInfo::Compressed { size } => *size,
        }
    }
}

/// The main database engine
///
/// Aurora combines a tiered storage architecture with document-oriented database features:
/// - Hot tier: In-memory cache for frequently accessed data
/// - Cold tier: Persistent disk storage for durability
/// - Primary indices: Fast key-based access
/// - Secondary indices: Fast field-based queries
///
/// # Examples
///
/// ```
/// // Open a database (creates if doesn't exist)
/// let db = Aurora::open("my_app.db")?;
///
/// // Insert a document
/// let doc_id = db.insert_into("users", vec![
///     ("name", Value::String("Alice".to_string())),
///     ("age", Value::Int(32)),
/// ])?;
///
/// // Retrieve a document
/// let user = db.get_document("users", &doc_id)?;
/// ```
pub struct Aurora {
    hot: HotStore,
    cold: Arc<ColdStore>,
    primary_indices: Arc<DashMap<String, PrimaryIndex>>,
    secondary_indices: Arc<DashMap<String, SecondaryIndex>>,
    indices_initialized: Arc<OnceCell<()>>,
    transaction_manager: crate::transaction::TransactionManager,
    indices: Arc<DashMap<String, Index>>,
    schema_cache: Arc<DashMap<String, Arc<Collection>>>,
    config: AuroraConfig,
    write_buffer: Option<WriteBuffer>,
    pubsub: crate::pubsub::PubSubSystem,
    // Write-ahead log for durability (optional, based on config)
    wal: Option<Arc<RwLock<WriteAheadLog>>>,
    // Background checkpoint task
    checkpoint_shutdown: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    // Background compaction task
    compaction_shutdown: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

impl fmt::Debug for Aurora {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Aurora")
            .field("hot", &"HotStore")
            .field("cold", &"ColdStore")
            .field("primary_indices_count", &self.primary_indices.len())
            .field("secondary_indices_count", &self.secondary_indices.len())
            .field(
                "active_transactions",
                &self.transaction_manager.active_count(),
            )
            .field("indices_count", &self.indices.len())
            .finish()
    }
}

impl Aurora {
    /// Open or create a database at the specified location
    ///
    /// # Arguments
    /// * `path` - Path to the database file or directory
    ///   - Absolute paths (like `/data/myapp.db`) are used as-is
    ///   - Relative paths (like `./data/myapp.db`) are resolved relative to the current directory
    ///   - Simple names (like `myapp.db`) use the current directory
    ///
    /// # Returns
    /// An initialized `Aurora` database instance
    ///
    /// # Examples
    ///
    /// ```
    /// // Use a specific location
    /// let db = Aurora::open("./data/my_application.db")?;
    ///
    /// // Just use a name (creates in current directory)
    /// let db = Aurora::open("customer_data.db")?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = AuroraConfig {
            db_path: Self::resolve_path(path)?,
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Helper method to resolve database path
    fn resolve_path<P: AsRef<Path>>(path: P) -> Result<PathBuf> {
        let path = path.as_ref();

        // If it's an absolute path, use it directly
        if path.is_absolute() {
            return Ok(path.to_path_buf());
        }

        // Otherwise, resolve relative to current directory
        match std::env::current_dir() {
            Ok(current_dir) => Ok(current_dir.join(path)),
            Err(e) => Err(AuroraError::IoError(format!(
                "Failed to resolve current directory: {}",
                e
            ))),
        }
    }

    /// Open a database with custom configuration
    pub fn with_config(config: AuroraConfig) -> Result<Self> {
        let path = Self::resolve_path(&config.db_path)?;

        if config.create_dirs
            && let Some(parent) = path.parent()
                && !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }

        // Fix method calls to pass all required parameters
        let cold = Arc::new(ColdStore::with_config(
            path.to_str().unwrap(),
            config.cold_cache_capacity_mb,
            config.cold_flush_interval_ms,
            config.cold_mode,
        )?);

        let hot = HotStore::with_config_and_eviction(
            config.hot_cache_size_mb,
            config.hot_cache_cleanup_interval_secs,
            config.eviction_policy,
        );

        // Initialize write buffer if enabled
        let write_buffer = if config.enable_write_buffering {
            Some(WriteBuffer::new(
                Arc::clone(&cold),
                config.write_buffer_size,
                config.write_buffer_flush_interval_ms,
            ))
        } else {
            None
        };

        // Store auto_compact before moving config
        let auto_compact = config.auto_compact;
        let enable_wal = config.enable_wal;

        let pubsub = crate::pubsub::PubSubSystem::new(10000);

        // Initialize WAL if enabled and check for recovery
        let (wal, wal_entries) = if enable_wal {
            let wal_path = path.to_str().unwrap();
            match WriteAheadLog::new(wal_path) {
                Ok(mut wal_log) => {
                    let entries = wal_log.recover().unwrap_or_else(|_| Vec::new());
                    (Some(Arc::new(RwLock::new(wal_log))), entries)
                }
                Err(_) => (None, Vec::new()),
            }
        } else {
            (None, Vec::new())
        };

        // Spawn background checkpoint task if WAL is enabled
        let checkpoint_shutdown = if wal.is_some() {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
            let cold_clone = Arc::clone(&cold);
            let wal_clone = wal.clone();
            let checkpoint_interval = config.checkpoint_interval_ms;

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(checkpoint_interval));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            // Checkpoint: flush cold storage
                            if let Err(e) = cold_clone.flush() {
                                eprintln!("Background checkpoint flush error: {}", e);
                            }
                            // Truncate WAL after successful flush
                            if let Some(ref wal) = wal_clone
                                && let Ok(mut wal_guard) = wal.write() {
                                    let _ = wal_guard.truncate();
                                }
                        }
                        _ = shutdown_rx.recv() => {
                            // Final checkpoint before shutdown
                            let _ = cold_clone.flush();
                            if let Some(ref wal) = wal_clone
                                && let Ok(mut wal_guard) = wal.write() {
                                    let _ = wal_guard.truncate();
                                }
                            break;
                        }
                    }
                }
            });

            Some(shutdown_tx)
        } else {
            None
        };

        // Spawn background compaction task if auto_compact is enabled
        let compaction_shutdown = if auto_compact {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
            let cold_clone = Arc::clone(&cold);
            let compact_interval = config.compact_interval_mins;

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(compact_interval * 60));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Err(e) = cold_clone.compact() {
                                eprintln!("Background compaction error: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            let _ = cold_clone.compact();
                            break;
                        }
                    }
                }
            });

            Some(shutdown_tx)
        } else {
            None
        };

        // Initialize the database
        let db = Self {
            hot,
            cold,
            primary_indices: Arc::new(DashMap::new()),
            secondary_indices: Arc::new(DashMap::new()),
            indices_initialized: Arc::new(OnceCell::new()),
            transaction_manager: crate::transaction::TransactionManager::new(),
            indices: Arc::new(DashMap::new()),
            schema_cache: Arc::new(DashMap::new()),
            config,
            write_buffer,
            pubsub,
            wal,
            checkpoint_shutdown,
            compaction_shutdown,
        };

        // Replay WAL entries if any
        if !wal_entries.is_empty() {
            db.replay_wal(wal_entries)?;
        }

        Ok(db)
    }

    // Lazy index initialization
    pub async fn ensure_indices_initialized(&self) -> Result<()> {
        self.indices_initialized
            .get_or_init(|| async {
                println!("Initializing indices...");
                if let Err(e) = self.initialize_indices() {
                    eprintln!("Failed to initialize indices: {:?}", e);
                }
                println!("Indices initialized");
                
            })
            .await;
        Ok(())
    }

    fn initialize_indices(&self) -> Result<()> {
        for result in self.cold.scan() {
            let (key, value) = result?;
            let key_str = std::str::from_utf8(key.as_bytes())
                .map_err(|_| AuroraError::InvalidKey("Invalid UTF-8".into()))?;

            if let Some(collection_name) = key_str.split(':').next() {
                self.index_value(collection_name, key_str, &value)?;
            }
        }
        Ok(())
    }

    // Fast key-value operations with index support
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Check hot cache first
        if let Some(value) = self.hot.get(key) {
            return Ok(Some(value));
        }

        // Check if key exists in primary index
        if let Some(collection) = key.split(':').next()
            && let Some(index) = self.primary_indices.get(collection)
                && index.contains_key(key) {
                    // Key exists in index, fetch from cold storage
                    if let Some(value) = self.cold.get(key)? {
                        // Promote to hot cache for future fast access
                        self.hot.set(key.to_string(), value.clone(), None);
                        return Ok(Some(value));
                    }
                }

        // Fallback to cold storage
        let value = self.cold.get(key)?;
        if let Some(v) = &value {
            self.hot.set(key.to_string(), v.clone(), None);
        }
        Ok(value)
    }

    /// Get value with zero-copy Arc reference (10-100x faster than get!)
    /// Only checks hot cache - returns None if not cached
    pub fn get_hot_ref(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.hot.get_ref(key)
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> crate::storage::hot::CacheStats {
        self.hot.get_stats()
    }

    // ============================================
    // PubSub API - Real-time Change Notifications
    // ============================================

    /// Listen for changes on a specific collection
    ///
    /// # Examples
    ///
    /// ```
    /// let mut listener = db.listen("users");
    ///
    /// // In another task, insert a document
    /// db.insert_into("users", vec![...]).await?;
    ///
    /// // Listener receives the event
    /// let event = listener.recv().await?;
    /// println!("Change: {:?}", event);
    /// ```
    pub fn listen(&self, collection: impl Into<String>) -> crate::pubsub::ChangeListener {
        self.pubsub.listen(collection)
    }

    /// Listen for all changes across all collections
    pub fn listen_all(&self) -> crate::pubsub::ChangeListener {
        self.pubsub.listen_all()
    }

    /// Get the number of active listeners for a collection
    pub fn listener_count(&self, collection: &str) -> usize {
        self.pubsub.listener_count(collection)
    }

    /// Get total number of active listeners
    pub fn total_listeners(&self) -> usize {
        self.pubsub.total_listeners()
    }

    /// Flushes all buffered writes to disk to ensure durability.
    ///
    /// This method forces all pending writes from:
    /// - Write buffer (if enabled)
    /// - Cold storage internal buffers
    /// - Write-ahead log (if enabled)
    ///
    /// Call this when you need to ensure data persistence before
    /// a critical operation or shutdown.
    ///
    /// # Examples
    ///
    /// ```
    /// db.insert_into("users", data).await?;
    /// db.flush()?;  // Ensure data is persisted to disk
    /// ```
    pub fn flush(&self) -> Result<()> {
        // Flush write buffer if present
        if let Some(ref write_buffer) = self.write_buffer {
            write_buffer.flush()?;
        }

        // Flush cold storage
        self.cold.flush()?;

        // Truncate WAL after successful flush (data is now in cold storage)
        if let Some(ref wal) = self.wal
            && let Ok(mut wal_lock) = wal.write() {
                wal_lock.truncate()?;
            }

        Ok(())
    }

    pub fn put(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        const MAX_BLOB_SIZE: usize = 50 * 1024 * 1024;

        if value.len() > MAX_BLOB_SIZE {
            return Err(AuroraError::InvalidOperation(format!(
                "Blob size {} exceeds maximum allowed size of {}MB",
                value.len() / (1024 * 1024),
                MAX_BLOB_SIZE / (1024 * 1024)
            )));
        }

        if let Some(ref wal) = self.wal
            && self.config.durability_mode != DurabilityMode::None {
                wal.write()
                    .unwrap()
                    .append(Operation::Put, &key, Some(&value))?;
            }

        if let Some(ref write_buffer) = self.write_buffer {
            write_buffer.write(key.clone(), value.clone())?;
        } else {
            self.cold.set(key.clone(), value.clone())?;
        }

        self.hot.set(key.clone(), value.clone(), ttl);

        if let Some(collection_name) = key.split(':').next()
            && !collection_name.starts_with('_') {
                self.index_value(collection_name, &key, &value)?;
            }

        Ok(())
    }

    /// Replay WAL entries to recover from crash
    fn replay_wal(&self, entries: Vec<crate::wal::LogEntry>) -> Result<()> {
        for entry in entries {
            match entry.operation {
                Operation::Put => {
                    if let Some(value) = entry.value {
                        // Write directly to cold storage (skip WAL, already logged)
                        self.cold.set(entry.key.clone(), value.clone())?;

                        // Update hot cache
                        self.hot.set(entry.key.clone(), value.clone(), None);

                        // Rebuild indices
                        if let Some(collection) = entry.key.split(':').next()
                            && !collection.starts_with('_') {
                                self.index_value(collection, &entry.key, &value)?;
                            }
                    }
                }
                Operation::Delete => {
                    self.cold.delete(&entry.key)?;
                    self.hot.delete(&entry.key);
                    // TODO: Remove from indices
                }
                Operation::BeginTx | Operation::CommitTx | Operation::RollbackTx => {
                    // Transaction boundaries - future implementation
                }
            }
        }

        // Flush after replay and truncate WAL
        self.cold.flush()?;
        if let Some(ref wal) = self.wal {
            wal.write().unwrap().truncate()?;
        }

        Ok(())
    }

    fn index_value(&self, collection: &str, key: &str, value: &[u8]) -> Result<()> {
        // Update primary index with metadata only (not the full value)
        let location = DiskLocation::new(value.len());
        self.primary_indices
            .entry(collection.to_string())
            .or_default()
            .insert(key.to_string(), location);

        // Try to get schema from cache first, otherwise load and cache it
        let collection_obj = match self.schema_cache.get(collection) {
            Some(cached_schema) => Arc::clone(cached_schema.value()),
            None => {
                // Schema not in cache - load it
                let collection_key = format!("_collection:{}", collection);
                let schema_data = match self.get(&collection_key)? {
                    Some(data) => data,
                    None => return Ok(()), // No schema = no secondary indices
                };

                let obj: Collection = match serde_json::from_slice(&schema_data) {
                    Ok(obj) => obj,
                    Err(_) => return Ok(()), // Invalid schema = skip indexing
                };

                // Cache the schema for future use
                let arc_obj = Arc::new(obj);
                self.schema_cache
                    .insert(collection.to_string(), Arc::clone(&arc_obj));
                arc_obj
            }
        };

        // Build list of fields that should be indexed (unique or explicitly indexed)
        let indexed_fields: Vec<String> = collection_obj
            .fields
            .iter()
            .filter(|(_, def)| def.indexed || def.unique)
            .map(|(name, _)| name.clone())
            .collect();

        // If no fields need indexing, we're done
        if indexed_fields.is_empty() {
            return Ok(());
        }

        // Update secondary indices - ONLY for indexed/unique fields
        if let Ok(doc) = serde_json::from_slice::<Document>(value) {
            for (field, field_value) in doc.data {
                // CRITICAL FIX: Skip fields that aren't indexed
                if !indexed_fields.contains(&field) {
                    continue;
                }

                // Use consistent string formatting for indexing
                let value_str = match &field_value {
                    Value::String(s) => s.clone(),
                    _ => field_value.to_string(),
                };

                let index_key = format!("{}:{}", collection, field);
                let secondary_index = self
                    .secondary_indices
                    .entry(index_key)
                    .or_default();

                // Check if we're at the index limit
                let max_entries = self.config.max_index_entries_per_field;

                secondary_index
                    .entry(value_str)
                    .and_modify(|doc_ids| {
                        // Only add if we haven't exceeded the limit per value
                        if doc_ids.len() < max_entries {
                            doc_ids.push(key.to_string());
                        }
                    })
                    .or_insert_with(|| vec![key.to_string()]);
            }
        }
        Ok(())
    }

    // Simplified collection scan (fallback)
    fn scan_collection(&self, collection: &str) -> Result<Vec<Document>> {
        let prefix = format!("{}:", collection);
        let mut documents = Vec::new();

        // Read from cold storage instead of primary index
        for result in self.cold.scan_prefix(&prefix) {
            if let Ok((_key, value)) = result {
                if let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                    documents.push(doc);
                }
            }
        }

        Ok(documents)
    }

    // Restore missing methods
    pub async fn put_blob(&self, key: String, file_path: &Path) -> Result<()> {
        const MAX_FILE_SIZE: usize = 50 * 1024 * 1024; // 50MB limit

        // Get file metadata to check size before reading
        let metadata = tokio::fs::metadata(file_path).await?;
        let file_size = metadata.len() as usize;

        if file_size > MAX_FILE_SIZE {
            return Err(AuroraError::InvalidOperation(format!(
                "File size {} MB exceeds maximum allowed size of {} MB",
                file_size / (1024 * 1024),
                MAX_FILE_SIZE / (1024 * 1024)
            )));
        }

        let mut file = File::open(file_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // Add BLOB: prefix to mark this as blob data
        let mut blob_data = Vec::with_capacity(5 + buffer.len());
        blob_data.extend_from_slice(b"BLOB:");
        blob_data.extend_from_slice(&buffer);

        self.put(key, blob_data, None)
    }

    /// Create a new collection with the given schema
    ///
    /// # Arguments
    /// * `name` - Name of the collection to create
    /// * `fields` - Schema definition as a list of field definitions:
    ///   * Field name (accepts both &str and String)
    ///   * Field type (String, Int, Float, Boolean, etc.)
    ///   * Whether the field requires a unique value
    ///
    /// # Returns
    /// Success or an error (e.g., collection already exists)
    ///
    /// # Examples
    ///
    /// ```
    /// // Define a collection with schema - accepts &str
    /// db.new_collection("products", vec![
    ///     ("name", FieldType::String, false),
    ///     ("price", FieldType::Float, false),
    ///     ("sku", FieldType::String, true),  // unique field
    ///     ("description", FieldType::String, false),
    ///     ("in_stock", FieldType::Bool, false),
    /// ])?;
    /// ```
    pub fn new_collection<S: Into<String>>(
        &self,
        name: &str,
        fields: Vec<(S, FieldType, bool)>,
    ) -> Result<()> {
        let collection_key = format!("_collection:{}", name);

        // Check if collection already exists - if so, just return Ok (idempotent)
        if self.get(&collection_key)?.is_some() {
            return Ok(());
        }

        // Create field definitions
        let mut field_definitions = HashMap::new();
        for (field_name, field_type, unique) in fields {
            field_definitions.insert(
                field_name.into(),
                FieldDefinition {
                    field_type,
                    unique,
                    indexed: unique, // Auto-index unique fields
                },
            );
        }

        let collection = Collection {
            name: name.to_string(),
            fields: field_definitions,
            // REMOVED: unique_fields is now derived from fields
        };

        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None)?;

        // Invalidate schema cache since we just created/updated the collection schema
        self.schema_cache.remove(name);

        Ok(())
    }

    /// Insert a document into a collection
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to insert into
    /// * `data` - Document fields and values to insert
    ///
    /// # Returns
    /// The ID of the inserted document or an error
    ///
    /// # Examples
    ///
    /// ```
    /// // Insert a document
    /// let doc_id = db.insert_into("users", vec![
    ///     ("name", Value::String("John Doe".to_string())),
    ///     ("email", Value::String("john@example.com".to_string())),
    ///     ("active", Value::Bool(true)),
    /// ])?;
    /// ```
    pub async fn insert_into(&self, collection: &str, data: Vec<(&str, Value)>) -> Result<String> {
        // Convert Vec<(&str, Value)> to HashMap<String, Value>
        let data_map: HashMap<String, Value> =
            data.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

        // Validate unique constraints before inserting
        self.validate_unique_constraints(collection, &data_map)
            .await?;

        let doc_id = Uuid::new_v4().to_string();
        let document = Document {
            id: doc_id.clone(),
            data: data_map,
        };

        self.put(
            format!("{}:{}", collection, doc_id),
            serde_json::to_vec(&document)?,
            None,
        )?;

        // Publish insert event
        let event = crate::pubsub::ChangeEvent::insert(collection, &doc_id, document.clone());
        let _ = self.pubsub.publish(event);

        Ok(doc_id)
    }

    pub async fn insert_map(
        &self,
        collection: &str,
        data: HashMap<String, Value>,
    ) -> Result<String> {
        // Validate unique constraints before inserting
        self.validate_unique_constraints(collection, &data).await?;

        let doc_id = Uuid::new_v4().to_string();
        let document = Document {
            id: doc_id.clone(),
            data,
        };

        self.put(
            format!("{}:{}", collection, doc_id),
            serde_json::to_vec(&document)?,
            None,
        )?;

        // Publish insert event
        let event = crate::pubsub::ChangeEvent::insert(collection, &doc_id, document.clone());
        let _ = self.pubsub.publish(event);

        Ok(doc_id)
    }

    /// Batch insert multiple documents with optimized write path
    /// Bypasses write buffer for better performance on large batches
    /// Use this for bulk data loading scenarios
    pub async fn batch_insert(
        &self,
        collection: &str,
        documents: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<String>> {
        let mut doc_ids = Vec::with_capacity(documents.len());
        let mut pairs = Vec::with_capacity(documents.len());

        // Prepare all documents
        for data in documents {
            // Validate unique constraints
            self.validate_unique_constraints(collection, &data).await?;

            let doc_id = Uuid::new_v4().to_string();
            let document = Document {
                id: doc_id.clone(),
                data,
            };

            let key = format!("{}:{}", collection, doc_id);
            let value = serde_json::to_vec(&document)?;

            pairs.push((key, value));
            doc_ids.push(doc_id);
        }

        // Write to WAL in batch (if enabled)
        if let Some(ref wal) = self.wal
            && self.config.durability_mode != DurabilityMode::None {
                let mut wal_lock = wal.write().unwrap();
                for (key, value) in &pairs {
                    wal_lock.append(Operation::Put, key, Some(value))?;
                }
            }

        // Bypass write buffer - go directly to cold storage batch API
        self.cold.batch_set(pairs.clone())?;

        // Note: Durability is handled by background checkpoint process

        // Update hot cache and indices
        for (key, value) in pairs {
            self.hot.set(key.clone(), value.clone(), None);

            if let Some(collection_name) = key.split(':').next()
                && !collection_name.starts_with('_') {
                    self.index_value(collection_name, &key, &value)?;
                }
        }

        // Publish events
        for doc_id in &doc_ids {
            if let Ok(Some(doc)) = self.get_document(collection, doc_id) {
                let event = crate::pubsub::ChangeEvent::insert(collection, doc_id, doc);
                let _ = self.pubsub.publish(event);
            }
        }

        Ok(doc_ids)
    }

    /// Update a document by ID
    ///
    /// # Arguments
    /// * `collection` - Collection name
    /// * `doc_id` - Document ID to update
    /// * `data` - New field values to set
    ///
    /// # Returns
    /// Ok(()) on success, or an error if the document doesn't exist
    ///
    /// # Examples
    ///
    /// ```
    /// db.update_document("users", &user_id, vec![
    ///     ("status", Value::String("active".to_string())),
    ///     ("last_login", Value::String(chrono::Utc::now().to_rfc3339())),
    /// ]).await?;
    /// ```
    pub async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        updates: Vec<(&str, Value)>,
    ) -> Result<()> {
        // Get existing document
        let mut document = self
            .get_document(collection, doc_id)?
            .ok_or_else(|| AuroraError::NotFound(format!("Document not found: {}", doc_id)))?;

        // Store old document for event
        let old_document = document.clone();

        // Apply updates
        for (field, value) in updates {
            document.data.insert(field.to_string(), value);
        }

        // Validate unique constraints after update (excluding current document)
        self.validate_unique_constraints_excluding(collection, &document.data, doc_id)
            .await?;

        // Save updated document
        self.put(
            format!("{}:{}", collection, doc_id),
            serde_json::to_vec(&document)?,
            None,
        )?;

        // Publish update event
        let event =
            crate::pubsub::ChangeEvent::update(collection, doc_id, old_document, document.clone());
        let _ = self.pubsub.publish(event);

        Ok(())
    }

    pub async fn get_all_collection(&self, collection: &str) -> Result<Vec<Document>> {
        self.ensure_indices_initialized().await?;
        // Flush write buffer to ensure all data is in cold storage before scanning
        self.flush()?;
        self.scan_collection(collection)
    }

    pub fn get_data_by_pattern(&self, pattern: &str) -> Result<Vec<(String, DataInfo)>> {
        let mut data = Vec::new();

        // Scan from cold storage instead of primary index
        for result in self.cold.scan() {
            if let Ok((key, value)) = result {
                if key.contains(pattern) {
                    let info = if value.starts_with(b"BLOB:") {
                        DataInfo::Blob { size: value.len() }
                    } else {
                        DataInfo::Data {
                            size: value.len(),
                            preview: String::from_utf8_lossy(&value[..value.len().min(50)])
                                .into_owned(),
                        }
                    };

                    data.push((key.clone(), info));
                }
            }
        }

        Ok(data)
    }

    /// Begin a transaction
    ///
    /// All operations after beginning a transaction will be part of the transaction
    /// until either commit_transaction() or rollback_transaction() is called.
    ///
    /// # Returns
    /// Success or an error (e.g., if a transaction is already in progress)
    ///
    /// # Examples
    ///
    /// ```
    /// // Start a transaction for atomic operations
    /// db.begin_transaction()?;
    ///
    /// // Perform multiple operations
    /// db.insert_into("accounts", vec![("user_id", Value::String(user_id)), ("balance", Value::Float(100.0))])?;
    /// db.insert_into("audit_log", vec![("action", Value::String("account_created".to_string()))])?;
    ///
    /// // Commit all changes or roll back if there's an error
    /// if all_ok {
    ///     db.commit_transaction()?;
    /// } else {
    ///     db.rollback_transaction()?;
    /// }
    /// ```
    pub fn begin_transaction(&self) -> crate::transaction::TransactionId {
        let buffer = self.transaction_manager.begin();
        buffer.id
    }

    /// Commit a transaction
    ///
    /// Makes all changes in the transaction permanent.
    ///
    /// # Arguments
    /// * `tx_id` - Transaction ID returned from begin_transaction()
    ///
    /// # Returns
    /// Success or an error if transaction not found
    pub fn commit_transaction(&self, tx_id: crate::transaction::TransactionId) -> Result<()> {
        let buffer = self
            .transaction_manager
            .active_transactions
            .get(&tx_id)
            .ok_or_else(|| {
                AuroraError::InvalidOperation("Transaction not found or already completed".into())
            })?;

        for item in buffer.writes.iter() {
            let key = item.key();
            let value = item.value();
            self.cold.set(key.clone(), value.clone())?;
            self.hot.set(key.clone(), value.clone(), None);
            if let Some(collection_name) = key.split(':').next()
                && !collection_name.starts_with('_') {
                    self.index_value(collection_name, key, value)?;
                }
        }

        for item in buffer.deletes.iter() {
            let key = item.key();
            if let Some((collection, id)) = key.split_once(':')
                && let Ok(Some(doc)) = self.get_document(collection, id) {
                    self.remove_from_indices(collection, &doc)?;
                }
            self.cold.delete(key)?;
            self.hot.delete(key);
        }

        // Drop the buffer reference to release the DashMap read lock
        // before calling commit which needs to remove the entry (write lock)
        drop(buffer);

        self.transaction_manager.commit(tx_id)?;

        self.cold.compact()?;

        Ok(())
    }

    /// Roll back a transaction
    ///
    /// Discards all changes made in the transaction.
    ///
    /// # Arguments
    /// * `tx_id` - Transaction ID returned from begin_transaction()
    ///
    /// # Returns
    /// Success or an error if transaction not found
    pub fn rollback_transaction(&self, tx_id: crate::transaction::TransactionId) -> Result<()> {
        self.transaction_manager.rollback(tx_id)
    }

    pub async fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        // Check if collection exists
        if self.get(&format!("_collection:{}", collection))?.is_none() {
            return Err(AuroraError::CollectionNotFound(collection.to_string()));
        }

        // Generate a default index name
        let index_name = format!("idx_{}_{}", collection, field);

        // Create index definition
        let definition = IndexDefinition {
            name: index_name.clone(),
            collection: collection.to_string(),
            fields: vec![field.to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };

        // Create the index
        let index = Index::new(definition.clone());

        // Index all existing documents in the collection
        let prefix = format!("{}:", collection);
        for result in self.cold.scan_prefix(&prefix) {
            if let Ok((_, data)) = result
                && let Ok(doc) = serde_json::from_slice::<Document>(&data) {
                    let _ = index.insert(&doc);
                }
        }

        // Store the index
        self.indices.insert(index_name, index);

        // Store the index definition for persistence
        let index_key = format!("_index:{}:{}", collection, field);
        self.put(index_key, serde_json::to_vec(&definition)?, None)?;

        Ok(())
    }

    /// Create a query builder for advanced document queries
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to query
    ///
    /// # Returns
    /// A `QueryBuilder` for constructing and executing queries
    ///
    /// # Examples
    ///
    /// ```
    /// // Query for documents matching criteria
    /// let active_premium_users = db.query("users")
    ///     .filter(|f| f.eq("status", "active") && f.eq("plan", "premium"))
    ///     .order_by("joined_date", false)  // newest first
    ///     .limit(10)
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn query<'a>(&'a self, collection: &str) -> QueryBuilder<'a> {
        QueryBuilder::new(self, collection)
    }

    /// Create a search builder for full-text search
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to search
    ///
    /// # Returns
    /// A `SearchBuilder` for configuring and executing searches
    ///
    /// # Examples
    ///
    /// ```
    /// // Search for documents containing text
    /// let search_results = db.search("articles")
    ///     .field("content")
    ///     .matching("quantum computing")
    ///     .fuzzy(true)  // Enable fuzzy matching for typo tolerance
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn search<'a>(&'a self, collection: &str) -> SearchBuilder<'a> {
        SearchBuilder::new(self, collection)
    }

    /// Retrieve a document by ID
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to query
    /// * `id` - ID of the document to retrieve
    ///
    /// # Returns
    /// The document if found, None if not found, or an error
    ///
    /// # Examples
    ///
    /// ```
    /// // Get a document by ID
    /// if let Some(user) = db.get_document("users", &user_id)? {
    ///     println!("Found user: {}", user.data.get("name").unwrap());
    /// } else {
    ///     println!("User not found");
    /// }
    /// ```
    pub fn get_document(&self, collection: &str, id: &str) -> Result<Option<Document>> {
        let key = format!("{}:{}", collection, id);
        if let Some(data) = self.get(&key)? {
            Ok(Some(serde_json::from_slice(&data)?))
        } else {
            Ok(None)
        }
    }

    /// Delete a document by ID
    ///
    /// # Arguments
    /// * `collection` - Name of the collection containing the document
    /// * `id` - ID of the document to delete
    ///
    /// # Returns
    /// Success or an error
    ///
    /// # Examples
    ///
    /// ```
    /// // Delete a specific document
    /// db.delete("users", &user_id)?;
    /// ```
    pub async fn delete(&self, key: &str) -> Result<()> {
        // Extract collection and id from key (format: "collection:id")
        let (collection, id) = if let Some((coll, doc_id)) = key.split_once(':') {
            (coll, doc_id)
        } else {
            return Err(AuroraError::InvalidOperation(
                "Invalid key format, expected 'collection:id'".into(),
            ));
        };

        // CRITICAL FIX: Get document BEFORE deletion to clean up secondary indices
        let document = self.get_document(collection, id)?;

        // Delete from hot cache
        if self.hot.get(key).is_some() {
            self.hot.delete(key);
        }

        // Delete from cold storage
        self.cold.delete(key)?;

        // CRITICAL FIX: Clean up ALL indices (primary + secondary)
        if let Some(doc) = document {
            self.remove_from_indices(collection, &doc)?;
        } else {
            // Fallback: at least remove from primary index
            if let Some(index) = self.primary_indices.get_mut(collection) {
                index.remove(id);
            }
        }

        // Publish delete event
        let event = crate::pubsub::ChangeEvent::delete(collection, id);
        let _ = self.pubsub.publish(event);

        Ok(())
    }

    pub async fn delete_collection(&self, collection: &str) -> Result<()> {
        let prefix = format!("{}:", collection);

        // Get all keys in collection
        let keys: Vec<String> = self
            .cold
            .scan()
            .filter_map(|r| r.ok())
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| k)
            .collect();

        // Delete each key
        for key in keys {
            self.delete(&key).await?;
        }

        // Remove collection indices
        self.primary_indices.remove(collection);
        self.secondary_indices
            .retain(|k, _| !k.starts_with(&prefix));

        // Invalidate schema cache
        self.schema_cache.remove(collection);

        Ok(())
    }

    fn remove_from_indices(&self, collection: &str, doc: &Document) -> Result<()> {
        // Remove from primary index
        if let Some(index) = self.primary_indices.get(collection) {
            index.remove(&doc.id);
        }

        // Remove from secondary indices
        for (field, value) in &doc.data {
            let index_key = format!("{}:{}", collection, field);
            if let Some(index) = self.secondary_indices.get(&index_key)
                && let Some(mut doc_ids) = index.get_mut(&value.to_string()) {
                    doc_ids.retain(|id| id != &doc.id);
                }
        }

        Ok(())
    }

    pub async fn search_text(
        &self,
        collection: &str,
        field: &str,
        query: &str,
    ) -> Result<Vec<Document>> {
        let mut results = Vec::new();
        let docs = self.get_all_collection(collection).await?;

        for doc in docs {
            if let Some(Value::String(text)) = doc.data.get(field)
                && text.to_lowercase().contains(&query.to_lowercase()) {
                    results.push(doc);
                }
        }

        Ok(results)
    }

    /// Export a collection to a JSON file
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to export
    /// * `output_path` - Path to the output JSON file
    ///
    /// # Returns
    /// Success or an error
    ///
    /// # Examples
    ///
    /// ```
    /// // Backup a collection to JSON
    /// db.export_as_json("users", "./backups/users_2023-10-15.json")?;
    /// ```
    pub fn export_as_json(&self, collection: &str, output_path: &str) -> Result<()> {
        let output_path = if !output_path.ends_with(".json") {
            format!("{}.json", output_path)
        } else {
            output_path.to_string()
        };

        let mut docs = Vec::new();

        // Get all documents from the specified collection
        for result in self.cold.scan() {
            let (key, value) = result?;

            // Only process documents from the specified collection
            if let Some(key_collection) = key.split(':').next()
                && key_collection == collection && !key.starts_with("_collection:")
                    && let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                        // Convert Value enum to raw JSON values
                        let mut clean_doc = serde_json::Map::new();
                        for (k, v) in doc.data {
                            match v {
                                Value::String(s) => clean_doc.insert(k, JsonValue::String(s)),
                                Value::Int(i) => clean_doc.insert(k, JsonValue::Number(i.into())),
                                Value::Float(f) => {
                                    if let Some(n) = serde_json::Number::from_f64(f) {
                                        clean_doc.insert(k, JsonValue::Number(n))
                                    } else {
                                        clean_doc.insert(k, JsonValue::Null)
                                    }
                                }
                                Value::Bool(b) => clean_doc.insert(k, JsonValue::Bool(b)),
                                Value::Array(arr) => {
                                    let clean_arr: Vec<JsonValue> = arr
                                        .into_iter()
                                        .map(|v| match v {
                                            Value::String(s) => JsonValue::String(s),
                                            Value::Int(i) => JsonValue::Number(i.into()),
                                            Value::Float(f) => serde_json::Number::from_f64(f)
                                                .map(JsonValue::Number)
                                                .unwrap_or(JsonValue::Null),
                                            Value::Bool(b) => JsonValue::Bool(b),
                                            Value::Null => JsonValue::Null,
                                            _ => JsonValue::Null,
                                        })
                                        .collect();
                                    clean_doc.insert(k, JsonValue::Array(clean_arr))
                                }
                                Value::Uuid(u) => {
                                    clean_doc.insert(k, JsonValue::String(u.to_string()))
                                }
                                Value::Null => clean_doc.insert(k, JsonValue::Null),
                                Value::Object(_) => None, // Handle nested objects if needed
                            };
                        }
                        docs.push(JsonValue::Object(clean_doc));
                    }
        }

        let output = JsonValue::Object(serde_json::Map::from_iter(vec![(
            collection.to_string(),
            JsonValue::Array(docs),
        )]));

        let mut file = StdFile::create(&output_path)?;
        serde_json::to_writer_pretty(&mut file, &output)?;
        println!("Exported collection '{}' to {}", collection, &output_path);
        Ok(())
    }

    /// Export specific collection to CSV file
    pub fn export_as_csv(&self, collection: &str, filename: &str) -> Result<()> {
        let output_path = if !filename.ends_with(".csv") {
            format!("{}.csv", filename)
        } else {
            filename.to_string()
        };

        let mut writer = csv::Writer::from_path(&output_path)?;
        let mut headers = Vec::new();
        let mut first_doc = true;

        // Get all documents from the specified collection
        for result in self.cold.scan() {
            let (key, value) = result?;

            // Only process documents from the specified collection
            if let Some(key_collection) = key.split(':').next()
                && key_collection == collection && !key.starts_with("_collection:")
                    && let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                        // Write headers from first document
                        if first_doc && !doc.data.is_empty() {
                            headers = doc.data.keys().cloned().collect();
                            writer.write_record(&headers)?;
                            first_doc = false;
                        }

                        // Write the document values
                        let values: Vec<String> = headers
                            .iter()
                            .map(|header| {
                                doc.data
                                    .get(header)
                                    .map(|v| v.to_string())
                                    .unwrap_or_default()
                            })
                            .collect();
                        writer.write_record(&values)?;
                    }
        }

        writer.flush()?;
        println!("Exported collection '{}' to {}", collection, &output_path);
        Ok(())
    }

    // Helper method to create filter-based queries
    pub fn find<'a>(&'a self, collection: &str) -> QueryBuilder<'a> {
        self.query(collection)
    }

    // Convenience methods that build on top of the FilterBuilder

    pub async fn find_by_id(&self, collection: &str, id: &str) -> Result<Option<Document>> {
        self.query(collection)
            .filter(|f| f.eq("id", id))
            .first_one()
            .await
    }

    pub async fn find_one<F>(&self, collection: &str, filter_fn: F) -> Result<Option<Document>>
    where
        F: Fn(&FilterBuilder) -> bool + Send + Sync + 'static,
    {
        self.query(collection).filter(filter_fn).first_one().await
    }

    pub async fn find_by_field<T: Into<Value> + Clone + Send + Sync + 'static>(
        &self,
        collection: &str,
        field: &'static str,
        value: T,
    ) -> Result<Vec<Document>> {
        let value_clone = value.clone();
        self.query(collection)
            .filter(move |f| f.eq(field, value_clone.clone()))
            .collect()
            .await
    }

    pub async fn find_by_fields(
        &self,
        collection: &str,
        fields: Vec<(&str, Value)>,
    ) -> Result<Vec<Document>> {
        let mut query = self.query(collection);

        for (field, value) in fields {
            let field_owned = field.to_owned();
            let value_owned = value.clone();
            query = query.filter(move |f| f.eq(&field_owned, value_owned.clone()));
        }

        query.collect().await
    }

    // Advanced example: find documents with a field value in a specific range
    pub async fn find_in_range<T: Into<Value> + Clone + Send + Sync + 'static>(
        &self,
        collection: &str,
        field: &'static str,
        min: T,
        max: T,
    ) -> Result<Vec<Document>> {
        self.query(collection)
            .filter(move |f| f.between(field, min.clone(), max.clone()))
            .collect()
            .await
    }

    // Complex query example: build with multiple combined filters
    pub async fn find_complex<'a>(&'a self, collection: &str) -> QueryBuilder<'a> {
        self.query(collection)
    }

    // Create a full-text search query with added filter options
    pub fn advanced_search<'a>(&'a self, collection: &str) -> SearchBuilder<'a> {
        self.search(collection)
    }

    // Utility methods for common operations
    pub async fn upsert(
        &self,
        collection: &str,
        id: &str,
        data: Vec<(&str, Value)>,
    ) -> Result<String> {
        // Convert Vec<(&str, Value)> to HashMap<String, Value>
        let data_map: HashMap<String, Value> =
            data.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

        // Check if document exists
        if let Some(mut doc) = self.get_document(collection, id)? {
            // Update existing document - merge new data
            for (key, value) in data_map {
                doc.data.insert(key, value);
            }

            // Validate unique constraints for the updated document
            // We need to exclude the current document from the uniqueness check
            self.validate_unique_constraints_excluding(collection, &doc.data, id)
                .await?;

            self.put(
                format!("{}:{}", collection, id),
                serde_json::to_vec(&doc)?,
                None,
            )?;
            Ok(id.to_string())
        } else {
            // Insert new document with specified ID - validate unique constraints
            self.validate_unique_constraints(collection, &data_map)
                .await?;

            let document = Document {
                id: id.to_string(),
                data: data_map,
            };

            self.put(
                format!("{}:{}", collection, id),
                serde_json::to_vec(&document)?,
                None,
            )?;
            Ok(id.to_string())
        }
    }

    // Atomic increment/decrement
    pub async fn increment(
        &self,
        collection: &str,
        id: &str,
        field: &str,
        amount: i64,
    ) -> Result<i64> {
        if let Some(mut doc) = self.get_document(collection, id)? {
            // Get current value
            let current = match doc.data.get(field) {
                Some(Value::Int(i)) => *i,
                _ => 0,
            };

            // Increment
            let new_value = current + amount;
            doc.data.insert(field.to_string(), Value::Int(new_value));

            // Save changes
            self.put(
                format!("{}:{}", collection, id),
                serde_json::to_vec(&doc)?,
                None,
            )?;

            Ok(new_value)
        } else {
            Err(AuroraError::NotFound(format!(
                "Document {}:{} not found",
                collection, id
            )))
        }
    }

    // Delete documents by query
    pub async fn delete_by_query<F>(&self, collection: &str, filter_fn: F) -> Result<usize>
    where
        F: Fn(&FilterBuilder) -> bool + Send + Sync + 'static,
    {
        let docs = self.query(collection).filter(filter_fn).collect().await?;

        let mut deleted_count = 0;

        for doc in docs {
            let key = format!("{}:{}", collection, doc.id);
            self.delete(&key).await?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }

    /// Import documents from a JSON file into a collection
    ///
    /// This method validates documents against the collection schema
    /// and skips documents that already exist in the database.
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to import into
    /// * `filename` - Path to the JSON file containing documents
    ///
    /// # Returns
    /// Statistics about the import operation or an error
    ///
    /// # Examples
    ///
    /// ```
    /// // Import documents from JSON
    /// let stats = db.import_from_json("users", "./data/new_users.json").await?;
    /// println!("Imported: {}, Skipped: {}, Failed: {}",
    ///     stats.imported, stats.skipped, stats.failed);
    /// ```
    pub async fn import_from_json(&self, collection: &str, filename: &str) -> Result<ImportStats> {
        // Validate that the collection exists
        let collection_def = self.get_collection_definition(collection)?;

        // Load JSON file
        let json_string = read_to_string(filename)
            .await
            .map_err(|e| AuroraError::IoError(format!("Failed to read import file: {}", e)))?;

        // Parse JSON
        let documents: Vec<JsonValue> = from_str(&json_string)
            .map_err(|e| AuroraError::SerializationError(format!("Failed to parse JSON: {}", e)))?;

        let mut stats = ImportStats::default();

        // Process each document
        for doc_json in documents {
            match self
                .import_document(collection, &collection_def, doc_json)
                .await
            {
                Ok(ImportResult::Imported) => stats.imported += 1,
                Ok(ImportResult::Skipped) => stats.skipped += 1,
                Err(_) => stats.failed += 1,
            }
        }

        Ok(stats)
    }

    /// Import a single document, performing schema validation and duplicate checking
    async fn import_document(
        &self,
        collection: &str,
        collection_def: &Collection,
        doc_json: JsonValue,
    ) -> Result<ImportResult> {
        if !doc_json.is_object() {
            return Err(AuroraError::InvalidOperation("Expected JSON object".into()));
        }

        // Extract document ID if present
        let doc_id = doc_json
            .get("id")
            .and_then(|id| id.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        // Check if document with this ID already exists
        if self.get_document(collection, &doc_id)?.is_some() {
            return Ok(ImportResult::Skipped);
        }

        // Convert JSON to our document format and validate against schema
        let mut data_map = HashMap::new();

        if let Some(obj) = doc_json.as_object() {
            for (field_name, field_def) in &collection_def.fields {
                if let Some(json_value) = obj.get(field_name) {
                    // Validate value against field type
                    if !self.validate_field_value(json_value, &field_def.field_type) {
                        return Err(AuroraError::InvalidOperation(format!(
                            "Field '{}' has invalid type",
                            field_name
                        )));
                    }

                    // Convert JSON value to our Value type
                    let value = self.json_to_value(json_value)?;
                    data_map.insert(field_name.clone(), value);
                } else if field_def.unique {
                    // Missing required unique field
                    return Err(AuroraError::InvalidOperation(format!(
                        "Missing required unique field '{}'",
                        field_name
                    )));
                }
            }
        }

        // Check for duplicates by unique fields
        let unique_fields = self.get_unique_fields(collection_def);
        for unique_field in &unique_fields {
            if let Some(value) = data_map.get(unique_field) {
                // Query for existing documents with this unique value
                let query_results = self
                    .query(collection)
                    .filter(move |f| f.eq(unique_field, value.clone()))
                    .limit(1)
                    .collect()
                    .await?;

                if !query_results.is_empty() {
                    // Found duplicate by unique field
                    return Ok(ImportResult::Skipped);
                }
            }
        }

        // Create and insert document
        let document = Document {
            id: doc_id,
            data: data_map,
        };

        self.put(
            format!("{}:{}", collection, document.id),
            serde_json::to_vec(&document)?,
            None,
        )?;

        Ok(ImportResult::Imported)
    }

    /// Validate that a JSON value matches the expected field type
    fn validate_field_value(&self, value: &JsonValue, field_type: &FieldType) -> bool {
        match field_type {
            FieldType::String => value.is_string(),
            FieldType::Int => value.is_i64() || value.is_u64(),
            FieldType::Float => value.is_number(),
            FieldType::Bool => value.is_boolean(),
            FieldType::Array => value.is_array(),
            FieldType::Object => value.is_object(),
            FieldType::Uuid => {
                value.is_string() && Uuid::parse_str(value.as_str().unwrap_or("")).is_ok()
            }
        }
    }

    /// Convert a JSON value to our internal Value type
    #[allow(clippy::only_used_in_recursion)]
    fn json_to_value(&self, json_value: &JsonValue) -> Result<Value> {
        match json_value {
            JsonValue::Null => Ok(Value::Null),
            JsonValue::Bool(b) => Ok(Value::Bool(*b)),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    Err(AuroraError::InvalidOperation("Invalid number value".into()))
                }
            }
            JsonValue::String(s) => {
                // Try parsing as UUID first
                if let Ok(uuid) = Uuid::parse_str(s) {
                    Ok(Value::Uuid(uuid))
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            JsonValue::Array(arr) => {
                let mut values = Vec::new();
                for item in arr {
                    values.push(self.json_to_value(item)?);
                }
                Ok(Value::Array(values))
            }
            JsonValue::Object(obj) => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), self.json_to_value(v)?);
                }
                Ok(Value::Object(map))
            }
        }
    }

    /// Get collection definition
    fn get_collection_definition(&self, collection: &str) -> Result<Collection> {
        if let Some(data) = self.get(&format!("_collection:{}", collection))? {
            let collection_def: Collection = serde_json::from_slice(&data)?;
            Ok(collection_def)
        } else {
            Err(AuroraError::CollectionNotFound(collection.to_string()))
        }
    }

    /// Get storage statistics and information about the database
    pub fn get_database_stats(&self) -> Result<DatabaseStats> {
        let hot_stats = self.hot.get_stats();
        let cold_stats = self.cold.get_stats()?;

        Ok(DatabaseStats {
            hot_stats,
            cold_stats,
            estimated_size: self.cold.estimated_size(),
            collections: self.get_collection_stats()?,
        })
    }

    /// Check if a key is currently stored in the hot cache
    pub fn is_in_hot_cache(&self, key: &str) -> bool {
        self.hot.is_hot(key)
    }

    /// Start background cleanup of hot cache with specified interval
    pub async fn start_hot_cache_maintenance(&self, interval_secs: u64) {
        let hot_store = Arc::new(self.hot.clone());
        hot_store.start_cleanup_with_interval(interval_secs).await;
    }

    /// Clear the hot cache (useful when memory needs to be freed)
    pub fn clear_hot_cache(&self) {
        self.hot.clear();
        println!(
            "Hot cache cleared, current hit ratio: {:.2}%",
            self.hot.hit_ratio() * 100.0
        );
    }

    /// Prewarm the cache by loading frequently accessed data from cold storage
    ///
    /// This loads the most recently modified documents from a collection into
    /// the hot cache to improve initial query performance after startup.
    ///
    /// # Arguments
    /// * `collection` - The collection to prewarm
    /// * `limit` - Maximum number of documents to load (default: 1000)
    ///
    /// # Returns
    /// Number of documents loaded into cache
    pub async fn prewarm_cache(&self, collection: &str, limit: Option<usize>) -> Result<usize> {
        let limit = limit.unwrap_or(1000);
        let prefix = format!("{}:", collection);
        let mut loaded = 0;

        for entry in self.cold.scan_prefix(&prefix) {
            if loaded >= limit {
                break;
            }

            if let Ok((key, value)) = entry {
                // Load into hot cache
                self.hot.set(key.clone(), value, None);
                loaded += 1;
            }
        }

        println!("Prewarmed {} with {} documents", collection, loaded);
        Ok(loaded)
    }

    /// Prewarm cache for all collections
    pub async fn prewarm_all_collections(
        &self,
        docs_per_collection: Option<usize>,
    ) -> Result<HashMap<String, usize>> {
        let mut stats = HashMap::new();

        // Get all collections
        let collections: Vec<String> = self
            .cold
            .scan()
            .filter_map(|r| r.ok())
            .map(|(k, _)| k)
            .filter(|k| k.starts_with("_collection:"))
            .map(|k| k.trim_start_matches("_collection:").to_string())
            .collect();

        for collection in collections {
            let loaded = self.prewarm_cache(&collection, docs_per_collection).await?;
            stats.insert(collection, loaded);
        }

        Ok(stats)
    }

    /// Store multiple key-value pairs efficiently in a single batch operation
    pub fn batch_write(&self, pairs: Vec<(String, Vec<u8>)>) -> Result<()> {
        // Group pairs by collection name
        let mut collections: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
        for (key, value) in &pairs {
            if let Some(collection_name) = key.split(':').next() {
                collections
                    .entry(collection_name.to_string())
                    .or_default()
                    .push((key.clone(), value.clone()));
            }
        }

        // First, do the batch write to cold storage for all pairs
        self.cold.batch_set(pairs)?;

        // Then, process each collection for in-memory updates
        for (collection_name, batch) in collections {
            // --- Optimized Batch Indexing ---

            // 1. Get schema once for the entire collection batch
            let collection_obj = match self.schema_cache.get(&collection_name) {
                Some(cached_schema) => Arc::clone(cached_schema.value()),
                None => {
                    let collection_key = format!("_collection:{}", collection_name);
                    match self.get(&collection_key)? {
                        Some(data) => {
                            let obj: Collection = serde_json::from_slice(&data)?;
                            let arc_obj = Arc::new(obj);
                            self.schema_cache
                                .insert(collection_name.to_string(), Arc::clone(&arc_obj));
                            arc_obj
                        }
                        None => continue,
                    }
                }
            };

            let indexed_fields: Vec<String> = collection_obj
                .fields
                .iter()
                .filter(|(_, def)| def.indexed || def.unique)
                .map(|(name, _)| name.clone())
                .collect();

            let primary_index = self
                .primary_indices
                .entry(collection_name.to_string())
                .or_default();

            for (key, value) in batch {
                // 2. Update hot cache
                self.hot.set(key.clone(), value.clone(), None);

                // 3. Update primary index with metadata only
                let location = DiskLocation::new(value.len());
                primary_index.insert(key.clone(), location);

                // 4. Update secondary indices
                if !indexed_fields.is_empty()
                    && let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                        for (field, field_value) in doc.data {
                            if indexed_fields.contains(&field) {
                                let value_str = match &field_value {
                                    Value::String(s) => s.clone(),
                                    _ => field_value.to_string(),
                                };
                                let index_key = format!("{}:{}", collection_name, field);
                                let secondary_index = self
                                    .secondary_indices
                                    .entry(index_key)
                                    .or_default();

                                let max_entries = self.config.max_index_entries_per_field;
                                secondary_index
                                    .entry(value_str)
                                    .and_modify(|doc_ids| {
                                        if doc_ids.len() < max_entries {
                                            doc_ids.push(key.to_string());
                                        }
                                    })
                                    .or_insert_with(|| vec![key.to_string()]);
                            }
                        }
                    }
            }
        }

        Ok(())
    }

    /// Scan for keys with a specific prefix
    pub fn scan_with_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.cold.scan_prefix(prefix)
    }

    /// Get storage efficiency metrics for the database
    pub fn get_collection_stats(&self) -> Result<HashMap<String, CollectionStats>> {
        let mut stats = HashMap::new();

        // Scan all collections
        let collections: Vec<String> = self
            .cold
            .scan()
            .filter_map(|r| r.ok())
            .map(|(k, _)| k)
            .filter(|k| k.starts_with("_collection:"))
            .map(|k| k.trim_start_matches("_collection:").to_string())
            .collect();

        for collection in collections {
            let prefix = format!("{}:", collection);

            // Count documents
            let count = self.cold.scan_prefix(&prefix).count();

            // Estimate size
            let size: usize = self
                .cold
                .scan_prefix(&prefix)
                .filter_map(|r| r.ok())
                .map(|(_, v)| v.len())
                .sum();

            stats.insert(
                collection,
                CollectionStats {
                    count,
                    size_bytes: size,
                    avg_doc_size: if count > 0 { size / count } else { 0 },
                },
            );
        }

        Ok(stats)
    }

    /// Search for documents by exact value using an index
    ///
    /// This method performs a fast lookup using a pre-created index
    pub fn search_by_value(
        &self,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> Result<Vec<Document>> {
        let index_key = format!("_index:{}:{}", collection, field);

        if let Some(index_data) = self.get(&index_key)? {
            let index_def: IndexDefinition = serde_json::from_slice(&index_data)?;
            let index = Index::new(index_def);

            // Use the previously unused search method
            if let Some(doc_ids) = index.search(value) {
                // Load the documents by ID
                let mut docs = Vec::new();
                for id in doc_ids {
                    if let Some(doc_data) = self.get(&format!("{}:{}", collection, id))? {
                        let doc: Document = serde_json::from_slice(&doc_data)?;
                        docs.push(doc);
                    }
                }
                return Ok(docs);
            }
        }

        // Return empty result if no index or no matches
        Ok(Vec::new())
    }

    /// Perform a full-text search on an indexed text field
    ///
    /// This provides more advanced text search capabilities including
    /// relevance ranking of results
    pub fn full_text_search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
    ) -> Result<Vec<Document>> {
        let index_key = format!("_index:{}:{}", collection, field);

        if let Some(index_data) = self.get(&index_key)? {
            let index_def: IndexDefinition = serde_json::from_slice(&index_data)?;

            // Ensure this is a full-text index
            if !matches!(index_def.index_type, IndexType::FullText) {
                return Err(AuroraError::InvalidOperation(format!(
                    "Field '{}' is not indexed as full-text",
                    field
                )));
            }

            let index = Index::new(index_def);

            // Use the previously unused search_text method
            if let Some(doc_id_scores) = index.search_text(query) {
                // Load the documents by ID, preserving score order
                let mut docs = Vec::new();
                for (id, _score) in doc_id_scores {
                    if let Some(doc_data) = self.get(&format!("{}:{}", collection, id))? {
                        let doc: Document = serde_json::from_slice(&doc_data)?;
                        docs.push(doc);
                    }
                }
                return Ok(docs);
            }
        }

        // Return empty result if no index or no matches
        Ok(Vec::new())
    }

    /// Create a full-text search index on a text field
    pub fn create_text_index(
        &self,
        collection: &str,
        field: &str,
        _enable_stop_words: bool,
    ) -> Result<()> {
        // Check if collection exists
        if self.get(&format!("_collection:{}", collection))?.is_none() {
            return Err(AuroraError::CollectionNotFound(collection.to_string()));
        }

        // Create index definition
        let index_def = IndexDefinition {
            name: format!("{}_{}_fulltext", collection, field),
            collection: collection.to_string(),
            fields: vec![field.to_string()],
            index_type: IndexType::FullText,
            unique: false,
        };

        // Store index definition
        let index_key = format!("_index:{}:{}", collection, field);
        self.put(index_key, serde_json::to_vec(&index_def)?, None)?;

        // Create the actual index
        let index = Index::new(index_def);

        // Index all existing documents in the collection
        let prefix = format!("{}:", collection);
        for (_, data) in self.cold.scan_prefix(&prefix).flatten() {
            let doc: Document = serde_json::from_slice(&data)?;
            index.insert(&doc)?;
        }

        Ok(())
    }

    pub async fn execute_simple_query(
        &self,
        builder: &SimpleQueryBuilder,
    ) -> Result<Vec<Document>> {
        // Ensure indices are initialized
        self.ensure_indices_initialized().await?;

        // A place to store the IDs of the documents we need to fetch
        let mut doc_ids_to_load: Option<Vec<String>> = None;
        let mut used_filter_index: Option<usize> = None;

        // --- The "Query Planner" ---
        // Look for an opportunity to use an index
        for (filter_idx, filter) in builder.filters.iter().enumerate() {
            match filter {
                Filter::Eq(field, value) => {
                    let index_key = format!("{}:{}", &builder.collection, field);

                    // Do we have a secondary index for this field?
                    if let Some(index) = self.secondary_indices.get(&index_key) {
                        // Yes! Let's use it.
                        if let Some(matching_ids) = index.get(&value.to_string()) {
                            doc_ids_to_load = Some(matching_ids.clone());
                            used_filter_index = Some(filter_idx);
                            break; // Stop searching for other indexes for now
                        }
                    }
                }
                Filter::Gt(field, value)
                | Filter::Gte(field, value)
                | Filter::Lt(field, value)
                | Filter::Lte(field, value) => {
                    let index_key = format!("{}:{}", &builder.collection, field);

                    // Do we have a secondary index for this field?
                    if let Some(index) = self.secondary_indices.get(&index_key) {
                        // For range queries, we need to scan through the index values
                        let mut matching_ids = Vec::new();

                        for entry in index.iter() {
                            let index_value_str = entry.key();

                            // Try to parse the index value to compare with our filter value
                            if let Ok(index_value) =
                                self.parse_value_from_string(index_value_str, value)
                            {
                                let matches = match filter {
                                    Filter::Gt(_, filter_val) => index_value > *filter_val,
                                    Filter::Gte(_, filter_val) => index_value >= *filter_val,
                                    Filter::Lt(_, filter_val) => index_value < *filter_val,
                                    Filter::Lte(_, filter_val) => index_value <= *filter_val,
                                    _ => false,
                                };

                                if matches {
                                    matching_ids.extend(entry.value().clone());
                                }
                            }
                        }

                        if !matching_ids.is_empty() {
                            doc_ids_to_load = Some(matching_ids);
                            used_filter_index = Some(filter_idx);
                            break;
                        }
                    }
                }
                Filter::Contains(field, search_term) => {
                    let index_key = format!("{}:{}", &builder.collection, field);

                    // Do we have a secondary index for this field?
                    if let Some(index) = self.secondary_indices.get(&index_key) {
                        let mut matching_ids = Vec::new();

                        for entry in index.iter() {
                            let index_value_str = entry.key();

                            // Check if this indexed value contains our search term
                            if index_value_str
                                .to_lowercase()
                                .contains(&search_term.to_lowercase())
                            {
                                matching_ids.extend(entry.value().clone());
                            }
                        }

                        if !matching_ids.is_empty() {
                            // Remove duplicates since a document could match multiple indexed values
                            matching_ids.sort();
                            matching_ids.dedup();

                            doc_ids_to_load = Some(matching_ids);
                            used_filter_index = Some(filter_idx);
                            break;
                        }
                    }
                }
            }
        }

        let mut final_docs: Vec<Document>;

        if let Some(ids) = doc_ids_to_load {
            final_docs = Vec::with_capacity(ids.len());

            for id in ids {
                let doc_key = format!("{}:{}", &builder.collection, id);
                if let Some(data) = self.get(&doc_key)?
                    && let Ok(doc) = serde_json::from_slice::<Document>(&data) {
                        final_docs.push(doc);
                    }
            }
        } else {
            // --- Path 2: Full Collection Scan (Fallback) ---

            final_docs = self.get_all_collection(&builder.collection).await?;
        }

        // Now, apply the *rest* of the filters in memory
        // This is important for queries with multiple filters, where only one might be indexed
        // Skip the filter we already used for the index lookup
        final_docs.retain(|doc| {
            builder.filters.iter().enumerate().all(|(idx, filter)| {
                // Skip the filter we already used for index lookup
                if Some(idx) == used_filter_index {
                    return true;
                }

                match filter {
                    Filter::Eq(field, value) => doc.data.get(field) == Some(value),
                    Filter::Gt(field, value) => doc.data.get(field).is_some_and(|v| v > value),
                    Filter::Gte(field, value) => doc.data.get(field).is_some_and(|v| v >= value),
                    Filter::Lt(field, value) => doc.data.get(field).is_some_and(|v| v < value),
                    Filter::Lte(field, value) => doc.data.get(field).is_some_and(|v| v <= value),
                    Filter::Contains(field, value_str) => {
                        doc.data.get(field).is_some_and(|v| match v {
                            Value::String(s) => s.contains(value_str),
                            Value::Array(arr) => arr.contains(&Value::String(value_str.clone())),
                            _ => false,
                        })
                    }
                }
            })
        });

        // Apply ordering
        if let Some((field, ascending)) = &builder.order_by {
            final_docs.sort_by(|a, b| match (a.data.get(field), b.data.get(field)) {
                (Some(v1), Some(v2)) => {
                    let cmp = v1.cmp(v2);
                    if *ascending {
                        cmp
                    } else {
                        cmp.reverse()
                    }
                }
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            });
        }

        // Apply offset and limit
        let start = builder.offset.unwrap_or(0);
        let end = builder
            .limit
            .map(|l| start.saturating_add(l))
            .unwrap_or(final_docs.len());

        let end = end.min(final_docs.len());
        Ok(final_docs.get(start..end).unwrap_or(&[]).to_vec())
    }

    /// Helper method to parse a string value back to a Value for comparison
    fn parse_value_from_string(&self, value_str: &str, reference_value: &Value) -> Result<Value> {
        match reference_value {
            Value::Int(_) => {
                if let Ok(i) = value_str.parse::<i64>() {
                    Ok(Value::Int(i))
                } else {
                    Err(AuroraError::InvalidOperation("Failed to parse int".into()))
                }
            }
            Value::Float(_) => {
                if let Ok(f) = value_str.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Err(AuroraError::InvalidOperation(
                        "Failed to parse float".into(),
                    ))
                }
            }
            Value::String(_) => Ok(Value::String(value_str.to_string())),
            _ => Ok(Value::String(value_str.to_string())),
        }
    }

    pub async fn execute_dynamic_query(
        &self,
        collection: &str,
        payload: &QueryPayload,
    ) -> Result<Vec<Document>> {
        let mut docs = self.get_all_collection(collection).await?;

        // 1. Apply Filters
        if let Some(filters) = &payload.filters {
            docs.retain(|doc| {
                filters.iter().all(|filter| {
                    doc.data
                        .get(&filter.field)
                        .is_some_and(|doc_val| check_filter(doc_val, filter))
                })
            });
        }

        // 2. Apply Sorting
        if let Some(sort_options) = &payload.sort {
            docs.sort_by(|a, b| {
                let a_val = a.data.get(&sort_options.field);
                let b_val = b.data.get(&sort_options.field);
                let ordering = a_val
                    .partial_cmp(&b_val)
                    .unwrap_or(std::cmp::Ordering::Equal);
                if sort_options.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            });
        }

        // 3. Apply Pagination
        if let Some(offset) = payload.offset {
            docs = docs.into_iter().skip(offset).collect();
        }
        if let Some(limit) = payload.limit {
            docs = docs.into_iter().take(limit).collect();
        }

        // 4. Apply Field Selection (Projection)
        if let Some(select_fields) = &payload.select
            && !select_fields.is_empty() {
                docs = docs
                    .into_iter()
                    .map(|mut doc| {
                        doc.data.retain(|key, _| select_fields.contains(key));
                        doc
                    })
                    .collect();
            }

        Ok(docs)
    }

    pub async fn process_network_request(
        &self,
        request: crate::network::protocol::Request,
    ) -> crate::network::protocol::Response {
        use crate::network::protocol::Response;

        match request {
            crate::network::protocol::Request::Get(key) => match self.get(&key) {
                Ok(value) => Response::Success(value),
                Err(e) => Response::Error(e.to_string()),
            },
            crate::network::protocol::Request::Put(key, value) => {
                match self.put(key, value, None) {
                    Ok(_) => Response::Done,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::Delete(key) => match self.delete(&key).await {
                Ok(_) => Response::Done,
                Err(e) => Response::Error(e.to_string()),
            },
            crate::network::protocol::Request::NewCollection { name, fields } => {
                let fields_for_db: Vec<(String, crate::types::FieldType, bool)> = fields
                    .iter()
                    .map(|(name, ft, unique)| (name.clone(), ft.clone(), *unique))
                    .collect();

                match self.new_collection(&name, fields_for_db) {
                    Ok(_) => Response::Done,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::Insert { collection, data } => {
                match self.insert_map(&collection, data).await {
                    Ok(id) => Response::Message(id),
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::GetDocument { collection, id } => {
                match self.get_document(&collection, &id) {
                    Ok(doc) => Response::Document(doc),
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::Query(builder) => {
                match self.execute_simple_query(&builder).await {
                    Ok(docs) => Response::Documents(docs),
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::BeginTransaction => {
                let tx_id = self.begin_transaction();
                Response::TransactionId(tx_id.as_u64())
            }
            crate::network::protocol::Request::CommitTransaction(tx_id_u64) => {
                let tx_id = crate::transaction::TransactionId::from_u64(tx_id_u64);
                match self.commit_transaction(tx_id) {
                    Ok(_) => Response::Done,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::RollbackTransaction(tx_id_u64) => {
                let tx_id = crate::transaction::TransactionId::from_u64(tx_id_u64);
                match self.rollback_transaction(tx_id) {
                    Ok(_) => Response::Done,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
        }
    }

    /// Create indices for commonly queried fields automatically
    ///
    /// This is a convenience method that creates indices for fields that are
    /// likely to be queried frequently, improving performance.
    ///
    /// # Arguments
    /// * `collection` - Name of the collection
    /// * `fields` - List of field names to create indices for
    ///
    /// # Examples
    /// ```
    /// // Create indices for commonly queried fields
    /// db.create_indices("users", &["email", "status", "created_at"]).await?;
    /// ```
    pub async fn create_indices(&self, collection: &str, fields: &[&str]) -> Result<()> {
        for field in fields {
            if let Err(e) = self.create_index(collection, field).await {
                eprintln!(
                    "Warning: Failed to create index for {}.{}: {}",
                    collection, field, e
                );
            } else {
                println!("Created index for {}.{}", collection, field);
            }
        }
        Ok(())
    }

    /// Get index statistics for a collection
    ///
    /// This helps understand which indices exist and how effective they are.
    pub fn get_index_stats(&self, collection: &str) -> HashMap<String, IndexStats> {
        let mut stats = HashMap::new();

        for entry in self.secondary_indices.iter() {
            let key = entry.key();
            if key.starts_with(&format!("{}:", collection)) {
                let field = key.split(':').nth(1).unwrap_or("unknown");
                let index = entry.value();

                let unique_values = index.len();
                let total_documents: usize = index.iter().map(|entry| entry.value().len()).sum();

                stats.insert(
                    field.to_string(),
                    IndexStats {
                        unique_values,
                        total_documents,
                        avg_docs_per_value: if unique_values > 0 {
                            total_documents / unique_values
                        } else {
                            0
                        },
                    },
                );
            }
        }

        stats
    }

    /// Optimize a collection by creating indices for frequently filtered fields
    ///
    /// This analyzes common query patterns and suggests/creates optimal indices.
    pub async fn optimize_collection(&self, collection: &str) -> Result<()> {
        if let Ok(collection_def) = self.get_collection_definition(collection) {
            let field_names: Vec<&str> = collection_def.fields.keys().map(|s| s.as_str()).collect();
            self.create_indices(collection, &field_names).await?;
        }

        Ok(())
    }

    // Helper method to get unique fields from a collection
    fn get_unique_fields(&self, collection: &Collection) -> Vec<String> {
        collection
            .fields
            .iter()
            .filter(|(_, def)| def.unique)
            .map(|(name, _)| name.clone())
            .collect()
    }

    // Update the validation method to use the helper
    async fn validate_unique_constraints(
        &self,
        collection: &str,
        data: &HashMap<String, Value>,
    ) -> Result<()> {
        self.ensure_indices_initialized().await?;
        let collection_def = self.get_collection_definition(collection)?;
        let unique_fields = self.get_unique_fields(&collection_def);

        for unique_field in &unique_fields {
            if let Some(value) = data.get(unique_field) {
                let index_key = format!("{}:{}", collection, unique_field);
                if let Some(index) = self.secondary_indices.get(&index_key) {
                    // Get the raw string value without JSON formatting
                    let value_str = match value {
                        Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    if index.contains_key(&value_str) {
                        return Err(AuroraError::UniqueConstraintViolation(
                            unique_field.clone(),
                            value_str,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate unique constraints excluding a specific document ID (for updates)
    async fn validate_unique_constraints_excluding(
        &self,
        collection: &str,
        data: &HashMap<String, Value>,
        exclude_id: &str,
    ) -> Result<()> {
        self.ensure_indices_initialized().await?;
        let collection_def = self.get_collection_definition(collection)?;
        let unique_fields = self.get_unique_fields(&collection_def);

        for unique_field in &unique_fields {
            if let Some(value) = data.get(unique_field) {
                let index_key = format!("{}:{}", collection, unique_field);
                if let Some(index) = self.secondary_indices.get(&index_key) {
                    // Get the raw string value without JSON formatting
                    let value_str = match value {
                        Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    if let Some(doc_ids) = index.get(&value_str) {
                        // Check if any document other than the excluded one has this value
                        let exclude_key = format!("{}:{}", collection, exclude_id);
                        for doc_key in doc_ids.value() {
                            if doc_key != &exclude_key {
                                return Err(AuroraError::UniqueConstraintViolation(
                                    unique_field.clone(),
                                    value_str,
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for Aurora {
    fn drop(&mut self) {
        // Signal checkpoint task to shutdown gracefully
        if let Some(ref shutdown_tx) = self.checkpoint_shutdown {
            let _ = shutdown_tx.send(());
        }
        // Signal compaction task to shutdown gracefully
        if let Some(ref shutdown_tx) = self.compaction_shutdown {
            let _ = shutdown_tx.send(());
        }
    }
}

fn check_filter(doc_val: &Value, filter: &HttpFilter) -> bool {
    let filter_val = match json_to_value(&filter.value) {
        Ok(v) => v,
        Err(_) => return false,
    };

    match filter.operator {
        FilterOperator::Eq => doc_val == &filter_val,
        FilterOperator::Ne => doc_val != &filter_val,
        FilterOperator::Gt => doc_val > &filter_val,
        FilterOperator::Gte => doc_val >= &filter_val,
        FilterOperator::Lt => doc_val < &filter_val,
        FilterOperator::Lte => doc_val <= &filter_val,
        FilterOperator::Contains => match (doc_val, &filter_val) {
            (Value::String(s), Value::String(fv)) => s.contains(fv),
            (Value::Array(arr), _) => arr.contains(&filter_val),
            _ => false,
        },
    }
}

/// Results of importing a document
enum ImportResult {
    Imported,
    Skipped,
}

/// Statistics from an import operation
#[derive(Debug, Default)]
pub struct ImportStats {
    /// Number of documents successfully imported
    pub imported: usize,
    /// Number of documents skipped (usually because they already exist)
    pub skipped: usize,
    /// Number of documents that failed to import
    pub failed: usize,
}

/// Statistics for a specific collection
#[derive(Debug)]
pub struct CollectionStats {
    /// Number of documents in the collection
    pub count: usize,
    /// Total size of the collection in bytes
    pub size_bytes: usize,
    /// Average document size in bytes
    pub avg_doc_size: usize,
}

/// Statistics for an index
#[derive(Debug)]
pub struct IndexStats {
    /// Number of unique values in the index
    pub unique_values: usize,
    /// Total number of documents covered by the index
    pub total_documents: usize,
    /// Average number of documents per unique value
    pub avg_docs_per_value: usize,
}

/// Combined database statistics
#[derive(Debug)]
pub struct DatabaseStats {
    /// Hot cache statistics
    pub hot_stats: crate::storage::hot::CacheStats,
    /// Cold storage statistics
    pub cold_stats: crate::storage::cold::ColdStoreStats,
    /// Estimated total database size in bytes
    pub estimated_size: u64,
    /// Statistics for each collection
    pub collections: HashMap<String, CollectionStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Test collection creation
        db.new_collection(
            "users",
            vec![
                ("name", FieldType::String, false),
                ("age", FieldType::Int, false),
                ("email", FieldType::String, true),
            ],
        )?;

        // Test document insertion
        let doc_id = db
            .insert_into(
                "users",
                vec![
                    ("name", Value::String("John Doe".to_string())),
                    ("age", Value::Int(30)),
                    ("email", Value::String("john@example.com".to_string())),
                ],
            )
            .await?;

        // Test document retrieval
        let doc = db.get_document("users", &doc_id)?.unwrap();
        assert_eq!(
            doc.data.get("name").unwrap(),
            &Value::String("John Doe".to_string())
        );
        assert_eq!(doc.data.get("age").unwrap(), &Value::Int(30));

        Ok(())
    }

    #[tokio::test]
    async fn test_transactions() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Create collection
        db.new_collection("test", vec![("field", FieldType::String, false)])?;

        // Start transaction
        let tx_id = db.begin_transaction();

        // Insert document
        let doc_id = db
            .insert_into("test", vec![("field", Value::String("value".to_string()))])
            .await?;

        // Commit transaction
        db.commit_transaction(tx_id)?;

        // Verify document exists
        let doc = db.get_document("test", &doc_id)?.unwrap();
        assert_eq!(
            doc.data.get("field").unwrap(),
            &Value::String("value".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_query_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Test collection creation
        db.new_collection(
            "books",
            vec![
                ("title", FieldType::String, false),
                ("author", FieldType::String, false),
                ("year", FieldType::Int, false),
            ],
        )?;

        // Test document insertion
        db.insert_into(
            "books",
            vec![
                ("title", Value::String("Book 1".to_string())),
                ("author", Value::String("Author 1".to_string())),
                ("year", Value::Int(2020)),
            ],
        )
        .await?;

        db.insert_into(
            "books",
            vec![
                ("title", Value::String("Book 2".to_string())),
                ("author", Value::String("Author 2".to_string())),
                ("year", Value::Int(2021)),
            ],
        )
        .await?;

        // Test query
        let results = db
            .query("books")
            .filter(|f| f.gt("year", Value::Int(2019)))
            .order_by("year", true)
            .collect()
            .await?;

        assert_eq!(results.len(), 2);
        assert!(results[0].data.get("year").unwrap() < results[1].data.get("year").unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Create test file
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, b"Hello, World!")?;

        // Test blob storage
        db.put_blob("test:blob".to_string(), &file_path).await?;

        // Verify blob exists
        let data = db.get_data_by_pattern("test:blob")?;
        assert_eq!(data.len(), 1);
        match &data[0].1 {
            DataInfo::Blob { size } => assert_eq!(*size, 13 + 5), // content + "BLOB:" prefix
            _ => panic!("Expected Blob type"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_size_limit() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Create a test file that's too large (201MB)
        let large_file_path = temp_dir.path().join("large_file.bin");
        let large_data = vec![0u8; 201 * 1024 * 1024];
        std::fs::write(&large_file_path, &large_data)?;

        // Attempt to store the large file
        let result = db
            .put_blob("test:large_blob".to_string(), &large_file_path)
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AuroraError::InvalidOperation(_)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_constraints() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Create collection with unique email field
        db.new_collection(
            "users",
            vec![
                ("name", FieldType::String, false),
                ("email", FieldType::String, true), // unique field
                ("age", FieldType::Int, false),
            ],
        )?;

        // Insert first document
        let _doc_id1 = db
            .insert_into(
                "users",
                vec![
                    ("name", Value::String("John Doe".to_string())),
                    ("email", Value::String("john@example.com".to_string())),
                    ("age", Value::Int(30)),
                ],
            )
            .await?;

        // Try to insert second document with same email - should fail
        let result = db
            .insert_into(
                "users",
                vec![
                    ("name", Value::String("Jane Doe".to_string())),
                    ("email", Value::String("john@example.com".to_string())), // duplicate email
                    ("age", Value::Int(25)),
                ],
            )
            .await;

        assert!(result.is_err());
        if let Err(AuroraError::UniqueConstraintViolation(field, value)) = result {
            assert_eq!(field, "email");
            assert_eq!(value, "john@example.com");
        } else {
            panic!("Expected UniqueConstraintViolation error");
        }

        // Test upsert with unique constraint
        // Should succeed for new document
        let _doc_id2 = db
            .upsert(
                "users",
                "user2",
                vec![
                    ("name", Value::String("Alice Smith".to_string())),
                    ("email", Value::String("alice@example.com".to_string())),
                    ("age", Value::Int(28)),
                ],
            )
            .await?;

        // Should fail when trying to upsert with duplicate email
        let result = db
            .upsert(
                "users",
                "user3",
                vec![
                    ("name", Value::String("Bob Wilson".to_string())),
                    ("email", Value::String("alice@example.com".to_string())), // duplicate
                    ("age", Value::Int(35)),
                ],
            )
            .await;

        assert!(result.is_err());

        // Should succeed when updating existing document with same email (no change)
        let result = db
            .upsert(
                "users",
                "user2",
                vec![
                    ("name", Value::String("Alice Updated".to_string())),
                    ("email", Value::String("alice@example.com".to_string())), // same email, same doc
                    ("age", Value::Int(29)),
                ],
            )
            .await;

        assert!(result.is_ok());

        Ok(())
    }
}
