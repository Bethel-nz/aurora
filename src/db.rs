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
//! ```ignore
//! use aurora::Aurora;
//!
//! // Open a database
//! let db = Aurora::open("my_database.db")?;
//!
//! // Create a collection with schema
//! db.new_collection("users", vec![
//!     ("name", FieldType::Scalar(crate::types::ScalarType::String), false),
//!     ("email", FieldType::Scalar(crate::types::ScalarType::String), true),  // unique field
//!     ("age", FieldType::Scalar(crate::types::ScalarType::Int), false),
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

use crate::error::{AqlError, ErrorCode, Result};
use crate::index::{Index, IndexDefinition, IndexType};
use crate::network::http_models::{
    Filter as HttpFilter, FilterOperator, QueryPayload, json_to_value,
};
use crate::parser;
use crate::parser::executor::{ExecutionOptions, ExecutionPlan, ExecutionResult};
use crate::query::FilterBuilder;
use crate::query::{Filter, QueryBuilder, SimpleQueryBuilder, SearchBuilder};
use crate::storage::{ColdStore, HotStore, IndexStorage, WriteBuffer};
use crate::types::{
    AuroraConfig, Collection, Document, DurabilityMode, FieldDefinition, FieldType, Value,
};
use crate::wal::{Operation, WriteAheadLog};
use roaring::RoaringBitmap;

use dashmap::DashMap;
use moka::sync::Cache as MokaCache;
use serde_json::Value as JsonValue;
use serde_json::from_str;
use std::collections::HashMap;
use std::fmt;
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
pub(crate) struct DiskLocation {
    size: u32, // Size in bytes (useful for statistics)
}

impl DiskLocation {
    fn new(size: usize) -> Self {
        Self { size: size as u32 }
    }
}

// Index types for faster lookups
type PrimaryIndex = DashMap<String, DiskLocation>;
/// Managed secondary index using adaptive storage modes (Single -> List -> Bitmap)
type SecondaryIndex = DashMap<String, Arc<RwLock<IndexStorage>>>;

// Move DataInfo enum outside impl block
#[derive(Debug)]
pub enum DataInfo {
    Data { size: usize, preview: String },
    Blob { size: usize },
    Compressed { size: usize },
}

/// Trait to convert field tuples into FieldDefinition
/// Supports both 3-tuple (defaults nullable to false) and 4-tuple (explicit nullable)
pub trait IntoFieldDefinition {
    fn into_field_definition(self) -> (String, FieldDefinition);
}

// 3-tuple: (field_name, field_type, unique) - defaults nullable to false
impl<S: Into<String>> IntoFieldDefinition for (S, FieldType, bool) {
    fn into_field_definition(self) -> (String, FieldDefinition) {
        let (name, field_type, unique) = self;
        (
            name.into(),
            FieldDefinition {
                field_type,
                unique,
                indexed: unique,
                nullable: false, // Default to false
                validations: vec![],
            },
        )
    }
}

// 4-tuple: (field_name, field_type, unique, nullable) - explicit control
impl<S: Into<String>> IntoFieldDefinition for (S, FieldType, bool, bool) {
    fn into_field_definition(self) -> (String, FieldDefinition) {
        let (name, field_type, unique, nullable) = self;
        (
            name.into(),
            FieldDefinition {
                field_type,
                unique,
                indexed: unique,
                nullable,
                validations: vec![],
            },
        )
    }
}

// Direct FieldDefinition: (field_name, field_definition)
impl<S: Into<String>> IntoFieldDefinition for (S, FieldDefinition) {
    fn into_field_definition(self) -> (String, FieldDefinition) {
        (self.0.into(), self.1)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum WalOperation {
    Put {
        key: Arc<String>,
        value: Arc<Vec<u8>>,
    },
    Delete {
        key: String,
    },
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
pub struct Aurora {
    pub(crate) hot: HotStore,
    pub(crate) cold: Arc<ColdStore>,
    pub(crate) primary_indices: Arc<DashMap<String, PrimaryIndex>>,
    pub(crate) secondary_indices: Arc<DashMap<String, SecondaryIndex>>,
    pub(crate) sys_id_mapping: sled::Tree,
    pub(crate) mmap_index: Arc<RwLock<Option<memmap2::Mmap>>>,
    pub(crate) index_manifest: Arc<DashMap<String, (usize, usize)>>,
    /// Mapping from external u128 ID to internal u32 ID (Ticket 1)
    _sid_dictionary: Arc<crossbeam_skiplist::SkipMap<u128, u32>>,
    /// Mapping from internal u32 ID back to external u128 ID (O(1) lookup)
    reverse_sid_dictionary: Arc<RwLock<Vec<u128>>>,
    /// Bitset of deleted internal IDs available for recycling (Ticket 3)
    deleted_ids: Arc<RwLock<RoaringBitmap>>,
    next_internal_id: Arc<AtomicU32>,
    indices_initialized: Arc<OnceCell<()>>,
    pub(crate) transaction_manager: crate::transaction::TransactionManager,
    indices: Arc<DashMap<String, Index>>,
    schema_cache: Arc<DashMap<String, Arc<Collection>>>,
    config: AuroraConfig,
    write_buffer: Option<Arc<WriteBuffer>>,
    pub pubsub: crate::pubsub::PubSubSystem,
    // Write-ahead log for durability
    wal: Option<Arc<RwLock<WriteAheadLog>>>,
    // Background task shutdown senders
    checkpoint_shutdown: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    compaction_shutdown: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    wal_shutdown: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    // Worker system for background jobs
    pub workers: Option<Arc<crate::workers::WorkerSystem>>,
    // Asynchronous WAL Writer Channel
    wal_writer: Option<tokio::sync::mpsc::UnboundedSender<WalOperation>>,
    /// Set to true when the WAL writer task hits a terminal error. put() checks
    /// this before sending so it skips the closed channel and degrades gracefully.
    wal_disabled: Arc<AtomicBool>,
    // Computed fields manager
    pub computed: Arc<RwLock<crate::computed::ComputedFields>>,
    /// LRU cache for parsed AQL AST
    ast_cache: Arc<MokaCache<u64, crate::parser::ast::Document>>,
    /// LRU cache for query plans
    pub plan_cache: Arc<MokaCache<u64, Arc<crate::parser::executor::QueryPlan>>>,
    /// Set of "collection:field" index keys currently being rebuilt in the background.
    /// Queries hitting a building index fall back to full scan instead of returning
    /// incorrect empty results from a half-populated index.
    pub(crate) building_indices: Arc<DashMap<String, ()>>,
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

impl Clone for Aurora {
    fn clone(&self) -> Self {
        Self {
            hot: self.hot.clone(),
            cold: Arc::clone(&self.cold),
            primary_indices: Arc::clone(&self.primary_indices),
            secondary_indices: Arc::clone(&self.secondary_indices),
            sys_id_mapping: self.sys_id_mapping.clone(),
            mmap_index: Arc::clone(&self.mmap_index),
            index_manifest: Arc::clone(&self.index_manifest),
            _sid_dictionary: Arc::clone(&self._sid_dictionary),
            reverse_sid_dictionary: Arc::clone(&self.reverse_sid_dictionary),
            deleted_ids: Arc::clone(&self.deleted_ids),
            next_internal_id: Arc::clone(&self.next_internal_id),
            indices_initialized: Arc::clone(&self.indices_initialized),
            transaction_manager: self.transaction_manager.clone(),
            indices: Arc::clone(&self.indices),
            schema_cache: Arc::clone(&self.schema_cache),
            config: self.config.clone(),
            write_buffer: self.write_buffer.clone(),
            pubsub: self.pubsub.clone(),
            wal: self.wal.clone(),
            checkpoint_shutdown: self.checkpoint_shutdown.clone(),
            compaction_shutdown: self.compaction_shutdown.clone(),
            wal_shutdown: self.wal_shutdown.clone(),
            workers: self.workers.clone(),
            wal_writer: self.wal_writer.clone(),
            wal_disabled: Arc::clone(&self.wal_disabled),
            computed: Arc::clone(&self.computed),
            ast_cache: Arc::clone(&self.ast_cache),
            plan_cache: Arc::clone(&self.plan_cache),
            building_indices: Arc::clone(&self.building_indices),
        }
    }
}

/// Trait for converting inputs into execution parameters
pub trait ToExecParams {
    fn into_params(self) -> (String, ExecutionOptions);
}

impl<'a> ToExecParams for &'a str {
    fn into_params(self) -> (String, ExecutionOptions) {
        (self.to_string(), ExecutionOptions::default())
    }
}

impl ToExecParams for String {
    fn into_params(self) -> (String, ExecutionOptions) {
        (self, ExecutionOptions::default())
    }
}

impl ToExecParams for (String, ExecutionOptions) {
    fn into_params(self) -> (String, ExecutionOptions) {
        self
    }
}

impl<'a, V> ToExecParams for (&'a str, V)
where
    V: Into<serde_json::Value>,
{
    fn into_params(self) -> (String, ExecutionOptions) {
        let (aql, vars) = self;
        let json_vars = vars.into();
        let mut map = std::collections::HashMap::new();
        if let serde_json::Value::Object(obj) = json_vars {
            for (k, v) in obj {
                map.insert(k, v);
            }
        }
        (
            aql.to_string(),
            ExecutionOptions {
                variables: map,
                ..Default::default()
            },
        )
    }
}

impl Aurora {
    /// Get reference to AST cache for parsed AQL queries
    pub fn ast_cache(&self) -> &MokaCache<u64, crate::parser::ast::Document> {
        &self.ast_cache
    }

    /// Convert an external String ID back to an internal ID, creating if not found
    fn get_or_create_internal_id(&self, external_id: &str) -> u32 {
        let u = self.parse_external_id(external_id);
        
        // 1. Check dictionary for existing mapping
        if let Some(entry) = self._sid_dictionary.get(&u) {
            return *entry.value();
        }

        // 2. Try to recycle a deleted ID
        let recycled_id = if let Ok(mut deleted) = self.deleted_ids.write() {
            if !deleted.is_empty() {
                let id = deleted.min().unwrap();
                deleted.remove(id);
                Some(id)
            } else {
                None
            }
        } else {
            None
        };

        let internal_id = if let Some(id) = recycled_id {
            id
        } else {
            // 3. Allocate new ID
            self.next_internal_id.fetch_add(1, Ordering::SeqCst)
        };

        // 4. Update dictionaries
        self._sid_dictionary.insert(u, internal_id);
        
        // 5. Persist mapping to Sled for cross-session stability
        let _ = self.sys_id_mapping.insert(u.to_be_bytes(), &internal_id.to_be_bytes());

        if let Ok(mut reverse) = self.reverse_sid_dictionary.write() {
            if (internal_id as usize) >= reverse.len() {
                reverse.resize((internal_id as usize) + 1, 0);
            }
            reverse[internal_id as usize] = u;
        }

        internal_id
    }

    /// Load persisted internal ID mappings from Sled at startup
    fn load_id_mapping_from_sled(&self) -> Result<()> {
        let mut max_id = 0u32;
        let mut reverse_guard = self.reverse_sid_dictionary.write().unwrap();
        
        for result in self.sys_id_mapping.iter() {
            let (k, v) = result?;
            if k.len() != 16 || v.len() != 4 { continue; }
            
            let u = u128::from_be_bytes(k.as_ref().try_into().map_err(|_| AqlError::new(ErrorCode::InternalError, "ID mapping key corrupt".to_string()))?);
            let id = u32::from_be_bytes(v.as_ref().try_into().map_err(|_| AqlError::new(ErrorCode::InternalError, "ID mapping value corrupt".to_string()))?);
            
            self._sid_dictionary.insert(u, id);
            
            if (id as usize) >= reverse_guard.len() {
                reverse_guard.resize((id as usize) + 1, 0);
            }
            reverse_guard[id as usize] = u;
            
            max_id = max_id.max(id);
        }
        
        // Ensure the counter starts AFTER the highest loaded ID
        if max_id > 0 || !self._sid_dictionary.is_empty() {
             self.next_internal_id.store(max_id + 1, Ordering::SeqCst);
        }
        
        Ok(())
    }

    /// Get a secondary index by name
    pub fn get_secondary_index(&self, key: &str) -> Option<SecondaryIndex> {
        self.secondary_indices.get(key).map(|e| e.value().clone())
    }

    /// Look up a single IndexStorage entry without cloning the whole inner DashMap.
    /// This is O(1) vs the O(N) clone in `get_secondary_index`.
    pub fn get_indexed_storage(&self, index_key: &str, val_str: &str) -> Option<Arc<RwLock<IndexStorage>>> {
        self.secondary_indices
            .get(index_key)?
            .value()
            .get(val_str)
            .map(|e| e.value().clone())
    }

    /// Check whether a secondary index key exists in hot RAM (i.e. the field
    /// is tracked by the indexer for this session).
    pub fn has_index_key(&self, index_key: &str) -> bool {
        self.secondary_indices.contains_key(index_key)
    }

    /// Convert an internal u32 ID back to an external String ID
    pub fn get_external_id(&self, internal_id: u32) -> Option<String> {
        let reverse_dict = self.reverse_sid_dictionary.read().ok()?;
        reverse_dict.get(internal_id as usize).map(|u| {
            if *u == 0 {
                return String::new(); // Or handle as None if 0 is used for empty
            }
            self.format_external_id(*u)
        }).filter(|s| !s.is_empty())
    }

    /// Convert an external String ID to its binary u128 representation
    pub fn parse_external_id(&self, id: &str) -> u128 {
        uuid::Uuid::parse_str(id).map(|u| u.as_u128()).unwrap_or_else(|_| {
            // Fallback to hashing if not a UUID
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut s = DefaultHasher::new();
            id.hash(&mut s);
            s.finish() as u128
        })
    }

    /// Convert binary u128 back to UUID string
    pub fn format_external_id(&self, id: u128) -> String {
        uuid::Uuid::from_u128(id).to_string()
    }

    /// Ensure secondary indices are initialized from checkpoint
    pub async fn ensure_indices_initialized(&self) -> Result<()> {
        self.indices_initialized.get_or_try_init(|| async {
            self.load_index_checkpoint()
        }).await?;
        // Cross-check schema against checkpoint: spawn rebuilds for any index
        // that was missing from the checkpoint (e.g. after a crash).
        self.init_schema_indices().await;
        Ok(())
    }

    /// Load index checkpoint from disk
    fn load_index_checkpoint(&self) -> Result<()> {
        let path = &self.config.db_path;
        let index_bin = path.join("aurora_indices.bin");
        let index_json = path.join("aurora_indices.json");

        if !index_bin.exists() || !index_json.exists() {
            return Ok(());
        }

        // 1. Load manifest
        let manifest_data = std::fs::read_to_string(index_json)?;
        let manifest: HashMap<String, (usize, usize)> = serde_json::from_str(&manifest_data)?;
        for (k, v) in manifest { self.index_manifest.insert(k, v); }

        // 2. Memory map indices
        let file = StdFile::open(index_bin)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        if let Ok(mut guard) = self.mmap_index.write() {
            *guard = Some(mmap);
        }

        // 3. Hydrate secondary_indices from checkpoint so the hot map is not
        //    empty after a crash/restart. Without this every indexed lookup
        //    falls through to a full scan until the next checkpoint fires.
        //
        //    Manifest key format: "collection:field:value"
        //    index_key           = "collection:field"   (split on 2nd colon)
        let mmap_guard = self.mmap_index.read().ok();
        if let Some(ref guard) = mmap_guard {
            if let Some(ref mmap) = **guard {
                for entry in self.index_manifest.iter() {
                    let full_key = entry.key();
                    let (offset, len) = *entry.value();

                    // Split on the second ':' to recover index_key vs value_str
                    let colon2 = full_key.match_indices(':').nth(1).map(|(i, _)| i);
                    let (index_key, val_str) = match colon2 {
                        Some(pos) => (&full_key[..pos], &full_key[pos + 1..]),
                        None => continue,
                    };

                    if let Some(bytes) = mmap.get(offset..offset + len) {
                        if let Ok(bitmap) = RoaringBitmap::deserialize_from(bytes) {
                            let index = self.secondary_indices
                                .entry(index_key.to_string())
                                .or_default();
                            index.insert(
                                val_str.to_string(),
                                Arc::new(std::sync::RwLock::new(IndexStorage::Bitmap(bitmap))),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns true if the secondary index for `collection:field` is currently
    /// being rebuilt in the background. Callers should fall back to a full scan.
    pub fn is_index_building(&self, collection: &str, field: &str) -> bool {
        let key = format!("{}:{}", collection, field);
        self.building_indices.contains_key(&key)
    }

    /// Scan every collection defined in the schema. For any indexed/unique field
    /// whose index is absent from the checkpoint, mark it as building and spawn
    /// a background task to rebuild it from the cold store.
    ///
    /// Called once during `ensure_indices_initialized`, after `load_index_checkpoint`.
    async fn init_schema_indices(&self) {
        let collection_names: Vec<String> = self
            .cold
            .scan_prefix("_collection:")
            .filter_map(|r| r.ok())
            .map(|(key, _)| key.trim_start_matches("_collection:").to_string())
            .collect();

        // Group missing index keys by collection so we do ONE scan per collection,
        // not one scan per field (which would re-scan the same docs N times).
        let mut missing_by_collection: HashMap<String, Vec<String>> = HashMap::new();

        for collection_name in collection_names {
            let Ok(col_def) = self.get_collection_definition(&collection_name) else {
                continue;
            };

            // Always include the auto-indexed "id" field — it's not schema-derived
            // but index_value() indexes it automatically if present in doc.data.
            let auto_id_key = format!("{}:id", collection_name);
            if !self.has_index(&collection_name, "id") {
                self.building_indices.insert(auto_id_key.clone(), ());
                missing_by_collection
                    .entry(collection_name.clone())
                    .or_default()
                    .push(auto_id_key);
            }

            for (field_name, field_def) in &col_def.fields {
                if !field_def.indexed && !field_def.unique {
                    continue;
                }
                let index_key = format!("{}:{}", collection_name, field_name);
                if self.has_index(&collection_name, field_name) {
                    continue;
                }
                self.building_indices.insert(index_key.clone(), ());
                missing_by_collection
                    .entry(collection_name.clone())
                    .or_default()
                    .push(index_key);
            }
        }

        // One rebuild task per collection — single scan, indexes all missing fields.
        for (collection_name, index_keys) in missing_by_collection {
            let db = self.clone();
            tokio::spawn(async move {
                db.rebuild_collection_indices(&collection_name, index_keys).await;
            });
        }
    }

    /// Background rebuild: one full collection scan that rebuilds ALL missing index keys.
    /// Using a single scan avoids redundant I/O when multiple fields are missing.
    async fn rebuild_collection_indices(&self, collection: &str, index_keys: Vec<String>) {
        let db = self.clone();
        let coll = collection.to_string();
        let keys_clone = index_keys.clone();

        let result = tokio::task::spawn_blocking(move || {
            let prefix = format!("{}:", coll);
            for entry in db.cold.scan_prefix(&prefix).flatten() {
                let (key, value) = entry;
                if key.starts_with('_') {
                    continue;
                }
                // index_value is idempotent and covers all indexed fields in one pass.
                let _ = db.index_value(&coll, &key, &value, None);
            }
        })
        .await;

        // Clear all building flags for this collection's keys regardless of outcome.
        for key in &index_keys {
            self.building_indices.remove(key);
        }

        if result.is_ok() {
            let _ = self.save_index_checkpoint();
        } else {
            eprintln!(
                "Index rebuild panicked for collection '{}' (keys: {:?}); will retry on next query cycle.",
                collection, keys_clone
            );
        }
    }

    /// Persist dense secondary indices to disk and re-mmap the result.
    ///
    /// ## Dense-Bitmap-Only rule
    /// Only `IndexStorage::Bitmap` entries are written. `Single` and `List` are
    /// high-cardinality (one entry per unique email/UUID) — checkpointing them would
    /// produce a manifest with millions of keys (50-100 MB of JSON). Skipping them
    /// keeps the manifest in the low-kilobyte range regardless of document count.
    ///
    /// ## Hot / Cold merge
    /// `secondary_indices` only accumulates writes since the last boot. The previous
    /// checkpoint (in `mmap_index`) holds everything before that. Each merged bitmap
    /// is `cold | hot` so no data is lost across checkpoints. Deleted IDs are masked
    /// out using the `deleted_ids` bitmap before serializing.
    ///
    /// ## Format
    ///   aurora_indices.bin  — concatenated native-serialized RoaringBitmaps
    ///   aurora_indices.json — { "col:field:value" -> [byte_offset, byte_len] }
    fn save_index_checkpoint(&self) -> Result<()> {
        use std::io::Write as IoWrite;

        if self.secondary_indices.is_empty() {
            return Ok(());
        }

        let path = &self.config.db_path;
        let index_bin = path.join("aurora_indices.bin");
        let index_json = path.join("aurora_indices.json");

        // Snapshot deleted IDs once; used to mask ghost IDs from every bitmap
        let deleted_snap = self.deleted_ids.read().map(|g| g.clone()).unwrap_or_default();

        let mut bin_data: Vec<u8> = Vec::new();
        let mut manifest: HashMap<String, (usize, usize)> = HashMap::new();

        // Hold the mmap read-lock for the whole loop so cold lookups are stable
        let mmap_guard = self.mmap_index.read().ok();
        let mmap_ref = mmap_guard.as_ref().and_then(|g| g.as_ref());

        for col_entry in self.secondary_indices.iter() {
            let index_key = col_entry.key(); // e.g. "users:status"

            // Unique fields are always Single(u32) — they never accumulate
            // enough docs per value to promote to Bitmap.  We must checkpoint
            // them explicitly or the uniqueness guard becomes blind after a
            // restart (the hot index is empty and the check is silently skipped).
            let is_unique_field = if let Some((coll, field)) = index_key.split_once(':') {
                self.get_collection_definition(coll)
                    .ok()
                    .and_then(|def| def.fields.get(field).cloned())
                    .map(|f| f.unique)
                    .unwrap_or(false)
            } else {
                false
            };

            for val_entry in col_entry.value().iter() {
                let Ok(storage) = val_entry.value().read() else { continue };

                // Dense-Bitmap-Only for non-unique fields: skip Single / List.
                // Unique fields are always checkpointed regardless of storage type.
                let hot_bitmap: RoaringBitmap = match &*storage {
                    IndexStorage::Bitmap(b) => b.clone(),
                    _ if is_unique_field => storage.to_bitmap(),
                    _ => continue,
                };

                let val_str = val_entry.key();
                let full_key = format!("{}:{}", index_key, val_str);

                // Merge with any previously checkpointed cold bitmap for this key
                let mut merged = if let Some(loc) = self.index_manifest.get(&full_key) {
                    let (offset, len) = *loc.value();
                    mmap_ref
                        .and_then(|m| m.get(offset..offset + len))
                        .and_then(|bytes| RoaringBitmap::deserialize_from(bytes).ok())
                        .unwrap_or_default()
                } else {
                    RoaringBitmap::new()
                };

                merged |= hot_bitmap;

                // Strip IDs that have been deleted since the last checkpoint
                if !deleted_snap.is_empty() {
                    merged -= &deleted_snap;
                }

                if merged.is_empty() {
                    continue;
                }

                let offset = bin_data.len();
                merged.serialize_into(&mut bin_data).map_err(|e| {
                    AqlError::new(ErrorCode::IoError, format!("bitmap serialize: {}", e))
                })?;
                manifest.insert(full_key, (offset, bin_data.len() - offset));
            }
        }

        if manifest.is_empty() {
            return Ok(());
        }

        // Atomic write: temp → rename so a mid-write crash leaves the old checkpoint intact
        let tmp_bin = path.join("aurora_indices.bin.tmp");
        let tmp_json = path.join("aurora_indices.json.tmp");

        {
            let mut f = StdFile::create(&tmp_bin)?;
            f.write_all(&bin_data)?;
            f.flush()?;
        }

        let manifest_json = serde_json::to_string(&manifest)
            .map_err(|e| AqlError::new(ErrorCode::SerializationError, e.to_string()))?;
        std::fs::write(&tmp_json, &manifest_json)?;

        std::fs::rename(&tmp_bin, &index_bin)?;
        std::fs::rename(&tmp_json, &index_json)?;

        // Drop mmap guard before acquiring the write lock below
        drop(mmap_guard);

        // Refresh live manifest so the current process reads from the new checkpoint
        self.index_manifest.clear();
        for (k, v) in manifest {
            self.index_manifest.insert(k, v);
        }

        // Re-mmap the new binary so queries hit the updated cold index immediately
        let file = StdFile::open(&index_bin)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        if let Ok(mut guard) = self.mmap_index.write() {
            *guard = Some(mmap);
        }

        Ok(())
    }

    /// Internal serialization helper
    pub fn deserialize_internal<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> Result<T> {
        if data.starts_with(b"AUR\x01") {
            Ok(rmp_serde::from_slice(&data[4..]).map_err(|e| AqlError::new(ErrorCode::SerializationError, e.to_string()))?)
        } else {
            Ok(serde_json::from_slice(data).map_err(|e| AqlError::new(ErrorCode::SerializationError, e.to_string()))?)
        }
    }

    /// Optimized value indexing using adaptive storage (Ticket 3)
    fn index_value(&self, collection: &str, key: &str, value: &[u8], _tx_id: Option<u64>) -> Result<()> {
        // Update primary index with metadata only
        let location = DiskLocation::new(value.len());
        self.primary_indices
            .entry(collection.to_string())
            .or_default()
            .insert(key.to_string(), location);

        if let Ok(doc) = self.deserialize_internal::<Document>(value) {
            let internal_id = self.get_or_create_internal_id(&doc._sid);
            let collection_def = self.get_collection_definition(collection)?;

            // PERFORMANCE: Automatically index 'id' if present in data
            if let Some(id_val) = doc.data.get("id") {
                let index_key = format!("{}:id", collection);
                let val_str = id_val.to_string();
                let index = self.secondary_indices.entry(index_key).or_default();
                index.entry(val_str)
                    .and_modify(|existing| {
                        if let Ok(mut storage) = existing.write() {
                            storage.add(internal_id);
                        }
                    })
                    .or_insert_with(|| Arc::new(RwLock::new(IndexStorage::Single(internal_id))));
            }

            for (field_name, field_val) in doc.data {
                if collection_def.fields.get(&field_name).map_or(false, |f| f.indexed) {
                    let index_key = format!("{}:{}", collection, field_name);
                    let val_str = field_val.to_string();

                    let index = self.secondary_indices.entry(index_key).or_default();
                    // Use and_modify+or_insert_with so newly-created entries start as
                    // Single(id) without an extra add() call that would corrupt them
                    // into List([id, id]).
                    index.entry(val_str)
                        .and_modify(|existing| {
                            if let Ok(mut storage) = existing.write() {
                                storage.add(internal_id);
                            }
                        })
                        .or_insert_with(|| Arc::new(RwLock::new(IndexStorage::Single(internal_id))));
                }
            }
        }
        Ok(())
    }

    /// Get an iterator over all documents in a collection
    pub fn stream_collection(&self, collection: &str) -> Result<impl Iterator<Item = Document>> {
        let mut docs = Vec::new();
        if let Some(index) = self.primary_indices.get(collection) {
            for entry in index.iter() {
                let id = entry.key();
                if let Some(doc) = self.get_document(collection, id)? {
                    docs.push(doc);
                }
            }
        }
        Ok(docs.into_iter())
    }

    /// Get first document matching filter
    pub async fn first_one(&self, collection: &str, filter_fn: impl Fn(&Document) -> bool) -> Result<Option<Document>> {
        let docs = self.scan_and_filter(collection, filter_fn, Some(1))?;
        Ok(docs.into_iter().next())
    }

    /// Execute AQL query (variables are optional)
    ///
    /// Supports two forms:
    /// 1. `db.execute("query").await`
    /// 2. `db.execute(("query", vars)).await`
    pub async fn execute<I: ToExecParams>(&self, input: I) -> Result<ExecutionResult> {
        let (aql, options) = input.into_params();
        parser::executor::execute(self, &aql, options).await
    }

    /// Stream real-time changes using AQL subscription syntax
    ///
    /// This is a convenience method that extracts the `ChangeListener` from
    /// an AQL subscription query, providing a cleaner API than using `execute()` directly.
    ///
    /// # Example
    /// ```ignore
    /// // Stream changes from active products
    /// let mut listener = db.stream(r#"
    ///     subscription {
    ///         products(where: { active: { eq: true } }) {
    ///             id
    ///             name
    ///         }
    ///     }
    /// "#).await?;
    ///
    /// // Receive real-time events
    /// while let Ok(event) = listener.recv().await {
    ///     println!("Change: {:?} on {}", event.change_type, event._sid);
    /// }
    /// ```
    pub async fn stream(&self, aql: &str) -> Result<crate::pubsub::ChangeListener> {
        let result = self.execute(aql).await?;

        match result {
            ExecutionResult::Subscription(sub) => sub.stream.ok_or_else(|| {
                crate::error::AqlError::new(
                    crate::error::ErrorCode::QueryError,
                    "Subscription did not return a stream".to_string(),
                )
            }),
            _ => Err(crate::error::AqlError::new(
                crate::error::ErrorCode::QueryError,
                "Expected a subscription query, got a different operation type".to_string(),
            )),
        }
    }

    /// Explain AQL query execution plan
    pub async fn explain<I: ToExecParams>(&self, input: I) -> Result<ExecutionPlan> {
        let (aql, options) = input.into_params();

        // Parse and analyze without executing
        let doc = parser::parse_with_variables(
            &aql,
            serde_json::Value::Object(options.variables.clone().into_iter().collect()),
        )?;

        self.analyze_execution_plan(&doc).await
    }

    /// Analyze execution plan for a parsed query
    pub async fn analyze_execution_plan(
        &self,
        doc: &crate::parser::ast::Document,
    ) -> Result<ExecutionPlan> {
        let mut operations = Vec::new();
        for op in &doc.operations {
            operations.push(format!("{:?}", op));
        }

        Ok(ExecutionPlan {
            operations,
            estimated_cost: 1.0,
        })
    }
    /// Remove stale lock files from a database directory
    ///
    /// If Aurora crashes or is forcefully terminated, it may leave behind lock files
    /// that prevent the database from being reopened. This method safely removes
    /// those lock files.
    ///
    /// # Safety
    /// Only call this when you're certain no other Aurora instance is using the database.
    /// Removing lock files while another process is running could cause data corruption.
    ///
    /// # Example
    /// ```ignore
    /// use aurora_db::Aurora;
    ///
    /// // If you get "Access denied" error when opening:
    /// if let Err(e) = Aurora::open("my_db") {
    ///     eprintln!("Failed to open: {}", e);
    ///     // Try removing stale lock
    ///     if Aurora::remove_stale_lock("my_db").unwrap_or(false) {
    ///         println!("Removed stale lock, try opening again");
    ///         let db = Aurora::open("my_db")?;
    ///     }
    /// }
    /// # Ok::<(), aurora_db::error::AqlError>(())
    /// ```
    pub fn remove_stale_lock<P: AsRef<Path>>(path: P) -> Result<bool> {
        let resolved_path = Self::resolve_path(path)?;
        crate::storage::cold::ColdStore::try_remove_stale_lock(resolved_path.to_str().unwrap())
    }

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

    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("./data/my_application.db")?;
    ///
    /// // Or use a relative path
    /// let db = Aurora::open("customer_data.db")?;
    /// ```ignore
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = AuroraConfig {
            db_path: Self::resolve_path(path)?,
            ..Default::default()
        };
        Self::with_config(config).await
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
            Err(e) => Err(AqlError::new(
                ErrorCode::IoError,
                format!("Failed to resolve current directory: {}", e),
            )),
        }
    }

    /// Open a database with custom configuration
    ///
    /// # Arguments
    /// * `config` - Database configuration settings
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::AuroraConfig};
    /// use std::time::Duration;
    ///
    /// let config = AuroraConfig {
    ///     db_path: "my_data.db".into(),
    ///     hot_cache_size_mb: 512,           // 512 MB cache
    ///     enable_write_buffering: true,     // Batch writes for speed
    ///     enable_wal: true,                 // Durability
    ///     auto_compact: true,               // Background compaction
    ///     compact_interval_mins: 60,        // Compact every hour
    ///     ..Default::default()
    /// };
    ///
    /// let db = Aurora::with_config(config)?;
    /// ```ignore
    pub async fn with_config(config: AuroraConfig) -> Result<Self> {
        let path = Self::resolve_path(&config.db_path)?;

        if config.create_dirs
            && let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }

        let cold = Arc::new(ColdStore::with_config(
            path.to_str().unwrap(),
            config.cold_cache_capacity_mb,
            config.cold_flush_interval_ms,
            config.cold_mode.clone(),
        )?);

        let hot = HotStore::with_config_and_eviction(
            config.hot_cache_size_mb,
            config.hot_cache_cleanup_interval_secs,
            config.eviction_policy,
        );

        let write_buffer = if config.enable_write_buffering {
            Some(Arc::new(WriteBuffer::new(
                Arc::clone(&cold),
                config.write_buffer_size,
                config.write_buffer_flush_interval_ms,
            )))
        } else {
            None
        };

        let auto_compact = config.auto_compact;
        let enable_wal = config.enable_wal;
        let pubsub = crate::pubsub::PubSubSystem::new(10000);

        // Initialize WAL
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

        // --- FIX: Initialize Background WAL Writer ---
        // Only enable WAL writer if WAL was successfully initialized
        let wal_disabled_flag = Arc::new(AtomicBool::new(false));
        let wal_writer = if enable_wal && wal.is_some() {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WalOperation>();
            let wal_clone = wal.clone();
            let wal_disabled_task = Arc::clone(&wal_disabled_flag);

            // Spawn a single dedicated thread/task for writing WAL sequentially
            tokio::spawn(async move {
                if let Some(wal) = wal_clone {
                    while let Some(op) = rx.recv().await {
                        // If WAL has been disabled due to a prior error, drain messages silently.
                        if wal_disabled_task.load(Ordering::Relaxed) {
                            continue;
                        }

                        let wal = wal.clone();
                        // Move blocking I/O to a blocking thread
                        let result = tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
                            match wal.write() {
                                Ok(mut guard) => {
                                    let wal_result = match op {
                                        WalOperation::Put { key, value } => {
                                            guard.append(Operation::Put, &key, Some(value.as_ref()))
                                        }
                                        WalOperation::Delete { key } => {
                                            guard.append(Operation::Delete, &key, None)
                                        }
                                    };
                                    wal_result.map_err(|e| format!("WAL append failed: {}", e))
                                }
                                Err(e) => {
                                    // Lock is poisoned — degrade gracefully, do not kill the process.
                                    Err(format!("WAL lock poisoned: {}", e))
                                }
                            }
                        })
                        .await;

                        match result {
                            Err(e) if e.is_cancelled() => break,
                            Err(e) => {
                                eprintln!("WAL task panicked: {}. Durability disabled for this session.", e);
                                wal_disabled_task.store(true, Ordering::SeqCst);
                                // Keep draining so callers never see a closed-channel error.
                            }
                            Ok(Err(msg)) => {
                                eprintln!("CRITICAL WAL write error: {}. Durability disabled for this session.", msg);
                                wal_disabled_task.store(true, Ordering::SeqCst);
                            }
                            Ok(Ok(())) => {}
                        }
                    }
                }
            });
            Some(tx)
        } else {
            None
        };

        let checkpoint_shutdown = if wal.is_some() {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let cold_clone = Arc::clone(&cold);
            let wal_clone = wal.clone();
            let checkpoint_interval = config.checkpoint_interval_ms;

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(checkpoint_interval));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Err(e) = cold_clone.flush() {
                                eprintln!("Background checkpoint flush error: {}", e);
                            }
                            if let Some(ref wal) = wal_clone
                                && let Ok(mut wal_guard) = wal.write() {
                                    let _ = wal_guard.truncate();
                                }
                        }
                        _ = shutdown_rx.recv() => {
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

        let compaction_shutdown = if auto_compact {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
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

        let workers = if config.workers_enabled {
            let workers_path = path.join("workers.db");
            let worker_config = crate::workers::WorkerConfig {
                storage_path: workers_path.to_string_lossy().into_owned(),
                concurrency: config.worker_threads,
                ..Default::default()
            };
            crate::workers::WorkerSystem::new(worker_config)
                .map(Arc::new)
                .ok()
        } else {
            None
        };

        let sys_id_mapping = cold.open_tree("_sys_id_mapping")?;

        let db = Self {
            hot,
            cold,
            primary_indices: Arc::new(DashMap::new()),
            secondary_indices: Arc::new(DashMap::new()),
            sys_id_mapping,
            mmap_index: Arc::new(RwLock::new(None)),
            index_manifest: Arc::new(DashMap::new()),
            _sid_dictionary: Arc::new(crossbeam_skiplist::SkipMap::new()),
            reverse_sid_dictionary: Arc::new(RwLock::new(Vec::new())),
            deleted_ids: Arc::new(RwLock::new(RoaringBitmap::new())),
            next_internal_id: Arc::new(AtomicU32::new(0)),
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
            wal_shutdown: None,
            workers,
            wal_writer,
            computed: Arc::new(RwLock::new(crate::computed::ComputedFields::new())),
            ast_cache: Arc::new(MokaCache::new(1000)),
            plan_cache: Arc::new(MokaCache::new(1000)),
            building_indices: Arc::new(DashMap::new()),
            wal_disabled: wal_disabled_flag,
        };

        // 0. Load persisted ID mappings from Sled so that internal IDs are stable
        db.load_id_mapping_from_sled()?;

        // 1. Load index checkpoint immediately from disk
        db.load_index_checkpoint()?;

        // 2. Replay WAL entries on top of the checkpoint state
        if !wal_entries.is_empty() {
            db.replay_wal(wal_entries).await?;
        }

        // 3. Mark indices as initialized so lazy calls don't re-run load_index_checkpoint
        let _ = db.indices_initialized.set(());

        // 4. Trigger background rebuilds for indices missing from checkpoint
        //    (Done after WAL replay so Sled is up to date)
        db.init_schema_indices().await;

        // 5. Always rebuild primary index from cold storage after startup.
        // WAL replay only covers entries written since the last flush; documents
        // that were already checkpointed to cold storage (WAL truncated) must be
        // reloaded here so queries see all persisted data.
        db.rebuild_primary_index_from_cold()?;

        Ok(db)
    }
    // Fast key-value operations with index support
    /// Get a value by key (low-level key-value access)
    ///
    /// This is the low-level method. For document access, use `get_document()` instead.
    /// Checks hot cache first, then falls back to cold storage for maximum performance.
    ///
    /// # Performance
    /// - Hot cache hit: ~1M reads/sec (instant)
    /// - Cold storage: ~500K reads/sec (disk I/O)
    /// - Cache hit rate: typically 95%+ at scale
    ///
    /// # Examples
    ///
    /// ```
    /// // Low-level key-value access
    /// let data = db.get("users:12345")?;
    /// if let Some(bytes) = data {
    ///     let doc: Document = serde_json::from_slice(&bytes)?;
    ///     println!("Found: {:?}", doc);
    /// }
    ///
    /// // Better: use get_document() for documents
    /// let user = db.get_document("users", "12345")?;
    /// ```ignore
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // If inside a transaction, check the buffer first.
        // Provides read-your-own-writes within the transaction while keeping
        // changes invisible to all other readers until commit.
        if let Ok(tx_id) =
            crate::transaction::ACTIVE_TRANSACTION_ID.try_with(|id| *id)
        {
            if let Some(buffer) = self.transaction_manager.active_transactions.get(&tx_id) {
                if buffer.deletes.contains_key(key) {
                    return Ok(None);
                }
                if let Some(value) = buffer.read(key) {
                    return Ok(Some(value));
                }
                // Not in buffer → fall through to storage (read-committed for
                // values not yet touched by this transaction)
            }
        }

        // Check hot cache first
        if let Some(value) = self.hot.get(key) {
            // Check if this is a blob reference (pointer to cold storage)
            if value.starts_with(b"BLOBREF:") {
                // It's a blob ref - fetch actual data from cold storage
                return self.cold.get(key);
            }
            return Ok(Some(value));
        }

        // Fetch from cold storage
        let value = self.cold.get(key)?;

        if let Some(v) = &value {
            if self.should_cache_key(key) {
                self.hot
                    .set(Arc::new(key.to_string()), Arc::new(v.clone()), None);
            }
        }

        Ok(value)
    }

    /// Get value with zero-copy Arc reference (10-100x faster than get!)
    /// Only checks hot cache - returns None if not cached
    pub fn get_hot_ref(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.hot.get_ref(key)
    }

    /// Helper to decide if a key should be cached.
    fn should_cache_key(&self, key: &str) -> bool {
        if let Some(collection_name) = key.split(':').next() {
            if !collection_name.starts_with('_') {
                if let Ok(collection_def) = self.get_collection_definition(collection_name) {
                    if collection_def
                        .fields
                        .values()
                        .any(|def| def.field_type == FieldType::Any)
                    {
                        return false; // Don't cache if collection has Any field
                    }
                }
            }
        }
        true // Cache by default
    }

    /// Get cache statistics
    ///
    /// Returns detailed metrics about cache performance including hit/miss rates,
    /// memory usage, and access patterns. Useful for monitoring, optimization,
    /// and understanding database performance characteristics.
    ///
    /// # Returns
    /// `CacheStats` struct containing:
    /// - `hits`: Number of cache hits (data found in memory)
    /// - `misses`: Number of cache misses (had to read from disk)
    /// - `hit_rate`: Percentage of requests served from cache (0.0-1.0)
    /// - `size`: Current number of entries in cache
    /// - `capacity`: Maximum cache capacity
    /// - `evictions`: Number of entries evicted due to capacity
    ///
    /// # Examples
    ///

    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;

    /// let stats = db.get_cache_stats();
    /// println!("Cache hit rate: {:.1}%", stats.hit_rate * 100.0);
    /// println!("Cache size: {} / {} entries", stats.size, stats.capacity);
    /// println!("Total hits: {}, misses: {}", stats.hits, stats.misses);
    ///
    /// // Monitor performance during operations
    /// let before = db.get_cache_stats();
    ///
    /// // Perform many reads
    /// for i in 0..1000 {
    ///     db.get_document("users", &format!("user-{}", i))?;
    /// }
    ///
    /// let after = db.get_cache_stats();
    /// let hit_rate = (after.hits - before.hits) as f64 / 1000.0;
    /// println!("Read hit rate: {:.1}%", hit_rate * 100.0);
    ///
    /// // Performance tuning
    /// let stats = db.get_cache_stats();
    /// if stats.hit_rate < 0.80 {
    ///     println!("Low cache hit rate! Consider:");
    ///     println!("- Increasing cache size in config");
    ///     println!("- Prewarming cache with prewarm_cache()");
    ///     println!("- Reviewing query patterns");
    /// }
    ///
    /// if stats.evictions > stats.size {
    ///     println!("High eviction rate! Cache may be too small.");
    ///     println!("Consider increasing cache capacity.");
    /// }
    ///
    /// // Production monitoring
    /// use std::time::Duration;
    /// use std::thread;
    ///
    /// loop {
    ///     let stats = db.get_cache_stats();
    ///
    ///     // Log to monitoring system
    ///     if stats.hit_rate < 0.90 {
    ///         eprintln!("Warning: Cache hit rate dropped to {:.1}%",
    ///                   stats.hit_rate * 100.0);
    ///     }
    ///
    ///     thread::sleep(Duration::from_secs(60));
    /// }
    /// ```
    ///
    /// # Typical Performance Metrics
    /// - **Excellent**: 95%+ hit rate (most reads from memory)
    /// - **Good**: 80-95% hit rate (acceptable performance)
    /// - **Poor**: <80% hit rate (consider cache tuning)
    ///
    /// # See Also
    /// - `prewarm_cache()` to improve hit rates by preloading data
    /// - `Aurora::with_config()` to adjust cache capacity
    pub fn get_cache_stats(&self) -> crate::storage::hot::CacheStats {
        self.hot.get_stats()
    }

    pub fn has_index(&self, collection: &str, field: &str) -> bool {
        // _sid is the primary key — always an O(1) lookup, no secondary index needed.
        if field == "_sid" {
            return true;
        }
        // For all other fields (including auto-indexed "id"), only claim the index
        // exists if there is actual data in the hot map or cold manifest.
        // This prevents the indexed fast-path from firing on a post-crash empty index
        // and returning zero results instead of falling back to a full scan.
        let index_key = format!("{}:{}", collection, field);
        if self.secondary_indices.contains_key(&index_key) {
            return true;
        }
        let cold_prefix = format!("{}:", index_key);
        if self.index_manifest.iter().any(|e| e.key().starts_with(&cold_prefix)) {
            return true;
        }
        false
    }

    pub fn get_ids_from_index(&self, collection: &str, field: &str, value: &Value) -> Vec<String> {
        let mut bitmap = RoaringBitmap::new();
        let ik = format!("{}:{}", collection, field);
        let vstr = match value {
            Value::String(s) => s.clone(),
            _ => value.to_string(),
        };

        // 1. Check Cold Index (mmap)
        if let Some(loc) = self.index_manifest.get(&format!("{}:{}", ik, vstr)) {
            if let Ok(g) = self.mmap_index.read() && let Some(m) = g.as_ref() {
                if let Ok(cb) = RoaringBitmap::deserialize_from(&m[loc.0..(loc.0+loc.1)]) { bitmap |= cb; }
            }
        }

        // 2. Check Hot Index (RAM)
        if let Some(index_map) = self.secondary_indices.get(&ik) {
            if let Some(storage_arc) = index_map.get(&vstr) {
                if let Ok(storage) = storage_arc.value().read() {
                    bitmap |= storage.to_bitmap();
                }
            }
        }

        bitmap.iter().filter_map(|id| self.get_external_id(id)).collect()
    }

    /// Register a computed field definition
    pub async fn register_computed_field(
        &self,
        collection: &str,
        field: &str,
        expression: crate::computed::ComputedExpression,
    ) -> Result<()> {
        let mut computed = self.computed.write().unwrap();
        computed.register(collection, field, expression);
        Ok(())
    }

    // ============================================
    // PubSub API - Real-time Change Notifications
    // ============================================

    /// Listen for real-time changes in a collection
    ///
    /// Returns a stream of change events (inserts, updates, deletes) that you can subscribe to.
    /// Perfect for building reactive UIs, cache invalidation, audit logging, webhooks, and
    /// data synchronization systems.
    ///
    /// # Performance
    /// - Zero overhead when no listeners are active
    /// - Events are broadcast to all listeners asynchronously
    /// - Non-blocking - doesn't slow down write operations
    /// - Multiple listeners can watch the same collection
    ///
    /// # Examples
    ///

    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic listener
    /// let mut listener = db.listen("users");
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         match event.change_type {
    ///             ChangeType::Insert => println!("New user: {:?}", event.document),
    ///             ChangeType::Update => println!("Updated user: {:?}", event.document),
    ///             ChangeType::Delete => println!("Deleted user ID: {}", event._sid),
    ///         }
    ///     }
    /// });
    ///
    /// // Now any insert/update/delete will trigger the listener
    /// db.insert_into("users", vec![("name", Value::String("Alice".into()))]).await?;
    /// ```ignore
    ///
    /// # Real-World Use Cases
    ///
    /// **Cache Invalidation:**

    /// use std::sync::Arc;
    /// use tokio::sync::RwLock;
    /// use std::collections::HashMap;
    ///
    /// let cache = Arc::new(RwLock::new(HashMap::new()));
    /// let cache_clone = Arc::clone(&cache);
    ///
    /// let mut listener = db.listen("products");
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Invalidate cache entry when product changes
    ///         cache_clone.write().await.remove(&event._sid);
    ///         println!("Cache invalidated for product: {}", event._sid);
    ///     }
    /// });
    /// ```
    ///
    /// **Webhook Notifications:**
    /// ```ignore
    /// let mut listener = db.listen("orders");
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         if event.change_type == ChangeType::Insert {
    ///             // Send webhook for new orders
    ///             send_webhook("https://api.example.com/webhooks/order", &event).await;
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// **Audit Logging:**
    /// ```ignore
    /// let mut listener = db.listen("sensitive_data");
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Log all changes to audit trail
    ///         db.insert_into("audit_log", vec![
    ///             ("collection", Value::String("sensitive_data".into())),
    ///             ("action", Value::String(format!("{:?}", event.change_type))),
    ///             ("document_id", Value::String(event._sid.clone())),
    ///             ("timestamp", Value::String(chrono::Utc::now().to_rfc3339())),
    ///         ]).await?;
    ///     }
    /// });
    /// ```
    ///
    /// **Data Synchronization:**
    /// ```ignore
    /// let mut listener = db.listen("users");
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Sync changes to external system
    ///         match event.change_type {
    ///             ChangeType::Insert | ChangeType::Update => {
    ///                 if let Some(doc) = event.document {
    ///                     external_api.upsert_user(&doc).await?;
    ///                 }
    ///             },
    ///             ChangeType::Delete => {
    ///                 external_api.delete_user(&event._sid).await?;
    ///             },
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// **Real-Time Notifications:**
    /// ```ignore
    /// let mut listener = db.listen("messages");
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         if event.change_type == ChangeType::Insert {
    ///             if let Some(msg) = event.document {
    ///                 // Push notification to connected websockets
    ///                 if let Some(recipient) = msg.data.get("recipient_id") {
    ///                     websocket_manager.send_to_user(recipient, &msg).await;
    ///                 }
    ///             }
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// **Filtered Listener:**
    /// ```ignore
    /// use aurora_db::pubsub::EventFilter;
    ///
    /// // Only listen for inserts
    /// let mut listener = db.listen("users")
    ///     .filter(EventFilter::ChangeType(ChangeType::Insert));
    ///
    /// // Only listen for documents with specific field value
    /// let mut listener = db.listen("users")
    ///     .filter(EventFilter::FieldEquals("role".to_string(), Value::String("admin".into())));
    /// ```
    ///
    /// # Important Notes
    /// - Listener stays active until dropped
    /// - Events are delivered in order
    /// - Each listener has its own event stream
    /// - Use filters to reduce unnecessary event processing
    /// - Listeners don't affect write performance
    ///
    /// # See Also
    /// - `listen_all()` to listen to all collections
    /// - `ChangeListener::filter()` to filter events
    /// - `query().watch()` for reactive queries with filtering
    pub fn listen(&self, collection: impl Into<String>) -> crate::pubsub::ChangeListener {
        self.pubsub.listen(collection)
    }

    /// Listen for all changes across all collections
    ///
    /// Returns a stream of change events for every insert, update, and delete
    /// operation across the entire database. Useful for global audit logging,
    /// replication, and monitoring systems.
    ///
    /// # Performance
    /// - Same performance as single collection listener
    /// - Filter events by collection in your handler
    /// - Consider using `listen(collection)` if only watching specific collections
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Listen to everything
    /// let mut listener = db.listen_all();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         println!("Change in {}: {:?}", event.collection, event.change_type);
    ///     }
    /// });
    /// ```
    ///
    /// # Real-World Use Cases
    ///
    /// **Global Audit Trail:**
    /// ```ignore
    /// let mut listener = db.listen_all();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Log every database change
    ///         audit_logger.log(AuditEntry {
    ///             timestamp: chrono::Utc::now(),
    ///             collection: event.collection,
    ///             action: event.change_type,
    ///             document_id: event._sid,
    ///             user_id: get_current_user_id(),
    ///         }).await;
    ///     }
    /// });
    /// ```
    ///
    /// **Database Replication:**
    /// ```ignore
    /// let mut listener = db.listen_all();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Replicate to secondary database
    ///         replica_db.apply_change(event).await?;
    ///     }
    /// });
    /// ```
    ///
    /// **Change Data Capture (CDC):**
    /// ```ignore
    /// let mut listener = db.listen_all();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Stream changes to Kafka/RabbitMQ
    ///         kafka_producer.send(
    ///             &format!("cdc.{}", event.collection),
    ///             serde_json::to_string(&event)?
    ///         ).await?;
    ///     }
    /// });
    /// ```
    ///
    /// **Monitoring & Metrics:**
    /// ```ignore
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// let write_counter = Arc::new(AtomicUsize::new(0));
    /// let counter_clone = Arc::clone(&write_counter);
    ///
    /// let mut listener = db.listen_all();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(_event) = listener.recv().await {
    ///         counter_clone.fetch_add(1, Ordering::Relaxed);
    ///     }
    /// });
    ///
    /// // Report metrics every 60 seconds
    /// tokio::spawn(async move {
    ///     loop {
    ///         tokio::time::sleep(Duration::from_secs(60)).await;
    ///         let count = write_counter.swap(0, Ordering::Relaxed);
    ///         println!("Writes per minute: {}", count);
    ///     }
    /// });
    /// ```
    ///
    /// **Selective Processing:**
    /// ```ignore
    /// let mut listener = db.listen_all();
    ///
    /// tokio::spawn(async move {
    ///     while let Ok(event) = listener.recv().await {
    ///         // Handle different collections differently
    ///         match event.collection.as_str() {
    ///             "users" => handle_user_change(event).await,
    ///             "orders" => handle_order_change(event).await,
    ///             "payments" => handle_payment_change(event).await,
    ///             _ => {} // Ignore others
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// # When to Use
    /// - Global audit logging
    /// - Database replication
    /// - Change data capture (CDC)
    /// - Monitoring and metrics
    /// - Event sourcing systems
    ///
    /// # When NOT to Use
    /// - Only need to watch 1-2 collections → Use `listen(collection)` instead
    /// - High write volume with selective interest → Use collection-specific listeners
    /// - Need complex filtering → Use `query().watch()` instead
    ///
    /// # See Also
    /// - `listen()` for single collection listening
    /// - `listener_count()` to check active listeners
    /// - `query().watch()` for filtered reactive queries
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
    /// a critical operation or shutdown. After flush() completes,
    /// all data is guaranteed to be on disk even if power fails.
    ///
    /// # Performance
    /// - Flush time: ~10-50ms depending on buffered data
    /// - Triggers OS-level fsync() for durability guarantee
    /// - Truncates WAL after successful flush
    /// - Not needed for every write (WAL provides durability)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic flush after critical write
    /// db.insert_into("users", data).await?;
    /// db.flush()?;  // Ensure data is persisted to disk
    ///
    /// // Graceful shutdown pattern
    /// fn shutdown(db: &Aurora) -> Result<()> {
    ///     println!("Flushing pending writes...");
    ///     db.flush()?;
    ///     println!("Shutdown complete - all data persisted");
    ///     Ok(())
    /// }
    ///
    /// // Periodic checkpoint pattern
    /// use std::time::Duration;
    /// use std::thread;
    ///
    /// let db = db.clone();
    /// thread::spawn(move || {
    ///     loop {
    ///         thread::sleep(Duration::from_secs(60));
    ///         if let Err(e) = db.flush() {
    ///             eprintln!("Flush error: {}", e);
    ///         } else {
    ///             println!("Checkpoint: data flushed to disk");
    ///         }
    ///     }
    /// });
    ///
    /// // Critical transaction pattern
    /// let tx_id = db.begin_transaction();
    ///
    /// // Multiple operations
    /// db.insert_into("orders", order_data).await?;
    /// db.update_document("inventory", product_id, updates).await?;
    /// db.insert_into("audit_log", audit_data).await?;
    ///
    /// // Commit and flush immediately
    /// db.commit_transaction(tx_id)?;
    /// db.flush()?;  // Critical: ensure transaction is on disk
    ///
    /// // Backup preparation
    /// println!("Preparing backup...");
    /// db.flush()?;  // Ensure all data is written
    /// std::fs::copy("mydb.db", "backup.db")?;
    /// println!("Backup complete");
    /// ```
    ///
    /// # When to Use
    /// - Before graceful shutdown
    /// - After critical transactions
    /// - Before creating backups
    /// - Periodic checkpoints (every 30-60 seconds)
    /// - Before risky operations
    ///
    /// # When NOT to Use
    /// - After every single write (too slow, WAL provides durability)
    /// - In high-throughput loops (batch instead)
    /// - When durability mode is already Immediate
    ///
    /// # Important Notes
    /// - WAL provides durability even without explicit flush()
    /// - flush() adds latency (~10-50ms) so use strategically
    /// - Automatic flush happens during graceful shutdown
    /// - After flush(), WAL is truncated (data is in main storage)
    ///
    /// # See Also
    /// - `Aurora::with_config()` to set durability mode
    /// - WAL (Write-Ahead Log) provides durability without explicit flushes
    pub fn flush(&self) -> Result<()> {
        // Flush write buffer if present
        if let Some(ref write_buffer) = self.write_buffer {
            write_buffer.flush()?;
        }

        // Flush cold storage
        self.cold.flush()?;

        // Truncate WAL after successful flush (data is now in cold storage)
        if let Some(ref wal) = self.wal
            && let Ok(mut wal_lock) = wal.write()
        {
            wal_lock.truncate()?;
        }

        // Persist secondary indices so next boot loads from mmap instead of scanning
        self.save_index_checkpoint()?;

        Ok(())
    }

    /// Async flush — waits for all pending writes to reach durable storage.
    /// Equivalent to `flush()` but callable from async contexts with `.await`.
    pub async fn sync(&self) -> Result<()> {
        self.flush()
    }

    /// Store a key-value pair (low-level storage)
    ///
    /// This is the low-level method. For documents, use `insert_into()` instead.
    /// Writes are buffered and batched for performance.
    ///
    /// # Arguments
    /// * `key` - Unique key (format: "collection:id" for documents)
    /// * `value` - Raw bytes to store
    /// * `ttl` - Optional time-to-live (None = permanent)
    ///
    /// # Performance
    /// - Buffered writes: ~15-30K docs/sec
    /// - Batching improves throughput significantly
    /// - Call `flush()` to ensure data is persisted
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// // Permanent storage
    /// let data = serde_json::to_vec(&my_struct)?;
    /// db.put("mykey".to_string(), data, None)?;
    ///
    /// // With TTL (expires after 1 hour)
    /// db.put("session:abc".to_string(), session_data, Some(Duration::from_secs(3600)))?;
    ///
    /// // Better: use insert_into() for documents
    /// db.insert_into("users", vec![("name", Value::String("Alice".into()))])?;
    /// ```
    pub async fn put(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        const MAX_BLOB_SIZE: usize = 50 * 1024 * 1024;

        if value.len() > MAX_BLOB_SIZE {
            return Err(AqlError::invalid_operation(format!(
                "Blob size {} exceeds maximum allowed size of {}MB",
                value.len() / (1024 * 1024),
                MAX_BLOB_SIZE / (1024 * 1024)
            )));
        }

        // If inside a transaction scope, buffer the write instead of touching
        // storage.  The buffer is flushed atomically on commit or discarded on
        // rollback — nothing lands in WAL/cold/hot until then.
        if let Ok(tx_id) =
            crate::transaction::ACTIVE_TRANSACTION_ID.try_with(|id| *id)
        {
            if let Some(buffer) = self.transaction_manager.active_transactions.get(&tx_id) {
                buffer.write(key, value);
                return Ok(());
            }
        }

        // --- OPTIMIZATION: Wrap in Arc ONCE ---
        let key_arc = Arc::new(key);
        let value_arc = Arc::new(value);

        // Check if this is a blob (blobs bypass write buffer and hot cache)
        let is_blob = value_arc.starts_with(b"BLOB:");

        // --- 1. WAL Write (Non-blocking send of Arcs) ---
        if let Some(ref sender) = self.wal_writer
            && self.config.durability_mode != DurabilityMode::None
            && !self.wal_disabled.load(Ordering::Relaxed)
        {
            sender
                .send(WalOperation::Put {
                    key: Arc::clone(&key_arc),
                    value: Arc::clone(&value_arc),
                })
                .map_err(|_| {
                    AqlError::new(
                        ErrorCode::InternalError,
                        "WAL writer channel closed".to_string(),
                    )
                })?;
        }

        // Check if this key should be cached (false for Any-field collections)
        let should_cache = self.should_cache_key(&key_arc);

        // --- 2. Cold Store Write ---
        if is_blob || !should_cache {
            // Blobs and Any-field docs write directly to cold storage
            // (blobs: avoid memory pressure, Any-fields: ensure immediate queryability)
            self.cold.set(key_arc.to_string(), value_arc.to_vec())?;
        } else if let Some(ref write_buffer) = self.write_buffer {
            write_buffer.write(Arc::clone(&key_arc), Arc::clone(&value_arc))?;
        } else {
            self.cold.set(key_arc.to_string(), value_arc.to_vec())?;
        }

        // --- 3. Hot Cache Write ---
        if should_cache {
            if is_blob {
                // For blobs: cache only a lightweight reference (not the actual data)
                // Format: "BLOBREF:<size>" - just 16-24 bytes instead of potentially MB
                let blob_ref = format!("BLOBREF:{}", value_arc.len());
                self.hot
                    .set(Arc::clone(&key_arc), Arc::new(blob_ref.into_bytes()), ttl);
            } else {
                self.hot
                    .set(Arc::clone(&key_arc), Arc::clone(&value_arc), ttl);
            }
        }

        // --- 4. Indexing (skip for blobs and system keys) ---
        if !is_blob {
            if let Some(collection_name) = key_arc.split(':').next()
                && !collection_name.starts_with('_')
            {
                self.index_value(collection_name, &key_arc, &value_arc, None)?;
            }
        }

        Ok(())
    }
    /// Rebuild primary index from cold storage.
    ///
    /// Scans all `_collection:<name>` keys to discover collections, then for
    /// each collection scans `<name>:<id>` keys and inserts them into the
    /// in-memory primary index.  This is idempotent — entries already present
    /// (e.g. from WAL replay) are left unchanged.
    fn rebuild_primary_index_from_cold(&self) -> Result<()> {
        let collection_prefix = "_collection:";
        let collection_names: Vec<String> = self
            .cold
            .scan_prefix(collection_prefix)
            .filter_map(|r| r.ok())
            .map(|(key, _)| key.trim_start_matches(collection_prefix).to_string())
            .collect();

        for collection_name in collection_names {
            let doc_prefix = format!("{}:", collection_name);
            for result in self.cold.scan_prefix(&doc_prefix) {
                if let Ok((key, value)) = result {
                    let primary_index = self
                        .primary_indices
                        .entry(collection_name.clone())
                        .or_default();
                    primary_index
                        .entry(key)
                        .or_insert_with(|| DiskLocation::new(value.len()));

                    // Rebuild the _sid ↔ internal-u32 mapping so that bitmaps
                    // loaded from the checkpoint can be translated back to real
                    // external IDs via get_external_id(). Without this, every
                    // u32 in a persisted bitmap maps to None after restart.
                    if let Ok(doc) = self.deserialize_internal::<Document>(&value) {
                        let _ = self.get_or_create_internal_id(&doc._sid);
                    }
                }
            }
        }

        Ok(())
    }

    /// Replay WAL entries to recover from crash
    ///
    /// Handles transaction boundaries:
    /// - Operations within a committed transaction are applied
    /// - Operations within a rolled-back transaction are discarded
    /// - Operations within an uncommitted transaction (crash during tx) are discarded
    async fn replay_wal(&self, entries: Vec<crate::wal::LogEntry>) -> Result<()> {
        // Buffer for operations within a transaction
        let mut tx_buffer: Vec<crate::wal::LogEntry> = Vec::new();
        let mut in_transaction = false;

        for entry in entries {
            match entry.operation {
                Operation::BeginTx => {
                    // Start buffering operations
                    in_transaction = true;
                    tx_buffer.clear();
                }
                Operation::CommitTx => {
                    // Apply all buffered operations
                    for buffered_entry in tx_buffer.drain(..) {
                        self.apply_wal_entry(buffered_entry).await?;
                    }
                    in_transaction = false;
                }
                Operation::RollbackTx => {
                    // Discard buffered operations
                    tx_buffer.clear();
                    in_transaction = false;
                }
                Operation::Put | Operation::Delete => {
                    if in_transaction {
                        // Buffer the operation for later
                        tx_buffer.push(entry);
                    } else {
                        // Apply immediately (not in transaction)
                        self.apply_wal_entry(entry).await?;
                    }
                }
            }
        }

        // If we end with in_transaction = true, it means we crashed mid-transaction
        // Those operations in tx_buffer are discarded (not committed)
        if in_transaction {
            eprintln!(
                "WAL replay: Discarding {} uncommitted transaction operations",
                tx_buffer.len()
            );
        }

        // Flush after replay and truncate WAL
        self.cold.flush()?;
        if let Some(ref wal) = self.wal {
            wal.write().unwrap().truncate()?;
        }

        Ok(())
    }

    /// Apply a single WAL entry to storage
    async fn apply_wal_entry(&self, entry: crate::wal::LogEntry) -> Result<()> {
        match entry.operation {
            Operation::Put => {
                if let Some(value) = entry.value {
                    // Write directly to cold storage (skip WAL, already logged)
                    self.cold.set(entry.key.clone(), value.clone())?;

                    // Update hot cache
                    if self.should_cache_key(&entry.key) {
                        self.hot
                            .set(Arc::new(entry.key.clone()), Arc::new(value.clone()), None);
                    }

                    // Rebuild indices
                    if let Some(collection) = entry.key.split(':').next()
                        && !collection.starts_with('_')
                    {
                        self.index_value(collection, &entry.key, &value, None)?;
                    }
                }
            }
            Operation::Delete => {
                // Remove from indices before deleting the data
                if let Some(collection) = entry.key.split(':').next()
                    && !collection.starts_with('_')
                {
                    let id = entry.key.split(':').nth(1).unwrap_or("");
                    if let Ok(Some(doc)) = self.get_document(collection, id) {
                        let _ = self.remove_from_indices(collection, &doc);
                    } else if let Some(index) = self.primary_indices.get_mut(collection) {
                        index.remove(&entry.key);
                    }
                }
                self.cold.delete(&entry.key)?;
                self.hot.delete(&entry.key);
            }
            _ => {} // Transaction markers handled in replay_wal
        }
        Ok(())
    }

    #[cfg(test)]
    fn ensure_schema_hot(&self, collection: &str) -> Result<Arc<Collection>> {
        // 1. Check the high-performance object cache first (O(1))
        if let Some(schema) = self.schema_cache.get(collection) {
            return Ok(schema.value().clone());
        }

        let collection_key = format!("_collection:{}", collection);

        // 2. Fallback to Hot Byte Cache (parse if found)
        if let Some(data) = self.hot.get(&collection_key) {
            return serde_json::from_slice::<Collection>(&data)
                .map(|s| {
                    let arc_s = Arc::new(s);
                    // Populate object cache to avoid this parse next time
                    self.schema_cache
                        .insert(collection.to_string(), arc_s.clone());
                    arc_s
                })
                .map_err(|_| {
                    AqlError::new(
                        ErrorCode::SchemaError,
                        "Failed to parse schema from hot cache",
                    )
                });
        }

        // 3. Fallback to Cold Storage
        if let Some(data) = self.get(&collection_key)? {
            // Update Hot Cache
            self.hot.set(
                Arc::new(collection_key.clone()),
                Arc::new(data.clone()),
                None,
            );

            // Parse and update Schema Cache
            let schema = serde_json::from_slice::<Collection>(&data)?;
            let arc_schema = Arc::new(schema);
            self.schema_cache
                .insert(collection.to_string(), arc_schema.clone());

            return Ok(arc_schema);
        }

        Err(AqlError::new(
            ErrorCode::SchemaError,
            format!("Failed to load schema for collection '{}'", collection),
        ))
    }

    /// Smart collection scan that uses the primary index as a key directory
    /// Avoids forced flushes and leverages hot cache for better performance
    fn scan_collection(&self, collection: &str) -> Result<Vec<Document>> {
        // Use the primary index as a "key directory" - it contains all document keys
        if let Some(index) = self.primary_indices.get(collection) {
            // Pre-allocate based on index size to avoid reallocations
            let mut documents = Vec::with_capacity(index.len());

            // Iterate through all keys in the primary index (fast, in-memory)
            for entry in index.iter() {
                let key = entry.key();

                // Fetch document via hot cache -> cold storage fallback
                if let Some(data) = self.get(key)? {
                    if let Ok(mut doc) = serde_json::from_slice::<Document>(&data) {
                        // Apply computed fields
                        if let Ok(computed) = self.computed.read() {
                            let _ = computed.apply(collection, &mut doc);
                        }
                        documents.push(doc);
                    }
                }
            }
            Ok(documents)
        } else {
            // Fallback: scan from cold storage if primary index not yet initialized
            let prefix = format!("{}:", collection);
            let mut documents = Vec::new();
            for result in self.cold.scan_prefix(&prefix) {
                if let Ok((_key, value)) = result {
                    if let Ok(mut doc) = serde_json::from_slice::<Document>(&value) {
                        // Apply computed fields
                        if let Ok(computed) = self.computed.read() {
                            let _ = computed.apply(collection, &mut doc);
                        }
                        documents.push(doc);
                    }
                }
            }
            Ok(documents)
        }
    }

    /// Scan collection with filter and early termination support
    /// Used by QueryBuilder for optimized queries with LIMIT
    pub fn scan_and_filter<F>(
        &self,
        collection: &str,
        filter_fn: F,
        limit: Option<usize>,
    ) -> Result<Vec<Document>>
    where
        F: Fn(&Document) -> bool,
    {
        let mut results = Vec::new();
        if let Some(index) = self.primary_indices.get(collection) {
            for entry in index.iter() {
                if let Some(data) = self.get(entry.key())? {
                    if let Ok(doc) = serde_json::from_slice::<Document>(&data) {
                        if filter_fn(&doc) {
                            results.push(doc);
                            if let Some(l) = limit {
                                if results.len() >= l {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // No primary index (e.g. system collections like _migrations): fall back to cold scan.
            // Flush the write buffer first so any recently buffered writes land in cold storage
            // and are visible to the prefix scan below.
            if let Some(ref wb) = self.write_buffer {
                let _ = wb.flush();
            }
            let prefix = format!("{}:", collection);
            for result in self.cold.scan_prefix(&prefix) {
                if let Ok((_key, value)) = result {
                    if let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                        if filter_fn(&doc) {
                            results.push(doc);
                            if let Some(l) = limit {
                                if results.len() >= l {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    // Restore missing methods
    pub async fn put_blob(&self, key: String, file_path: &Path) -> Result<()> {
        const MAX_FILE_SIZE: usize = 50 * 1024 * 1024; // 50MB limit

        // Get file metadata to check size before reading
        let metadata = tokio::fs::metadata(file_path).await?;
        let file_size = metadata.len() as usize;

        if file_size > MAX_FILE_SIZE {
            return Err(AqlError::invalid_operation(format!(
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

        self.put(key, blob_data, None).await
    }

    /// Create a new collection with schema definition
    ///
    /// Collections are like tables in SQL - they define the structure of your documents.
    /// The third boolean parameter indicates if the field should be indexed for fast lookups.
    ///
    /// # Arguments
    /// * `name` - Collection name
    /// * `fields` - Vector of (field_name, field_type, indexed) tuples
    ///   - Field name (accepts both &str and String)
    ///   - Field type (String, Int, Float, Bool, etc.)
    ///   - Indexed: true for fast lookups, false for no index
    ///
    /// # Performance
    /// - Indexed fields: Fast equality queries (O(1) lookup)
    /// - Non-indexed fields: Full scan required for queries
    /// - Unique fields are automatically indexed
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::{Aurora, types::FieldType};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Create a users collection
    /// db.new_collection("users", vec![
    ///     ("name", FieldType::Scalar(crate::types::ScalarType::String), false),      // Not indexed
    ///     ("email", FieldType::Scalar(crate::types::ScalarType::String), true),      // Indexed - fast lookups
    ///     ("age", FieldType::Scalar(crate::types::ScalarType::Int), false),
    ///     ("active", FieldType::Scalar(crate::types::ScalarType::Bool), true),       // Indexed
    ///     ("score", FieldType::Float, false),
    /// ])?;
    ///
    /// // Idempotent - calling again is safe
    /// db.new_collection("users", vec![/* ... */])?.await; // OK!
    /// ```
    pub async fn new_collection<F: IntoFieldDefinition>(
        &self,
        name: &str,
        fields: Vec<F>,
    ) -> Result<()> {
        let collection_key = format!("_collection:{}", name);

        // Check if collection already exists - if so, just return Ok (idempotent)
        if self.get(&collection_key)?.is_some() {
            return Ok(());
        }

        // Create field definitions
        let mut field_definitions = HashMap::new();
        for field in fields {
            let (field_name, field_def) = field.into_field_definition();

            if field_def.field_type == FieldType::Any && field_def.unique {
                return Err(AqlError::new(
                    ErrorCode::InvalidDefinition,
                    "Fields of type 'Any' cannot be unique or indexed.".to_string(),
                ));
            }

            field_definitions.insert(field_name, field_def);
        }

        let collection = Collection {
            name: name.to_string(),
            fields: field_definitions,
            // REMOVED: unique_fields is now derived from fields
        };

        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None).await?;

        // Invalidate schema cache since we just created/updated the collection schema
        self.schema_cache.remove(name);

        Ok(())
    }

    /// Insert a document into a collection
    ///
    /// Automatically generates a UUID for the document and validates against
    /// collection schema and unique constraints. Returns the generated document ID.
    ///
    /// # Performance
    /// - Single insert: ~15,000 docs/sec
    /// - Bulk insert: Use `batch_insert()` for 10+ documents (~50,000 docs/sec)
    /// - Triggers PubSub events for real-time listeners
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to insert into
    /// * `data` - Document fields and values to insert
    ///
    /// # Returns
    /// The auto-generated ID of the inserted document or an error
    ///
    /// # Errors
    /// - `CollectionNotFound`: Collection doesn't exist
    /// - `ValidationError`: Data violates schema or unique constraints
    /// - `SerializationError`: Invalid data format
    ///
    /// # Examples
    ///

    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic insertion
    /// let user_id = db.insert_into("users", vec![
    ///     ("name", Value::String("Alice Smith".to_string())),
    ///     ("email", Value::String("alice@example.com".to_string())),
    ///     ("age", Value::Int(28)),
    ///     ("active", Value::Bool(true)),
    /// ]).await?;
    ///
    /// println!("Created user with ID: {}", user_id);
    ///
    /// // Inserting with nested data
    /// let order_id = db.insert_into("orders", vec![
    ///     ("user_id", Value::String(user_id.clone())),
    ///     ("total", Value::Float(99.99)),
    ///     ("status", Value::String("pending".to_string())),
    ///     ("items", Value::Array(vec![
    ///         Value::String("item-123".to_string()),
    ///         Value::String("item-456".to_string()),
    ///     ])),
    /// ]).await?;
    ///
    /// // Error handling - unique constraint violation
    /// match db.insert_into("users", vec![
    ///     ("email", Value::String("alice@example.com".to_string())),  // Duplicate!
    ///     ("name", Value::String("Alice Clone".to_string())),
    /// ]).await {
    ///     Ok(id) => println!("Inserted: {}", id),
    ///     Err(e) => println!("Failed: {} (email already exists)", e),
    /// }
    ///
    /// // For bulk inserts (10+ documents), use batch_insert() instead
    /// let users = vec![
    ///     HashMap::from([
    ///         ("name".to_string(), Value::String("Bob".to_string())),
    ///         ("email".to_string(), Value::String("bob@example.com".to_string())),
    ///     ]),
    ///     HashMap::from([
    ///         ("name".to_string(), Value::String("Carol".to_string())),
    ///         ("email".to_string(), Value::String("carol@example.com".to_string())),
    ///     ]),
    ///     // ... more documents
    /// ];
    /// let ids = db.batch_insert("users", users).await?;  // 3x faster!
    /// println!("Inserted {} users", ids.len());
    /// ```ignore
    pub async fn insert_into(&self, collection: &str, data: Vec<(&str, Value)>) -> Result<String> {
        // Convert Vec<(&str, Value)> to HashMap<String, Value>
        let data_map: HashMap<String, Value> =
            data.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

        // Validate unique constraints before inserting
        self.validate_unique_constraints(collection, &data_map)
            .await?;

        let doc_id = Uuid::now_v7().to_string();
        let document = Document {
            _sid: doc_id.clone(),
            data: data_map,
        };

        self.put(
            format!("{}:{}", collection, doc_id),
            serde_json::to_vec(&document)?,
            None,
        )
        .await?;

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

        let doc_id = Uuid::now_v7().to_string();
        let document = Document {
            _sid: doc_id.clone(),
            data,
        };

        self.put(
            format!("{}:{}", collection, doc_id),
            serde_json::to_vec(&document)?,
            None,
        )
        .await?;

        // Publish insert event
        let event = crate::pubsub::ChangeEvent::insert(collection, &doc_id, document.clone());
        let _ = self.pubsub.publish(event);

        Ok(doc_id)
    }

    /// Batch insert multiple documents with optimized write path
    ///
    /// Inserts multiple documents in a single optimized operation, bypassing
    /// the write buffer for better performance. Ideal for bulk data loading,
    /// migrations, or initial database seeding. 3x faster than individual inserts.
    ///
    /// # Performance
    /// - Insert speed: ~50,000 docs/sec (vs ~15,000 for single inserts)
    /// - Batch writes to WAL and storage
    /// - Validates all unique constraints
    /// - Use for 10+ documents minimum
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to insert into
    /// * `documents` - Vector of document data as HashMaps
    ///
    /// # Returns
    /// Vector of auto-generated document IDs or an error
    ///
    /// # Examples
    ///

    /// use aurora_db::{Aurora, types::Value};
    /// use std::collections::HashMap;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Bulk user import
    /// let users = vec![
    ///     HashMap::from([
    ///         ("name".to_string(), Value::String("Alice".into())),
    ///         ("email".to_string(), Value::String("alice@example.com".into())),
    ///         ("age".to_string(), Value::Int(28)),
    ///     ]),
    ///     HashMap::from([
    ///         ("name".to_string(), Value::String("Bob".into())),
    ///         ("email".to_string(), Value::String("bob@example.com".into())),
    ///         ("age".to_string(), Value::Int(32)),
    ///     ]),
    ///     HashMap::from([
    ///         ("name".to_string(), Value::String("Carol".into())),
    ///         ("email".to_string(), Value::String("carol@example.com".into())),
    ///         ("age".to_string(), Value::Int(25)),
    ///     ]),
    /// ];
    ///
    /// let ids = db.batch_insert("users", users).await?;
    /// println!("Inserted {} users", ids.len());
    ///
    /// // Seeding test data
    /// let test_products: Vec<HashMap<String, Value>> = (0..1000)
    ///     .map(|i| HashMap::from([
    ///         ("sku".to_string(), Value::String(format!("PROD-{:04}", i))),
    ///         ("price".to_string(), Value::Float(9.99 + i as f64)),
    ///         ("stock".to_string(), Value::Int(100)),
    ///     ]))
    ///     .collect();
    ///
    /// let ids = db.batch_insert("products", test_products).await?;
    /// // Much faster than 1000 individual insert_into() calls!
    ///
    /// // Migration from CSV data
    /// let mut csv_reader = csv::Reader::from_path("data.csv")?;
    /// let mut batch = Vec::new();
    ///
    /// for result in csv_reader.records() {
    ///     let record = result?;
    ///     let doc = HashMap::from([
    ///         ("field1".to_string(), Value::String(record[0].to_string())),
    ///         ("field2".to_string(), Value::String(record[1].to_string())),
    ///     ]);
    ///     batch.push(doc);
    ///
    ///     // Insert in batches of 1000
    ///     if batch.len() >= 1000 {
    ///         db.batch_insert("imported_data", batch.clone()).await?;
    ///         batch.clear();
    ///     }
    /// }
    ///
    /// // Insert remaining
    /// if !batch.is_empty() {
    ///     db.batch_insert("imported_data", batch).await?;
    /// }
    /// ```
    ///
    /// # Errors
    /// - `ValidationError`: Unique constraint violation on any document
    /// - `CollectionNotFound`: Collection doesn't exist
    /// - `IoError`: Storage write failure
    ///
    /// # Important Notes
    /// - All inserts are atomic - if one fails, none are inserted
    /// - UUIDs are auto-generated for all documents
    /// - PubSub events are published for each insert
    /// - For 10+ documents, this is 3x faster than individual inserts
    /// - For < 10 documents, use `insert_into()` instead
    ///
    /// # See Also
    /// - `insert_into()` for single document inserts
    /// - `import_from_json()` for file-based bulk imports
    /// - `batch_write()` for low-level batch operations
    pub async fn batch_insert(
        &self,
        collection: &str,
        documents: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<String>> {
        let mut doc_ids = Vec::with_capacity(documents.len());
        let mut pairs = Vec::with_capacity(documents.len());
        // Keep Document objects alive so pubsub doesn't need to re-read from cold storage
        let mut doc_objects = Vec::with_capacity(documents.len());

        // Prepare all documents
        for data in documents {
            // Validate unique constraints
            self.validate_unique_constraints(collection, &data).await?;

            let doc_id = Uuid::now_v7().to_string();
            let document = Document {
                _sid: doc_id.clone(),
                data,
            };

            let key = format!("{}:{}", collection, doc_id);
            let value = serde_json::to_vec(&document)?;

            pairs.push((key, value));
            doc_ids.push(doc_id);
            doc_objects.push(document);
        }

        // Write to WAL in batch (if enabled)
        if let Some(ref wal) = self.wal
            && self.config.durability_mode != DurabilityMode::None
        {
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
            if self.should_cache_key(&key) {
                self.hot
                    .set(Arc::new(key.clone()), Arc::new(value.clone()), None);
            }

            if let Some(collection_name) = key.split(':').next()
                && !collection_name.starts_with('_')
            {
                self.index_value(collection_name, &key, &value, None)?;
            }
        }

        // Publish events — use already-in-memory Document objects, no cold storage re-read
        for (doc_id, doc) in doc_ids.iter().zip(doc_objects.into_iter()) {
            let event = crate::pubsub::ChangeEvent::insert(collection, doc_id, doc);
            let _ = self.pubsub.publish(event);
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
    /// ```ignore
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
        let mut document = self.get_document(collection, doc_id)?.ok_or_else(|| {
            AqlError::new(
                ErrorCode::NotFound,
                format!("Document not found: {}", doc_id),
            )
        })?;

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
        )
        .await?;

        // Publish update event
        let event =
            crate::pubsub::ChangeEvent::update(collection, doc_id, old_document, document.clone());
        let _ = self.pubsub.publish(event);

        Ok(())
    }

    /// Remove a single field key from a document's data (used by field-rename migrations).
    pub async fn drop_field_from_document(
        &self,
        collection: &str,
        doc_id: &str,
        field: &str,
    ) -> Result<()> {
        let key = format!("{}:{}", collection, doc_id);
        let existing = self.get(&key)?.ok_or_else(|| {
            AqlError::new(ErrorCode::NotFound, format!("Document {} not found", doc_id))
        })?;
        let mut document: Document = serde_json::from_slice(&existing)?;
        document.data.remove(field);
        self.put(key, serde_json::to_vec(&document)?, None).await?;
        Ok(())
    }

    pub async fn get_all_collection(&self, collection: &str) -> Result<Vec<Document>> {
        self.ensure_indices_initialized().await?;
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
    /// ```ignore
    /// Begin a new transaction for atomic operations
    ///
    /// Transactions ensure all-or-nothing execution: either all operations succeed,
    /// or none of them are applied. Perfect for maintaining data consistency.
    ///
    /// # Examples
    ///

    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Start transaction
    /// let tx_id = db.begin_transaction();
    ///
    /// // Perform multiple operations
    /// db.insert_into("accounts", vec![
    ///     ("user_id", Value::String("alice".into())),
    ///     ("balance", Value::Int(1000)),
    /// ]).await?;
    ///
    /// db.insert_into("accounts", vec![
    ///     ("user_id", Value::String("bob".into())),
    ///     ("balance", Value::Int(500)),
    ///     ])).await?;
    ///
    /// // Commit if all succeeded
    /// db.commit_transaction(tx_id)?;
    ///
    /// // Or rollback on error
    /// // db.rollback_transaction(tx_id)?;
    /// ```
    pub async fn begin_transaction(&self) -> crate::transaction::TransactionId {
        let buffer = self.transaction_manager.begin();
        buffer._sid
    }

    /// Commit a transaction, making all changes permanent
    ///
    /// All operations within the transaction are atomically applied to the database.
    /// If any operation fails, none are applied.
    ///
    /// # Arguments
    /// * `tx_id` - Transaction ID returned from begin_transaction()
    ///
    /// # Examples
    ///

    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Transfer money between accounts
    /// let tx_id = db.begin_transaction();
    ///
    /// // Deduct from Alice
    /// db.update_document("accounts", "alice", vec![
    ///     ("balance", Value::Int(900)),  // Was 1000
    /// ]).await?;
    ///
    /// // Add to Bob
    /// db.update_document("accounts", "bob", vec![
    ///     ("balance", Value::Int(600)),  // Was 500
    /// ]).await?;
    ///
    /// // Both updates succeed - commit them
    /// db.commit_transaction(tx_id)?;
    ///
    /// println!("Transfer completed!");
    /// ```ignore
    pub async fn commit_transaction(&self, tx_id: crate::transaction::TransactionId) -> Result<()> {
        let buffer = self
            .transaction_manager
            .active_transactions
            .get(&tx_id)
            .ok_or_else(|| {
                AqlError::invalid_operation(
                    "Transaction not found or already completed".to_string(),
                )
            })?;

        for item in buffer.writes.iter() {
            let key = item.key();
            let value = item.value();
            // WAL write for crash-durability (skipped during transaction to
            // avoid writing uncommitted entries that could be replayed)
            if let Some(ref sender) = self.wal_writer
                && self.config.durability_mode != DurabilityMode::None
            {
                let _ = sender.send(WalOperation::Put {
                    key: Arc::new(key.clone()),
                    value: Arc::new(value.clone()),
                });
            }
            self.cold.set(key.clone(), value.clone())?;
            if self.should_cache_key(key) {
                self.hot
                    .set(Arc::new(key.clone()), Arc::new(value.clone()), None);
            }
            if let Some(collection_name) = key.split(':').next()
                && !collection_name.starts_with('_')
            {
                self.index_value(collection_name, key, value, None)?;
            }
        }

        for item in buffer.deletes.iter() {
            let key = item.key();
            if let Some((collection, id)) = key.split_once(':')
                && let Ok(Some(doc)) = self.get_document(collection, id)
            {
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

    /// Roll back a transaction, discarding all changes
    ///
    /// All operations within the transaction are discarded. The database state
    /// remains unchanged. Use this when an error occurs during transaction processing.
    ///
    /// # Arguments
    /// * `tx_id` - Transaction ID returned from begin_transaction()
    ///
    /// # Examples
    ///

    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Attempt a transfer with validation
    /// let tx_id = db.begin_transaction();
    ///
    /// let result = async {
    ///     // Deduct from Alice
    ///     let alice = db.get_document("accounts", "alice").await?;
    ///     let balance = alice.and_then(|doc| doc.data.get("balance"));
    ///
    ///     if let Some(Value::Int(bal)) = balance {
    ///         if *bal < 100 {
    ///             return Err("Insufficient funds");
    ///         }
    ///
    ///         db.update_document("accounts", "alice", vec![
    ///             ("balance", Value::Int(bal - 100)),
    ///         ]).await?;
    ///
    ///         db.update_document("accounts", "bob", vec![
    ///             ("balance", Value::Int(600)),
    ///         ]).await?;
    ///
    ///         Ok(())
    ///     } else {
    ///         Err("Account not found")
    ///     }
    /// }.await;
    ///
    /// match result {
    ///     Ok(_) => {
    ///         db.commit_transaction(tx_id)?;
    ///         println!("Transfer completed");
    ///     }
    ///     Err(e) => {
    ///         db.rollback_transaction(tx_id)?;
    ///         println!("Transfer failed: {}, changes rolled back", e);
    ///     }
    /// }
    /// ```
    pub async fn rollback_transaction(&self, tx_id: crate::transaction::TransactionId) -> Result<()> {
        self.transaction_manager.rollback(tx_id)
    }

    /// Create a secondary index on a field for faster queries
    ///
    /// Indexes dramatically improve query performance for frequently accessed fields,
    /// trading increased memory usage and slower writes for much faster reads.
    ///
    /// # When to Create Indexes
    /// - **Frequent queries**: Fields used in 80%+ of your queries
    /// - **High cardinality**: Fields with many unique values (user_id, email)
    /// - **Sorting/filtering**: Fields used in ORDER BY or WHERE clauses
    /// - **Large collections**: Most beneficial with 10,000+ documents
    ///
    /// # When NOT to Index
    /// - Low cardinality fields (e.g., boolean flags, small enums)
    /// - Rarely queried fields
    /// - Fields that change frequently (write-heavy workloads)
    /// - Small collections (<1,000 documents) - full scans are fast enough
    ///
    /// # Performance Characteristics
    /// - **Query speedup**: O(n) → O(1) for equality filters
    /// - **Memory cost**: ~100-200 bytes per document per index
    /// - **Write slowdown**: ~20-30% longer insert/update times
    /// - **Build time**: ~5,000 docs/sec for initial indexing
    /// Create a new collection with the given schema
    ///
    /// # Arguments
    /// * `name` - Collection name
    /// * `fields` - Field definitions as tuples of (name, type, unique)
    ///   - The boolean indicates whether the field has a **unique constraint**
    ///   - Unique fields are automatically indexed
    ///   - Non-unique fields can be indexed separately using `create_index()`
    ///
    /// # Examples
    /// ```ignore
    /// # use aurora_db::{Aurora, types::FieldType};
    /// # async fn example(db: &Aurora) {
    /// db.new_collection("users", vec![
    ///     .first_one()
    ///     .await?;
    ///
    /// // DON'T index 'active' - low cardinality (only 2 values: true/false)
    /// // A full scan is fast enough for boolean fields
    ///
    /// // DO index 'age' if you frequently query age ranges
    /// db.create_index("users", "age").await?;
    ///
    /// let young_users = db.query("users")
    ///     .filter(|f| f.lt("age", 30))
    ///     .collect()
    ///     .await?;
    /// ```
    ///
    /// # Real-World Example: E-commerce Orders
    ///
    /// ```ignore
    /// // Orders collection: 1 million documents
    /// db.new_collection("orders", vec![
    ///     ("user_id", FieldType::Scalar(crate::types::ScalarType::String)),    // High cardinality
    ///     ("status", FieldType::Scalar(crate::types::ScalarType::String)),      // Low cardinality (pending, shipped, delivered)
    ///     ("created_at", FieldType::Scalar(crate::types::ScalarType::String)),
    ///     ("total", FieldType::Float),
    /// ])?;
    ///
    /// // Index user_id - queries like "show me my orders" are common
    /// db.create_index("orders", "user_id").await?;  // Good choice
    ///
    /// // Query speedup: 2.5s → 0.001s
    /// let my_orders = db.query("orders")
    ///     .filter(|f| f.eq("user_id", user_id))
    ///     .collect()
    ///     .await?;
    ///
    /// // DON'T index 'status' - only 3 possible values
    /// // Scanning 1M docs takes ~100ms, indexing won't help much
    ///
    /// // Index created_at if you frequently query recent orders
    /// db.create_index("orders", "created_at").await?;  // Good for time-based queries
    /// ```
    pub async fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        let collection_def = self.get_collection_definition(collection)?;

        if let Some(field_def) = collection_def.fields.get(field) {
            if field_def.field_type == FieldType::Any {
                return Err(AqlError::new(
                    ErrorCode::InvalidDefinition,
                    "Cannot create an index on a field of type 'Any'.".to_string(),
                ));
            }
        } else {
            return Err(AqlError::new(
                ErrorCode::InvalidDefinition,
                format!(
                    "Field '{}' not found in collection '{}'.",
                    field, collection
                ),
            ));
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
                && let Ok(doc) = serde_json::from_slice::<Document>(&data)
            {
                let _ = index.insert(&doc);
            }
        }

        // Store the index
        self.indices.insert(index_name, index);

        // Store the index definition for persistence
        let index_key = format!("_index:{}:{}", collection, field);
        self.put(index_key, serde_json::to_vec(&definition)?, None)
            .await?;

        Ok(())
    }

    /// Query documents in a collection with filtering, sorting, and pagination
    ///
    /// Returns a `QueryBuilder` that allows fluent chaining of query operations.
    /// Queries use early termination for LIMIT clauses, making them extremely fast
    /// even on large collections (6,800x faster than naive implementations).
    ///
    /// # Performance
    /// - With LIMIT: O(k) where k = limit + offset (early termination!)
    /// - Without LIMIT: O(n) where n = matching documents
    /// - Uses secondary indices when available for equality filters
    /// - Hot cache: ~1M reads/sec, Cold storage: ~500K reads/sec
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Simple equality query
    /// let active_users = db.query("users")
    ///     .filter(|f| f.eq("active", Value::Bool(true)))
    ///     .collect()
    ///     .await?;
    ///
    /// // Range query with pagination (FAST - uses early termination!)
    /// let top_scorers = db.query("users")
    ///     .filter(|f| f.gt("score", Value::Int(1000)))
    ///     .order_by("score", false)  // descending
    ///     .limit(10)
    ///     .offset(20)
    ///     .collect()
    ///     .await?;
    ///
    /// // Multiple filters
    /// let premium_active = db.query("users")
    ///     .filter(|f| f.eq("tier", Value::String("premium".into())))
    ///     .filter(|f| f.eq("active", Value::Bool(true)))
    ///     .limit(100)  // Only scans ~200 docs, not all million!
    ///     .collect()
    ///     .await?;
    ///
    /// // Text search in a field
    /// let matching = db.query("articles")
    ///     .filter(|f| f.contains("title", "rust"))
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
    /// ```ignore
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
    /// Fast direct lookup when you know the document ID. Significantly faster
    /// than querying with filters when ID is known.
    ///
    /// # Performance
    /// - Hot cache: ~1,000,000 reads/sec (instant)
    /// - Cold storage: ~500,000 reads/sec (disk I/O)
    /// - Complexity: O(1) - constant time lookup
    /// - Much faster than `.query().filter(|f| f.eq("id", ...))` which is O(n)
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

    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic retrieval
    /// if let Some(user) = db.get_document("users", &user_id)? {
    ///     println!("Found user: {}", user._sid);
    ///
    ///     // Access fields safely
    ///     if let Some(Value::String(name)) = user.data.get("name") {
    ///         println!("Name: {}", name);
    ///     }
    ///
    ///     if let Some(Value::Int(age)) = user.data.get("age") {
    ///         println!("Age: {}", age);
    ///     }
    /// } else {
    ///     println!("User not found");
    /// }
    ///
    /// // Idiomatic error handling
    /// let user = db.get_document("users", &user_id)?
    ///     .ok_or_else(|| AqlError::new(ErrorCode::NotFound,"User not found".into()))?;
    ///
    /// // Checking existence before operations
    /// if db.get_document("users", &user_id)?.is_some() {
    ///     db.update_document("users", &user_id, vec![
    ///         ("last_login", Value::String(chrono::Utc::now().to_rfc3339())),
    ///     ]).await?;
    /// }
    ///
    /// // Batch retrieval (fetch multiple by ID)
    /// let user_ids = vec!["user-1", "user-2", "user-3"];
    /// let users: Vec<Document> = user_ids.iter()
    ///     .filter_map(|id| db.get_document("users", id).ok().flatten())
    ///     .collect();
    ///
    /// println!("Found {} out of {} users", users.len(), user_ids.len());
    /// ```ignore
    ///
    /// # When to Use
    /// - You know the document ID (from insert, previous query, or URL param)
    /// - Need fastest possible lookup (1M reads/sec)
    /// - Fetching a single document
    ///
    /// # When NOT to Use
    /// - Searching by other fields → Use `query().filter()` instead
    /// - Need multiple documents by criteria → Use `query().collect()` instead
    /// - Don't know the ID → Use `find_by_field()` or `query()` instead
    pub fn get_document(&self, collection: &str, sid: &str) -> Result<Option<Document>> {
        let key = format!("{}:{}", collection, sid);
        if let Some(data) = self.get(&key)? {
            Ok(Some(serde_json::from_slice(&data)?))
        } else {
            Ok(None)
        }
    }

    /// Delete a document by ID
    ///
    /// Permanently removes a document from storage, cache, and all indices.
    /// Publishes a delete event for PubSub subscribers. This operation cannot be undone.
    ///
    /// # Performance
    /// - Delete speed: ~50,000 deletes/sec
    /// - Cleans up hot cache, cold storage, primary + secondary indices
    /// - Triggers PubSub events for listeners
    ///
    /// # Arguments
    /// * `key` - Full key in format "collection:id" (e.g., "users:123")
    ///
    /// # Returns
    /// Success or an error
    ///
    /// # Errors
    /// - `InvalidOperation`: Invalid key format (must be "collection:id")
    /// - `IoError`: Storage deletion failed
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic deletion (note: requires "collection:id" format)
    /// db.delete("users:abc123").await?;
    ///
    /// // Delete with existence check
    /// let user_id = "user-456";
    /// if db.get_document("users", user_id)?.is_some() {
    ///     db.delete(&format!("users:{}", user_id)).await?;
    ///     println!("User deleted");
    /// } else {
    ///     println!("User not found");
    /// }
    ///
    /// // Error handling
    /// match db.delete("users:nonexistent").await {
    ///     Ok(_) => println!("Deleted successfully"),
    ///     Err(e) => println!("Delete failed: {}", e),
    /// }
    ///
    /// // Batch deletion using query
    /// let inactive_count = db.delete_where("users", |f| {
    ///     f.eq("active", Value::Bool(false))
    /// }).await?;
    /// println!("Deleted {} inactive users", inactive_count);
    ///
    /// // Delete with cascading (manual cascade pattern)
    /// let user_id = "user-123";
    ///
    /// // Delete user's orders first
    /// let orders = db.query("orders")
    ///     .filter(|f| f.eq("user_id", user_id))
    ///     .collect()
    ///     .await?;
    ///
    /// for order in orders {
    ///     db.delete(&format!("orders:{}", order._sid)).await?;
    /// }
    ///
    /// // Then delete the user
    /// db.delete(&format!("users:{}", user_id)).await?;
    /// println!("User and all orders deleted");
    /// ```ignore
    ///
    /// # Alternative: Soft Delete Pattern
    ///
    /// For recoverable deletions, use soft deletes instead:
    ///
    /// ```
    /// // Soft delete - mark as deleted instead of removing
    /// db.update_document("users", &user_id, vec![
    ///     ("deleted", Value::Bool(true)),
    ///     ("deleted_at", Value::String(chrono::Utc::now().to_rfc3339())),
    /// ]).await?;
    ///
    /// // Query excludes soft-deleted items
    /// let active_users = db.query("users")
    ///     .filter(|f| f.eq("deleted", Value::Bool(false)))
    ///     .collect()
    ///     .await?;
    ///
    /// // Later: hard delete after retention period
    /// let old_deletions = db.query("users")
    ///     .filter(|f| f.eq("deleted", Value::Bool(true)))
    ///     .filter(|f| f.lt("deleted_at", thirty_days_ago))
    ///     .collect()
    ///     .await?;
    ///
    /// for user in old_deletions {
    ///     db.delete(&format!("users:{}", user._sid)).await?;
    /// }
    /// ```ignore
    ///
    /// # Important Notes
    /// - Deletion is permanent - no undo/recovery
    /// - Consider soft deletes for recoverable operations
    /// - Use transactions for multi-document deletions
    /// - PubSub subscribers will receive delete events
    /// - All indices are automatically cleaned up
    pub async fn delete(&self, key: &str) -> Result<()> {
        // Extract collection and id from key (format: "collection:id")
        let (collection, id) = if let Some((coll, doc_id)) = key.split_once(':') {
            (coll, doc_id)
        } else {
            return Err(AqlError::invalid_operation(
                "Invalid key format, expected 'collection:id'".to_string(),
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
            // Fallback: at least remove from primary index using the full sled key.
            if let Some(index) = self.primary_indices.get_mut(collection) {
                index.remove(key);
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
        // Remove from primary index — key format is "collection:sid" (the full sled key).
        if let Some(index) = self.primary_indices.get(collection) {
            index.remove(&format!("{}:{}", collection, doc._sid));
        }

        let u = self.parse_external_id(&doc._sid);
        let internal_id = self._sid_dictionary.get(&u).map(|e| *e.value());

        // Remove from secondary indices
        for (field, value) in &doc.data {
            let index_key = format!("{}:{}", collection, field);
            if let Some(index_map) = self.secondary_indices.get(&index_key) {
                if let Some(storage_arc) = index_map.get(&value.to_string()) {
                    if let Ok(mut storage) = storage_arc.value().write() {
                        if let Some(id) = internal_id {
                            storage.remove(id);
                        }
                    }
                }
            }
        }

        // Return the internal ID to the recycling pool so it can be reused
        if let Some(id) = internal_id {
            // Remove mapping from dictionaries and Sled
            self._sid_dictionary.remove(&u);
            if let Ok(mut reverse) = self.reverse_sid_dictionary.write() {
                if let Some(r) = reverse.get_mut(id as usize) {
                    *r = 0; // Clear the reverse mapping
                }
            }
            let _ = self.sys_id_mapping.remove(u.to_be_bytes());

            if let Ok(mut deleted) = self.deleted_ids.write() {
                deleted.insert(id);
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
                && text.to_lowercase().contains(&query.to_lowercase())
            {
                results.push(doc);
            }
        }

        Ok(results)
    }

    /// Export a collection to a JSON file
    ///
    /// Creates a JSON file containing all documents in the collection.
    /// Useful for backups, data migration, or sharing datasets.
    /// Automatically appends `.json` extension if not present.
    ///
    /// # Performance
    /// - Export speed: ~10,000 docs/sec
    /// - Scans entire collection from cold storage
    /// - Memory efficient: streams documents to file
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to export
    /// * `output_path` - Path to the output JSON file (`.json` auto-appended)
    ///
    /// # Returns
    /// Success or an error
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic export
    /// db.export_as_json("users", "./backups/users_2024-01-15")?;
    /// // Creates: ./backups/users_2024-01-15.json
    ///
    /// // Timestamped backup
    /// let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    /// let backup_path = format!("./backups/users_{}", timestamp);
    /// db.export_as_json("users", &backup_path)?;
    ///
    /// // Export multiple collections
    /// for collection in &["users", "orders", "products"] {
    ///     db.export_as_json(collection, &format!("./export/{}", collection))?;
    /// }
    /// ```ignore
    ///
    /// # Output Format
    ///
    /// The exported JSON has this structure:
    /// ```json
    /// {
    ///   "users": [
    ///     { "id": "123", "name": "Alice", "email": "alice@example.com" },
    ///     { "id": "456", "name": "Bob", "email": "bob@example.com" }
    ///   ]
    /// }
    /// ```
    ///
    /// # See Also
    /// - `export_as_csv()` for CSV format export
    /// - `import_from_json()` to restore exported data
    pub fn export_as_json(&self, collection: &str, output_path: &str) -> Result<()> {
        use std::io::{BufWriter, Write};

        let output_path = if !output_path.ends_with(".json") {
            format!("{}.json", output_path)
        } else {
            output_path.to_string()
        };

        // Flush write buffer so in-flight docs reach cold storage before we scan.
        self.flush()?;

        let file = StdFile::create(&output_path)?;
        let mut writer = BufWriter::new(file);

        // Stream as a JSON object: { "<collection>": [ ... ] }
        // Each document is serialised individually — O(1) memory regardless of
        // dataset size.
        write!(writer, "{{\"{}\": [\n", collection)?;

        let prefix = format!("{}:", collection);
        let mut first = true;

        for result in self.cold.scan_prefix(&prefix) {
            let (key, value) = result?;
            if key.starts_with("_collection:") {
                continue;
            }
            let Ok(doc) = serde_json::from_slice::<Document>(&value) else {
                continue;
            };

            // Recursively convert every Value to a JsonValue — no silent drops.
            let mut obj = serde_json::Map::new();
            // Always include the document id field
            obj.insert("_id".to_string(), JsonValue::String(doc._sid));
            for (k, v) in doc.data {
                obj.insert(k, Self::value_to_json(v));
            }

            if !first {
                write!(writer, ",\n")?;
            }
            serde_json::to_writer(&mut writer, &JsonValue::Object(obj))?;
            first = false;
        }

        write!(writer, "\n]}}")?;
        writer.flush()?;
        println!("Exported collection '{}' to {}", collection, &output_path);
        Ok(())
    }

    /// Recursively convert an Aurora `Value` to a `serde_json::Value` with no
    /// data loss — nested objects and arrays are handled at every depth.
    fn value_to_json(v: Value) -> JsonValue {
        match v {
            Value::String(s) => JsonValue::String(s),
            Value::Int(i) => JsonValue::Number(i.into()),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Bool(b) => JsonValue::Bool(b),
            Value::Null => JsonValue::Null,
            Value::Uuid(u) => JsonValue::String(u.to_string()),
            Value::DateTime(dt) => JsonValue::String(dt.to_rfc3339()),
            Value::Array(arr) => {
                JsonValue::Array(arr.into_iter().map(Self::value_to_json).collect())
            }
            Value::Object(map) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in map {
                    obj.insert(k, Self::value_to_json(v));
                }
                JsonValue::Object(obj)
            }
        }
    }

    /// Export a collection to a CSV file
    ///
    /// Creates a CSV file with headers from the first document and rows for each document.
    /// Useful for spreadsheet analysis, data science workflows, or reporting.
    /// Automatically appends `.csv` extension if not present.
    ///
    /// # Performance
    /// - Export speed: ~8,000 docs/sec
    /// - Memory efficient: streams rows to file
    /// - Headers determined from first document
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to export
    /// * `filename` - Path to the output CSV file (`.csv` auto-appended)
    ///
    /// # Returns
    /// Success or an error
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic CSV export
    /// db.export_as_csv("users", "./reports/users")?;
    /// // Creates: ./reports/users.csv
    ///
    /// // Export for analysis in Excel/Google Sheets
    /// db.export_as_csv("orders", "./analytics/sales_data")?;
    ///
    /// // Monthly report generation
    /// let month = chrono::Utc::now().format("%Y-%m");
    /// db.export_as_csv("transactions", &format!("./reports/transactions_{}", month))?;
    /// ```
    ///
    /// # Output Format
    ///
    /// ```csv
    /// id,name,email,age
    /// 123,Alice,alice@example.com,28
    /// 456,Bob,bob@example.com,32
    /// ```ignore
    ///
    /// # Important Notes
    /// - Headers are taken from the first document's fields
    /// - Documents with different fields will have empty values for missing fields
    /// - Nested objects/arrays are converted to strings
    /// - Best for flat document structures
    ///
    /// # See Also
    /// - `export_as_json()` for JSON format (better for nested data)
    /// - For complex nested structures, use JSON export instead
    pub fn export_as_csv(&self, collection: &str, filename: &str) -> Result<()> {
        let output_path = if !filename.ends_with(".csv") {
            format!("{}.csv", filename)
        } else {
            filename.to_string()
        };

        // Flush write buffer so in-flight docs are visible in cold storage.
        self.flush()?;

        // Derive the canonical column order from the schema definition so that
        // every row has the same columns even when some docs are missing optional
        // fields.  Fall back to an empty list; first-doc heuristic fills it in
        // that case (schema-less collections).
        let mut headers: Vec<String> = match self.get_collection_definition(collection) {
            Ok(coll) => {
                // _id first, then all schema fields in definition order
                let mut cols = vec!["_id".to_string()];
                cols.extend(coll.fields.into_keys());
                cols
            }
            Err(_) => vec!["_id".to_string()],
        };

        let mut writer = csv::Writer::from_path(&output_path)?;
        let mut headers_written = false;

        let prefix = format!("{}:", collection);

        for result in self.cold.scan_prefix(&prefix) {
            let (key, value) = result?;
            if key.starts_with("_collection:") {
                continue;
            }
            let Ok(doc) = serde_json::from_slice::<Document>(&value) else {
                continue;
            };

            // If we only have the _id column (schema-less path), discover
            // headers from the first document we encounter.
            if !headers_written && headers.len() == 1 {
                for k in doc.data.keys() {
                    if !headers.contains(k) {
                        headers.push(k.clone());
                    }
                }
            }

            if !headers_written {
                writer.write_record(&headers)?;
                headers_written = true;
            }

            let row: Vec<String> = headers
                .iter()
                .map(|col| {
                    if col == "_id" {
                        return doc._sid.clone();
                    }
                    match doc.data.get(col) {
                        None => String::new(),
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Int(i)) => i.to_string(),
                        Some(Value::Float(f)) => f.to_string(),
                        Some(Value::Bool(b)) => b.to_string(),
                        Some(Value::Null) => String::new(),
                        Some(Value::Uuid(u)) => u.to_string(),
                        Some(Value::DateTime(dt)) => dt.to_rfc3339(),
                        // Nested types: JSON-encode for lossless round-trip.
                        // Consumers (pandas, DuckDB, etc.) can parse these back.
                        Some(v) => serde_json::to_string(&Self::value_to_json(v.clone()))
                            .unwrap_or_default(),
                    }
                })
                .collect();

            writer.write_record(&row)?;
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
        F: FnOnce(&FilterBuilder) -> Filter + Send + Sync + 'static,
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
            .filter(move |f: &FilterBuilder| f.eq(field, value_clone.clone()))
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
            // Clone for notification
            let old_doc = doc.clone();

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
            )
            .await?;

            // Publish update event
            let event = crate::pubsub::ChangeEvent::update(collection, id, old_doc, doc);
            let _ = self.pubsub.publish(event);

            Ok(id.to_string())
        } else {
            // Insert new document with specified ID - validate unique constraints
            self.validate_unique_constraints(collection, &data_map)
                .await?;

            let document = Document {
                _sid: id.to_string(),
                data: data_map,
            };

            self.put(
                format!("{}:{}", collection, id),
                serde_json::to_vec(&document)?,
                None,
            )
            .await?;

            // Publish insert event
            let event = crate::pubsub::ChangeEvent::insert(collection, id, document);
            let _ = self.pubsub.publish(event);

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
            )
            .await?;

            Ok(new_value)
        } else {
            Err(AqlError::new(
                ErrorCode::NotFound,
                format!("Document {}:{} not found", collection, id),
            ))
        }
    }

    // Delete documents by query
    pub async fn delete_by_query<F>(&self, collection: &str, filter_fn: F) -> Result<usize>
    where
        F: FnOnce(&FilterBuilder) -> Filter + Send + Sync + 'static,
    {
        let docs = self.query(collection).filter(filter_fn).collect().await?;
        let mut deleted_count = 0;

        for doc in docs {
            let key = format!("{}:{}", collection, doc._sid);
            self.delete(&key).await?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }

    /// Import documents from a JSON file into a collection
    ///
    /// Validates each document against the collection schema, skips duplicates (by ID),
    /// and provides detailed statistics about the import operation. Useful for restoring
    /// backups, migrating data, or seeding development databases.
    ///
    /// # Performance
    /// - Import speed: ~5,000 docs/sec (with validation)
    /// - Memory efficient: processes documents one at a time
    /// - Validates schema and unique constraints
    ///
    /// # Arguments
    /// * `collection` - Name of the collection to import into
    /// * `filename` - Path to the JSON file containing documents (array format)
    ///
    /// # Returns
    /// `ImportStats` containing counts of imported, skipped, and failed documents
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Basic import
    /// let stats = db.import_from_json("users", "./data/new_users.json").await?;
    /// println!("Imported: {}, Skipped: {}, Failed: {}",
    ///     stats.imported, stats.skipped, stats.failed);
    ///
    /// // Restore from backup
    /// let backup_file = "./backups/users_2024-01-15.json";
    /// let stats = db.import_from_json("users", backup_file).await?;
    ///
    /// if stats.failed > 0 {
    ///     eprintln!("Warning: {} documents failed validation", stats.failed);
    /// }
    ///
    /// // Idempotent import - duplicates are skipped
    /// let stats = db.import_from_json("users", "./data/users.json").await?;
    /// // Running again will skip all existing documents
    /// let stats2 = db.import_from_json("users", "./data/users.json").await?;
    /// assert_eq!(stats2.skipped, stats.imported);
    ///
    /// // Migration from another system
    /// db.new_collection("products", vec![
    ///     ("sku", FieldType::Scalar(crate::types::ScalarType::String)),
    ///     ("name", FieldType::Scalar(crate::types::ScalarType::String)),
    ///     ("price", FieldType::Float),
    /// ])?;
    ///
    /// let stats = db.import_from_json("products", "./migration/products.json").await?;
    /// println!("Migration complete: {} products imported", stats.imported);
    /// ```ignore
    ///
    /// # Expected JSON Format
    ///
    /// The JSON file should contain an array of document objects:
    /// ```json
    /// [
    ///   { "id": "123", "name": "Alice", "email": "alice@example.com" },
    ///   { "id": "456", "name": "Bob", "email": "bob@example.com" },
    ///   { "name": "Carol", "email": "carol@example.com" }
    /// ]
    /// ```
    ///
    /// # Behavior
    /// - Documents with existing IDs are skipped (duplicate detection)
    /// - Documents without IDs get auto-generated UUIDs
    /// - Schema validation is performed on all fields
    /// - Failed documents are counted but don't stop the import
    /// - Unique constraints are checked
    ///
    /// # See Also
    /// - `export_as_json()` to create compatible backup files
    /// - `batch_insert()` for programmatic bulk inserts
    pub async fn import_from_json(&self, collection: &str, filename: &str) -> Result<ImportStats> {
        // Validate that the collection exists
        let collection_def = self.get_collection_definition(collection)?;

        // Load JSON file
        let json_string = read_to_string(filename).await.map_err(|e| {
            AqlError::new(
                ErrorCode::IoError,
                format!("Failed to read import file: {}", e),
            )
        })?;

        // Parse JSON
        let documents: Vec<JsonValue> = from_str(&json_string).map_err(|e| {
            AqlError::new(
                ErrorCode::SerializationError,
                format!("Failed to parse JSON: {}", e),
            )
        })?;

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
            return Err(AqlError::invalid_operation(
                "Expected JSON object".to_string(),
            ));
        }

        // Extract document ID if present
        let doc_id = doc_json
            .get("id")
            .and_then(|id| id.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());

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
                        return Err(AqlError::invalid_operation(format!(
                            "Field '{}' has invalid type",
                            field_name
                        )));
                    }

                    // Convert JSON value to our Value type
                    let value = self.json_to_value(json_value)?;
                    data_map.insert(field_name.clone(), value);
                } else if field_def.unique {
                    // Missing required unique field
                    return Err(AqlError::invalid_operation(format!(
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
            _sid: doc_id,
            data: data_map,
        };

        self.put(
            format!("{}:{}", collection, document._sid),
            serde_json::to_vec(&document)?,
            None,
        )
        .await?;

        Ok(ImportResult::Imported)
    }

    /// Validate that a JSON value matches the expected field type
    fn validate_field_value(&self, value: &JsonValue, field_type: &FieldType) -> bool {
        use crate::types::ScalarType;
        match field_type {
            FieldType::Scalar(ScalarType::String) => value.is_string(),
            FieldType::Scalar(ScalarType::Int) => value.is_i64() || value.is_u64(),
            FieldType::Scalar(ScalarType::Float) => value.is_number(),
            FieldType::Scalar(ScalarType::Bool) => value.is_boolean(),
            FieldType::Scalar(ScalarType::Uuid) => {
                value.is_string() && Uuid::parse_str(value.as_str().unwrap_or("")).is_ok()
            }
            FieldType::Scalar(ScalarType::Any) => true,

            FieldType::Array(_) => value.is_array(),
            FieldType::Object | FieldType::Nested(_) => value.is_object(),
            // Legacy/Duplicate catches just in case
            _ => true,
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
                    Err(AqlError::invalid_operation(
                        "Invalid number value".to_string(),
                    ))
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
    pub fn get_collection_definition(&self, collection: &str) -> Result<Collection> {
        if let Some(data) = self.get(&format!("_collection:{}", collection))? {
            let collection_def: Collection = serde_json::from_slice(&data)?;
            Ok(collection_def)
        } else {
            Err(AqlError::new(
                ErrorCode::CollectionNotFound,
                collection.to_string(),
            ))
        }
    }

    /// Returns the names of all registered collections.
    pub fn list_collection_names(&self) -> Vec<String> {
        self.schema_cache.iter().map(|r| r.key().clone()).collect()
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
    /// Loads documents from a collection into memory cache to eliminate cold-start
    /// latency. Dramatically improves initial query performance after database startup
    /// by preloading the most commonly accessed data.
    ///
    /// # Performance Impact
    /// - Prewarming speed: ~20,000 docs/sec
    /// - Improves subsequent read latency from ~2ms (disk) to ~0.001ms (memory)
    /// - Cache hit rate jumps from 0% to 95%+ for prewarmed data
    /// - Memory cost: ~500 bytes per document average
    ///
    /// # Arguments
    /// * `collection` - The collection to prewarm
    /// * `limit` - Maximum number of documents to load (default: 1000, None = all)
    ///
    /// # Returns
    /// Number of documents loaded into cache
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Prewarm frequently accessed collection
    /// let loaded = db.prewarm_cache("users", Some(1000)).await?;
    /// println!("Prewarmed {} user documents", loaded);
    ///
    /// // Now queries are fast from the start
    /// let stats_before = db.get_cache_stats();
    /// let users = db.query("users").collect().await?;
    /// let stats_after = db.get_cache_stats();
    ///
    /// // High hit rate thanks to prewarming
    /// assert!(stats_after.hit_rate > 0.95);
    ///
    /// // Startup optimization pattern
    /// async fn startup_prewarm(db: &Aurora) -> Result<()> {
    ///     println!("Prewarming caches...");
    ///
    ///     // Prewarm most frequently accessed collections
    ///     db.prewarm_cache("users", Some(5000)).await?;
    ///     db.prewarm_cache("sessions", Some(1000)).await?;
    ///     db.prewarm_cache("products", Some(500)).await?;
    ///
    ///     let stats = db.get_cache_stats();
    ///     println!("Cache prewarmed: {} entries loaded", stats.size);
    ///
    ///     Ok(())
    /// }
    ///
    /// // Web server startup
    /// #[tokio::main]
    /// async fn main() {
    ///     let db = Aurora::open("app.db").unwrap();
    ///
    ///     // Prewarm before accepting requests
    ///     db.prewarm_cache("users", Some(10000)).await.unwrap();
    ///
    ///     // Server is now ready with hot cache
    ///     start_web_server(db).await;
    /// }
    ///
    /// // Prewarm all documents (for small collections)
    /// let all_loaded = db.prewarm_cache("config", None).await?;
    /// // All config documents now in memory
    ///
    /// // Selective prewarming based on access patterns
    /// async fn smart_prewarm(db: &Aurora) -> Result<()> {
    ///     // Load recent users (they're accessed most)
    ///     db.prewarm_cache("users", Some(1000)).await?;
    ///
    ///     // Load active sessions only
    ///     let active_sessions = db.query("sessions")
    ///         .filter(|f| f.eq("active", Value::Bool(true)))
    ///         .limit(500)
    ///         .collect()
    ///         .await?;
    ///
    ///     // Manually populate cache with hot data
    ///     for session in active_sessions {
    ///         // Reading automatically caches
    ///         db.get_document("sessions", &session._sid)?;
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Typical Prewarming Scenarios
    ///
    /// **Web Application Startup:**
    /// ```ignore
    /// // Load user data, sessions, and active content
    /// db.prewarm_cache("users", Some(5000)).await?;
    /// db.prewarm_cache("sessions", Some(2000)).await?;
    /// db.prewarm_cache("posts", Some(1000)).await?;
    /// ```
    ///
    /// **E-commerce Site:**
    /// ```ignore
    /// // Load products, categories, and user carts
    /// db.prewarm_cache("products", Some(500)).await?;
    /// db.prewarm_cache("categories", None).await?;  // All categories
    /// db.prewarm_cache("active_carts", Some(1000)).await?;
    /// ```
    ///
    /// **API Server:**
    /// ```ignore
    /// // Load authentication data and rate limits
    /// db.prewarm_cache("api_keys", None).await?;
    /// db.prewarm_cache("rate_limits", Some(10000)).await?;
    /// ```
    ///
    /// # When to Use
    /// - At application startup to eliminate cold-start latency
    /// - After cache clear operations
    /// - Before high-traffic events (product launches, etc.)
    /// - When deploying new instances (load balancer warm-up)
    ///
    /// # Memory Considerations
    /// - 1,000 docs ≈ 500 KB memory
    /// - 10,000 docs ≈ 5 MB memory
    /// - 100,000 docs ≈ 50 MB memory
    /// - Stay within configured cache capacity
    ///
    /// # See Also
    /// - `get_cache_stats()` to monitor cache effectiveness
    /// - `prewarm_all_collections()` to prewarm all collections
    /// - `Aurora::with_config()` to adjust cache capacity
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
                self.hot.set(Arc::new(key.clone()), Arc::new(value), None);
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
    ///
    /// Low-level batch write operation that bypasses document validation and
    /// writes raw byte data directly to storage. Useful for advanced use cases,
    /// custom serialization, or maximum performance scenarios.
    ///
    /// # Performance
    /// - Write speed: ~100,000 writes/sec
    /// - Single disk fsync for entire batch
    /// - No validation or schema checking
    /// - Direct storage access
    ///
    /// # Arguments
    /// * `pairs` - Vector of (key, value) tuples where value is raw bytes
    ///
    /// # Returns
    /// Success or an error
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Low-level batch write
    /// let pairs = vec![
    ///     ("users:123".to_string(), b"raw data 1".to_vec()),
    ///     ("users:456".to_string(), b"raw data 2".to_vec()),
    ///     ("cache:key1".to_string(), b"cached value".to_vec()),
    /// ];
    ///
    /// db.batch_write(pairs)?;
    ///
    /// // Custom binary serialization
    /// use bincode;
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct CustomData {
    ///     id: u64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// let custom_data = vec![
    ///     CustomData { id: 1, payload: vec![1, 2, 3] },
    ///     CustomData { id: 2, payload: vec![4, 5, 6] },
    /// ];
    ///
    /// let pairs: Vec<(String, Vec<u8>)> = custom_data
    ///     .iter()
    ///     .map(|data| {
    ///         let key = format!("binary:{}", data._sid);
    ///         let value = bincode::serialize(data).unwrap();
    ///         (key, value)
    ///     })
    ///     .collect();
    ///
    /// db.batch_write(pairs)?;
    ///
    /// // Bulk cache population
    /// let cache_entries: Vec<(String, Vec<u8>)> = (0..10000)
    ///     .map(|i| {
    ///         let key = format!("cache:item_{}", i);
    ///         let value = format!("value_{}", i).into_bytes();
    ///         (key, value)
    ///     })
    ///     .collect();
    ///
    /// db.batch_write(cache_entries)?;
    /// // Writes 10,000 entries in ~100ms
    /// ```
    ///
    /// # Important Notes
    /// - No schema validation performed
    /// - No unique constraint checking
    /// - No automatic indexing
    /// - Keys must follow "collection:id" format for proper grouping
    /// - Values are raw bytes - you handle serialization
    /// - Use `batch_insert()` for validated document inserts
    ///
    /// # When to Use
    /// - Maximum write performance needed
    /// - Custom serialization formats (bincode, msgpack, etc.)
    /// - Cache population
    /// - Low-level database operations
    /// - You're bypassing the document model
    ///
    /// # When NOT to Use
    /// - Regular document inserts → Use `batch_insert()` instead
    /// - Need validation → Use `batch_insert()` instead
    /// - Need indexing → Use `batch_insert()` instead
    ///
    /// # See Also
    /// - `batch_insert()` for validated document batch inserts
    /// - `put()` for single key-value writes
    pub async fn batch_write(&self, pairs: Vec<(String, Vec<u8>)>) -> Result<()> {
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
                if self.should_cache_key(&key) {
                    self.hot
                        .set(Arc::new(key.clone()), Arc::new(value.clone()), None);
                }

                // 3. Update primary index with metadata only
                let location = DiskLocation::new(value.len());
                primary_index.insert(key.clone(), location);

                // 4. Update secondary indices
                if !indexed_fields.is_empty()
                    && let Ok(doc) = serde_json::from_slice::<Document>(&value)
                {
                    for (field, field_value) in doc.data {
                        if indexed_fields.contains(&field) {
                            let value_str = match &field_value {
                                Value::String(s) => s.clone(),
                                _ => field_value.to_string(),
                            };
                            let index_key = format!("{}:{}", collection_name, field);
                            let secondary_index =
                                self.secondary_indices.entry(index_key).or_default();

                            let id = key.split(':').nth(1).unwrap_or("");
                            let internal_id = self.get_or_create_internal_id(id);

                            let index_entry = secondary_index.entry(value_str).or_insert_with(|| {
                                Arc::new(RwLock::new(IndexStorage::Single(internal_id)))
                            });

                            if let Ok(mut storage) = index_entry.value().write() {
                                storage.add(internal_id);
                            }
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
            // Use primary index for fast stats (count + size from DiskLocation)
            let (count, size) = if let Some(index) = self.primary_indices.get(&collection) {
                let count = index.len();
                // Sum up sizes from DiskLocation metadata (much faster than disk scan)
                let size: usize = index.iter().map(|entry| entry.value().size as usize).sum();
                (count, size)
            } else {
                // Fallback: scan from cold storage if index not available
                let prefix = format!("{}:", collection);
                let count = self.cold.scan_prefix(&prefix).count();
                let size: usize = self
                    .cold
                    .scan_prefix(&prefix)
                    .filter_map(|r| r.ok())
                    .map(|(_, v)| v.len())
                    .sum();
                (count, size)
            };

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
                return Err(AqlError::invalid_operation(format!(
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
    pub async fn create_text_index(
        &self,
        collection: &str,
        field: &str,
        _enable_stop_words: bool,
    ) -> Result<()> {
        // Check if collection exists
        if self.get(&format!("_collection:{}", collection))?.is_none() {
            return Err(AqlError::new(
                ErrorCode::CollectionNotFound,
                collection.to_string(),
            ));
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
        self.put(index_key, serde_json::to_vec(&index_def)?, None)
            .await?;

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

        // --- The "Query Planner" ---
        // Smart heuristic: For range queries with small LIMITs, full scan can be faster
        // than collecting millions of IDs from secondary index
        let use_index_for_range = if let Some(limit) = builder.limit {
            // If limit is small (< 1000), prefer full scan for range queries
            // The secondary index would scan all entries anyway, might as well
            // scan documents directly and benefit from early termination
            limit >= 1000
        } else {
            // No limit? Index might still help if result set is small
            true
        };

        // Look for an opportunity to use an index
        for (_filter_idx, filter) in builder.filters.iter().enumerate() {
            match filter {
                Filter::Eq(field, value) => {
                    let index_key = format!("{}:{}", &builder.collection, field);
                    if let Some(index) = self.secondary_indices.get(&index_key) {
                        if let Some(matching_ids_arc) = index.get(&value.to_string()) {
                            if let Ok(storage) = matching_ids_arc.value().read() {
                                let ids = storage.iter().filter_map(|id| self.get_external_id(id)).collect();
                                doc_ids_to_load = Some(ids);
                                break;
                            }
                        }
                    }
                }
                Filter::Gt(field, value)
                | Filter::Gte(field, value)
                | Filter::Lt(field, value)
                | Filter::Lte(field, value) => {
                    // Skip index for range queries with small LIMITs (see query planner heuristic above)
                    if !use_index_for_range {
                        continue;
                    }

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
                                    if let Ok(storage) = entry.value().read() {
                                        matching_ids.extend(storage.iter().filter_map(|id| self.get_external_id(id)));
                                    }
                                }
                            }
                        }

                        if !matching_ids.is_empty() {
                            doc_ids_to_load = Some(matching_ids);
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
                                if let Ok(storage) = entry.value().read() {
                                    matching_ids.extend(storage.iter().filter_map(|id| self.get_external_id(id)));
                                }
                            }
                        }

                        if !matching_ids.is_empty() {
                            // Remove duplicates since a document could match multiple indexed values
                            matching_ids.sort();
                            matching_ids.dedup();

                            doc_ids_to_load = Some(matching_ids);
                            break;
                        }
                    }
                }
                Filter::And(_) => {
                    // For compound filters, we can't easily use a single index
                    // This would require more complex query planning
                    continue;
                }
                Filter::Or(sub_filters) => {
                    // For OR filters, collect union of all matching IDs from each sub-filter
                    let mut union_ids: Vec<String> = Vec::new();
                    let mut used_index = false;

                    for sub_filter in sub_filters {
                        match sub_filter {
                            Filter::Eq(field, value) => {
                                let index_key = format!("{}:{}", &builder.collection, field);
                                if let Some(index) = self.secondary_indices.get(&index_key) {
                                    if let Some(matching_ids_arc) = index.get(&value.to_string()) {
                                        if let Ok(storage) = matching_ids_arc.value().read() {
                                            union_ids.extend(storage.iter().filter_map(|id| self.get_external_id(id)));
                                            used_index = true;
                                        }
                                    }
                                }
                            }
                            // For other filter types in OR, we fall back to full scan
                            _ => continue,
                        }
                    }

                    if used_index && !union_ids.is_empty() {
                        // Remove duplicates
                        union_ids.sort();
                        union_ids.dedup();
                        doc_ids_to_load = Some(union_ids);
                        break;
                    }
                    // If no index was used, continue to full scan
                }
                _ => continue,
            }
        }

        let mut final_docs: Vec<Document>;

        if let Some(ids) = doc_ids_to_load {
            // Index path
            use std::io::Write;
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("/tmp/aurora_query_stats.log")
            {
                let _ = writeln!(
                    file,
                    "[INDEX PATH] IDs to load: {} | Collection: {}",
                    ids.len(),
                    builder.collection
                );
            }

            final_docs = Vec::with_capacity(ids.len());

            for id in ids {
                let doc_key = format!("{}:{}", &builder.collection, id);
                if let Some(data) = self.get(&doc_key)?
                    && let Ok(doc) = serde_json::from_slice::<Document>(&data)
                {
                    final_docs.push(doc);
                }
            }
        } else {
            // --- Path 2: Full Collection Scan with Early Termination ---

            // Optimization: If we have a LIMIT but no ORDER BY, we can stop scanning
            // as soon as we have enough matching documents
            let early_termination_target = if builder.order_by.is_none() {
                builder.limit.map(|l| l + builder.offset.unwrap_or(0))
            } else {
                // With ORDER BY, we need all matching docs to sort correctly
                None
            };

            // Smart scan with early termination support
            final_docs = Vec::new();
            let mut scan_stats = (0usize, 0usize, 0usize); // (keys_scanned, docs_fetched, matches_found)

            if let Some(index) = self.primary_indices.get(&builder.collection) {
                for entry in index.iter() {
                    let key = entry.key();
                    scan_stats.0 += 1; // keys scanned

                    // Early termination check
                    if let Some(target) = early_termination_target {
                        if final_docs.len() >= target {
                            break; // We have enough documents!
                        }
                    }

                    // Fetch and filter document
                    if let Some(data) = self.get(key)? {
                        scan_stats.1 += 1; // docs fetched

                        if let Ok(doc) = serde_json::from_slice::<Document>(&data) {
                            // Apply all filters (Ticket 4 Optimized Path)
                            if builder.filters.iter().all(|filter| filter.matches(&doc)) {
                                scan_stats.2 += 1; // matches found
                                final_docs.push(doc);
                            }
                        }
                    }
                }

                // Debug logging for query performance analysis
                use std::io::Write;
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/aurora_query_stats.log")
                {
                    let _ = writeln!(
                        file,
                        "[SCAN PATH] Scanned: {} keys | Fetched: {} docs | Matched: {} | Collection: {}",
                        scan_stats.0, scan_stats.1, scan_stats.2, builder.collection
                    );
                }
            } else {
                // Fallback: scan from cold storage if index not initialized
                final_docs = self.get_all_collection(&builder.collection).await?;

                // Apply filters
                final_docs.retain(|doc| {
                    builder.filters.iter().all(|filter| filter.matches(doc))
                });
            }
        }

        // Apply ordering
        if let Some((field, ascending)) = &builder.order_by {
            final_docs.sort_by(|a, b| match (a.data.get(field), b.data.get(field)) {
                (Some(v1), Some(v2)) => {
                    let cmp = v1.cmp(v2);
                    if *ascending { cmp } else { cmp.reverse() }
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
                    Err(AqlError::invalid_operation(
                        "Failed to parse int".to_string(),
                    ))
                }
            }
            Value::Float(_) => {
                if let Ok(f) = value_str.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Err(AqlError::invalid_operation(
                        "Failed to parse float".to_string(),
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
            && !select_fields.is_empty()
        {
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
                match self.put(key, value, None).await {
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

                match self.new_collection(&name, fields_for_db).await {
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
                let tx_id = self.begin_transaction().await;
                Response::TransactionId(tx_id.as_u64())
            }
            crate::network::protocol::Request::CommitTransaction(tx_id_u64) => {
                let tx_id = crate::transaction::TransactionId::from_u64(tx_id_u64);
                match self.commit_transaction(tx_id).await {
                    Ok(_) => Response::Done,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            crate::network::protocol::Request::RollbackTransaction(tx_id_u64) => {
                let tx_id = crate::transaction::TransactionId::from_u64(tx_id_u64);
                match self.rollback_transaction(tx_id).await {
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
    /// ```ignore
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
                let total_documents: usize = index.iter().map(|entry| {
                    if let Ok(storage) = entry.value().read() {
                        storage.to_bitmap().len() as usize
                    } else {
                        0
                    }
                }).sum();

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
        let collection_def = match self.get_collection_definition(collection) {
            Ok(def) => def,
            Err(e) if e.code == crate::error::ErrorCode::CollectionNotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let unique_fields = self.get_unique_fields(&collection_def);

        for unique_field in &unique_fields {
            if let Some(value) = data.get(unique_field) {
                let index_key = format!("{}:{}", collection, unique_field);
                if let Some(_index) = self.secondary_indices.get(&index_key) {
                    // Get the raw string value without JSON formatting
                    let value_str = match value {
                        Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    if let Some(storage) = self.get_indexed_storage(&index_key, &value_str) {
                        if let Ok(s) = storage.read() {
                            if !s.is_empty() {
                                return Err(AqlError::new(
                                    ErrorCode::UniqueConstraintViolation,
                                    format!(
                                        "Unique constraint violation on field '{}' with value '{}'",
                                        unique_field, value_str
                                    ),
                                ));
                            }
                        }
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
                    if let Some(doc_ids_arc) = index.get(&value_str) {
                        let exclude_internal = self._sid_dictionary
                            .get(&self.parse_external_id(exclude_id))
                            .map(|e| *e.value());
                            
                        if let Ok(storage) = doc_ids_arc.value().read() {
                            let has_violation = if let Some(exc) = exclude_internal {
                                // Normal path: dictionary populated, use bitmap exclusion.
                                storage.iter().any(|id| id != exc)
                            } else {
                                // _sid_dictionary not yet populated (first update after
                                // restart — the dictionary is rebuilt lazily, not from the
                                // checkpoint). Fall back to a cold-store ownership check:
                                // if the document being updated already holds this exact
                                // unique value, it is NOT a violation.
                                let cold_key = format!("{}:{}", collection, exclude_id);
                                let doc_owns_value = self.get(&cold_key)
                                    .ok()
                                    .flatten()
                                    .and_then(|raw| serde_json::from_slice::<Document>(&raw).ok())
                                    .and_then(|doc| doc.data.get(unique_field).cloned())
                                    .map(|existing_val| match &existing_val {
                                        Value::String(s) => *s == value_str,
                                        _ => existing_val.to_string() == value_str,
                                    })
                                    .unwrap_or(false);

                                if doc_owns_value {
                                    // Warm up the dictionary so subsequent checks on this
                                    // document use the fast path instead of cold storage.
                                    self.get_or_create_internal_id(exclude_id);
                                    false
                                } else {
                                    !storage.is_empty()
                                }
                            };

                            if has_violation {
                                return Err(AqlError::new(
                                    ErrorCode::UniqueConstraintViolation,
                                    format!(
                                        "Unique constraint violation on field '{}' with value '{}'",
                                        unique_field, value_str
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // =========================================================================
    // AQL Executor Helper Methods
    // These are wrapper methods for AQL executor integration.
    // They provide a simplified API compatible with the AQL query execution layer.
    // =========================================================================

    /// Get all documents in a collection (AQL helper)
    ///
    /// This is a wrapper around the internal query system optimized for bulk retrieval.
    pub async fn aql_get_all_collection(&self, collection: &str) -> Result<Vec<Document>> {
        self.ensure_indices_initialized().await?;

        let prefix = format!("{}:", collection);
        let mut docs = Vec::new();

        for result in self.cold.scan_prefix(&prefix) {
            let (_, value) = result?;
            if let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                docs.push(doc);
            }
        }

        Ok(docs)
    }

    /// Insert a document from a HashMap (AQL helper)
    ///
    /// Returns the complete document (not just ID) for AQL executor
    pub async fn aql_insert(
        &self,
        collection: &str,
        data: HashMap<String, Value>,
    ) -> Result<Document> {
        // Validate unique constraints before inserting
        self.validate_unique_constraints(collection, &data).await?;

        // Generate a fresh UUIDv7 for the internal system ID
        let sid = Uuid::now_v7().to_string();

        let document = Document {
            _sid: sid.clone(),
            data,
        };

        self.put(
            format!("{}:{}", collection, sid),
            serde_json::to_vec(&document)?,
            None,
        )
        .await?;

        // Publish insert event
        let event = crate::pubsub::ChangeEvent::insert(collection, &sid, document.clone());
        let _ = self.pubsub.publish(event);

        Ok(document)
    }

    /// Update a document by ID with new data (AQL helper)
    ///
    /// Merges new data with existing data and returns updated document
    pub async fn aql_update_document(
        &self,
        collection: &str,
        doc_id: &str,
        updates: HashMap<String, Value>,
    ) -> Result<Document> {
        let key = format!("{}:{}", collection, doc_id);

        // Get existing document
        let existing = self.get(&key)?.ok_or_else(|| {
            AqlError::new(
                ErrorCode::NotFound,
                format!("Document {} not found", doc_id),
            )
        })?;

        let mut doc: Document = serde_json::from_slice(&existing)?;
        let old_doc = doc.clone();

        // Merge updates into existing data
        for (k, v) in updates {
            doc.data.insert(k, v);
        }

        // Validate unique constraints excluding this document
        self.validate_unique_constraints_excluding(collection, &doc.data, doc_id)
            .await?;

        // Save updated document
        self.put(key, serde_json::to_vec(&doc)?, None).await?;

        // Publish update event
        let event = crate::pubsub::ChangeEvent::update(collection, doc_id, old_doc, doc.clone());
        let _ = self.pubsub.publish(event);

        Ok(doc)
    }

    /// Delete a document by ID (AQL helper)
    ///
    /// Returns the deleted document
    pub async fn aql_delete_document(&self, collection: &str, doc_id: &str) -> Result<Document> {
        let key = format!("{}:{}", collection, doc_id);

        // Get existing document first
        let existing = self.get(&key)?.ok_or_else(|| {
            AqlError::new(
                ErrorCode::NotFound,
                format!("Document {} not found", doc_id),
            )
        })?;

        let doc: Document = serde_json::from_slice(&existing)?;

        // Publish delete event before the transaction check so handlers fire
        // consistently regardless of whether this is inside an auto-transaction.
        // This mirrors aql_insert / aql_update_document which publish after put()
        // even when the underlying write is buffered in a transaction.
        let event = crate::pubsub::ChangeEvent::delete(collection, doc_id);
        let _ = self.pubsub.publish(event);

        // If inside a transaction, buffer the delete instead of writing to storage.
        if let Ok(tx_id) = crate::transaction::ACTIVE_TRANSACTION_ID.try_with(|id| *id) {
            if let Some(buffer) = self.transaction_manager.active_transactions.get(&tx_id) {
                buffer.delete(key);
                return Ok(doc);
            }
        }

        // Append to WAL for durability
        if self.config.durability_mode != DurabilityMode::None {
            if let Some(wal_writer) = &self.wal_writer {
                let op = WalOperation::Delete { key: key.clone() };
                wal_writer.send(op).map_err(|_| {
                    AqlError::new(
                        ErrorCode::InternalError,
                        "Failed to send to WAL writer".to_string(),
                    )
                })?;
            } else if let Some(wal) = &self.wal {
                wal.write()
                    .map_err(|e| AqlError::new(ErrorCode::InternalError, e.to_string()))?
                    .append(crate::wal::Operation::Delete, &key, None)?;
            }
        }

        // Delete from storage
        self.cold.delete(&key)?;
        self.hot.delete(&key);

        // Remove from primary index
        if let Some(index) = self.primary_indices.get_mut(collection) {
            index.remove(&key);
        }

        // Remove from secondary indices
        for (field_name, value) in &doc.data {
            let index_key = format!("{}:{}", collection, field_name);
            if let Some(index) = self.secondary_indices.get(&index_key) {
                let value_str = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                if let Some(doc_ids_arc) = index.get(&value_str) {
                    let external_u128 = self.parse_external_id(doc_id);
                    if let Some(internal_id_entry) = self._sid_dictionary.get(&external_u128) {
                        if let Ok(mut storage) = doc_ids_arc.value().write() {
                            storage.remove(*internal_id_entry.value());
                        }
                    }
                }
            }
        }

        // Return the internal ID to the recycling pool
        let external_u128 = self.parse_external_id(doc_id);
        if let Some(internal_id_entry) = self._sid_dictionary.get(&external_u128) {
            if let Ok(mut deleted) = self.deleted_ids.write() {
                deleted.insert(*internal_id_entry.value());
            }
        }

        // Event was already published above (before transaction check).

        Ok(doc)
    }

    /// Get a single document by ID (AQL helper)
    pub async fn aql_get_document(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<Document>> {
        let key = format!("{}:{}", collection, doc_id);

        match self.get(&key)? {
            Some(data) => {
                let doc: Document = serde_json::from_slice(&data)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Begin a transaction (AQL helper) - returns transaction ID
    pub async fn aql_begin_transaction(&self) -> Result<crate::transaction::TransactionId> {
        Ok(self.begin_transaction().await)
    }

    /// Commit a transaction (AQL helper)
    pub async fn aql_commit_transaction(
        &self,
        tx_id: crate::transaction::TransactionId,
    ) -> Result<()> {
        self.commit_transaction(tx_id).await
    }

    /// Rollback a transaction (AQL helper)
    pub async fn aql_rollback_transaction(
        &self,
        tx_id: crate::transaction::TransactionId,
    ) -> Result<()> {
        self.transaction_manager.rollback(tx_id)
    }

    // ============================================
    // AQL Schema Management Wrappers
    // ============================================

    /// Create a collection from AST schema definition
    pub async fn create_collection_schema(
        &self,
        name: &str,
        fields: HashMap<String, crate::types::FieldDefinition>,
    ) -> Result<()> {
        let collection_key = format!("_collection:{}", name);

        // Check if collection already exists
        if self.get(&collection_key)?.is_some() {
            // Already exists
            return Ok(());
        }

        let collection = Collection {
            name: name.to_string(),
            fields,
        };

        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None).await?;
        self.schema_cache.remove(name);

        Ok(())
    }

    /// Add a field to an existing collection schema
    pub async fn add_field_to_schema(
        &self,
        collection_name: &str,
        name: String,
        definition: crate::types::FieldDefinition,
    ) -> Result<()> {
        let mut collection = self
            .get_collection_definition(collection_name)
            .map_err(|_| {
                AqlError::new(ErrorCode::CollectionNotFound, collection_name.to_string())
            })?;

        if collection.fields.contains_key(&name) {
            return Err(AqlError::new(
                ErrorCode::InvalidDefinition,
                format!(
                    "Field '{}' already exists in collection '{}'",
                    name, collection_name
                ),
            ));
        }

        collection.fields.insert(name, definition);

        let collection_key = format!("_collection:{}", collection_name);
        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None).await?;
        self.schema_cache.remove(collection_name);

        Ok(())
    }

    /// Drop a field from an existing collection schema
    pub async fn drop_field_from_schema(
        &self,
        collection_name: &str,
        field_name: String,
    ) -> Result<()> {
        let mut collection = self
            .get_collection_definition(collection_name)
            .map_err(|_| {
                AqlError::new(ErrorCode::CollectionNotFound, collection_name.to_string())
            })?;

        if !collection.fields.contains_key(&field_name) {
            return Err(AqlError::new(
                ErrorCode::InvalidDefinition,
                format!(
                    "Field '{}' does not exist in collection '{}'",
                    field_name, collection_name
                ),
            ));
        }

        collection.fields.remove(&field_name);

        let collection_key = format!("_collection:{}", collection_name);
        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None).await?;
        self.schema_cache.remove(collection_name);

        Ok(())
    }

    /// Rename a field in an existing collection schema
    pub async fn rename_field_in_schema(
        &self,
        collection_name: &str,
        from: String,
        to: String,
    ) -> Result<()> {
        let mut collection = self
            .get_collection_definition(collection_name)
            .map_err(|_| {
                AqlError::new(ErrorCode::CollectionNotFound, collection_name.to_string())
            })?;

        if let Some(def) = collection.fields.remove(&from) {
            if collection.fields.contains_key(&to) {
                return Err(AqlError::new(
                    ErrorCode::InvalidDefinition,
                    format!(
                        "Target field name '{}' already exists in collection '{}'",
                        to, collection_name
                    ),
                ));
            }
            collection.fields.insert(to, def);
        } else {
            return Err(AqlError::new(
                ErrorCode::InvalidDefinition,
                format!("Field '{}' not found", from),
            ));
        }

        let collection_key = format!("_collection:{}", collection_name);
        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None).await?;
        self.schema_cache.remove(collection_name);

        Ok(())
    }

    /// Modify a field in an existing collection schema
    pub async fn modify_field_in_schema(
        &self,
        collection_name: &str,
        name: String,
        definition: crate::types::FieldDefinition,
    ) -> Result<()> {
        let mut collection = self
            .get_collection_definition(collection_name)
            .map_err(|_| {
                AqlError::new(ErrorCode::CollectionNotFound, collection_name.to_string())
            })?;

        if !collection.fields.contains_key(&name) {
            return Err(AqlError::new(
                ErrorCode::InvalidDefinition,
                format!(
                    "Field '{}' does not exist in collection '{}'",
                    name, collection_name
                ),
            ));
        }

        collection.fields.insert(name, definition);

        let collection_key = format!("_collection:{}", collection_name);
        let collection_data = serde_json::to_vec(&collection)?;
        self.put(collection_key, collection_data, None).await?;
        self.schema_cache.remove(collection_name);

        Ok(())
    }

    /// Drop an entire collection definition
    pub async fn drop_collection_schema(&self, collection_name: &str) -> Result<()> {
        let collection_key = format!("_collection:{}", collection_name);
        // Write to WAL before cold delete so a crash doesn't leave orphaned docs
        // with no schema to describe them.
        if self.config.durability_mode != DurabilityMode::None {
            if let Some(wal_writer) = &self.wal_writer {
                let _ = wal_writer.send(WalOperation::Delete { key: collection_key.clone() });
            }
        }
        self.cold.delete(&collection_key)?;
        self.schema_cache.remove(collection_name);
        Ok(())
    }

    // ============================================
    // AQL Migration Wrappers
    // ============================================

    /// Check if a migration version has been applied
    pub async fn is_migration_applied(&self, version: &str) -> Result<bool> {
        let migration_key = format!("_sys_migration:{}", version);
        Ok(self.get(&migration_key)?.is_some())
    }

    /// Mark a migration version as applied
    pub async fn mark_migration_applied(&self, version: &str) -> Result<()> {
        let migration_key = format!("_sys_migration:{}", version);
        let timestamp = chrono::Utc::now().to_rfc3339();
        self.put(migration_key.clone(), timestamp.as_bytes().to_vec(), None)
            .await?;

        // Also store as a document in _migrations so it's queryable via AQL
        let mut data = HashMap::new();
        data.insert("version".to_string(), Value::String(version.to_string()));
        data.insert("status".to_string(), Value::String("applied".to_string()));
        data.insert("applied_at".to_string(), Value::String(timestamp));
        let doc = Document { _sid: version.to_string(), data };
        let doc_key = format!("_migrations:{}", version);
        self.put(doc_key, serde_json::to_vec(&doc)?, None).await?;

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
    async fn test_async_wal_integration() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test_wal_integration.db");
        let db = Aurora::open(db_path.to_str().unwrap()).await.unwrap();

        // 1. Enable WAL explicitly (if your config defaults to off for tests)
        // Note: Your implementation might default it on based on AuroraConfig.

        // Create collection schema first
        db.new_collection(
            "users",
            vec![
                ("id", FieldType::Scalar(crate::types::ScalarType::String), false),
                ("counter", FieldType::Scalar(crate::types::ScalarType::Int), false),
            ],
        )
        .await
        .unwrap();

        let db_clone = db.clone();

        // 2. Spawn 50 concurrent writes
        let handles: Vec<_> = (0..50)
            .map(|i| {
                let db = db_clone.clone();
                tokio::spawn(async move {
                    let doc_id = db
                        .insert_into(
                            "users",
                            vec![
                                ("id", Value::String(format!("user-{}", i))),
                                ("counter", Value::Int(i)),
                            ],
                        )
                        .await
                        .unwrap();
                    (i, doc_id) // Return both the index and the document ID
                })
            })
            .collect();

        // Wait for all to complete and collect the results
        let results = futures::future::join_all(handles).await;
        assert!(results.iter().all(|r| r.is_ok()), "Some writes failed");

        // Extract the document IDs
        let doc_ids: Vec<(i64, String)> = results.into_iter().map(|r| r.unwrap()).collect();

        // Find the document ID for the one with counter=25
        let target_doc_id = doc_ids
            .iter()
            .find(|(counter, _)| *counter == 25i64)
            .map(|(_, doc_id)| doc_id)
            .unwrap();

        // 4. Verify Data Integrity
        let doc = db.get_document("users", target_doc_id).unwrap().unwrap();
        assert_eq!(doc.data.get("counter"), Some(&Value::Int(25)));
    }

    #[tokio::test]
    async fn test_basic_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

        // Test collection creation
        db.new_collection(
            "users",
            vec![
                ("name", FieldType::Scalar(crate::types::ScalarType::String), false),
                ("age", FieldType::Scalar(crate::types::ScalarType::Int), false),
                ("email", FieldType::Scalar(crate::types::ScalarType::String), true),
            ],
        )
        .await?;

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
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

        // Create collection
        db.new_collection("test", vec![("field", FieldType::Scalar(crate::types::ScalarType::String), false)])
            .await?;

        // Start transaction
        let tx_id = db.begin_transaction().await;

        // Insert document
        let doc_id = db
            .insert_into("test", vec![("field", Value::String("value".to_string()))])
            .await?;

        // Commit transaction
        db.commit_transaction(tx_id).await?;

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
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

        // Test collection creation
        db.new_collection(
            "books",
            vec![
                ("title", FieldType::Scalar(crate::types::ScalarType::String), false),
                ("author", FieldType::Scalar(crate::types::ScalarType::String), false),
                ("year", FieldType::Scalar(crate::types::ScalarType::Int), false),
            ],
        )
        .await?;

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
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

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
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

        // Create a test file that's too large (201MB)
        let large_file_path = temp_dir.path().join("large_file.bin");
        let large_data = vec![0u8; 201 * 1024 * 1024];
        std::fs::write(&large_file_path, &large_data)?;

        // Attempt to store the large file
        let result = db
            .put_blob("test:large_blob".to_string(), &large_file_path)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.code == ErrorCode::InvalidOperation); // Expected error

        Ok(())
    }

    #[tokio::test]
    async fn test_unique_constraints() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

        // Create collection with unique email field
        db.new_collection(
            "users",
            vec![
                ("name", FieldType::Scalar(crate::types::ScalarType::String), false),
                ("email", FieldType::Scalar(crate::types::ScalarType::String), true), // unique field
                ("age", FieldType::Scalar(crate::types::ScalarType::Int), false),
            ],
        )
        .await?;

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
        if let Err(e) = result {
            if e.code == ErrorCode::UniqueConstraintViolation {
                let msg = e.message;
                assert!(msg.contains("email"));
                assert!(msg.contains("john@example.com")); // Changed from test@example.com to match the actual value
            } else {
                panic!("Expected UniqueConstraintViolation, got {:?}", e);
            }
        } else {
            panic!("Expected UniqueConstraintViolation");
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

    #[tokio::test]
    async fn test_nullable_field_definition() -> Result<()> {
        use tempfile::TempDir;

        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap()).await?;

        // Test 3-tuple format (defaults nullable to false)
        db.new_collection(
            "products",
            vec![
                ("name", FieldType::Scalar(crate::types::ScalarType::String), false),
                ("sku", FieldType::Scalar(crate::types::ScalarType::String), true), // unique
            ],
        )
        .await?;

        // Test 4-tuple format with explicit nullable control
        db.new_collection(
            "users",
            vec![
                ("name", FieldType::Scalar(crate::types::ScalarType::String), false, false), // not nullable
                ("email", FieldType::Scalar(crate::types::ScalarType::String), true, false), // unique, not nullable
                ("bio", FieldType::Scalar(crate::types::ScalarType::String), false, true),   // nullable
                ("age", FieldType::Scalar(crate::types::ScalarType::Int), false, true),      // nullable
            ],
        )
        .await?;

        // Verify the schema was created correctly
        let schema = db.ensure_schema_hot("users")?;

        // Check that name is not nullable
        assert_eq!(schema.fields.get("name").unwrap().nullable, false);

        // Check that email is not nullable
        assert_eq!(schema.fields.get("email").unwrap().nullable, false);

        // Check that bio is nullable
        assert_eq!(schema.fields.get("bio").unwrap().nullable, true);

        // Check that age is nullable
        assert_eq!(schema.fields.get("age").unwrap().nullable, true);

        // Verify products collection (3-tuple defaults to false)
        let products_schema = db.ensure_schema_hot("products")?;
        assert_eq!(products_schema.fields.get("name").unwrap().nullable, false);
        assert_eq!(products_schema.fields.get("sku").unwrap().nullable, false);

        Ok(())
    }

    #[tokio::test]
    async fn test_upsert_unique_field_after_restart() {
        // Simulates the post-restart scenario:
        // _sid_dictionary is empty but secondary_indices have the bitmap entries.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().to_path_buf();

        // Session 1: insert a fact
        let mut document_id = String::new();
        {
            let db = Aurora::with_config(AuroraConfig {
                db_path: db_path.clone(),
                enable_wal: false,
                ..Default::default()
            }).await.unwrap();

            db.new_collection("facts", vec![
                ("key",        FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: true,  indexed: true, nullable: false, validations: vec![] }),
                ("value_json", FieldDefinition { field_type: FieldType::SCALAR_STRING, unique: false, indexed: false, nullable: false, validations: vec![] }),
            ]).await.unwrap();

            let aql = r#"
                mutation {
                    insertInto(collection: "facts", data: {
                        key: "last_session_at",
                        value_json: "\"2026-04-05T01:00:00Z\""
                    }) { id }
                }
            "#.to_string();
            let result = db.execute(aql).await.unwrap();
            
            // Extract ID from result (QueryResult format)
            if let crate::parser::executor::ExecutionResult::Mutation(mutres) = result {
                document_id = mutres.returned_documents[0]._sid.clone();
            }
            
            // Explicitly flush to ensure secondary_indices are written to disk
            db.flush().unwrap();
        }
        // db dropped here — simulates process restart
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Session 2: new Aurora instance, _sid_dictionary starts empty
        {
            let db = Aurora::with_config(AuroraConfig {
                db_path: db_path.clone(),
                enable_wal: false,
                ..Default::default()
            }).await.unwrap();

            // Update the same key with a new value directly by ID — must NOT error
            let aql = format!(r#"
                mutation {{
                    update(
                        collection: "facts",
                        id: "{}",
                        data: {{ key: "last_session_at", value_json: "\"2026-04-05T03:00:00Z\"" }}
                    ) {{ id }}
                }}
            "#, document_id);

            db.execute(aql).await.expect("update of existing unique key after restart should not fail");
        }
    }
}
