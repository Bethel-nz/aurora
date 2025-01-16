use crate::error::{AuroraError, Result};
use crate::index::{Index, IndexDefinition};
use crate::storage::{ColdStore, HotStore};
use crate::types::{Collection, Document, FieldDefinition, FieldType, InsertData, Value};
use crate::wal::{Operation, WriteAheadLog};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use dashmap::DashMap;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;
use uuid::Uuid;
use serde_json::Value as JsonValue;
use std::fs::File as StdFile;

// Index types for faster lookups
type PrimaryIndex = DashMap<String, Vec<u8>>;
type SecondaryIndex = DashMap<String, Vec<String>>;

// Move DataInfo enum outside impl block
#[derive(Debug)]
pub enum DataInfo {
    Data { 
        size: usize,
        preview: String 
    },
    Blob { 
        size: usize 
    },
    Compressed { 
        size: usize 
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

pub struct Aurora {
    hot: HotStore,
    cold: ColdStore,
    // Indexing
    primary_indices: Arc<DashMap<String, PrimaryIndex>>,
    secondary_indices: Arc<DashMap<String, SecondaryIndex>>,
    indices_initialized: Arc<OnceCell<()>>,
    wal: Arc<Mutex<WriteAheadLog>>,
    in_transaction: std::sync::atomic::AtomicBool,
    transaction_ops: DashMap<String, Vec<u8>>,
    indices: Arc<DashMap<String, Index>>,
}

impl Aurora {
    /// Open or create a database with lazy loading
    pub fn open(path: &str) -> Result<Self> {
        let exists = Path::new(path).exists();
        
        // Only print if in debug mode
        #[cfg(debug_assertions)]
        println!("{} database at {}", if exists { "Opening" } else { "Creating" }, path);
        
        Ok(Self {
            hot: HotStore::new(),
            cold: ColdStore::new(path)?,
            primary_indices: Arc::new(DashMap::with_capacity(16)), // Pre-allocate with reasonable size
            secondary_indices: Arc::new(DashMap::with_capacity(16)),
            indices_initialized: Arc::new(OnceCell::new()),
            wal: Arc::new(Mutex::new(WriteAheadLog::new(path)?)),
            in_transaction: std::sync::atomic::AtomicBool::new(false),
            transaction_ops: DashMap::new(),
            indices: Arc::new(DashMap::with_capacity(16)),
        })
    }

    // Lazy index initialization
    async fn ensure_indices_initialized(&self) -> Result<()> {
        self.indices_initialized
            .get_or_init(|| async {
                println!("Initializing indices...");
                if let Err(e) = self.initialize_indices() {
                    eprintln!("Failed to initialize indices: {:?}", e);
                }
                println!("Indices initialized");
                ()
            })
            .await;
        Ok(())
    }

    fn initialize_indices(&self) -> Result<()> {
        // Scan existing data and build indices
        for result in self.cold.scan() {
            let (key, value) = result?;
            let key_str = std::str::from_utf8(&key.as_bytes())
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

        // Check primary index
        if let Some(collection) = key.split(':').next() {
            if let Some(index) = self.primary_indices.get(collection) {
                if let Some(value) = index.get(key) {
                    // Promote to hot cache
                    self.hot.set(key.to_string(), value.clone(), None);
                    return Ok(Some(value.clone()));
                }
            }
        }

        // Fallback to cold storage
        let value = self.cold.get(key)?;
        if let Some(v) = &value {
            self.hot.set(key.to_string(), v.clone(), None);
        }
        Ok(value)
    }

    pub fn put(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        const MAX_BLOB_SIZE: usize = 50 * 1024 * 1024; // 50MB limit
        
        if value.len() > MAX_BLOB_SIZE {
            return Err(AuroraError::InvalidOperation(
                format!("Blob size {} exceeds maximum allowed size of {}MB", 
                    value.len() / (1024 * 1024), 
                    MAX_BLOB_SIZE / (1024 * 1024)
                )
            ));
        }

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(Operation::Put, &key, Some(&value))?;
        }

        if self.in_transaction.load(std::sync::atomic::Ordering::SeqCst) {
            self.transaction_ops.insert(key.clone(), value.clone());
            return Ok(());
        }

        self.cold.set(key.clone(), value.clone())?;
        self.hot.set(key, value, ttl);
        Ok(())
    }

    fn index_value(&self, collection: &str, key: &str, value: &[u8]) -> Result<()> {
        // Update primary index
        self.primary_indices
            .entry(collection.to_string())
            .or_insert_with(DashMap::new)
            .insert(key.to_string(), value.to_vec());

        // Update secondary indices if it's a JSON document
        if let Ok(doc) = serde_json::from_slice::<Document>(value) {
            for (field, value) in doc.data {
                self.secondary_indices
                    .entry(format!("{}:{}", collection, field))
                    .or_insert_with(DashMap::new)
                    .entry(value.to_string())
                    .or_insert_with(Vec::new)
                    .push(key.to_string());
            }
        }
        Ok(())
    }

    // Simplified collection scan (fallback)
    fn scan_collection(&self, collection: &str) -> Result<Vec<Document>> {
        let _prefix = format!("{}:", collection);
        let mut documents = Vec::new();

        if let Some(index) = self.primary_indices.get(collection) {
            for entry in index.iter() {
                if let Ok(doc) = serde_json::from_slice(entry.value()) {
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
            return Err(AuroraError::InvalidOperation(
                format!("File size {} MB exceeds maximum allowed size of {} MB",
                    file_size / (1024 * 1024),
                    MAX_FILE_SIZE / (1024 * 1024)
                )
            ));
        }

        let mut file = File::open(file_path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
        self.put(key, buffer, None)
    }

    pub fn new_collection(&self, name: &str, fields: Vec<(&str, FieldType, bool)>) -> Result<()> {
        let collection = Collection {
            name: name.to_string(),
            fields: fields
                .into_iter()
                .map(|(name, field_type, unique)| {
                    (name.to_string(), 
                     FieldDefinition {
                        field_type,
                        unique,
                        indexed: unique,
                    })
                })
                .collect(),
            unique_fields: Vec::new(),
        };

        self.put(
            format!("_collection:{}", name),
            serde_json::to_vec(&collection)?,
            None,
        )
    }

    pub fn insert_into(&self, collection: &str, data: InsertData) -> Result<String> {
        let data_map: HashMap<String, Value> = data
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

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

        Ok(doc_id)
    }

    pub async fn get_all_collection(&self, collection: &str) -> Result<Vec<Document>> {
        self.ensure_indices_initialized().await?;
        self.scan_collection(collection)
    }

    pub fn get_data_by_pattern(&self, pattern: &str) -> Result<Vec<(String, DataInfo)>> {
        let mut data = Vec::new();
        
        if let Some(index) = self.primary_indices.get(pattern.split(':').next().unwrap_or("")) {
            for entry in index.iter() {
                if entry.key().contains(pattern) {
                    let value = entry.value();
                    let info = if value.starts_with(b"BLOB:") {
                        DataInfo::Blob { 
                            size: value.len() 
                        }
                    } else {
                        DataInfo::Data { 
                            size: value.len(),
                            preview: String::from_utf8_lossy(&value[..value.len().min(50)]).into_owned()
                        }
                    };
                    
                    data.push((entry.key().clone(), info));
                }
            }
        }
        
        Ok(data)
    }

    pub fn begin_transaction(&self) -> Result<()> {
        if self.in_transaction.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(AuroraError::InvalidOperation("Transaction already in progress".into()));
        }
        let mut wal = self.wal.lock().unwrap();
        wal.append(Operation::BeginTx, "", None)?;
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<()> {
        if !self.in_transaction.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuroraError::InvalidOperation("No transaction in progress".into()));
        }

        // Apply all transaction operations
        for entry in self.transaction_ops.iter() {
            self.cold.set(entry.key().clone(), entry.value().clone())?;
        }

        let mut wal = self.wal.lock().unwrap();
        wal.append(Operation::CommitTx, "", None)?;
        self.transaction_ops.clear();
        self.in_transaction.store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub fn rollback_transaction(&self) -> Result<()> {
        if !self.in_transaction.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuroraError::InvalidOperation("No transaction in progress".into()));
        }

        let mut wal = self.wal.lock().unwrap();
        wal.append(Operation::RollbackTx, "", None)?;
        self.transaction_ops.clear();
        self.in_transaction.store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub async fn create_index(&self, definition: IndexDefinition) -> Result<()> {
        let index = Index::new(definition.clone());
        
        // Build index from existing data
        let collection = definition.collection.clone();
        for doc in self.get_all_collection(&collection).await? {
            index.insert(&doc)?;
        }
        
        self.indices.insert(definition.name.clone(), index);
        Ok(())
    }

    pub fn query<'a>(&'a self, collection: &str) -> QueryBuilder<'a> {
        QueryBuilder::new(self, collection)
    }

    pub fn search<'a>(&'a self, collection: &str) -> SearchBuilder<'a> {
        SearchBuilder::new(self, collection)
    }

    pub fn get_document(&self, collection: &str, id: &str) -> Result<Option<Document>> {
        let key = format!("{}:{}", collection, id);
        if let Some(data) = self.get(&key)? {
            Ok(Some(serde_json::from_slice(&data)?))
        } else {
            Ok(None)
        }
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        // Remove from hot cache
        self.hot.delete(key);

        // Remove from cold storage
        self.cold.delete(key)?;

        // Log deletion
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(Operation::Delete, key, None)?;
        }

        // Remove from indices if it's a document
        if let Some(collection) = key.split(':').next() {
            if let Some(value) = self.get(key)? {
                if let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                    self.remove_from_indices(collection, &doc)?;
                }
            }
        }

        Ok(())
    }

    pub async fn delete_collection(&self, collection: &str) -> Result<()> {
        let prefix = format!("{}:", collection);
        
        // Get all keys in collection
        let keys: Vec<String> = self.cold.scan()
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
        self.secondary_indices.retain(|k, _| !k.starts_with(&prefix));

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
            if let Some(index) = self.secondary_indices.get(&index_key) {
                if let Some(mut doc_ids) = index.get_mut(&value.to_string()) {
                    doc_ids.retain(|id| id != &doc.id);
                }
            }
        }

        Ok(())
    }

    pub async fn search_text(&self, collection: &str, field: &str, query: &str) -> Result<Vec<Document>> {
        let mut results = Vec::new();
        let docs = self.get_all_collection(collection).await?;
        
        for doc in docs {
            if let Some(Value::String(text)) = doc.data.get(field) {
                if text.to_lowercase().contains(&query.to_lowercase()) {
                    results.push(doc);
                }
            }
        }
        
        Ok(results)
    }

    /// Export specific collection to JSON file
    pub fn export_as_json(&self, collection: &str, filename: &str) -> Result<()> {
        let output_path = if !filename.ends_with(".json") {
            format!("{}.json", filename)
        } else {
            filename.to_string()
        };

        let mut docs = Vec::new();

        // Get all documents from the specified collection
        for result in self.cold.scan() {
            let (key, value) = result?;
            
            // Only process documents from the specified collection
            if let Some(key_collection) = key.split(':').next() {
                if key_collection == collection && !key.starts_with("_collection:") {
                    if let Ok(doc) = serde_json::from_slice::<Document>(&value) {
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
                                },
                                Value::Bool(b) => clean_doc.insert(k, JsonValue::Bool(b)),
                                Value::Array(arr) => {
                                    let clean_arr: Vec<JsonValue> = arr.into_iter()
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
                                },
                                Value::Uuid(u) => clean_doc.insert(k, JsonValue::String(u.to_string())),
                                Value::Null => clean_doc.insert(k, JsonValue::Null),
                                Value::Object(_) => None, // Handle nested objects if needed
                            };
                        }
                        docs.push(JsonValue::Object(clean_doc));
                    }
                }
            }
        }

        let output = JsonValue::Object(serde_json::Map::from_iter(vec![
            (collection.to_string(), JsonValue::Array(docs))
        ]));

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
            if let Some(key_collection) = key.split(':').next() {
                if key_collection == collection && !key.starts_with("_collection:") {
                    if let Ok(doc) = serde_json::from_slice::<Document>(&value) {
                        // Write headers from first document
                        if first_doc && !doc.data.is_empty() {
                            headers = doc.data.keys().cloned().collect();
                            writer.write_record(&headers)?;
                            first_doc = false;
                        }

                        // Write the document values
                        let values: Vec<String> = headers.iter()
                            .map(|header| {
                                doc.data.get(header)
                                    .map(|v| v.to_string())
                                    .unwrap_or_default()
                            })
                            .collect();
                        writer.write_record(&values)?;
                    }
                }
            }
        }

        writer.flush()?;
        println!("Exported collection '{}' to {}", collection, &output_path);
        Ok(())
    }
}

pub struct QueryBuilder<'a> {
    db: &'a Aurora,
    collection: String,
    filters: Vec<Box<dyn Fn(&Document) -> bool + 'a>>,
    order_by: Option<(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
}

// Create a FilterBuilder to use inside closures
pub struct FilterBuilder<'a> {
    doc: &'a Document,
}

impl<'a> FilterBuilder<'a> {
    fn new(doc: &'a Document) -> Self {
        Self { doc }
    }

    pub fn eq<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).map_or(false, |v| v == &value)
    }

    pub fn gt<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).map_or(false, |v| v > &value)
    }

    pub fn lt<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).map_or(false, |v| v < &value)
    }

    pub fn contains(&self, field: &str, value: &str) -> bool {
        self.doc.data.get(field).map_or(false, |v| {
            match v {
                Value::String(s) => s.contains(value),
                Value::Array(arr) => arr.contains(&Value::String(value.to_string())),
                _ => false,
            }
        })
    }

    pub fn in_values<T: Into<Value> + Clone>(&self, field: &str, values: &[T]) -> bool {
        let values: Vec<Value> = values.iter().map(|v| v.clone().into()).collect();
        self.doc.data.get(field).map_or(false, |v| values.contains(v))
    }
}

impl<'a> QueryBuilder<'a> {
    pub fn new(db: &'a Aurora, collection: &str) -> Self {
        Self {
            db,
            collection: collection.to_string(),
            filters: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    pub fn filter<F>(mut self, predicate: F) -> Self 
    where 
        F: Fn(&FilterBuilder) -> bool + 'a
    {
        self.filters.push(Box::new(move |doc| {
            let filter = FilterBuilder::new(doc);
            predicate(&filter)
        }));
        self
    }

    pub fn order_by(mut self, field: &str, ascending: bool) -> Self {
        self.order_by = Some((field.to_string(), ascending));
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub async fn collect(self) -> Result<Vec<Document>> {
        let mut docs = self.db.get_all_collection(&self.collection).await?;
        
        // Apply filters
        docs.retain(|doc| self.filters.iter().all(|f| f(doc)));

        // Apply ordering
        if let Some((field, ascending)) = self.order_by {
            docs.sort_by(|a, b| {
                match (a.data.get(&field), b.data.get(&field)) {
                    (Some(v1), Some(v2)) => {
                        let cmp = v1.cmp(v2);
                        if ascending { cmp } else { cmp.reverse() }
                    },
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            });
        }

        // Apply offset and limit safely
        let start = self.offset.unwrap_or(0);
        let end = self.limit
            .map(|l| start.saturating_add(l))
            .unwrap_or(docs.len());
        
        // Ensure we don't go out of bounds
        let end = end.min(docs.len());
        Ok(docs.get(start..end).unwrap_or(&[]).to_vec())
    }

    // Helper methods for common filters
    pub fn eq<T: Into<Value>>(field: &'a str, value: T) -> impl Fn(&Document) -> bool + 'a {
        let value = value.into();
        move |doc| doc.data.get(field).map_or(false, |v| v == &value)
    }

    pub fn gt<T: Into<Value>>(field: &'a str, value: T) -> impl Fn(&Document) -> bool + 'a {
        let value = value.into();
        move |doc| doc.data.get(field).map_or(false, |v| v > &value)
    }

    pub fn lt<T: Into<Value>>(field: &'a str, value: T) -> impl Fn(&Document) -> bool + 'a {
        let value = value.into();
        move |doc| doc.data.get(field).map_or(false, |v| v < &value)
    }

    pub fn contains(field: &'a str, value: &'a str) -> impl Fn(&Document) -> bool + 'a {
        move |doc| {
            doc.data.get(field).map_or(false, |v| {
                match v {
                    Value::String(s) => s.contains(value),
                    Value::Array(arr) => arr.contains(&Value::String(value.to_string())),
                    _ => false,
                }
            })
        }
    }

    pub fn in_values<T: Into<Value> + Clone>(field: &'a str, values: &'a [T]) -> impl Fn(&Document) -> bool + 'a {
        let values: Vec<Value> = values.iter().map(|v| v.clone().into()).collect();
        move |doc| doc.data.get(field).map_or(false, |v| values.contains(v))
    }

    pub fn not_in<T: Into<Value> + Clone>(field: &'a str, values: &'a [T]) -> impl Fn(&Document) -> bool + 'a {
        let values: Vec<Value> = values.iter().map(|v| v.clone().into()).collect();
        move |doc| doc.data.get(field).map_or(true, |v| !values.contains(v))
    }

    pub fn or<F1, F2>(f1: F1, f2: F2) -> impl Fn(&Document) -> bool + 'a 
    where
        F1: Fn(&Document) -> bool + 'a,
        F2: Fn(&Document) -> bool + 'a,
    {
        move |doc| f1(doc) || f2(doc)
    }

    pub fn and<F1, F2>(f1: F1, f2: F2) -> impl Fn(&Document) -> bool + 'a 
    where
        F1: Fn(&Document) -> bool + 'a,
        F2: Fn(&Document) -> bool + 'a,
    {
        move |doc| f1(doc) && f2(doc)
    }

    pub async fn sum(self, field: &str) -> Result<f64> {
        let docs = self.db.get_all_collection(&self.collection).await?;
        Ok(docs.iter()
            .filter(|doc| self.filters.iter().all(|f| f(doc)))
            .filter_map(|doc| {
                doc.data.get(field).and_then(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
            })
            .sum())
    }

    pub async fn min(self, field: &str) -> Result<Option<Value>> {
        let docs = self.db.get_all_collection(&self.collection).await?;
        Ok(docs.iter()
            .filter(|doc| self.filters.iter().all(|f| f(doc)))
            .filter_map(|doc| doc.data.get(field))
            .min()
            .cloned())
    }

    pub async fn max(self, field: &str) -> Result<Option<Value>> {
        let docs = self.db.get_all_collection(&self.collection).await?;
        Ok(docs.iter()
            .filter(|doc| self.filters.iter().all(|f| f(doc)))
            .filter_map(|doc| doc.data.get(field))
            .max()
            .cloned())
    }
}

pub struct SearchBuilder<'a> {
    db: &'a Aurora,
    collection: String,
    field: Option<String>,
    query: Option<String>,
    fuzzy: bool,
}

impl<'a> SearchBuilder<'a> {
    pub fn new(db: &'a Aurora, collection: &str) -> Self {
        Self {
            db,
            collection: collection.to_string(),
            field: None,
            query: None,
            fuzzy: false,
        }
    }

    pub fn field(mut self, field: &str) -> Self {
        self.field = Some(field.to_string());
        self
    }

    pub fn matching(mut self, query: &str) -> Self {
        self.query = Some(query.to_string());
        self
    }

    pub fn fuzzy(mut self, enable: bool) -> Self {
        self.fuzzy = enable;
        self
    }

    pub async fn collect(self) -> Result<Vec<Document>> {
        let field = self.field.ok_or_else(|| AuroraError::InvalidOperation("Search field not specified".into()))?;
        let query = self.query.ok_or_else(|| AuroraError::InvalidOperation("Search query not specified".into()))?;
        
        self.db.search_text(&self.collection, &field, &query).await
    }
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
        db.new_collection("users", vec![
            ("name", FieldType::String, false),
            ("age", FieldType::Int, false),
            ("email", FieldType::String, true),
        ])?;

        // Test document insertion
        let doc_id = db.insert_into("users", vec![
            ("name", Value::String("John Doe".to_string())),
            ("age", Value::Int(30)),
            ("email", Value::String("john@example.com".to_string())),
        ])?;

        // Test document retrieval
        let doc = db.get_document("users", &doc_id)?.unwrap();
        assert_eq!(doc.data.get("name").unwrap(), &Value::String("John Doe".to_string()));
        assert_eq!(doc.data.get("age").unwrap(), &Value::Int(30));

        Ok(())
    }

    #[tokio::test]
    async fn test_transactions() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Start transaction
        db.begin_transaction()?;

        // Insert document
        let doc_id = db.insert_into("test", vec![
            ("field", Value::String("value".to_string())),
        ])?;

        // Commit transaction
        db.commit_transaction()?;

        // Verify document exists
        let doc = db.get_document("test", &doc_id)?.unwrap();
        assert_eq!(doc.data.get("field").unwrap(), &Value::String("value".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_query_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.aurora");
        let db = Aurora::open(db_path.to_str().unwrap())?;

        // Create test collection
        db.new_collection("books", vec![
            ("title", FieldType::String, false),
            ("author", FieldType::String, false),
            ("year", FieldType::Int, false),
        ])?;

        // Insert test data
        db.insert_into("books", vec![
            ("title", Value::String("Book 1".to_string())),
            ("author", Value::String("Author 1".to_string())),
            ("year", Value::Int(2020)),
        ])?;

        db.insert_into("books", vec![
            ("title", Value::String("Book 2".to_string())),
            ("author", Value::String("Author 2".to_string())),
            ("year", Value::Int(2021)),
        ])?;

        // Test query using the new builder API
        let results = db.query("books")
            .filter(|doc| doc.gt("year", 2020))
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
        let result = db.put_blob("test:large_blob".to_string(), &large_file_path).await;
        
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AuroraError::InvalidOperation(_)
        ));

        Ok(())
    }
}
