//! # Aurora Query System
//!
//! This module provides a powerful, fluent query interface for filtering, sorting,
//! and retrieving documents from Aurora collections.

use crate::Aurora;
use crate::error::Result;
use crate::types::{Document, Value};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use roaring::RoaringBitmap;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SimpleQueryBuilder {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub order_by: Option<(String, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Builder for creating and executing document queries.
pub struct QueryBuilder<'a> {
    db: &'a Aurora,
    collection: String,
    filters: Vec<Filter>,
    order_by: Option<(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
    fields: Option<Vec<String>>,
    debounce_duration: Option<std::time::Duration>,
}

/// Builder for constructing document filter expressions.
pub struct FilterBuilder;

impl FilterBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn eq<T: Into<Value>>(&self, field: &str, value: T) -> Filter {
        Filter::Eq(field.to_string(), value.into())
    }

    pub fn ne<T: Into<Value>>(&self, field: &str, value: T) -> Filter {
        Filter::Ne(field.to_string(), value.into())
    }

    pub fn in_values<T: Into<Value> + Clone>(&self, field: &str, values: &[T]) -> Filter {
        Filter::In(field.to_string(), values.iter().cloned().map(|v| v.into()).collect())
    }

    pub fn starts_with(&self, field: &str, value: &str) -> Filter {
        Filter::StartsWith(field.to_string(), value.to_string())
    }

    pub fn contains(&self, field: &str, value: &str) -> Filter {
        Filter::Contains(field.to_string(), value.to_string())
    }

    pub fn gt<T: Into<Value>>(&self, field: &str, value: T) -> Filter {
        Filter::Gt(field.to_string(), value.into())
    }

    pub fn gte<T: Into<Value>>(&self, field: &str, value: T) -> Filter {
        Filter::Gte(field.to_string(), value.into())
    }

    pub fn lt<T: Into<Value>>(&self, field: &str, value: T) -> Filter {
        Filter::Lt(field.to_string(), value.into())
    }

    pub fn lte<T: Into<Value>>(&self, field: &str, value: T) -> Filter {
        Filter::Lte(field.to_string(), value.into())
    }

    pub fn in_vec<T: Into<Value>>(&self, field: &str, values: Vec<T>) -> Filter {
        Filter::In(field.to_string(), values.into_iter().map(|v| v.into()).collect())
    }

    pub fn between<T: Into<Value> + Clone>(&self, field: &str, min: T, max: T) -> Filter {
        Filter::And(vec![
            Filter::Gte(field.to_string(), min.into()),
            Filter::Lte(field.to_string(), max.into()),
        ])
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
            fields: None,
            debounce_duration: None,
        }
    }

    pub fn filter<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&FilterBuilder) -> Filter,
    {
        let builder = FilterBuilder::new();
        self.filters.push(f(&builder));
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

    pub fn select(mut self, fields: Vec<&str>) -> Self {
        self.fields = Some(fields.into_iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn debounce(mut self, duration: std::time::Duration) -> Self {
        self.debounce_duration = Some(duration);
        self
    }

    pub async fn first_one(self) -> Result<Option<Document>> {
        let docs = self.limit(1).collect().await?;
        Ok(docs.into_iter().next())
    }

    /// Executes the query and returns the matching documents.
    /// Uses secondary indices (Roaring Bitmaps) for optimized filtering when possible.
    pub async fn collect(self) -> Result<Vec<Document>> {
        self.db.ensure_indices_initialized().await?;

        // Optimized Bitwise Intersection Path
        let mut candidate_bitmap: Option<RoaringBitmap> = None;

        for filter in &self.filters {
            if let Filter::Eq(field, value) = filter {
                // Check secondary indices
                let index_key = format!("{}:{}", self.collection, field);
                let val_str = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                let full_key = format!("{}:{}:{}", self.collection, field, val_str);

                let mut current_bitmap = RoaringBitmap::new();
                let mut found = false;

                // 1. Check Cold Index (mmap) — bounds-checked to avoid panic on corrupt manifest
                if let Some(loc) = self.db.index_manifest.get(&full_key) {
                    let (offset, len) = *loc.value();
                    if let Ok(guard) = self.db.mmap_index.read() {
                        if let Some(mmap) = guard.as_ref() {
                            if offset + len <= mmap.len() {
                                let bytes = &mmap[offset..(offset + len)];
                                if let Ok(cold_bitmap) = RoaringBitmap::deserialize_from(bytes) {
                                    current_bitmap |= cold_bitmap;
                                    found = true;
                                }
                            }
                        }
                    }
                }

                // 2. Check Hot Index — direct entry lookup, no DashMap clone
                if let Some(storage_arc) = self.db.get_indexed_storage(&index_key, &val_str) {
                    if let Ok(storage) = storage_arc.read() {
                        current_bitmap |= storage.to_bitmap();
                        found = true;
                    }
                }

                if !found {
                    // Only short-circuit on an indexed miss when there is NO active
                    // transaction — transaction writes are buffered and never reach the
                    // bitmap index, so we must not hide them with an early empty return.
                    let in_transaction = crate::transaction::ACTIVE_TRANSACTION_ID
                        .try_with(|id| *id)
                        .ok()
                        .and_then(|id| self.db.transaction_manager.active_transactions.get(&id))
                        .is_some();

                    if !in_transaction && self.db.has_index_key(&index_key) {
                        return Ok(vec![]);
                    }
                    // Index absent or inside a transaction → fall through to scan path
                    candidate_bitmap = None;
                    break;
                }

                if let Some(ref mut existing) = candidate_bitmap {
                    *existing &= current_bitmap; // Bitwise AND intersection
                } else {
                    candidate_bitmap = Some(current_bitmap);
                }

                // Short-circuit on empty intersection — but only outside a transaction.
                // Buffered inserts are merged later and may still satisfy all filters.
                if let Some(ref b) = candidate_bitmap {
                    if b.is_empty() {
                        let in_transaction = crate::transaction::ACTIVE_TRANSACTION_ID
                            .try_with(|id| *id)
                            .ok()
                            .and_then(|id| self.db.transaction_manager.active_transactions.get(&id))
                            .is_some();
                        if !in_transaction {
                            return Ok(vec![]);
                        }
                    }
                }
            }
        }

        let mut docs = if let Some(bitmap) = candidate_bitmap {
            // OPTIMIZATION: If the query only requests 'id', we can bypass Sled hydration entirely
            let id_only = self.fields.as_ref().map(|f| f.len() == 1 && f[0] == "id").unwrap_or(false);
            
            // Check active transaction buffer for collection members
            let tx_id = crate::transaction::ACTIVE_TRANSACTION_ID
                .try_with(|id| *id)
                .ok();
            
            let tx_buffer = tx_id.and_then(|id| self.db.transaction_manager.active_transactions.get(&id));

            // Hydrate only the final matching IDs
            let mut final_docs = Vec::with_capacity(bitmap.len() as usize);
            for internal_id in bitmap {
                if let Some(external_id) = self.db.get_external_id(internal_id) {
                    // IMPORTANT: Check if this doc was deleted in the current transaction
                    if let Some(ref buffer) = tx_buffer {
                        let key = format!("{}:{}", self.collection, external_id);
                        if buffer.deletes.contains_key(&key) {
                            continue;
                        }
                    }

                    if id_only && self.filters.is_empty() {
                        // ULTRA FAST PATH: No filters and id only
                        final_docs.push(Document { id: external_id, data: HashMap::new() });
                        continue;
                    }

                    if let Ok(Some(doc)) = self.db.get_document(&self.collection, &external_id) {
                        // Double-check with full filter (for any non-indexed conditions)
                        if self.filters.iter().all(|f| f.matches(&doc)) {
                            final_docs.push(doc);
                        }
                    }
                }
            }

            // Also check for NEW documents in transaction that might match (not in bitmap index yet)
            if let Some(buffer) = tx_buffer {
                let prefix = format!("{}:", self.collection);
                for item in buffer.writes.iter() {
                    let key: &String = item.key();
                    if let Some(external_id) = key.strip_prefix(&prefix) {
                        
                        // If it's already in final_docs (from bitmap index), skip it
                        if final_docs.iter().any(|d| d.id == external_id) {
                            continue;
                        }

                        let data: &Vec<u8> = item.value();
                        if let Ok(doc) = self.db.deserialize_internal::<Document>(data) {
                            if self.filters.iter().all(|f| f.matches(&doc)) {
                                final_docs.push(doc);
                            }
                        }
                    }
                }
            }

            final_docs
        } else {
            // Fallback to scan if no indices were hit.
            // Only pre-limit when there is no sort — applying a limit before sorting
            // truncates the match set and returns the wrong page when scan order
            // differs from sort order.
            let scan_limit = if self.order_by.is_none() {
                self.limit.map(|l| l + self.offset.unwrap_or(0))
            } else {
                None
            };
            
            let db_filters = self.filters.clone();
            self.db.scan_and_filter(&self.collection, move |doc| {
                db_filters.iter().all(|f| f.matches(doc))
            }, scan_limit)?
        };

        // Apply Sorting
        if let Some((field, ascending)) = self.order_by {
            docs.sort_by(|a, b| {
                let v1 = a.data.get(&field);
                let v2 = b.data.get(&field);
                let ord = compare_values(v1, v2);
                if ascending { ord } else { ord.reverse() }
            });
        }

        // Apply Offset/Limit
        let mut start = self.offset.unwrap_or(0);
        if start > docs.len() { start = docs.len(); }
        let mut end = docs.len();
        if let Some(max) = self.limit {
            if start + max < end { end = start + max; }
        }

        let mut result = docs[start..end].to_vec();

        // Apply computed fields
        if let Ok(computed) = self.db.computed.read() {
            for doc in &mut result {
                let _ = computed.apply(&self.collection, doc);
            }
        }

        // Apply field projection when select() was called
        if let Some(ref fields) = self.fields {
            let field_set: std::collections::HashSet<&str> =
                fields.iter().map(|s| s.as_str()).collect();
            for doc in &mut result {
                doc.data.retain(|k, _| field_set.contains(k.as_str()));
            }
        }

        Ok(result)
    }

    pub async fn count(self) -> Result<usize> {
        let results = self.collect().await?;
        Ok(results.len())
    }

    pub async fn delete(self) -> Result<usize> {
        let db = self.db;
        let collection = self.collection.clone();
        let docs = self.collect().await?;
        let count = docs.len();
        for doc in docs {
            let _ = db.aql_delete_document(&collection, &doc.id).await;
        }
        Ok(count)
    }

    pub async fn watch(self) -> Result<crate::reactive::QueryWatcher> {
        let collection = self.collection.clone();
        let filters = self.filters.clone();
        let db_clone = self.db.clone();
        let debounce_duration = self.debounce_duration;

        let initial_results = self.collect().await?;
        let listener = db_clone.pubsub.listen(&collection);
        let state = Arc::new(crate::reactive::ReactiveQueryState::new(filters));

        Ok(crate::reactive::QueryWatcher::new(
            Arc::new(db_clone),
            collection,
            listener,
            state,
            initial_results,
            debounce_duration,
        ))
    }
}

/// Builder for full-text search queries.
pub struct SearchBuilder<'a> {
    db: &'a Aurora,
    collection: String,
    query: String,
    limit: Option<usize>,
    fuzzy: bool,
    distance: u8,
    search_fields: Option<Vec<String>>,
}

impl<'a> SearchBuilder<'a> {
    pub fn new(db: &'a Aurora, collection: &str) -> Self {
        Self {
            db,
            collection: collection.to_string(),
            query: String::new(),
            limit: None,
            fuzzy: false,
            distance: 0,
            search_fields: None,
        }
    }

    pub fn query(mut self, query: &str) -> Self {
        self.query = query.to_string();
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn fuzzy(mut self, distance: u8) -> Self {
        self.fuzzy = true;
        self.distance = distance;
        self
    }

    /// Restrict search to specific field names (None = all string fields)
    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.search_fields = Some(fields);
        self
    }

    /// Like collect() but accepts an optional field filter inline
    pub async fn collect_with_fields(self, fields: Option<&[String]>) -> Result<Vec<Document>> {
        let builder = if let Some(f) = fields {
            Self { search_fields: Some(f.to_vec()), ..self }
        } else {
            self
        };
        builder.collect().await
    }

    pub async fn collect(self) -> Result<Vec<Document>> {
        let query = self.query.to_lowercase();
        let mut results = Vec::new();

        if let Some(index) = self.db.primary_indices.get(&self.collection) {
            for entry in index.iter() {
                if let Some(data) = self.db.get(entry.key())? {
                    if let Ok(doc) = serde_json::from_slice::<Document>(&data) {
                        let matches = if query.is_empty() {
                            true
                        } else {
                            let fields_to_check = self.search_fields.as_deref();
                            doc.data.iter().any(|(k, v)| {
                                if let Some(ref allowed) = fields_to_check {
                                    if !allowed.contains(k) {
                                        return false;
                                    }
                                }
                                if let crate::types::Value::String(s) = v {
                                    s.to_lowercase().contains(&query)
                                } else {
                                    false
                                }
                            })
                        };
                        if matches {
                            results.push(doc);
                            if let Some(l) = self.limit {
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
}


fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(v1), Some(v2)) => v1.partial_cmp(v2).unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// Supported filter operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    Eq(String, Value),
    Ne(String, Value),
    Gt(String, Value),
    Gte(String, Value),
    Lt(String, Value),
    Lte(String, Value),
    In(String, Vec<Value>),
    Contains(String, String),
    StartsWith(String, String),
    IsNull(String),
    IsNotNull(String),
    Not(Box<Filter>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

/// Traverse a dotted field path (e.g. `"meta.author"`) through a Document's data map.
/// Returns `None` if any segment is missing or the intermediate value is not an Object.
fn get_nested<'a>(doc: &'a Document, field: &str) -> Option<&'a Value> {
    let mut parts = field.splitn(2, '.');
    let first = parts.next()?;
    let rest = parts.next();
    let val = doc.data.get(first)?;
    match rest {
        None => Some(val),
        Some(remaining) => get_nested_value(val, remaining),
    }
}

/// Like `get_nested` but also handles the virtual `"id"` field which lives in `doc.id`.
/// Returns an owned `Value` to avoid lifetime issues with the temporary id string.
fn get_field_owned(doc: &Document, field: &str) -> Option<Value> {
    if field == "id" && !doc.data.contains_key("id") {
        Some(Value::String(doc.id.clone()))
    } else {
        get_nested(doc, field).cloned()
    }
}

fn get_nested_value<'a>(val: &'a Value, path: &str) -> Option<&'a Value> {
    let mut parts = path.splitn(2, '.');
    let first = parts.next()?;
    let rest = parts.next();
    if let Value::Object(map) = val {
        let child = map.get(first)?;
        match rest {
            None => Some(child),
            Some(remaining) => get_nested_value(child, remaining),
        }
    } else {
        None
    }
}

impl std::ops::Not for Filter {
    type Output = Self;
    fn not(self) -> Self::Output {
        Filter::Not(Box::new(self))
    }
}

impl Filter {
    pub fn matches(&self, doc: &Document) -> bool {
        match self {
            Filter::Eq(f, v) => get_field_owned(doc, f).as_ref() == Some(v),
            Filter::Ne(f, v) => get_field_owned(doc, f).as_ref() != Some(v),
            Filter::Gt(f, v) => get_field_owned(doc, f).map_or(false, |dv| dv > *v),
            Filter::Gte(f, v) => get_field_owned(doc, f).map_or(false, |dv| dv >= *v),
            Filter::Lt(f, v) => get_field_owned(doc, f).map_or(false, |dv| dv < *v),
            Filter::Lte(f, v) => get_field_owned(doc, f).map_or(false, |dv| dv <= *v),
            Filter::In(f, v) => get_field_owned(doc, f).map_or(false, |dv| v.contains(&dv)),
            Filter::Contains(f, v) => get_field_owned(doc, f).map_or(false, |dv| {
                if let Value::String(s) = dv { s.contains(v.as_str()) } else { false }
            }),
            Filter::StartsWith(f, v) => get_field_owned(doc, f).map_or(false, |dv| {
                if let Value::String(s) = dv { s.starts_with(v.as_str()) } else { false }
            }),
            Filter::IsNull(f) => get_field_owned(doc, f).map_or(true, |v| matches!(v, Value::Null)),
            Filter::IsNotNull(f) => get_field_owned(doc, f).map_or(false, |v| !matches!(v, Value::Null)),
            Filter::Not(f) => !f.matches(doc),
            Filter::And(fs) => fs.iter().all(|f| f.matches(doc)),
            Filter::Or(fs) => fs.iter().any(|f| f.matches(doc)),
        }
    }
}

impl std::ops::BitAnd for Filter {
    type Output = Filter;
    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Filter::And(mut a), Filter::And(mut b)) => { a.append(&mut b); Filter::And(a) }
            (Filter::And(mut a), b) => { a.push(b); Filter::And(a) }
            (a, Filter::And(mut b)) => { b.insert(0, a); Filter::And(b) }
            (a, b) => Filter::And(vec![a, b]),
        }
    }
}

impl std::ops::BitOr for Filter {
    type Output = Filter;
    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Filter::Or(mut a), Filter::Or(mut b)) => { a.append(&mut b); Filter::Or(a) }
            (Filter::Or(mut a), b) => { a.push(b); Filter::Or(a) }
            (a, Filter::Or(mut b)) => { b.insert(0, a); Filter::Or(b) }
            (a, b) => Filter::Or(vec![a, b]),
        }
    }
}
