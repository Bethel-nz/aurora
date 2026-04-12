use crate::error::Result;
use crate::search::FullTextIndex;
use crate::types::{Document, Value};
use crossbeam_skiplist::{SkipMap, SkipSet};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    FullText,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub collection: String,
    pub fields: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
}

pub struct Index {
    definition: IndexDefinition,
    // Using SkipMap for lock-free concurrent sorted storage
    // Value -> Set of Document IDs (Sorted) to avoid O(N) cloning on insert
    data: Arc<SkipMap<Value, Arc<SkipSet<String>>>>,
    full_text: Option<Arc<FullTextIndex>>,
}

impl Index {
    pub fn new(definition: IndexDefinition) -> Self {
        let full_text = if matches!(definition.index_type, IndexType::FullText) {
            Some(Arc::new(FullTextIndex::new(
                &definition.collection,
                &definition.fields[0],
            )))
        } else {
            None
        };

        Self {
            definition,
            data: Arc::new(SkipMap::new()),
            full_text,
        }
    }

    pub fn insert(&self, doc: &Document) -> Result<()> {
        let key = self.extract_key(doc)?;
        let doc_id = doc._sid.clone();

        // Get or create the SkipSet for this key atomically, then insert the doc ID.
        // For unique fields we check *after* inserting: if the set now contains more
        // than one ID another writer slipped in concurrently, so we roll back and
        // return an error. This avoids the TOCTOU race of a pre-insert read.
        let id_set = self
            .data
            .get_or_insert(key.clone(), Arc::new(SkipSet::new()))
            .value()
            .clone();

        id_set.insert(doc_id.clone());

        if self.definition.unique && id_set.len() > 1 {
            id_set.remove(&doc_id);
            return Err(crate::error::AqlError::invalid_operation(
                "Unique constraint violation".to_string(),
            ));
        }

        if let Some(ft_index) = &self.full_text {
            ft_index.index_document(doc)?;
        }

        Ok(())
    }

    pub fn search(&self, value: &Value) -> Option<Vec<String>> {
        self.data
            .get(value)
            .map(|e| e.value().iter().map(|v| v.to_string()).collect())
    }

    pub fn remove(&self, doc: &Document) -> Result<()> {
        let key = self.extract_key(doc)?;
        if let Some(entry) = self.data.get(&key) {
            entry.value().remove(&doc._sid);
            // Prune the entry when the set becomes empty to avoid memory growth
            if entry.value().is_empty() {
                self.data.remove(&key);
            }
        }
        Ok(())
    }

    /// Return all IDs in the index, sorted by key value (Lock-free iteration)
    pub fn iter_ids(&self) -> Vec<String> {
        self.data
            .iter()
            .flat_map(|e| {
                let ids: Vec<String> = e.value().iter().map(|v| v.to_string()).collect();
                ids
            })
            .collect()
    }

    pub fn search_text(&self, query: &str) -> Option<Vec<(String, f32)>> {
        self.full_text.as_ref().map(|ft| ft.search(query))
    }

    fn extract_key(&self, doc: &Document) -> Result<Value> {
        if self.definition.fields.len() == 1 {
            Ok(doc
                .data
                .get(&self.definition.fields[0])
                .cloned()
                .unwrap_or(Value::Null))
        } else {
            let values: Vec<Value> = self
                .definition
                .fields
                .iter()
                .map(|f| doc.data.get(f).cloned().unwrap_or(Value::Null))
                .collect();
            Ok(Value::Array(values))
        }
    }

    #[allow(dead_code)]
    pub fn full_text(&self) -> Option<Arc<FullTextIndex>> {
        self.full_text.clone()
    }
}
