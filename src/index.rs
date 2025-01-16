use crate::error::Result;
use crate::types::{Document, Value};
use crate::search::FullTextIndex;
use dashmap::DashMap;
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
    data: Arc<DashMap<Value, Vec<String>>>, // Value -> Document IDs
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
            data: Arc::new(DashMap::new()),
            full_text,
        }
    }

    pub fn insert(&self, doc: &Document) -> Result<()> {
        let key = self.extract_key(doc)?;
        let doc_id = doc.id.clone();

        match self.data.get_mut(&key) {
            Some(mut ids) => {
                if self.definition.unique && !ids.is_empty() {
                    return Err(crate::error::AuroraError::InvalidOperation(
                        "Unique constraint violation".into(),
                    ));
                }
                ids.push(doc_id.clone());
            }
            None => {
                self.data.insert(key, vec![doc_id.clone()]);
            }
        }

        if let Some(ft_index) = &self.full_text {
            ft_index.index_document(doc)?;
        }

        Ok(())
    }

    pub fn search(&self, value: &Value) -> Option<Vec<String>> {
        self.data.get(value).map(|ids| ids.clone())
    }

    pub fn search_text(&self, query: &str) -> Option<Vec<(String, f32)>> {
        self.full_text.as_ref().map(|ft| ft.search(query))
    }

    fn extract_key(&self, doc: &Document) -> Result<Value> {
        if self.definition.fields.len() == 1 {
            Ok(doc.data
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
} 