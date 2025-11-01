// Reactive Queries - Live query results that auto-update
//
// This module provides reactive queries that automatically update
// when underlying data changes, similar to Firebase's real-time queries.

pub mod updates;
pub mod watcher;

pub use updates::{QueryUpdate, UpdateType};
pub use watcher::QueryWatcher;

use crate::types::Document;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Tracks the current state of a reactive query
pub struct ReactiveQueryState {
    /// Current results (keyed by document ID)
    results: Arc<RwLock<HashMap<String, Document>>>,
    /// Filter function to check if a document matches
    filter: Arc<dyn Fn(&Document) -> bool + Send + Sync>,
}

impl ReactiveQueryState {
    pub fn new<F>(filter: F) -> Self
    where
        F: Fn(&Document) -> bool + Send + Sync + 'static,
    {
        Self {
            results: Arc::new(RwLock::new(HashMap::new())),
            filter: Arc::new(filter),
        }
    }

    /// Check if a document matches the query filter
    pub fn matches(&self, doc: &Document) -> bool {
        (self.filter)(doc)
    }

    /// Add a document to results if it matches
    pub async fn add_if_matches(&self, doc: Document) -> Option<QueryUpdate> {
        if self.matches(&doc) {
            let mut results = self.results.write().await;
            let id = doc.id.clone();

            if results.contains_key(&id) {
                // Document already in results, this is a modification
                let old = results.insert(id, doc.clone());
                Some(QueryUpdate::Modified {
                    old: old.unwrap(),
                    new: doc,
                })
            } else {
                // New document added to results
                results.insert(id, doc.clone());
                Some(QueryUpdate::Added(doc))
            }
        } else {
            None
        }
    }

    /// Remove a document from results
    pub async fn remove(&self, id: &str) -> Option<QueryUpdate> {
        let mut results = self.results.write().await;
        results.remove(id).map(QueryUpdate::Removed)
    }

    /// Update a document, checking if it should be added/removed/modified
    pub async fn update(&self, id: &str, new_doc: Document) -> Option<QueryUpdate> {
        let should_be_in_results = self.matches(&new_doc);
        let mut results = self.results.write().await;
        let was_in_results = results.contains_key(id);

        match (was_in_results, should_be_in_results) {
            (true, true) => {
                let old = results.insert(id.to_string(), new_doc.clone());
                Some(QueryUpdate::Modified {
                    old: old.unwrap(),
                    new: new_doc,
                })
            }
            (true, false) => {
                results.remove(id).map(QueryUpdate::Removed)
            }
            (false, true) => {
                results.insert(id.to_string(), new_doc.clone());
                Some(QueryUpdate::Added(new_doc))
            }
            (false, false) => {
                None
            }
        }
    }

    /// Get current results as a Vec
    pub async fn get_results(&self) -> Vec<Document> {
        self.results.read().await.values().cloned().collect()
    }

    /// Get count of current results
    pub async fn count(&self) -> usize {
        self.results.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[tokio::test]
    async fn test_reactive_state_add_if_matches() {
        let state = ReactiveQueryState::new(|doc: &Document| {
            doc.data.get("active") == Some(&Value::Bool(true))
        });

        let mut data = HashMap::new();
        data.insert("active".to_string(), Value::Bool(true));
        let doc = Document {
            id: "1".to_string(),
            data,
        };

        let update = state.add_if_matches(doc.clone()).await;
        assert!(matches!(update, Some(QueryUpdate::Added(_))));
        assert_eq!(state.count().await, 1);
    }

    #[tokio::test]
    async fn test_reactive_state_filter() {
        let state = ReactiveQueryState::new(|doc: &Document| {
            doc.data.get("active") == Some(&Value::Bool(true))
        });

        let mut active_data = HashMap::new();
        active_data.insert("active".to_string(), Value::Bool(true));
        let active_doc = Document {
            id: "1".to_string(),
            data: active_data,
        };

        let mut inactive_data = HashMap::new();
        inactive_data.insert("active".to_string(), Value::Bool(false));
        let inactive_doc = Document {
            id: "2".to_string(),
            data: inactive_data,
        };

        // Active doc should be added
        assert!(state.add_if_matches(active_doc).await.is_some());
        assert_eq!(state.count().await, 1);

        // Inactive doc should NOT be added
        assert!(state.add_if_matches(inactive_doc).await.is_none());
        assert_eq!(state.count().await, 1);
    }

    #[tokio::test]
    async fn test_reactive_state_update_transitions() {
        let state = ReactiveQueryState::new(|doc: &Document| {
            doc.data.get("active") == Some(&Value::Bool(true))
        });

        // Add initial active document
        let mut data = HashMap::new();
        data.insert("active".to_string(), Value::Bool(true));
        let doc = Document {
            id: "1".to_string(),
            data,
        };
        state.add_if_matches(doc).await;

        // Update to inactive (should be removed)
        let mut inactive_data = HashMap::new();
        inactive_data.insert("active".to_string(), Value::Bool(false));
        let inactive_doc = Document {
            id: "1".to_string(),
            data: inactive_data,
        };

        let update = state.update("1", inactive_doc).await;
        assert!(matches!(update, Some(QueryUpdate::Removed(_))));
        assert_eq!(state.count().await, 0);
    }
}
