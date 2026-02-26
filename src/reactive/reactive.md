// Reactive Queries - Live query results that auto-update
//
// This module provides reactive queries that automatically update
// when underlying data changes, similar to Firebase's real-time queries.

pub mod updates;
pub mod watcher;

pub use updates::{QueryUpdate, UpdateType};
pub use watcher::{QueryWatcher, ThrottledQueryWatcher};

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

            match results.entry(id) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    // Document already in results, this is a modification
                    let old = e.insert(doc.clone());
                    Some(QueryUpdate::Modified { old, new: doc })
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    // New document added to results
                    e.insert(doc.clone());
                    Some(QueryUpdate::Added(doc))
                }
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
            (true, false) => results.remove(id).map(QueryUpdate::Removed),
            (false, true) => {
                results.insert(id.to_string(), new_doc.clone());
                Some(QueryUpdate::Added(new_doc))
            }
            (false, false) => None,
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
use crate::types::Document;
use serde::{Deserialize, Serialize};

/// Type of update that occurred in a reactive query
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateType {
    /// Document added to query results
    Added,
    /// Document removed from query results
    Removed,
    /// Document modified while still matching query
    Modified,
}

/// Update notification for a reactive query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryUpdate {
    /// A new document was added to the query results
    Added(Document),

    /// A document was removed from the query results
    Removed(Document),

    /// A document in the results was modified
    Modified { old: Document, new: Document },
}

impl QueryUpdate {
    /// Get the update type
    pub fn update_type(&self) -> UpdateType {
        match self {
            QueryUpdate::Added(_) => UpdateType::Added,
            QueryUpdate::Removed(_) => UpdateType::Removed,
            QueryUpdate::Modified { .. } => UpdateType::Modified,
        }
    }

    /// Get the document (current state for Added/Modified, old state for Removed)
    pub fn document(&self) -> &Document {
        match self {
            QueryUpdate::Added(doc) => doc,
            QueryUpdate::Removed(doc) => doc,
            QueryUpdate::Modified { new, .. } => new,
        }
    }

    /// Get the document ID
    pub fn id(&self) -> &str {
        &self.document().id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use std::collections::HashMap;

    #[test]
    fn test_query_update_types() {
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Test".into()));

        let doc = Document {
            id: "1".to_string(),
            data: data.clone(),
        };

        let added = QueryUpdate::Added(doc.clone());
        assert!(matches!(added.update_type(), UpdateType::Added));
        assert_eq!(added.id(), "1");

        let removed = QueryUpdate::Removed(doc.clone());
        assert!(matches!(removed.update_type(), UpdateType::Removed));

        let modified = QueryUpdate::Modified {
            old: doc.clone(),
            new: doc.clone(),
        };
        assert!(matches!(modified.update_type(), UpdateType::Modified));
    }
}
use super::{QueryUpdate, ReactiveQueryState};
use crate::pubsub::ChangeListener;
use crate::types::Document;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Watches a query and emits updates when results change
pub struct QueryWatcher {
    /// Receiver for query updates
    receiver: mpsc::UnboundedReceiver<QueryUpdate>,
    /// Collection being watched
    collection: String,
}

impl QueryWatcher {
    /// Create a new query watcher
    ///
    /// # Arguments
    /// * `collection` - Collection to watch
    /// * `listener` - Change listener for the collection
    /// * `state` - Reactive query state
    /// * `initial_results` - Initial query results to populate the state
    pub fn new(
        collection: impl Into<String>,
        mut listener: ChangeListener,
        state: Arc<ReactiveQueryState>,
        initial_results: Vec<Document>,
        debounce_duration: Option<std::time::Duration>,
    ) -> Self {
        let collection = collection.into();
        let (sender, receiver) = mpsc::unbounded_channel();

        // Populate initial results
        let init_state = Arc::clone(&state);
        let init_sender = sender.clone();
        tokio::spawn(async move {
            for doc in initial_results {
                if let Some(update) = init_state.add_if_matches(doc).await {
                    let _ = init_sender.send(update);
                }
            }
        });

        // Spawn background task to listen for changes
        tokio::spawn(async move {
            while let Ok(event) = listener.recv().await {
                let update = match event.change_type {
                    crate::pubsub::ChangeType::Insert => {
                        if let Some(doc) = event.document {
                            state.add_if_matches(doc).await
                        } else {
                            None
                        }
                    }
                    crate::pubsub::ChangeType::Update => {
                        if let Some(new_doc) = event.document {
                            state.update(&event.id, new_doc).await
                        } else {
                            None
                        }
                    }
                    crate::pubsub::ChangeType::Delete => state.remove(&event.id).await,
                };

                if let Some(u) = update
                    && sender.send(u).is_err()
                {
                    // Receiver dropped, stop watching
                    break;
                }
            }
        });

        // If debounce is requested, wrap the receiver in a throttling task
        let final_receiver = if let Some(duration) = debounce_duration {
            let (tx_throttled, rx_throttled) = mpsc::unbounded_channel();
            let mut raw_rx = receiver;

            tokio::spawn(async move {
                use std::collections::HashMap;
                use tokio::time::interval as tokio_interval;

                let mut tick = tokio_interval(duration);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                let mut pending: HashMap<String, QueryUpdate> = HashMap::new();

                loop {
                    tokio::select! {
                        biased;
                        maybe_update = raw_rx.recv() => {
                            match maybe_update {
                                Some(update) => {
                                    pending.insert(update.id().to_string(), update);
                                }
                                None => break,
                            }
                        }
                        _ = tick.tick() => {
                            if !pending.is_empty() {
                                for (_, update) in pending.drain() {
                                    if tx_throttled.send(update).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            });
            rx_throttled
        } else {
            receiver
        };

        Self {
            receiver: final_receiver,
            collection,
        }
    }

    /// Get the next query update
    /// Returns None when the watcher is closed
    pub async fn next(&mut self) -> Option<QueryUpdate> {
        self.receiver.recv().await
    }

    /// Get the collection name being watched
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Try to receive an update without blocking
    pub fn try_next(&mut self) -> Option<QueryUpdate> {
        self.receiver.try_recv().ok()
    }

    /// Convert to a throttled watcher for rate-limiting updates
    ///
    /// Events are buffered and emitted at most once per interval.
    /// Deduplicates by document ID, keeping only the latest state.
    pub fn throttled(self, interval: std::time::Duration) -> ThrottledQueryWatcher {
        ThrottledQueryWatcher::new(self.receiver, self.collection, interval)
    }
}

/// A throttled/debounced query watcher for rate-limiting reactive updates
///
/// Buffers incoming events and emits them at a fixed interval.
/// Deduplicates by document ID, keeping only the latest state per ID.
/// This prevents overwhelming the UI with high-frequency updates.
pub struct ThrottledQueryWatcher {
    receiver: mpsc::UnboundedReceiver<QueryUpdate>,
    collection: String,
}

impl ThrottledQueryWatcher {
    /// Create a new throttled watcher
    pub fn new(
        mut raw_receiver: mpsc::UnboundedReceiver<QueryUpdate>,
        collection: impl Into<String>,
        interval: std::time::Duration,
    ) -> Self {
        let collection = collection.into();
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            use std::collections::HashMap;
            use tokio::time::interval as tokio_interval;

            let mut tick = tokio_interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Buffer: doc_id -> latest update for that doc
            let mut pending: HashMap<String, QueryUpdate> = HashMap::new();

            loop {
                tokio::select! {
                    biased;

                    // Collect events as fast as they come
                    maybe_update = raw_receiver.recv() => {
                        match maybe_update {
                            Some(update) => {
                                // Dedupe by doc ID - keep latest state
                                pending.insert(update.id().to_string(), update);
                            }
                            None => break, // Raw receiver closed
                        }
                    }

                    // Every tick, flush the buffer
                    _ = tick.tick() => {
                        if !pending.is_empty() {
                            for (_, update) in pending.drain() {
                                if tx.send(update).is_err() {
                                    return; // Receiver dropped
                                }
                            }
                        }
                    }
                }
            }
        });

        Self {
            receiver: rx,
            collection,
        }
    }

    /// Get the next throttled update
    pub async fn next(&mut self) -> Option<QueryUpdate> {
        self.receiver.recv().await
    }

    /// Get the collection name
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Try to receive without blocking
    pub fn try_next(&mut self) -> Option<QueryUpdate> {
        self.receiver.try_recv().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::{ChangeEvent, PubSubSystem};
    use crate::types::Value;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_query_watcher_insert() {
        let pubsub = PubSubSystem::new(100);
        let listener = pubsub.listen("users");

        let state = Arc::new(ReactiveQueryState::new(|doc: &Document| {
            doc.data.get("active") == Some(&Value::Bool(true))
        }));

        let mut watcher = QueryWatcher::new("users", listener, state, vec![], None);

        // Publish an insert event for an active user
        let mut data = HashMap::new();
        data.insert("active".to_string(), Value::Bool(true));
        data.insert("name".to_string(), Value::String("Alice".into()));

        let doc = Document {
            id: "1".to_string(),
            data,
        };

        pubsub
            .publish(ChangeEvent::insert("users", "1", doc))
            .unwrap();

        // Should receive an Added update
        let update = watcher.next().await.unwrap();
        assert!(matches!(update, QueryUpdate::Added(_)));
        assert_eq!(update.id(), "1");
    }

    #[tokio::test]
    async fn test_query_watcher_filter() {
        let pubsub = PubSubSystem::new(100);
        let listener = pubsub.listen("users");

        let state = Arc::new(ReactiveQueryState::new(|doc: &Document| {
            doc.data.get("active") == Some(&Value::Bool(true))
        }));

        let mut watcher = QueryWatcher::new("users", listener, state, vec![], None);

        // Publish an inactive user (should be filtered)
        let mut inactive_data = HashMap::new();
        inactive_data.insert("active".to_string(), Value::Bool(false));

        pubsub
            .publish(ChangeEvent::insert(
                "users",
                "1",
                Document {
                    id: "1".to_string(),
                    data: inactive_data,
                },
            ))
            .unwrap();

        // Publish an active user (should pass filter)
        let mut active_data = HashMap::new();
        active_data.insert("active".to_string(), Value::Bool(true));

        pubsub
            .publish(ChangeEvent::insert(
                "users",
                "2",
                Document {
                    id: "2".to_string(),
                    data: active_data,
                },
            ))
            .unwrap();

        // Should only receive the active user
        let update = watcher.next().await.unwrap();
        assert_eq!(update.id(), "2");
    }

    #[tokio::test]
    async fn test_debounced_watcher() {
        use std::time::Duration;
        use tokio::sync::mpsc;

        // Create a channel that simulates raw query updates
        let (tx, rx) = mpsc::unbounded_channel();

        // Create throttled watcher with 100ms interval
        let mut throttled = ThrottledQueryWatcher::new(rx, "test", Duration::from_millis(100));

        // Send multiple updates for the same document rapidly
        let mut data1 = HashMap::new();
        data1.insert("value".to_string(), Value::Int(1));
        tx.send(QueryUpdate::Added(Document {
            id: "doc1".to_string(),
            data: data1,
        }))
        .unwrap();

        let mut data2 = HashMap::new();
        data2.insert("value".to_string(), Value::Int(2));
        tx.send(QueryUpdate::Modified {
            old: Document {
                id: "doc1".to_string(),
                data: HashMap::new(),
            },
            new: Document {
                id: "doc1".to_string(),
                data: data2,
            },
        })
        .unwrap();

        let mut data3 = HashMap::new();
        data3.insert("value".to_string(), Value::Int(3));
        tx.send(QueryUpdate::Modified {
            old: Document {
                id: "doc1".to_string(),
                data: HashMap::new(),
            },
            new: Document {
                id: "doc1".to_string(),
                data: data3.clone(),
            },
        })
        .unwrap();

        // Wait for throttle interval to pass
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should receive only the latest update (deduped by doc ID)
        let update = throttled.try_next();
        assert!(update.is_some());
        // The last one wins due to deduplication
        assert_eq!(update.unwrap().id(), "doc1");
    }

    #[tokio::test]
    async fn test_throttled_watcher_multiple_docs() {
        use std::time::Duration;
        use tokio::sync::mpsc;

        let (tx, rx) = mpsc::unbounded_channel();
        let mut throttled = ThrottledQueryWatcher::new(rx, "test", Duration::from_millis(100));

        // Send updates for different documents
        for i in 1..=3 {
            let mut data = HashMap::new();
            data.insert("value".to_string(), Value::Int(i));
            tx.send(QueryUpdate::Added(Document {
                id: format!("doc{}", i),
                data,
            }))
            .unwrap();
        }

        // Wait for throttle
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should receive all 3 (different IDs, no deduplication)
        let mut received = Vec::new();
        while let Some(update) = throttled.try_next() {
            received.push(update.id().to_string());
        }

        assert_eq!(received.len(), 3);
        assert!(received.contains(&"doc1".to_string()));
        assert!(received.contains(&"doc2".to_string()));
        assert!(received.contains(&"doc3".to_string()));
    }
}
