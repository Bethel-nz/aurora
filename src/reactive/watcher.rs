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

        Self {
            receiver,
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

        let mut watcher = QueryWatcher::new("users", listener, state, vec![]);

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

        let mut watcher = QueryWatcher::new("users", listener, state, vec![]);

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
