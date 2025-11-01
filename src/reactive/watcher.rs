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

                if let Some(u) = update {
                    if sender.send(u).is_err() {
                        // Receiver dropped, stop watching
                        break;
                    }
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
}
