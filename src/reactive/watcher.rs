use super::{QueryUpdate, ReactiveQueryState};
use crate::Aurora;
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
    /// Reference to the database for resyncing
    #[allow(dead_code)]
    db: Arc<Aurora>,
}

impl QueryWatcher {
    /// Create a new query watcher
    pub fn new(
        db: Arc<Aurora>,
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
        let db_clone = Arc::clone(&db);
        let coll_clone = collection.clone();
        let state_clone = Arc::clone(&state);
        let sender_clone = sender.clone();

        tokio::spawn(async move {
            let mut backoff_ms = 100; // Initial backoff

            loop {
                match listener.recv().await {
                    Ok(event) => {
                        // Successful receive, reset backoff gradually
                        if backoff_ms > 100 { backoff_ms -= 50; }

                        let update = match event.change_type {
                            crate::pubsub::ChangeType::Insert => {
                                if let Some(doc) = event.document {
                                    state_clone.add_if_matches(doc).await
                                } else {
                                    None
                                }
                            }
                            crate::pubsub::ChangeType::Update => {
                                if let Some(new_doc) = event.document {
                                    state_clone.update(&event.id, new_doc).await
                                } else {
                                    None
                                }
                            }
                            crate::pubsub::ChangeType::Delete => state_clone.remove(&event.id).await,
                        };

                        if let Some(u) = update && sender_clone.send(u).is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!(
                            "WARNING: Watcher for '{}' lagged by {} events. Applying {}ms backoff...",
                            coll_clone, skipped, backoff_ms
                        );

                        // 1. DRAIN: Empty everything currently in the channel (Ok and Lagged)
                        loop {
                            match listener.try_recv() {
                                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                                _ => break,
                            }
                        }

                        // 2. BACKOFF: Wait for the event storm to subside
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        
                        // Increase backoff for next time (max 2 seconds)
                        backoff_ms = (backoff_ms * 2).min(2000);

                        // 3. SNAPSHOT: Collect documents into a Vec inside the block to keep iterator usage local (not across await)
                        let docs_snapshot: Vec<Document> = if let Ok(iter) = db_clone.stream_collection(&coll_clone) {
                            iter.collect()
                        } else {
                            Vec::new()
                        };

                        // 4. SYNC: Synchronize ReactiveQueryState and emit deltas
                        let updates = state_clone.sync_state(docs_snapshot).await;
                        for u in updates {
                            if sender_clone.send(u).is_err() {
                                return;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // Throttling logic (unchanged)
        let final_receiver = if let Some(duration) = debounce_duration {
            let (tx_throttled, rx_throttled) = mpsc::unbounded_channel();
            let mut raw_rx = receiver;
            tokio::spawn(async move {
                use std::collections::HashMap;
                let mut tick = tokio::time::interval(duration);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut pending: HashMap<String, QueryUpdate> = HashMap::new();
                loop {
                    tokio::select! {
                        biased;
                        maybe_update = raw_rx.recv() => {
                            match maybe_update {
                                Some(update) => { pending.insert(update.id().to_string(), update); }
                                None => break,
                            }
                        }
                        _ = tick.tick() => {
                            if !pending.is_empty() {
                                for (_, update) in pending.drain() {
                                    if tx_throttled.send(update).is_err() { return; }
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
            db,
        }
    }

    pub async fn next(&mut self) -> Option<QueryUpdate> { self.receiver.recv().await }
    pub fn collection(&self) -> &str { &self.collection }
    pub fn try_next(&mut self) -> Option<QueryUpdate> { self.receiver.try_recv().ok() }
    pub fn throttled(self, interval: std::time::Duration) -> ThrottledQueryWatcher {
        ThrottledQueryWatcher::new(self.receiver, self.collection, interval)
    }
}

pub struct ThrottledQueryWatcher {
    receiver: mpsc::UnboundedReceiver<QueryUpdate>,
    collection: String,
}

impl ThrottledQueryWatcher {
    pub fn new(
        mut raw_receiver: mpsc::UnboundedReceiver<QueryUpdate>,
        collection: impl Into<String>,
        interval: std::time::Duration,
    ) -> Self {
        let collection = collection.into();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            use std::collections::HashMap;
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut pending: HashMap<String, QueryUpdate> = HashMap::new();
            loop {
                tokio::select! {
                    biased;
                    maybe_update = raw_receiver.recv() => {
                        match maybe_update {
                            Some(update) => { pending.insert(update.id().to_string(), update); }
                            None => break,
                        }
                    }
                    _ = tick.tick() => {
                        if !pending.is_empty() {
                            for (_, update) in pending.drain() {
                                if tx.send(update).is_err() { return; }
                            }
                        }
                    }
                }
            }
        });
        Self { receiver: rx, collection }
    }
    pub async fn next(&mut self) -> Option<QueryUpdate> { self.receiver.recv().await }
    pub fn collection(&self) -> &str { &self.collection }
    pub fn try_next(&mut self) -> Option<QueryUpdate> { self.receiver.try_recv().ok() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::ChangeEvent;
    use crate::types::Value;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_query_watcher_insert() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Aurora::open(temp_dir.path().join("test.db")).await.unwrap());
        let listener = db.pubsub.listen("users");
        let state = Arc::new(ReactiveQueryState::new(vec![
            crate::query::Filter::Eq("active".to_string(), Value::Bool(true))
        ]));
        let mut watcher = QueryWatcher::new(db.clone(), "users", listener, state, vec![], None);
        let mut data = HashMap::new();
        data.insert("active".to_string(), Value::Bool(true));
        data.insert("name".to_string(), Value::String("Alice".into()));
        let doc = Document { id: "1".to_string(), data };
        db.pubsub.publish(ChangeEvent::insert("users", "1", doc)).unwrap();
        let update = watcher.next().await.unwrap();
        assert!(matches!(update, QueryUpdate::Added(_)));
        assert_eq!(update.id(), "1");
    }
}
