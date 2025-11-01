pub mod events;

pub mod channel;
pub mod listener;

pub use channel::ChangeChannel;
pub use events::{ChangeEvent, ChangeType, EventFilter};
pub use listener::ChangeListener;

use crate::error::Result;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct PubSubSystem {
    channels: Arc<DashMap<String, broadcast::Sender<ChangeEvent>>>,
    global_channel: broadcast::Sender<ChangeEvent>,
    buffer_size: usize,
}

impl PubSubSystem {
    pub fn new(buffer_size: usize) -> Self {
        let (global_tx, _) = broadcast::channel(buffer_size);

        Self {
            channels: Arc::new(DashMap::new()),
            global_channel: global_tx,
            buffer_size,
        }
    }

    pub fn publish(&self, event: ChangeEvent) -> Result<()> {
        if let Some(tx) = self.channels.get(&event.collection) {
            let _ = tx.send(event.clone());
        }

        let _ = self.global_channel.send(event);

        Ok(())
    }

    pub fn listen(&self, collection: impl Into<String>) -> ChangeListener {
        let collection = collection.into();

        if !self.channels.contains_key(&collection) {
            self.channels.retain(|_, sender| sender.receiver_count() > 0);
        }

        let tx = self
            .channels
            .entry(collection.clone())
            .or_insert_with(|| broadcast::channel(self.buffer_size).0)
            .clone();

        ChangeListener::new(collection, tx.subscribe())
    }

    pub fn listen_all(&self) -> ChangeListener {
        ChangeListener::new("*".to_string(), self.global_channel.subscribe())
    }

    pub fn listener_count(&self, collection: &str) -> usize {
        self.channels
            .get(collection)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }

    pub fn total_listeners(&self) -> usize {
        self.channels
            .iter()
            .map(|entry| entry.value().receiver_count())
            .sum::<usize>()
            + self.global_channel.receiver_count()
    }
}

impl Clone for PubSubSystem {
    fn clone(&self) -> Self {
        Self {
            channels: Arc::clone(&self.channels),
            global_channel: self.global_channel.clone(),
            buffer_size: self.buffer_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Document, Value};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_pubsub_basic() {
        let pubsub = PubSubSystem::new(100);

        let mut listener = pubsub.listen("users");

        let mut data = HashMap::new();
        data.insert("id".to_string(), Value::String("123".into()));
        data.insert("name".to_string(), Value::String("Alice".into()));

        let event = ChangeEvent {
            collection: "users".to_string(),
            change_type: ChangeType::Insert,
            id: "123".to_string(),
            document: Some(Document {
                id: "123".to_string(),
                data,
            }),
            old_document: None,
        };

        pubsub.publish(event.clone()).unwrap();

        let received = listener.recv().await.unwrap();
        assert_eq!(received.collection, "users");
        assert_eq!(received.id, "123");
        assert!(matches!(received.change_type, ChangeType::Insert));
    }

    #[tokio::test]
    async fn test_pubsub_multiple_listeners() {
        let pubsub = PubSubSystem::new(100);

        let mut listener1 = pubsub.listen("users");
        let mut listener2 = pubsub.listen("users");

        assert_eq!(pubsub.listener_count("users"), 2);

        let event = ChangeEvent {
            collection: "users".to_string(),
            change_type: ChangeType::Insert,
            id: "123".to_string(),
            document: None,
            old_document: None,
        };

        pubsub.publish(event).unwrap();

        assert!(listener1.recv().await.is_ok());
        assert!(listener2.recv().await.is_ok());
    }

    #[tokio::test]
    async fn test_pubsub_global_listener() {
        let pubsub = PubSubSystem::new(100);

        let mut global_listener = pubsub.listen_all();

        pubsub
            .publish(ChangeEvent {
                collection: "users".to_string(),
                change_type: ChangeType::Insert,
                id: "1".to_string(),
                document: None,
                old_document: None,
            })
            .unwrap();

        pubsub
            .publish(ChangeEvent {
                collection: "posts".to_string(),
                change_type: ChangeType::Insert,
                id: "2".to_string(),
                document: None,
                old_document: None,
            })
            .unwrap();

        let event1 = global_listener.recv().await.unwrap();
        let event2 = global_listener.recv().await.unwrap();

        assert_eq!(event1.collection, "users");
        assert_eq!(event2.collection, "posts");
    }
}
