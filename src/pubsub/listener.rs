use super::events::{ChangeEvent, EventFilter};
use tokio::sync::broadcast;

/// Listener for change events on a collection
pub struct ChangeListener {
    collection: String,
    receiver: broadcast::Receiver<ChangeEvent>,
    filter: Option<EventFilter>,
}

impl ChangeListener {
    pub(crate) fn new(collection: String, receiver: broadcast::Receiver<ChangeEvent>) -> Self {
        Self {
            collection,
            receiver,
            filter: None,
        }
    }

    /// Add a filter to this listener
    /// Only events matching the filter will be received
    pub fn filter(mut self, filter: EventFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Receive the next change event
    /// Blocks until an event is available or the channel is closed
    pub async fn recv(&mut self) -> Result<ChangeEvent, broadcast::error::RecvError> {
        loop {
            let event = self.receiver.recv().await?;

            // If there's a filter, only return matching events
            if let Some(ref filter) = self.filter {
                if filter.matches(&event) {
                    return Ok(event);
                }
                // Non-matching event, continue to next
                continue;
            }

            return Ok(event);
        }
    }

    /// Try to receive an event without blocking
    /// Returns None if no event is available
    pub fn try_recv(&mut self) -> Result<ChangeEvent, broadcast::error::TryRecvError> {
        loop {
            let event = self.receiver.try_recv()?;

            // If there's a filter, only return matching events
            if let Some(ref filter) = self.filter {
                if filter.matches(&event) {
                    return Ok(event);
                }
                // Non-matching event, continue to next
                continue;
            }

            return Ok(event);
        }
    }

    /// Get the collection name this listener is watching
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Convert into a stream (useful for async iteration)
    pub fn into_stream(self) -> ChangeStream {
        ChangeStream { listener: self }
    }
}

/// Stream wrapper for ChangeListener
pub struct ChangeStream {
    listener: ChangeListener,
}

impl ChangeStream {
    /// Get the next event from the stream
    pub async fn next(&mut self) -> Option<ChangeEvent> {
        self.listener.recv().await.ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::PubSubSystem;
    use crate::types::{Document, Value};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_listener_filter() {
        let pubsub = PubSubSystem::new(100);

        // Listen with filter for only inserts
        let mut listener = pubsub.listen("users").filter(EventFilter::ChangeType(
            super::super::events::ChangeType::Insert,
        ));

        // Publish an insert event
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Alice".into()));

        pubsub
            .publish(ChangeEvent::insert(
                "users",
                "1",
                Document {
                    id: "1".to_string(),
                    data: data.clone(),
                },
            ))
            .unwrap();

        // Publish a delete event (should be filtered out)
        pubsub.publish(ChangeEvent::delete("users", "2")).unwrap();

        // Should only receive the insert
        let event = listener.recv().await.unwrap();
        assert!(matches!(
            event.change_type,
            super::super::events::ChangeType::Insert
        ));
        assert_eq!(event.id, "1");
    }

    #[tokio::test]
    async fn test_listener_field_filter() {
        let pubsub = PubSubSystem::new(100);

        // Listen for events where "active" = true
        let mut listener = pubsub.listen("users").filter(EventFilter::FieldEquals(
            "active".to_string(),
            Value::Bool(true),
        ));

        // Publish active user
        let mut active_data = HashMap::new();
        active_data.insert("active".to_string(), Value::Bool(true));

        pubsub
            .publish(ChangeEvent::insert(
                "users",
                "1",
                Document {
                    id: "1".to_string(),
                    data: active_data,
                },
            ))
            .unwrap();

        // Publish inactive user (should be filtered)
        let mut inactive_data = HashMap::new();
        inactive_data.insert("active".to_string(), Value::Bool(false));

        pubsub
            .publish(ChangeEvent::insert(
                "users",
                "2",
                Document {
                    id: "2".to_string(),
                    data: inactive_data,
                },
            ))
            .unwrap();

        // Should only receive active user
        let event = listener.recv().await.unwrap();
        assert_eq!(event.id, "1");
        assert_eq!(event.get_field("active"), Some(&Value::Bool(true)));
    }
}
