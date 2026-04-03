use super::events::{ChangeEvent, ChangeType, EventFilter};
use std::collections::HashSet;
use tokio::sync::broadcast;

/// Listener for change events on a collection
#[derive(Debug)]
pub struct ChangeListener {
    collection: String,
    receiver: broadcast::Receiver<ChangeEvent>,
    filter: Option<EventFilter>,
    /// Fields this watcher actually cares about. When set, update events that
    /// touch none of these fields are skipped before filter evaluation — O(1)
    /// fast-path that keeps CPU flat under high write throughput.
    watched_fields: Option<HashSet<String>>,
}

impl ChangeListener {
    pub(crate) fn new(collection: String, receiver: broadcast::Receiver<ChangeEvent>) -> Self {
        Self {
            collection,
            receiver,
            filter: None,
            watched_fields: None,
        }
    }

    /// Add a filter to this listener.
    /// Only events matching the filter will be received.
    pub fn filter(mut self, filter: EventFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Declare which fields this watcher cares about.
    ///
    /// Update events that don't touch any of these fields are skipped entirely
    /// before filter evaluation. This keeps reactive query overhead O(1) for
    /// writes that don't affect the watched fields, regardless of how many
    /// active watchers exist.
    ///
    /// # Example
    /// ```no_run
    /// listener.watch_fields(vec!["status", "role"])
    /// ```
    pub fn watch_fields(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let set: HashSet<String> = fields.into_iter().map(|f| f.into()).collect();
        // Empty set is disjoint with everything — would silently skip all updates.
        // Treat empty input as no fast-path (same as not calling watch_fields).
        if !set.is_empty() {
            self.watched_fields = Some(set);
        }
        self
    }

    /// Returns true if this event should be skipped based on the watched fields
    /// fast-path. Only skips Update events where none of the watched fields changed.
    fn should_skip(&self, event: &ChangeEvent) -> bool {
        if event.change_type != ChangeType::Update {
            return false;
        }
        if let Some(ref watched) = self.watched_fields {
            return event.changed_fields().is_disjoint(watched);
        }
        false
    }

    /// Receive the next change event.
    /// Blocks until an event is available or the channel is closed.
    pub async fn recv(&mut self) -> Result<ChangeEvent, broadcast::error::RecvError> {
        loop {
            let event = self.receiver.recv().await?;

            // Fast-path: skip update events that didn't touch watched fields
            if self.should_skip(&event) {
                continue;
            }

            if let Some(ref filter) = self.filter {
                if filter.matches(&event) {
                    return Ok(event);
                }
                continue;
            }

            return Ok(event);
        }
    }

    /// Try to receive an event without blocking.
    /// Returns None if no event is available.
    pub fn try_recv(&mut self) -> Result<ChangeEvent, broadcast::error::TryRecvError> {
        loop {
            let event = self.receiver.try_recv()?;

            // Fast-path: skip update events that didn't touch watched fields
            if self.should_skip(&event) {
                continue;
            }

            if let Some(ref filter) = self.filter {
                if filter.matches(&event) {
                    return Ok(event);
                }
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
                    _sid: "1".to_string(),
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
        assert_eq!(event._sid, "1");
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
                    _sid: "1".to_string(),
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
                    _sid: "2".to_string(),
                    data: inactive_data,
                },
            ))
            .unwrap();

        // Should only receive active user
        let event = listener.recv().await.unwrap();
        assert_eq!(event._sid, "1");
        assert_eq!(event.get_field("active"), Some(&Value::Bool(true)));
    }
}
