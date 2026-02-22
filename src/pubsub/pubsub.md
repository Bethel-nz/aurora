use super::events::ChangeEvent;
use tokio::sync::broadcast;

/// Wrapper around broadcast channel for change events
pub struct ChangeChannel {
    sender: broadcast::Sender<ChangeEvent>,
}

impl ChangeChannel {
    /// Create a new change channel with the specified buffer size
    pub fn new(buffer_size: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer_size);
        Self { sender }
    }

    /// Publish an event to this channel
    pub fn publish(
        &self,
        event: ChangeEvent,
    ) -> Result<(), broadcast::error::SendError<ChangeEvent>> {
        self.sender.send(event).map(|_| ())
    }

    /// Subscribe to this channel
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    /// Get the number of active receivers
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Clone for ChangeChannel {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
use crate::types::{Document, Value};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::collections::HashMap;

/// Type of change that occurred
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    /// New document inserted
    Insert,
    /// Existing document updated
    Update,
    /// Document deleted
    Delete,
}

/// Change event containing information about a database modification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Collection where the change occurred
    pub collection: String,
    /// Type of change
    pub change_type: ChangeType,
    /// Document ID
    pub id: String,
    /// New/current document (None for Delete)
    pub document: Option<Document>,
    /// Previous document (for Update only)
    pub old_document: Option<Document>,
}

impl ChangeEvent {
    /// Create an insert event
    pub fn insert(
        collection: impl Into<String>,
        id: impl Into<String>,
        document: Document,
    ) -> Self {
        Self {
            collection: collection.into(),
            change_type: ChangeType::Insert,
            id: id.into(),
            document: Some(document),
            old_document: None,
        }
    }

    /// Create an update event
    pub fn update(
        collection: impl Into<String>,
        id: impl Into<String>,
        old_document: Document,
        new_document: Document,
    ) -> Self {
        Self {
            collection: collection.into(),
            change_type: ChangeType::Update,
            id: id.into(),
            document: Some(new_document),
            old_document: Some(old_document),
        }
    }

    /// Create a delete event
    pub fn delete(collection: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            change_type: ChangeType::Delete,
            id: id.into(),
            document: None,
            old_document: None,
        }
    }

    /// Get a field value from the current document
    pub fn get_field(&self, field: &str) -> Option<&Value> {
        self.document.as_ref()?.data.get(field)
    }

    /// Get a field value from the old document (for updates)
    pub fn get_old_field(&self, field: &str) -> Option<&Value> {
        self.old_document.as_ref()?.data.get(field)
    }

    /// Check if a specific field changed (for updates)
    pub fn field_changed(&self, field: &str) -> bool {
        if self.change_type != ChangeType::Update {
            return false;
        }

        let old_value = self.get_old_field(field);
        let new_value = self.get_field(field);

        old_value != new_value
    }

    /// Get the list of changed fields (for updates)
    pub fn changed_fields(&self) -> Vec<String> {
        if self.change_type != ChangeType::Update {
            return vec![];
        }

        let old_doc = match &self.old_document {
            Some(doc) => doc,
            None => return vec![],
        };

        let new_doc = match &self.document {
            Some(doc) => doc,
            None => return vec![],
        };

        let mut changed = Vec::new();

        // Check all fields in new document
        for (field, new_value) in &new_doc.data {
            if old_doc.data.get(field) != Some(new_value) {
                changed.push(field.clone());
            }
        }

        // Check for deleted fields
        for field in old_doc.data.keys() {
            if !new_doc.data.contains_key(field) {
                changed.push(field.clone());
            }
        }

        changed
    }
}

/// Filter for events
#[derive(Debug, Clone)]
pub enum EventFilter {
    /// Match all events
    All,
    /// Match specific change types
    ChangeType(ChangeType),
    /// Match events where a specific field exists
    HasField(String),
    /// Match events where a field has a specific value
    FieldEquals(String, Value),
    /// Match events where a field changed (updates only)
    FieldChanged(String),
    /// Combine multiple filters with AND
    And(Vec<EventFilter>),
    /// Combine multiple filters with OR
    Or(Vec<EventFilter>),
    /// Invert a filter
    Not(Box<EventFilter>),
    /// Greater than
    Gt(String, Value),
    /// Greater than or equal
    Gte(String, Value),
    /// Less than
    Lt(String, Value),
    /// Less than or equal
    Lte(String, Value),
    /// Not equal
    Ne(String, Value),
    /// In list
    In(String, Value),
    /// Not in list
    NotIn(String, Value),
    /// Contains string
    Contains(String, Value),
    /// Starts with string
    StartsWith(String, Value),
    /// Ends with string
    EndsWith(String, Value),
    /// Is Null
    IsNull(String),
    /// Is Not Null
    IsNotNull(String),
}

impl EventFilter {
    /// Check if an event matches this filter
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        match self {
            EventFilter::All => true,
            EventFilter::ChangeType(ct) => &event.change_type == ct,
            EventFilter::HasField(field) => event.get_field(field).is_some(),
            EventFilter::FieldEquals(field, value) => event.get_field(field) == Some(value),
            EventFilter::FieldChanged(field) => event.field_changed(field),
            EventFilter::And(filters) => filters.iter().all(|f| f.matches(event)),
            EventFilter::Or(filters) => filters.iter().any(|f| f.matches(event)),
            EventFilter::Not(filter) => !filter.matches(event),
            EventFilter::Gt(field, val) => event.get_field(field).map_or(false, |v| v > val),
            EventFilter::Gte(field, val) => event.get_field(field).map_or(false, |v| v >= val),
            EventFilter::Lt(field, val) => event.get_field(field).map_or(false, |v| v < val),
            EventFilter::Lte(field, val) => event.get_field(field).map_or(false, |v| v <= val),
            EventFilter::Ne(field, val) => event.get_field(field).map_or(false, |v| v != val),
            EventFilter::In(field, val) => {
                if let Some(field_val) = event.get_field(field) {
                    if let Value::Array(arr) = val {
                        arr.contains(field_val)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            EventFilter::NotIn(field, val) => {
                if let Some(field_val) = event.get_field(field) {
                    if let Value::Array(arr) = val {
                        !arr.contains(field_val)
                    } else {
                        true
                    }
                } else {
                    true
                }
            }
            EventFilter::Contains(field, val) => {
                event.get_field(field).map_or(false, |v| match (v, val) {
                    (Value::String(s), Value::String(sub)) => s.contains(sub),
                    _ => false,
                })
            }
            EventFilter::StartsWith(field, val) => {
                event.get_field(field).map_or(false, |v| match (v, val) {
                    (Value::String(s), Value::String(sub)) => s.starts_with(sub),
                    _ => false,
                })
            }
            EventFilter::EndsWith(field, val) => {
                event.get_field(field).map_or(false, |v| match (v, val) {
                    (Value::String(s), Value::String(sub)) => s.ends_with(sub),
                    _ => false,
                })
            }
            EventFilter::IsNull(field) => event
                .get_field(field)
                .map_or(true, |v| matches!(v, Value::Null)),
            EventFilter::IsNotNull(field) => event
                .get_field(field)
                .map_or(false, |v| !matches!(v, Value::Null)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_event_insert() {
        let mut data = HashMap::new();
        data.insert("name".to_string(), Value::String("Alice".into()));

        let doc = Document {
            id: "123".to_string(),
            data,
        };

        let event = ChangeEvent::insert("users", "123", doc);

        assert_eq!(event.collection, "users");
        assert_eq!(event.id, "123");
        assert!(matches!(event.change_type, ChangeType::Insert));
        assert!(event.document.is_some());
        assert!(event.old_document.is_none());
    }

    #[test]
    fn test_change_event_field_changed() {
        let mut old_data = HashMap::new();
        old_data.insert("name".to_string(), Value::String("Alice".into()));
        old_data.insert("age".to_string(), Value::Int(25));

        let mut new_data = HashMap::new();
        new_data.insert("name".to_string(), Value::String("Alice".into()));
        new_data.insert("age".to_string(), Value::Int(26));

        let old_doc = Document {
            id: "123".to_string(),
            data: old_data,
        };

        let new_doc = Document {
            id: "123".to_string(),
            data: new_data,
        };

        let event = ChangeEvent::update("users", "123", old_doc, new_doc);

        assert!(!event.field_changed("name"));
        assert!(event.field_changed("age"));

        let changed = event.changed_fields();
        assert_eq!(changed, vec!["age"]);
    }

    #[test]
    fn test_event_filter() {
        let mut data = HashMap::new();
        data.insert("active".to_string(), Value::Bool(true));

        let doc = Document {
            id: "123".to_string(),
            data,
        };

        let event = ChangeEvent::insert("users", "123", doc);

        // Test filters
        assert!(EventFilter::All.matches(&event));
        assert!(EventFilter::ChangeType(ChangeType::Insert).matches(&event));
        assert!(!EventFilter::ChangeType(ChangeType::Delete).matches(&event));
        assert!(EventFilter::HasField("active".to_string()).matches(&event));
        assert!(!EventFilter::HasField("missing".to_string()).matches(&event));
        assert!(EventFilter::FieldEquals("active".to_string(), Value::Bool(true)).matches(&event));
        assert!(
            !EventFilter::FieldEquals("active".to_string(), Value::Bool(false)).matches(&event)
        );
    }
}
use super::events::{ChangeEvent, EventFilter};
use tokio::sync::broadcast;

/// Listener for change events on a collection
#[derive(Debug)]
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
            self.channels
                .retain(|_, sender| sender.receiver_count() > 0);
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
