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
