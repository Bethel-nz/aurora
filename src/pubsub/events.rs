use crate::types::{Document, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

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
    /// Fields that changed in this event (populated for Update events).
    /// Allows watchers to skip filter evaluation when none of their watched
    /// fields were touched — O(1) early exit instead of full filter traversal.
    #[serde(default)]
    pub changed_fields: HashSet<String>,
}

impl fmt::Display for ChangeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap_or_default())
    }
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChangeType::Insert => write!(f, "Insert"),
            ChangeType::Update => write!(f, "Update"),
            ChangeType::Delete => write!(f, "Delete"),
        }
    }
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
            changed_fields: HashSet::new(),
        }
    }

    /// Create an update event
    pub fn update(
        collection: impl Into<String>,
        id: impl Into<String>,
        old_document: Document,
        new_document: Document,
    ) -> Self {
        // Compute changed fields eagerly once at publish time so all receivers
        // can do O(1) field-intersection checks without re-diffing the docs.
        let mut changed = HashSet::new();
        for (field, new_val) in &new_document.data {
            if old_document.data.get(field) != Some(new_val) {
                changed.insert(field.clone());
            }
        }
        for field in old_document.data.keys() {
            if !new_document.data.contains_key(field) {
                changed.insert(field.clone());
            }
        }
        Self {
            collection: collection.into(),
            change_type: ChangeType::Update,
            id: id.into(),
            document: Some(new_document),
            old_document: Some(old_document),
            changed_fields: changed,
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
            changed_fields: HashSet::new(),
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

    /// Get the set of changed fields (for updates).
    /// Pre-computed at event creation — free to call repeatedly.
    pub fn changed_fields(&self) -> &HashSet<String> {
        &self.changed_fields
    }

    /// Returns true if this update event touched the given field.
    /// O(1) hash lookup. Always false for non-Update events.
    pub fn touches_field(&self, field: &str) -> bool {
        self.change_type == ChangeType::Update && self.changed_fields.contains(field)
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
    /// Match regex pattern (field, compiled_regex)
    Matches(String, regex::Regex),
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
            EventFilter::Matches(field, re) => {
                if let Some(Value::String(s)) = event.get_field(field) {
                    re.is_match(s)
                } else {
                    false
                }
            }
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
        assert!(changed.contains("age"));
        assert_eq!(changed.len(), 1);
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

    #[test]
    fn test_event_filter_matches_regex() {
        let mut data = HashMap::new();
        data.insert("email".to_string(), Value::String("alice@example.com".into()));
        data.insert("score".to_string(), Value::Int(42));

        let doc = Document { id: "1".to_string(), data };
        let event = ChangeEvent::insert("users", "1", doc);

        // Matching pattern
        let re_match = regex::Regex::new(r"@example\.com$").unwrap();
        assert!(EventFilter::Matches("email".to_string(), re_match).matches(&event));

        // Non-matching pattern
        let re_no_match = regex::Regex::new(r"@other\.com$").unwrap();
        assert!(!EventFilter::Matches("email".to_string(), re_no_match).matches(&event));

        // Non-string field returns false
        let re_any = regex::Regex::new(r".*").unwrap();
        assert!(!EventFilter::Matches("score".to_string(), re_any).matches(&event));

        // Missing field returns false
        let re_any2 = regex::Regex::new(r".*").unwrap();
        assert!(!EventFilter::Matches("missing".to_string(), re_any2).matches(&event));
    }
}
