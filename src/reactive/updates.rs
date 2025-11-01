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
    Modified {
        old: Document,
        new: Document,
    },
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
