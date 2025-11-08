//! # Aurora Query System
//!
//! This module provides a powerful, fluent query interface for filtering, sorting,
//! and retrieving documents from Aurora collections.
//!
//! ## Examples
//!
//! ```rust
//! // Get all active users over 21
//! let users = db.query("users")
//!     .filter(|f| f.eq("status", "active") && f.gt("age", 21))
//!     .order_by("last_login", false)
//!     .limit(20)
//!     .collect()
//!     .await?;
//! ```

use crate::Aurora;
use crate::error::AuroraError;
use crate::error::Result;
use crate::types::{Document, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Trait for objects that can filter documents.
///
/// This trait is implemented for closures that take a reference to a
/// `FilterBuilder` and return a boolean, allowing for a natural filter syntax.
pub trait Queryable {
    fn matches(&self, doc: &Document) -> bool;
}

impl<F> Queryable for F
where
    F: Fn(&Document) -> bool,
{
    fn matches(&self, doc: &Document) -> bool {
        self(doc)
    }
}

/// Type alias for document filter functions
type DocumentFilter<'a> = Box<dyn Fn(&Document) -> bool + Send + Sync + 'a>;

/// Builder for creating and executing document queries.
///
/// QueryBuilder uses a fluent interface pattern to construct
/// and execute queries against Aurora collections.
///
/// # Examples
///
/// ```
/// // Query for active premium users
/// let premium_users = db.query("users")
///     .filter(|f| f.eq("status", "active") && f.eq("account_type", "premium"))
///     .order_by("created_at", false)
///     .limit(10)
///     .collect()
///     .await?;
/// ```
pub struct QueryBuilder<'a> {
    db: &'a Aurora,
    collection: String,
    filters: Vec<DocumentFilter<'a>>,
    order_by: Option<(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
    fields: Option<Vec<String>>,
}

/// Builder for constructing document filter expressions.
///
/// This struct provides methods for comparing document fields
/// with values to create filter conditions.
///
/// # Examples
///
/// ```
/// // Combine multiple filter conditions
/// db.query("products")
///     .filter(|f| {
///         f.gte("price", 10.0) &&
///         f.lte("price", 50.0) &&
///         f.contains("name", "widget")
///     })
///     .collect()
///     .await?;
/// ```
pub struct FilterBuilder<'a, 'b> {
    doc: &'b Document,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, 'b> FilterBuilder<'a, 'b> {
    /// Create a new filter builder for the given document
    pub fn new(doc: &'b Document) -> Self {
        Self {
            doc,
            _marker: std::marker::PhantomData,
        }
    }

    /// Check if a field equals a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.eq("status", "active"))
    /// ```
    pub fn eq<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field) == Some(&value)
    }

    /// Check if a field is greater than a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.gt("age", 21))
    /// ```
    pub fn gt<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).is_some_and(|v| v > &value)
    }

    /// Check if a field is greater than or equal to a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.gte("age", 21))
    /// ```
    pub fn gte<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).is_some_and(|v| v >= &value)
    }

    /// Check if a field is less than a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.lt("age", 65))
    /// ```
    pub fn lt<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).is_some_and(|v| v < &value)
    }

    /// Check if a field is less than or equal to a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.lte("age", 65))
    /// ```
    pub fn lte<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).is_some_and(|v| v <= &value)
    }

    /// Check if a field contains a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.contains("name", "widget"))
    /// ```
    pub fn contains(&self, field: &str, value: &str) -> bool {
        self.doc.data.get(field).is_some_and(|v| match v {
            Value::String(s) => s.contains(value),
            Value::Array(arr) => arr.contains(&Value::String(value.to_string())),
            _ => false,
        })
    }

    /// Check if a field is in a list of values
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.in_values("status", &["active", "inactive"]))
    /// ```
    pub fn in_values<T: Into<Value> + Clone>(&self, field: &str, values: &[T]) -> bool {
        let values: Vec<Value> = values.iter().map(|v| v.clone().into()).collect();
        self.doc
            .data
            .get(field)
            .is_some_and(|v| values.contains(v))
    }

    /// Check if a field is between two values (inclusive)
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.between("age", 18, 65))
    /// ```
    pub fn between<T: Into<Value> + Clone>(&self, field: &str, min: T, max: T) -> bool {
        self.gte(field, min) && self.lte(field, max)
    }

    /// Check if a field exists and is not null
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.exists("email"))
    /// ```
    pub fn exists(&self, field: &str) -> bool {
        self.doc
            .data
            .get(field)
            .is_some_and(|v| !matches!(v, Value::Null))
    }

    /// Check if a field doesn't exist or is null
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.is_null("email"))
    /// ```
    pub fn is_null(&self, field: &str) -> bool {
        self.doc
            .data
            .get(field)
            .is_none_or(|v| matches!(v, Value::Null))
    }

    /// Check if a field starts with a prefix
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.starts_with("name", "John"))
    /// ```
    pub fn starts_with(&self, field: &str, prefix: &str) -> bool {
        self.doc.data.get(field).is_some_and(|v| match v {
            Value::String(s) => s.starts_with(prefix),
            _ => false,
        })
    }

    /// Check if a field ends with a suffix
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.ends_with("name", "son"))
    /// ```
    pub fn ends_with(&self, field: &str, suffix: &str) -> bool {
        self.doc.data.get(field).is_some_and(|v| match v {
            Value::String(s) => s.ends_with(suffix),
            _ => false,
        })
    }

    /// Check if a field is in an array
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.array_contains("status", "active"))
    /// ```
    pub fn array_contains(&self, field: &str, value: impl Into<Value>) -> bool {
        let value = value.into();
        self.doc.data.get(field).is_some_and(|v| match v {
            Value::Array(arr) => arr.contains(&value),
            _ => false,
        })
    }

    /// Check if an array has a specific length
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.array_len_eq("status", 2))
    /// ```
    pub fn array_len_eq(&self, field: &str, len: usize) -> bool {
        self.doc.data.get(field).is_some_and(|v| match v {
            Value::Array(arr) => arr.len() == len,
            _ => false,
        })
    }

    /// Access a nested field using dot notation
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.get_nested_value("user.address.city") == Some(&Value::String("New York")))
    /// ```
    pub fn get_nested_value(&self, path: &str) -> Option<&Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = self.doc.data.get(parts[0])?;

        for &part in &parts[1..] {
            if let Value::Object(map) = current {
                current = map.get(part)?;
            } else {
                return None;
            }
        }

        Some(current)
    }

    /// Check if a nested field equals a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.nested_eq("user.address.city", "New York"))
    /// ```
    pub fn nested_eq<T: Into<Value>>(&self, path: &str, value: T) -> bool {
        let value = value.into();
        self.get_nested_value(path) == Some(&value)
    }

    /// Check if a field matches a regular expression
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.matches_regex("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
    /// ```
    pub fn matches_regex(&self, field: &str, pattern: &str) -> bool {
        use regex::Regex;

        if let Ok(re) = Regex::new(pattern) {
            self.doc.data.get(field).is_some_and(|v| match v {
                Value::String(s) => re.is_match(s),
                _ => false,
            })
        } else {
            false
        }
    }
}

impl<'a> QueryBuilder<'a> {
    /// Create a new query builder for the specified collection
    ///
    /// # Examples
    /// ```
    /// let query = db.query("users");
    /// ```
    pub fn new(db: &'a Aurora, collection: &str) -> Self {
        Self {
            db,
            collection: collection.to_string(),
            filters: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            fields: None,
        }
    }

    /// Add a filter function to the query
    ///
    /// # Examples
    /// ```
    /// let active_users = db.query("users")
    ///     .filter(|f| f.eq("status", "active"))
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn filter<F>(mut self, filter_fn: F) -> Self
    where
        // CHANGE 2: Require the closure `F` to be `Send + Sync`.
        F: Fn(&FilterBuilder) -> bool + Send + Sync + 'a,
    {
        self.filters.push(Box::new(move |doc| {
            let filter_builder = FilterBuilder::new(doc);
            filter_fn(&filter_builder)
        }));
        self
    }

    /// Sort results by a field (ascending or descending)
    ///
    /// # Parameters
    /// * `field` - The field to sort by
    /// * `ascending` - `true` for ascending order, `false` for descending
    ///
    /// # Examples
    /// ```
    /// // Sort by age ascending
    /// .order_by("age", true)
    ///
    /// // Sort by creation date descending (newest first)
    /// .order_by("created_at", false)
    /// ```
    pub fn order_by(mut self, field: &str, ascending: bool) -> Self {
        self.order_by = Some((field.to_string(), ascending));
        self
    }

    /// Limit the number of results returned
    ///
    /// # Examples
    /// ```
    /// // Get at most 10 results
    /// .limit(10)
    /// ```
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip a number of results (for pagination)
    ///
    /// # Examples
    /// ```
    /// // For pagination: skip the first 20 results and get the next 10
    /// .offset(20).limit(10)
    /// ```
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Select only specific fields to return
    ///
    /// # Examples
    /// ```
    /// // Only return name and email fields
    /// .select(&["name", "email"])
    /// ```
    pub fn select(mut self, fields: &[&str]) -> Self {
        self.fields = Some(fields.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Execute the query and collect the results
    ///
    /// # Returns
    /// A vector of documents matching the query criteria
    ///
    /// # Examples
    /// ```
    /// let results = db.query("products")
    ///     .filter(|f| f.lt("price", 100))
    ///     .collect()
    ///     .await?;
    /// ```
    pub async fn collect(self) -> Result<Vec<Document>> {
        // Ensure indices are initialized
        self.db.ensure_indices_initialized().await?;

        // Optimization: Use early termination for queries with LIMIT but no ORDER BY
        let mut docs = if self.order_by.is_none() && self.limit.is_some() {
            // Early termination path - scan only until we have enough results
            let target = self.limit.unwrap() + self.offset.unwrap_or(0);
            let filter = |doc: &Document| self.filters.iter().all(|f| f(doc));
            self.db.scan_and_filter(&self.collection, filter, Some(target))?
        } else {
            // Standard path - need all matching docs (for sorting or no limit)
            let mut docs = self.db.get_all_collection(&self.collection).await?;
            docs.retain(|doc| self.filters.iter().all(|f| f(doc)));
            docs
        };

        // Apply ordering
        if let Some((field, ascending)) = self.order_by {
            docs.sort_by(|a, b| match (a.data.get(&field), b.data.get(&field)) {
                (Some(v1), Some(v2)) => {
                    let cmp = v1.cmp(v2);
                    if ascending { cmp } else { cmp.reverse() }
                }
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            });
        }

        // Apply field selection if specified
        if let Some(fields) = self.fields {
            // Create new documents with only selected fields
            docs = docs
                .into_iter()
                .map(|doc| {
                    let mut new_data = HashMap::new();
                    // Always include the ID
                    for field in &fields {
                        if let Some(value) = doc.data.get(field) {
                            new_data.insert(field.clone(), value.clone());
                        }
                    }
                    Document {
                        id: doc.id,
                        data: new_data,
                    }
                })
                .collect();
        }

        // Apply offset and limit safely
        let start = self.offset.unwrap_or(0);
        let end = self
            .limit
            .map(|l| start.saturating_add(l))
            .unwrap_or(docs.len());

        // Ensure we don't go out of bounds
        let end = end.min(docs.len());
        Ok(docs.get(start..end).unwrap_or(&[]).to_vec())
    }

    /// Watch the query for real-time updates
    ///
    /// Returns a QueryWatcher that streams live updates when documents are added,
    /// removed, or modified in ways that affect the query results. Perfect for
    /// building reactive UIs, live dashboards, and real-time applications.
    ///
    /// # Performance
    /// - Zero overhead for queries without watchers
    /// - Updates delivered asynchronously via channels
    /// - Automatic filtering - only matching changes are emitted
    /// - Memory efficient - only tracks matching documents
    ///
    /// # Requirements
    /// This method requires the QueryBuilder to have a 'static lifetime,
    /// which means the database reference must also be 'static (e.g., Arc<Aurora>).
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::Value};
    /// use std::sync::Arc;
    ///
    /// let db = Arc::new(Aurora::open("mydb.db")?);
    ///
    /// // Basic reactive query - watch active users
    /// let mut watcher = db.query("users")
    ///     .filter(|f| f.eq("active", Value::Bool(true)))
    ///     .watch()
    ///     .await?;
    ///
    /// // Receive updates in real-time
    /// while let Some(update) = watcher.next().await {
    ///     match update {
    ///         QueryUpdate::Added(doc) => {
    ///             println!("New active user: {}", doc.id);
    ///         },
    ///         QueryUpdate::Removed(doc) => {
    ///             println!("User deactivated: {}", doc.id);
    ///         },
    ///         QueryUpdate::Modified { old, new } => {
    ///             println!("User updated: {} -> {}", old.id, new.id);
    ///         },
    ///     }
    /// }
    /// ```
    ///
    /// # Real-World Use Cases
    ///
    /// **Live Leaderboard:**
    /// ```
    /// // Watch top players by score
    /// let mut leaderboard = db.query("players")
    ///     .filter(|f| f.gte("score", Value::Int(1000)))
    ///     .watch()
    ///     .await?;
    ///
    /// tokio::spawn(async move {
    ///     while let Some(update) = leaderboard.next().await {
    ///         // Update UI with new rankings
    ///         broadcast_to_clients(&update).await;
    ///     }
    /// });
    /// ```
    ///
    /// **Activity Feed:**
    /// ```
    /// // Watch recent posts for a user's feed
    /// let mut feed = db.query("posts")
    ///     .filter(|f| f.eq("author_id", user_id))
    ///     .watch()
    ///     .await?;
    ///
    /// // Stream updates to WebSocket
    /// while let Some(update) = feed.next().await {
    ///     match update {
    ///         QueryUpdate::Added(post) => {
    ///             websocket.send(json!({"type": "new_post", "post": post})).await?;
    ///         },
    ///         _ => {}
    ///     }
    /// }
    /// ```
    ///
    /// **Real-Time Dashboard:**
    /// ```
    /// // Watch critical metrics
    /// let mut alerts = db.query("metrics")
    ///     .filter(|f| f.gt("cpu_usage", Value::Float(80.0)))
    ///     .watch()
    ///     .await?;
    ///
    /// tokio::spawn(async move {
    ///     while let Some(update) = alerts.next().await {
    ///         if let QueryUpdate::Added(metric) = update {
    ///             // Alert on high CPU usage
    ///             send_alert(format!("High CPU: {:?}", metric)).await;
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// **Collaborative Editing:**
    /// ```
    /// // Watch document for changes from other users
    /// let doc_id = "doc-123";
    /// let mut changes = db.query("documents")
    ///     .filter(|f| f.eq("id", doc_id))
    ///     .watch()
    ///     .await?;
    ///
    /// tokio::spawn(async move {
    ///     while let Some(update) = changes.next().await {
    ///         if let QueryUpdate::Modified { old, new } = update {
    ///             // Merge changes from other editors
    ///             apply_remote_changes(&old, &new).await;
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// **Stock Ticker:**
    /// ```
    /// // Watch price changes
    /// let mut price_watcher = db.query("stocks")
    ///     .filter(|f| f.eq("symbol", "AAPL"))
    ///     .watch()
    ///     .await?;
    ///
    /// while let Some(update) = price_watcher.next().await {
    ///     if let QueryUpdate::Modified { old, new } = update {
    ///         if let (Some(old_price), Some(new_price)) =
    ///             (old.data.get("price"), new.data.get("price")) {
    ///             println!("AAPL: {} -> {}", old_price, new_price);
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// # Multiple Watchers Pattern
    ///
    /// ```
    /// // Watch multiple queries concurrently
    /// let mut high_priority = db.query("tasks")
    ///     .filter(|f| f.eq("priority", Value::String("high".into())))
    ///     .watch()
    ///     .await?;
    ///
    /// let mut urgent = db.query("tasks")
    ///     .filter(|f| f.eq("status", Value::String("urgent".into())))
    ///     .watch()
    ///     .await?;
    ///
    /// tokio::spawn(async move {
    ///     loop {
    ///         tokio::select! {
    ///             Some(update) = high_priority.next() => {
    ///                 println!("High priority: {:?}", update);
    ///             },
    ///             Some(update) = urgent.next() => {
    ///                 println!("Urgent: {:?}", update);
    ///             },
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// # Important Notes
    /// - Requires Arc<Aurora> for 'static lifetime
    /// - Updates are delivered asynchronously
    /// - Watcher keeps running until dropped
    /// - Only matching documents trigger updates
    /// - Use tokio::spawn to process updates in background
    ///
    /// # See Also
    /// - `Aurora::listen()` for collection-level change notifications
    /// - `QueryWatcher::next()` to receive the next update
    /// - `QueryWatcher::try_next()` for non-blocking checks
    pub async fn watch(mut self) -> Result<crate::reactive::QueryWatcher>
    where
        'a: 'static,
    {
        use crate::reactive::{QueryWatcher, ReactiveQueryState};
        use std::sync::Arc;

        // Extract the filters before consuming self
        let collection = self.collection.clone();
        let db = self.db;
        let filters = std::mem::take(&mut self.filters);

        // Get initial results
        let docs = self.collect().await?;

        // Create a listener for this collection
        let listener = db.listen(&collection);

        // Create filter closure that combines all the query filters
        let filter_fn = move |doc: &Document| -> bool { filters.iter().all(|f| f(doc)) };

        // Create reactive state
        let state = Arc::new(ReactiveQueryState::new(filter_fn));

        // Create and return watcher
        Ok(QueryWatcher::new(collection, listener, state, docs))
    }

    /// Get only the first matching document or None if no matches
    ///
    /// # Examples
    /// ```
    /// let user = db.query("users")
    ///     .filter(|f| f.eq("email", "jane@example.com"))
    ///     .first_one()
    ///     .await?;
    /// ```
    pub async fn first_one(self) -> Result<Option<Document>> {
        self.limit(1).collect().await.map(|mut docs| docs.pop())
    }

    /// Count the number of documents matching the query
    ///
    /// # Examples
    /// ```
    /// let active_count = db.query("users")
    ///     .filter(|f| f.eq("status", "active"))
    ///     .count()
    ///     .await?;
    /// ```
    pub async fn count(self) -> Result<usize> {
        self.collect().await.map(|docs| docs.len())
    }

    /// Update documents matching the query with new field values
    ///
    /// # Returns
    /// The number of documents updated
    ///
    /// # Examples
    /// ```
    /// let updated = db.query("products")
    ///     .filter(|f| f.lt("stock", 5))
    ///     .update([
    ///         ("status", Value::String("low_stock".to_string())),
    ///         ("needs_reorder", Value::Bool(true))
    ///     ].into_iter().collect())
    ///     .await?;
    /// ```
    pub async fn update(self, updates: HashMap<&str, Value>) -> Result<usize> {
        // Store a reference to the db and collection before consuming self
        let db = self.db;
        let collection = self.collection.clone();

        let docs = self.collect().await?;
        let mut updated_count = 0;

        for doc in docs {
            let mut updated_doc = doc.clone();
            let mut changed = false;

            for (field, value) in &updates {
                updated_doc.data.insert(field.to_string(), value.clone());
                changed = true;
            }

            if changed {
                // Update document in the database
                db.put(
                    format!("{}:{}", collection, updated_doc.id),
                    serde_json::to_vec(&updated_doc)?,
                    None,
                )?;
                updated_count += 1;
            }
        }

        Ok(updated_count)
    }
}

/// Builder for performing full-text search operations
///
/// # Examples
/// ```
/// let results = db.search("products")
///     .field("description")
///     .matching("wireless headphones")
///     .fuzzy(true)
///     .collect()
///     .await?;
/// ```
pub struct SearchBuilder<'a> {
    db: &'a Aurora,
    collection: String,
    field: Option<String>,
    query: Option<String>,
    fuzzy: bool,
}

impl<'a> SearchBuilder<'a> {
    /// Create a new search builder for the specified collection
    pub fn new(db: &'a Aurora, collection: &str) -> Self {
        Self {
            db,
            collection: collection.to_string(),
            field: None,
            query: None,
            fuzzy: false,
        }
    }

    /// Specify the field to search in
    ///
    /// # Examples
    /// ```
    /// .field("description")
    /// ```
    pub fn field(mut self, field: &str) -> Self {
        self.field = Some(field.to_string());
        self
    }

    /// Specify the search query text
    ///
    /// # Examples
    /// ```
    /// .matching("wireless headphones")
    /// ```
    pub fn matching(mut self, query: &str) -> Self {
        self.query = Some(query.to_string());
        self
    }

    /// Enable or disable fuzzy matching (for typo tolerance)
    ///
    /// # Examples
    /// ```
    /// .fuzzy(true)  // Enable fuzzy matching
    /// ```
    pub fn fuzzy(mut self, enable: bool) -> Self {
        self.fuzzy = enable;
        self
    }

    /// Execute the search and collect the results
    ///
    /// # Returns
    /// A vector of documents matching the search criteria
    ///
    /// # Examples
    /// ```
    /// let results = db.search("articles")
    ///     .field("content")
    ///     .matching("quantum computing")
    ///     .collect()
    ///     .await?;
    /// ```
    pub async fn collect(self) -> Result<Vec<Document>> {
        let field = self
            .field
            .ok_or_else(|| AuroraError::InvalidOperation("Search field not specified".into()))?;
        let query = self
            .query
            .ok_or_else(|| AuroraError::InvalidOperation("Search query not specified".into()))?;

        self.db.search_text(&self.collection, &field, &query).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleQueryBuilder {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub order_by: Option<(String, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl SimpleQueryBuilder {
    pub fn new(collection: String) -> Self {
        Self {
            collection,
            filters: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Filter for exact equality
    ///
    /// Uses secondary index if the field is indexed (O(1) lookup).
    /// Falls back to full collection scan if not indexed (O(n)).
    ///
    /// # Arguments
    /// * `field` - The field name to filter on
    /// * `value` - The exact value to match
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Find active users
    /// let active_users = db.query("users")
    ///     .filter(|f| f.eq("status", Value::String("active".into())))
    ///     .collect()
    ///     .await?;
    ///
    /// // Multiple equality filters (AND logic)
    /// let premium_active = db.query("users")
    ///     .filter(|f| f.eq("tier", Value::String("premium".into())))
    ///     .filter(|f| f.eq("active", Value::Bool(true)))
    ///     .collect()
    ///     .await?;
    ///
    /// // Numeric equality
    /// let age_30 = db.query("users")
    ///     .filter(|f| f.eq("age", Value::Int(30)))
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn eq(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Eq(field.to_string(), value))
    }

    /// Filter for greater than
    ///
    /// Finds all documents where the field value is strictly greater than
    /// the provided value. With LIMIT, uses early termination for performance.
    ///
    /// # Arguments
    /// * `field` - The field name to compare
    /// * `value` - The minimum value (exclusive)
    ///
    /// # Performance
    /// - Without LIMIT: O(n) - scans all documents
    /// - With LIMIT: O(k) where k = limit + offset (early termination)
    /// - No index support yet (planned for future)
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Find high scorers (with early termination)
    /// let high_scorers = db.query("users")
    ///     .filter(|f| f.gt("score", Value::Int(1000)))
    ///     .limit(100)  // Stops after finding 100 matches
    ///     .collect()
    ///     .await?;
    ///
    /// // Price range queries
    /// let expensive = db.query("products")
    ///     .filter(|f| f.gt("price", Value::Float(99.99)))
    ///     .order_by("price", false)  // Descending
    ///     .collect()
    ///     .await?;
    ///
    /// // Date filtering (timestamps as integers)
    /// let recent = db.query("events")
    ///     .filter(|f| f.gt("timestamp", Value::Int(1609459200)))  // After Jan 1, 2021
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn gt(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Gt(field.to_string(), value))
    }

    /// Filter for greater than or equal to
    ///
    /// Finds all documents where the field value is greater than or equal to
    /// the provided value. Inclusive version of `gt()`.
    ///
    /// # Arguments
    /// * `field` - The field name to compare
    /// * `value` - The minimum value (inclusive)
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Minimum age requirement (inclusive)
    /// let adults = db.query("users")
    ///     .filter(|f| f.gte("age", Value::Int(18)))
    ///     .collect()
    ///     .await?;
    ///
    /// // Inventory management
    /// let in_stock = db.query("products")
    ///     .filter(|f| f.gte("stock", Value::Int(1)))
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn gte(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Gte(field.to_string(), value))
    }

    /// Filter for less than
    ///
    /// Finds all documents where the field value is strictly less than
    /// the provided value.
    ///
    /// # Arguments
    /// * `field` - The field name to compare
    /// * `value` - The maximum value (exclusive)
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Low balance accounts
    /// let low_balance = db.query("accounts")
    ///     .filter(|f| f.lt("balance", Value::Float(10.0)))
    ///     .collect()
    ///     .await?;
    ///
    /// // Budget products
    /// let budget = db.query("products")
    ///     .filter(|f| f.lt("price", Value::Float(50.0)))
    ///     .order_by("price", true)  // Ascending
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn lt(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Lt(field.to_string(), value))
    }

    /// Filter for less than or equal to
    ///
    /// Finds all documents where the field value is less than or equal to
    /// the provided value. Inclusive version of `lt()`.
    ///
    /// # Arguments
    /// * `field` - The field name to compare
    /// * `value` - The maximum value (inclusive)
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::{Aurora, types::Value};
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Senior discount eligibility
    /// let seniors = db.query("users")
    ///     .filter(|f| f.lte("age", Value::Int(65)))
    ///     .collect()
    ///     .await?;
    ///
    /// // Clearance items
    /// let clearance = db.query("products")
    ///     .filter(|f| f.lte("price", Value::Float(20.0)))
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn lte(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Lte(field.to_string(), value))
    }

    /// Filter for substring containment
    ///
    /// Finds all documents where the field value contains the specified substring.
    /// Case-sensitive matching. For text search, consider using the `search()` API instead.
    ///
    /// # Arguments
    /// * `field` - The field name to search in (must be a string field)
    /// * `value` - The substring to search for
    ///
    /// # Performance
    /// - Always O(n) - scans all documents
    /// - Case-sensitive string matching
    /// - For full-text search, use `db.search()` instead
    ///
    /// # Examples
    ///
    /// ```
    /// use aurora_db::Aurora;
    ///
    /// let db = Aurora::open("mydb.db")?;
    ///
    /// // Find articles about Rust
    /// let rust_articles = db.query("articles")
    ///     .filter(|f| f.contains("title", "Rust"))
    ///     .collect()
    ///     .await?;
    ///
    /// // Email domain filtering
    /// let gmail_users = db.query("users")
    ///     .filter(|f| f.contains("email", "@gmail.com"))
    ///     .collect()
    ///     .await?;
    ///
    /// // Tag searching
    /// let rust_posts = db.query("posts")
    ///     .filter(|f| f.contains("tags", "rust"))
    ///     .collect()
    ///     .await?;
    /// ```
    ///
    /// # Note
    /// For case-insensitive search or more advanced text matching,
    /// use the full-text search API: `db.search(collection).query(text)`
    pub fn contains(self, field: &str, value: &str) -> Self {
        self.filter(Filter::Contains(field.to_string(), value.to_string()))
    }

    /// Convenience method for range queries
    pub fn between(self, field: &str, min: Value, max: Value) -> Self {
        self.filter(Filter::Gte(field.to_string(), min))
            .filter(Filter::Lte(field.to_string(), max))
    }

    /// Sort results by a field (ascending or descending)
    pub fn order_by(mut self, field: &str, ascending: bool) -> Self {
        self.order_by = Some((field.to_string(), ascending));
        self
    }

    /// Limit the number of results returned
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip a number of results (for pagination)
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Filter {
    Eq(String, Value),
    Gt(String, Value),
    Gte(String, Value),
    Lt(String, Value),
    Lte(String, Value),
    Contains(String, String),
}
