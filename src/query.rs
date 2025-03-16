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

use crate::types::{Document, Value};
use crate::Aurora;
use crate::error::Result;
use crate::error::AuroraError;
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
    F: Fn(&Document) -> bool
{
    fn matches(&self, doc: &Document) -> bool {
        self(doc)
    }
} 

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
    filters: Vec<Box<dyn Fn(&Document) -> bool + 'a>>,
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
        self.doc.data.get(field).map_or(false, |v| v == &value)
    }

    /// Check if a field is greater than a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.gt("age", 18))
    /// ```
    pub fn gt<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).map_or(false, |v| v > &value)
    }

    /// Check if a field is less than a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.lt("age", 18))
    /// ```
    pub fn lt<T: Into<Value>>(&self, field: &str, value: T) -> bool {
        let value = value.into();
        self.doc.data.get(field).map_or(false, |v| v < &value)
    }

    /// Check if a field contains a value
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.contains("name", "widget"))
    /// ```
    pub fn contains(&self, field: &str, value: &str) -> bool {
        self.doc.data.get(field).map_or(false, |v| {
            match v {
                Value::String(s) => s.contains(value),
                Value::Array(arr) => arr.contains(&Value::String(value.to_string())),
                _ => false,
            }
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
        self.doc.data.get(field).map_or(false, |v| values.contains(v))
    }

    /// Check if a field is between two values (inclusive)
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.between("age", 18, 65))
    /// ```
    pub fn between<T: Into<Value> + Clone>(&self, field: &str, min: T, max: T) -> bool {
        let min_val = min.into();
        let max_val = max.into();
        self.doc.data.get(field).map_or(false, |v| {
            v >= &min_val && v <= &max_val
        })
    }

    /// Check if a field exists and is not null
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.exists("email"))
    /// ```
    pub fn exists(&self, field: &str) -> bool {
        self.doc.data.get(field).map_or(false, |v| !matches!(v, Value::Null))
    }

    /// Check if a field doesn't exist or is null
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.is_null("email"))
    /// ```
    pub fn is_null(&self, field: &str) -> bool {
        self.doc.data.get(field).map_or(true, |v| matches!(v, Value::Null))
    }

    /// Check if a field starts with a prefix
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.starts_with("name", "John"))
    /// ```
    pub fn starts_with(&self, field: &str, prefix: &str) -> bool {
        self.doc.data.get(field).map_or(false, |v| {
            match v {
                Value::String(s) => s.starts_with(prefix),
                _ => false,
            }
        })
    }

    /// Check if a field ends with a suffix
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.ends_with("name", "son"))
    /// ```
    pub fn ends_with(&self, field: &str, suffix: &str) -> bool {
        self.doc.data.get(field).map_or(false, |v| {
            match v {
                Value::String(s) => s.ends_with(suffix),
                _ => false,
            }
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
        self.doc.data.get(field).map_or(false, |v| {
            match v {
                Value::Array(arr) => arr.contains(&value),
                _ => false,
            }
        })
    }

    /// Check if an array has a specific length
    ///
    /// # Examples
    /// ```
    /// .filter(|f| f.array_len_eq("status", 2))
    /// ```
    pub fn array_len_eq(&self, field: &str, len: usize) -> bool {
        self.doc.data.get(field).map_or(false, |v| {
            match v {
                Value::Array(arr) => arr.len() == len,
                _ => false,
            }
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
        self.get_nested_value(path).map_or(false, |v| v == &value)
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
            self.doc.data.get(field).map_or(false, |v| {
                match v {
                    Value::String(s) => re.is_match(s),
                    _ => false
                }
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
        F: Fn(&FilterBuilder) -> bool + 'a,
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
        let mut docs = self.db.get_all_collection(&self.collection).await?;
        
        // Apply filters
        docs.retain(|doc| self.filters.iter().all(|f| f(doc)));

        // Apply ordering
        if let Some((field, ascending)) = self.order_by {
            docs.sort_by(|a, b| {
                match (a.data.get(&field), b.data.get(&field)) {
                    (Some(v1), Some(v2)) => {
                        let cmp = v1.cmp(v2);
                        if ascending { cmp } else { cmp.reverse() }
                    },
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            });
        }

        // Apply field selection if specified
        if let Some(fields) = self.fields {
            // Create new documents with only selected fields
            docs = docs.into_iter().map(|doc| {
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
            }).collect();
        }

        // Apply offset and limit safely
        let start = self.offset.unwrap_or(0);
        let end = self.limit
            .map(|l| start.saturating_add(l))
            .unwrap_or(docs.len());
        
        // Ensure we don't go out of bounds
        let end = end.min(docs.len());
        Ok(docs.get(start..end).unwrap_or(&[]).to_vec())
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
                    None
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
        let field = self.field.ok_or_else(|| AuroraError::InvalidOperation("Search field not specified".into()))?;
        let query = self.query.ok_or_else(|| AuroraError::InvalidOperation("Search query not specified".into()))?;
        
        self.db.search_text(&self.collection, &field, &query).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleQueryBuilder {
    pub collection: String,
    pub filters: Vec<Filter>,
}

impl SimpleQueryBuilder {
    pub fn new(collection: String) -> Self {
        Self {
            collection,
            filters: Vec::new(),
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn eq(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Eq(field.to_string(), value))
    }

    pub fn gt(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Gt(field.to_string(), value))
    }

    pub fn lt(self, field: &str, value: Value) -> Self {
        self.filter(Filter::Lt(field.to_string(), value))
    }

    pub fn contains(self, field: &str, value: &str) -> Self {
        self.filter(Filter::Contains(field.to_string(), value.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    Eq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Contains(String, String),
}