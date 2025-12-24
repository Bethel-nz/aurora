//! AQL Executor - Connects parsed AQL to Aurora operations
//!
//! Provides the bridge between AQL AST and database operations.
//! This module focuses on:
//! - Filter evaluation against documents
//! - Projection (selecting fields)
//! - Value conversion between AQL and DB types
//! - Pagination helpers
//!
//! Note: Direct Aurora integration will be added separately in db.rs

use crate::error::{AuroraError, Result};
use crate::types::{Document, Value};
use super::ast::{self, Filter as AqlFilter};
use std::collections::HashMap;

/// Result of executing an AQL operation
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    /// Query result with documents
    Query(QueryResult),
    /// Mutation result with affected documents
    Mutation(MutationResult),
    /// Subscription ID for reactive queries
    Subscription(SubscriptionResult),
    /// Multiple results for batch operations
    Batch(Vec<ExecutionResult>),
}

/// Query execution result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub collection: String,
    pub documents: Vec<Document>,
    pub total_count: Option<usize>,
}

/// Mutation execution result
#[derive(Debug, Clone)]
pub struct MutationResult {
    pub operation: String,
    pub collection: String,
    pub affected_count: usize,
    pub returned_documents: Vec<Document>,
}

/// Subscription result
#[derive(Debug, Clone)]
pub struct SubscriptionResult {
    pub subscription_id: String,
    pub collection: String,
}

/// Execution options
#[derive(Debug, Clone, Default)]
pub struct ExecutionOptions {
    /// Skip validation (for performance when you trust the query)
    pub skip_validation: bool,
    /// Apply projections (return only requested fields)
    pub apply_projections: bool,
}

impl ExecutionOptions {
    pub fn new() -> Self {
        Self {
            skip_validation: false,
            apply_projections: true,
        }
    }

    pub fn skip_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }
}

// ============================================================================
// Filter Evaluation
// ============================================================================

/// Check if a document matches a filter
pub fn matches_filter(doc: &Document, filter: &AqlFilter, variables: &HashMap<String, ast::Value>) -> bool {
    match filter {
        AqlFilter::Eq(field, value) => {
            doc.data.get(field).map(|v| values_equal(v, value, variables)).unwrap_or(false)
        }
        AqlFilter::Ne(field, value) => {
            doc.data.get(field).map(|v| !values_equal(v, value, variables)).unwrap_or(true)
        }
        AqlFilter::Gt(field, value) => {
            doc.data.get(field).map(|v| value_compare(v, value, variables) == Some(std::cmp::Ordering::Greater)).unwrap_or(false)
        }
        AqlFilter::Gte(field, value) => {
            doc.data.get(field).map(|v| matches!(value_compare(v, value, variables), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))).unwrap_or(false)
        }
        AqlFilter::Lt(field, value) => {
            doc.data.get(field).map(|v| value_compare(v, value, variables) == Some(std::cmp::Ordering::Less)).unwrap_or(false)
        }
        AqlFilter::Lte(field, value) => {
            doc.data.get(field).map(|v| matches!(value_compare(v, value, variables), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))).unwrap_or(false)
        }
        AqlFilter::In(field, value) => {
            if let ast::Value::Array(arr) = value {
                doc.data.get(field).map(|v| arr.iter().any(|item| values_equal(v, item, variables))).unwrap_or(false)
            } else {
                false
            }
        }
        AqlFilter::NotIn(field, value) => {
            if let ast::Value::Array(arr) = value {
                doc.data.get(field).map(|v| !arr.iter().any(|item| values_equal(v, item, variables))).unwrap_or(true)
            } else {
                true
            }
        }
        AqlFilter::Contains(field, value) => {
            if let (Some(Value::String(doc_val)), ast::Value::String(search)) = (doc.data.get(field), value) {
                doc_val.contains(search)
            } else {
                false
            }
        }
        AqlFilter::StartsWith(field, value) => {
            if let (Some(Value::String(doc_val)), ast::Value::String(prefix)) = (doc.data.get(field), value) {
                doc_val.starts_with(prefix)
            } else {
                false
            }
        }
        AqlFilter::EndsWith(field, value) => {
            if let (Some(Value::String(doc_val)), ast::Value::String(suffix)) = (doc.data.get(field), value) {
                doc_val.ends_with(suffix)
            } else {
                false
            }
        }
        AqlFilter::Matches(field, value) => {
            // Simplified regex matching - contains for now
            if let (Some(Value::String(doc_val)), ast::Value::String(pattern)) = (doc.data.get(field), value) {
                doc_val.contains(pattern)
            } else {
                false
            }
        }
        AqlFilter::IsNull(field) => {
            doc.data.get(field).map(|v| matches!(v, Value::Null)).unwrap_or(true)
        }
        AqlFilter::IsNotNull(field) => {
            doc.data.get(field).map(|v| !matches!(v, Value::Null)).unwrap_or(false)
        }
        AqlFilter::And(filters) => filters.iter().all(|f| matches_filter(doc, f, variables)),
        AqlFilter::Or(filters) => filters.iter().any(|f| matches_filter(doc, f, variables)),
        AqlFilter::Not(inner) => !matches_filter(doc, inner, variables),
    }
}

/// Check if two values are equal
fn values_equal(db_val: &Value, aql_val: &ast::Value, variables: &HashMap<String, ast::Value>) -> bool {
    let resolved = resolve_if_variable(aql_val, variables);
    match (db_val, resolved) {
        (Value::Null, ast::Value::Null) => true,
        (Value::Bool(a), ast::Value::Boolean(b)) => *a == *b,
        (Value::Int(a), ast::Value::Int(b)) => *a == *b,
        (Value::Float(a), ast::Value::Float(b)) => (*a - *b).abs() < f64::EPSILON,
        (Value::Float(a), ast::Value::Int(b)) => (*a - (*b as f64)).abs() < f64::EPSILON,
        (Value::Int(a), ast::Value::Float(b)) => ((*a as f64) - *b).abs() < f64::EPSILON,
        (Value::String(a), ast::Value::String(b)) => a == b,
        _ => false,
    }
}

/// Compare two values
fn value_compare(db_val: &Value, aql_val: &ast::Value, variables: &HashMap<String, ast::Value>) -> Option<std::cmp::Ordering> {
    let resolved = resolve_if_variable(aql_val, variables);
    match (db_val, resolved) {
        (Value::Int(a), ast::Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), ast::Value::Float(b)) => a.partial_cmp(b),
        (Value::Float(a), ast::Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Int(a), ast::Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::String(a), ast::Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Resolve a variable reference
fn resolve_if_variable<'a>(val: &'a ast::Value, variables: &'a HashMap<String, ast::Value>) -> &'a ast::Value {
    if let ast::Value::Variable(name) = val {
        variables.get(name).unwrap_or(val)
    } else {
        val
    }
}

// ============================================================================
// Projection
// ============================================================================

/// Apply projection to a document (keep only selected fields)
pub fn apply_projection(mut doc: Document, fields: &[ast::Field]) -> Document {
    if fields.is_empty() {
        return doc;
    }
    
    let mut projected_data = HashMap::new();
    
    // Always include id
    if let Some(id_val) = doc.data.get("id") {
        projected_data.insert("id".to_string(), id_val.clone());
    }
    
    for field in fields {
        let field_name = field.alias.as_ref().unwrap_or(&field.name);
        let source_name = &field.name;
        
        if let Some(value) = doc.data.get(source_name) {
            projected_data.insert(field_name.clone(), value.clone());
        }
    }
    
    doc.data = projected_data;
    doc
}

// ============================================================================
// Pagination
// ============================================================================

/// Extract pagination parameters from field arguments
pub fn extract_pagination(args: &[ast::Argument]) -> (Option<usize>, usize) {
    let mut limit = None;
    let mut offset = 0;
    
    for arg in args {
        match arg.name.as_str() {
            "limit" | "first" | "take" => {
                if let ast::Value::Int(n) = arg.value {
                    limit = Some(n as usize);
                }
            }
            "offset" | "skip" => {
                if let ast::Value::Int(n) = arg.value {
                    offset = n as usize;
                }
            }
            _ => {}
        }
    }
    
    (limit, offset)
}

// ============================================================================
// Value Conversion
// ============================================================================

/// Convert AQL Value to DB Value
pub fn aql_value_to_db_value(val: &ast::Value) -> Result<Value> {
    match val {
        ast::Value::Null => Ok(Value::Null),
        ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
        ast::Value::Int(i) => Ok(Value::Int(*i)),
        ast::Value::Float(f) => Ok(Value::Float(*f)),
        ast::Value::String(s) => Ok(Value::String(s.clone())),
        ast::Value::Array(arr) => {
            let converted: Result<Vec<Value>> = arr.iter().map(aql_value_to_db_value).collect();
            Ok(Value::Array(converted?))
        }
        ast::Value::Object(map) => {
            let mut converted = HashMap::new();
            for (k, v) in map {
                converted.insert(k.clone(), aql_value_to_db_value(v)?);
            }
            Ok(Value::Object(converted))
        }
        ast::Value::Variable(name) => Err(AuroraError::Protocol(
            format!("Unresolved variable: {}", name)
        )),
        ast::Value::Enum(e) => Ok(Value::String(e.clone())),
    }
}

/// Convert DB Value to AQL Value
pub fn db_value_to_aql_value(val: &Value) -> ast::Value {
    match val {
        Value::Null => ast::Value::Null,
        Value::Bool(b) => ast::Value::Boolean(*b),
        Value::Int(i) => ast::Value::Int(*i),
        Value::Float(f) => ast::Value::Float(*f),
        Value::String(s) => ast::Value::String(s.clone()),
        Value::Array(arr) => ast::Value::Array(arr.iter().map(db_value_to_aql_value).collect()),
        Value::Object(map) => ast::Value::Object(
            map.iter().map(|(k, v)| (k.clone(), db_value_to_aql_value(v))).collect()
        ),
        Value::Uuid(u) => ast::Value::String(u.to_string()),
    }
}

/// Convert filter from AST Value
pub fn value_to_filter(value: &ast::Value) -> Result<AqlFilter> {
    match value {
        ast::Value::Object(map) => {
            let mut filters = Vec::new();
            for (key, val) in map {
                match key.as_str() {
                    "and" => if let ast::Value::Array(arr) = val {
                        let sub: Result<Vec<_>> = arr.iter().map(value_to_filter).collect();
                        filters.push(AqlFilter::And(sub?));
                    }
                    "or" => if let ast::Value::Array(arr) = val {
                        let sub: Result<Vec<_>> = arr.iter().map(value_to_filter).collect();
                        filters.push(AqlFilter::Or(sub?));
                    }
                    "not" => filters.push(AqlFilter::Not(Box::new(value_to_filter(val)?))),
                    field => if let ast::Value::Object(ops) = val {
                        for (op, op_val) in ops {
                            let f = match op.as_str() {
                                "eq" => AqlFilter::Eq(field.to_string(), op_val.clone()),
                                "ne" => AqlFilter::Ne(field.to_string(), op_val.clone()),
                                "gt" => AqlFilter::Gt(field.to_string(), op_val.clone()),
                                "gte" => AqlFilter::Gte(field.to_string(), op_val.clone()),
                                "lt" => AqlFilter::Lt(field.to_string(), op_val.clone()),
                                "lte" => AqlFilter::Lte(field.to_string(), op_val.clone()),
                                "in" => AqlFilter::In(field.to_string(), op_val.clone()),
                                "nin" => AqlFilter::NotIn(field.to_string(), op_val.clone()),
                                "contains" => AqlFilter::Contains(field.to_string(), op_val.clone()),
                                "startsWith" => AqlFilter::StartsWith(field.to_string(), op_val.clone()),
                                "endsWith" => AqlFilter::EndsWith(field.to_string(), op_val.clone()),
                                "isNull" => AqlFilter::IsNull(field.to_string()),
                                "isNotNull" => AqlFilter::IsNotNull(field.to_string()),
                                _ => continue,
                            };
                            filters.push(f);
                        }
                    }
                }
            }
            if filters.len() == 1 { Ok(filters.remove(0)) } else { Ok(AqlFilter::And(filters)) }
        }
        _ => Err(AuroraError::Protocol("Filter must be an object".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aql_value_conversion() {
        let aql_val = ast::Value::Object({
            let mut map = HashMap::new();
            map.insert("name".to_string(), ast::Value::String("John".to_string()));
            map.insert("age".to_string(), ast::Value::Int(30));
            map
        });

        let db_val = aql_value_to_db_value(&aql_val).unwrap();
        if let Value::Object(map) = db_val {
            assert_eq!(map.get("name"), Some(&Value::String("John".to_string())));
            assert_eq!(map.get("age"), Some(&Value::Int(30)));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_matches_filter_eq() {
        let mut doc = Document::new();
        doc.data.insert("name".to_string(), Value::String("Alice".to_string()));
        doc.data.insert("age".to_string(), Value::Int(25));

        let filter = AqlFilter::Eq("name".to_string(), ast::Value::String("Alice".to_string()));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::Eq("name".to_string(), ast::Value::String("Bob".to_string()));
        assert!(!matches_filter(&doc, &filter, &HashMap::new()));
    }

    #[test]
    fn test_matches_filter_comparison() {
        let mut doc = Document::new();
        doc.data.insert("age".to_string(), Value::Int(25));

        let filter = AqlFilter::Gt("age".to_string(), ast::Value::Int(20));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::Gt("age".to_string(), ast::Value::Int(30));
        assert!(!matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::Gte("age".to_string(), ast::Value::Int(25));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::Lt("age".to_string(), ast::Value::Int(30));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));
    }

    #[test]
    fn test_matches_filter_and_or() {
        let mut doc = Document::new();
        doc.data.insert("name".to_string(), Value::String("Alice".to_string()));
        doc.data.insert("age".to_string(), Value::Int(25));

        let filter = AqlFilter::And(vec![
            AqlFilter::Eq("name".to_string(), ast::Value::String("Alice".to_string())),
            AqlFilter::Gte("age".to_string(), ast::Value::Int(18)),
        ]);
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::Or(vec![
            AqlFilter::Eq("name".to_string(), ast::Value::String("Bob".to_string())),
            AqlFilter::Gte("age".to_string(), ast::Value::Int(18)),
        ]);
        assert!(matches_filter(&doc, &filter, &HashMap::new()));
    }

    #[test]
    fn test_matches_filter_string_ops() {
        let mut doc = Document::new();
        doc.data.insert("email".to_string(), Value::String("alice@example.com".to_string()));

        let filter = AqlFilter::Contains("email".to_string(), ast::Value::String("example".to_string()));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::StartsWith("email".to_string(), ast::Value::String("alice".to_string()));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::EndsWith("email".to_string(), ast::Value::String(".com".to_string()));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));
    }

    #[test]
    fn test_matches_filter_in() {
        let mut doc = Document::new();
        doc.data.insert("status".to_string(), Value::String("active".to_string()));

        let filter = AqlFilter::In("status".to_string(), ast::Value::Array(vec![
            ast::Value::String("active".to_string()),
            ast::Value::String("pending".to_string()),
        ]));
        assert!(matches_filter(&doc, &filter, &HashMap::new()));

        let filter = AqlFilter::In("status".to_string(), ast::Value::Array(vec![
            ast::Value::String("inactive".to_string()),
        ]));
        assert!(!matches_filter(&doc, &filter, &HashMap::new()));
    }

    #[test]
    fn test_apply_projection() {
        let mut doc = Document::new();
        doc.data.insert("id".to_string(), Value::String("123".to_string()));
        doc.data.insert("name".to_string(), Value::String("Alice".to_string()));
        doc.data.insert("email".to_string(), Value::String("alice@example.com".to_string()));
        doc.data.insert("password".to_string(), Value::String("secret".to_string()));

        let fields = vec![
            ast::Field {
                alias: None,
                name: "id".to_string(),
                arguments: vec![],
                directives: vec![],
                selection_set: vec![],
            },
            ast::Field {
                alias: None,
                name: "name".to_string(),
                arguments: vec![],
                directives: vec![],
                selection_set: vec![],
            },
        ];

        let projected = apply_projection(doc, &fields);
        assert_eq!(projected.data.len(), 2);
        assert!(projected.data.contains_key("id"));
        assert!(projected.data.contains_key("name"));
        assert!(!projected.data.contains_key("email"));
        assert!(!projected.data.contains_key("password"));
    }

    #[test]
    fn test_apply_projection_with_alias() {
        let mut doc = Document::new();
        doc.data.insert("first_name".to_string(), Value::String("Alice".to_string()));

        let fields = vec![
            ast::Field {
                alias: Some("name".to_string()),
                name: "first_name".to_string(),
                arguments: vec![],
                directives: vec![],
                selection_set: vec![],
            },
        ];

        let projected = apply_projection(doc, &fields);
        assert!(projected.data.contains_key("name"));
        assert!(!projected.data.contains_key("first_name"));
    }

    #[test]
    fn test_extract_pagination() {
        let args = vec![
            ast::Argument { name: "limit".to_string(), value: ast::Value::Int(10) },
            ast::Argument { name: "offset".to_string(), value: ast::Value::Int(20) },
        ];

        let (limit, offset) = extract_pagination(&args);
        assert_eq!(limit, Some(10));
        assert_eq!(offset, 20);
    }

    #[test]
    fn test_matches_filter_with_variables() {
        let mut doc = Document::new();
        doc.data.insert("age".to_string(), Value::Int(25));

        let mut variables = HashMap::new();
        variables.insert("minAge".to_string(), ast::Value::Int(18));

        let filter = AqlFilter::Gte("age".to_string(), ast::Value::Variable("minAge".to_string()));
        assert!(matches_filter(&doc, &filter, &variables));
    }
}
