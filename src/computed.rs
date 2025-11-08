// Computed Fields - Auto-calculated field values
//
// Supports dynamic field calculation based on other field values
// Examples: full_name from first_name + last_name, age from birthdate, etc.

use crate::error::Result;
use crate::types::{Document, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Computation expression for a field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComputedExpression {
    /// Concatenate string fields
    Concat(Vec<String>),
    /// Sum numeric fields
    Sum(Vec<String>),
    /// Multiply numeric fields
    Product(Vec<String>),
    /// Average numeric fields
    Average(Vec<String>),
    /// JavaScript-like custom expression (field name -> value extraction)
    Custom(String),
}

impl ComputedExpression {
    /// Evaluate the expression against a document
    pub fn evaluate(&self, doc: &Document) -> Option<Value> {
        match self {
            ComputedExpression::Concat(fields) => {
                let mut result = String::new();
                for field in fields {
                    if let Some(value) = doc.data.get(field)
                        && let Some(s) = value.as_str() {
                            if !result.is_empty() {
                                result.push(' ');
                            }
                            result.push_str(s);
                        }
                }
                Some(Value::String(result))
            }

            ComputedExpression::Sum(fields) => {
                let mut sum = 0i64;
                for field in fields {
                    if let Some(value) = doc.data.get(field)
                        && let Some(i) = value.as_i64() {
                            sum += i;
                        }
                }
                Some(Value::Int(sum))
            }

            ComputedExpression::Product(fields) => {
                let mut product = 1i64;
                for field in fields {
                    if let Some(value) = doc.data.get(field)
                        && let Some(i) = value.as_i64() {
                            product *= i;
                        }
                }
                Some(Value::Int(product))
            }

            ComputedExpression::Average(fields) => {
                let mut sum = 0.0;
                let mut count = 0;
                for field in fields {
                    if let Some(value) = doc.data.get(field) {
                        if let Some(f) = value.as_f64() {
                            sum += f;
                            count += 1;
                        } else if let Some(i) = value.as_i64() {
                            sum += i as f64;
                            count += 1;
                        }
                    }
                }
                if count > 0 {
                    Some(Value::Float(sum / count as f64))
                } else {
                    None
                }
            }

            ComputedExpression::Custom(_expr) => {
                // For now, custom expressions are not implemented
                // Could use a JS runtime like deno_core or rquickjs
                None
            }
        }
    }
}

/// Computed field registry
pub struct ComputedFields {
    // collection_name -> (field_name -> expression)
    fields: HashMap<String, HashMap<String, ComputedExpression>>,
}

impl ComputedFields {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Register a computed field
    pub fn register(
        &mut self,
        collection: impl Into<String>,
        field: impl Into<String>,
        expression: ComputedExpression,
    ) {
        let collection = collection.into();
        self.fields
            .entry(collection)
            .or_default()
            .insert(field.into(), expression);
    }

    /// Apply computed fields to a document
    pub fn apply(&self, collection: &str, doc: &mut Document) -> Result<()> {
        if let Some(computed) = self.fields.get(collection) {
            for (field_name, expression) in computed {
                if let Some(value) = expression.evaluate(doc) {
                    doc.data.insert(field_name.clone(), value);
                }
            }
        }
        Ok(())
    }

    /// Get computed fields for a collection
    pub fn get_fields(&self, collection: &str) -> Option<&HashMap<String, ComputedExpression>> {
        self.fields.get(collection)
    }
}

impl Default for ComputedFields {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat_expression() {
        let expr =
            ComputedExpression::Concat(vec!["first_name".to_string(), "last_name".to_string()]);

        let mut doc = Document::new();
        doc.data
            .insert("first_name".to_string(), Value::String("John".to_string()));
        doc.data
            .insert("last_name".to_string(), Value::String("Doe".to_string()));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::String("John Doe".to_string())));
    }

    #[test]
    fn test_sum_expression() {
        let expr = ComputedExpression::Sum(vec!["a".to_string(), "b".to_string(), "c".to_string()]);

        let mut doc = Document::new();
        doc.data.insert("a".to_string(), Value::Int(10));
        doc.data.insert("b".to_string(), Value::Int(20));
        doc.data.insert("c".to_string(), Value::Int(30));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::Int(60)));
    }

    #[test]
    fn test_average_expression() {
        let expr = ComputedExpression::Average(vec!["score1".to_string(), "score2".to_string()]);

        let mut doc = Document::new();
        doc.data.insert("score1".to_string(), Value::Float(85.5));
        doc.data.insert("score2".to_string(), Value::Float(92.5));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::Float(89.0)));
    }

    #[test]
    fn test_computed_fields_registry() {
        let mut registry = ComputedFields::new();

        registry.register(
            "users",
            "full_name",
            ComputedExpression::Concat(vec!["first_name".to_string(), "last_name".to_string()]),
        );

        let mut doc = Document::new();
        doc.data
            .insert("first_name".to_string(), Value::String("Jane".to_string()));
        doc.data
            .insert("last_name".to_string(), Value::String("Smith".to_string()));

        registry.apply("users", &mut doc).unwrap();

        assert_eq!(
            doc.data.get("full_name"),
            Some(&Value::String("Jane Smith".to_string()))
        );
    }
}
