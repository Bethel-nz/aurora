// Computed Fields - Auto-calculated field values
//
// Supports dynamic field calculation based on other field values
// Examples: full_name from first_name + last_name, age from birthdate, etc.
//
// IMPORTANT: Computed fields are evaluated at RETRIEVAL TIME ONLY.
// They do NOT affect mutations or pub/sub events.

use crate::error::Result;
use crate::types::{Document, Value};
use rhai::{Dynamic, Engine, Scope};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Computation expression for a field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComputedExpression {
    /// Concatenate string fields with space separator
    Concat(Vec<String>),
    /// Sum numeric fields
    Sum(Vec<String>),
    /// Multiply numeric fields
    Product(Vec<String>),
    /// Average numeric fields
    Average(Vec<String>),
    /// Template string with ${field} interpolation
    /// Example: "${first_name} ${last_name}"
    Template(String),
    /// Rhai script expression
    /// Example: "doc.price * doc.quantity"
    Script(String),
    /// Legacy custom expression (deprecated, use Script instead)
    #[serde(rename = "custom")]
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

            ComputedExpression::Template(template) => {
                Some(Value::String(interpolate_template(template, doc)))
            }

            ComputedExpression::Script(script) | ComputedExpression::Custom(script) => {
                evaluate_rhai_script(script, doc)
            }
        }
    }
}

/// Interpolate template string with document field values
/// Replaces ${field_name} with the field's string value
fn interpolate_template(template: &str, doc: &Document) -> String {
    let mut result = template.to_string();
    
    // Find all ${...} patterns and replace them
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let end = start + end;
            let field_name = &result[start + 2..end];
            
            let replacement = doc.data.get(field_name)
                .and_then(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    Value::Int(i) => Some(i.to_string()),
                    Value::Float(f) => Some(f.to_string()),
                    Value::Bool(b) => Some(b.to_string()),
                    _ => None,
                })
                .unwrap_or_default();
            
            result = format!("{}{}{}", &result[..start], replacement, &result[end + 1..]);
        } else {
            break;
        }
    }
    
    result
}

/// Convert Aurora Value to Rhai Dynamic
fn value_to_dynamic(value: &Value) -> Dynamic {
    match value {
        Value::Null => Dynamic::UNIT,
        Value::Bool(b) => Dynamic::from(*b),
        Value::Int(i) => Dynamic::from(*i),
        Value::Float(f) => Dynamic::from(*f),
        Value::String(s) => Dynamic::from(s.clone()),
        Value::Uuid(u) => Dynamic::from(u.to_string()),
        Value::Array(arr) => {
            let vec: Vec<Dynamic> = arr.iter().map(value_to_dynamic).collect();
            Dynamic::from(vec)
        }
        Value::Object(map) => {
            let mut rhai_map = rhai::Map::new();
            for (k, v) in map {
                rhai_map.insert(k.clone().into(), value_to_dynamic(v));
            }
            Dynamic::from(rhai_map)
        }
    }
}

/// Convert Rhai Dynamic to Aurora Value
fn dynamic_to_value(dyn_val: Dynamic) -> Option<Value> {
    if dyn_val.is_unit() {
        return Some(Value::Null);
    }
    if let Some(b) = dyn_val.clone().try_cast::<bool>() {
        return Some(Value::Bool(b));
    }
    if let Some(i) = dyn_val.clone().try_cast::<i64>() {
        return Some(Value::Int(i));
    }
    if let Some(f) = dyn_val.clone().try_cast::<f64>() {
        return Some(Value::Float(f));
    }
    if let Some(s) = dyn_val.clone().try_cast::<String>() {
        return Some(Value::String(s));
    }
    if let Some(arr) = dyn_val.clone().try_cast::<Vec<Dynamic>>() {
        let converted: Vec<Value> = arr.into_iter()
            .filter_map(dynamic_to_value)
            .collect();
        return Some(Value::Array(converted));
    }
    if let Some(map) = dyn_val.try_cast::<rhai::Map>() {
        let mut obj = HashMap::new();
        for (k, v) in map {
            if let Some(val) = dynamic_to_value(v) {
                obj.insert(k.to_string(), val);
            }
        }
        return Some(Value::Object(obj));
    }
    None
}

/// Evaluate a Rhai script with document fields available as `doc`
fn evaluate_rhai_script(script: &str, doc: &Document) -> Option<Value> {
    let engine = Engine::new();
    let mut scope = Scope::new();
    
    // Create a map for the document fields
    let mut doc_map = rhai::Map::new();
    for (key, value) in &doc.data {
        doc_map.insert(key.clone().into(), value_to_dynamic(value));
    }
    
    // Add doc to scope
    scope.push("doc", doc_map);
    
    // Evaluate the script
    match engine.eval_with_scope::<Dynamic>(&mut scope, script) {
        Ok(result) => dynamic_to_value(result),
        Err(_) => None, // Graceful degradation on script errors
    }
}

/// Rhai-powered computed field engine with caching
pub struct ComputedEngine {
    engine: Arc<Engine>,
}

impl ComputedEngine {
    /// Create a new computed engine with built-in functions
    pub fn new() -> Self {
        let mut engine = Engine::new();
        
        // Register built-in string functions
        engine.register_fn("uppercase", |s: &str| s.to_uppercase());
        engine.register_fn("lowercase", |s: &str| s.to_lowercase());
        engine.register_fn("trim", |s: &str| s.trim().to_string());
        engine.register_fn("len", |s: &str| s.len() as i64);
        
        // Register math functions
        engine.register_fn("abs", |x: i64| x.abs());
        engine.register_fn("abs", |x: f64| x.abs());
        engine.register_fn("round", |x: f64| x.round());
        engine.register_fn("floor", |x: f64| x.floor());
        engine.register_fn("ceil", |x: f64| x.ceil());
        engine.register_fn("min", |a: i64, b: i64| a.min(b));
        engine.register_fn("max", |a: i64, b: i64| a.max(b));
        
        Self {
            engine: Arc::new(engine),
        }
    }
    
    /// Evaluate a Rhai script with document context
    pub fn evaluate(&self, script: &str, doc: &Document) -> Option<Value> {
        let mut scope = Scope::new();
        
        // Create a map for the document fields
        let mut doc_map = rhai::Map::new();
        for (key, value) in &doc.data {
            doc_map.insert(key.clone().into(), value_to_dynamic(value));
        }
        
        scope.push("doc", doc_map);
        
        match self.engine.eval_with_scope::<Dynamic>(&mut scope, script) {
            Ok(result) => dynamic_to_value(result),
            Err(_) => None,
        }
    }
}

impl Default for ComputedEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Computed field registry
pub struct ComputedFields {
    // collection_name -> (field_name -> expression)
    fields: HashMap<String, HashMap<String, ComputedExpression>>,
    engine: ComputedEngine,
}

impl ComputedFields {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            engine: ComputedEngine::new(),
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

    /// Apply computed fields to a document (retrieval time only)
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
    
    /// Evaluate a script expression with document context
    pub fn evaluate_script(&self, script: &str, doc: &Document) -> Option<Value> {
        self.engine.evaluate(script, doc)
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

    #[test]
    fn test_template_expression() {
        let expr = ComputedExpression::Template("Hello, ${name}! You are ${age} years old.".to_string());

        let mut doc = Document::new();
        doc.data.insert("name".to_string(), Value::String("Alice".to_string()));
        doc.data.insert("age".to_string(), Value::Int(30));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::String("Hello, Alice! You are 30 years old.".to_string())));
    }

    #[test]
    fn test_rhai_simple_expression() {
        let expr = ComputedExpression::Script("doc.price * doc.quantity".to_string());

        let mut doc = Document::new();
        doc.data.insert("price".to_string(), Value::Int(100));
        doc.data.insert("quantity".to_string(), Value::Int(5));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::Int(500)));
    }

    #[test]
    fn test_rhai_string_concat() {
        let expr = ComputedExpression::Script(r#"doc.first + " " + doc.last"#.to_string());

        let mut doc = Document::new();
        doc.data.insert("first".to_string(), Value::String("John".to_string()));
        doc.data.insert("last".to_string(), Value::String("Doe".to_string()));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::String("John Doe".to_string())));
    }

    #[test]
    fn test_rhai_conditional() {
        let expr = ComputedExpression::Script(
            r#"if doc.age >= 18 { "adult" } else { "minor" }"#.to_string()
        );

        let mut doc = Document::new();
        doc.data.insert("age".to_string(), Value::Int(25));

        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::String("adult".to_string())));

        doc.data.insert("age".to_string(), Value::Int(15));
        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::String("minor".to_string())));
    }

    #[test]
    fn test_rhai_null_handling() {
        let expr = ComputedExpression::Script("doc.missing_field".to_string());

        let doc = Document::new();
        
        // Script accessing missing field returns Null (Rhai returns unit for missing keys)
        let result = expr.evaluate(&doc);
        assert_eq!(result, Some(Value::Null));
    }

    #[test]
    fn test_computed_engine_builtin_functions() {
        let engine = ComputedEngine::new();
        
        let mut doc = Document::new();
        doc.data.insert("name".to_string(), Value::String("hello world".to_string()));
        doc.data.insert("value".to_string(), Value::Float(3.7));

        // Test uppercase
        let result = engine.evaluate(r#"uppercase(doc.name)"#, &doc);
        assert_eq!(result, Some(Value::String("HELLO WORLD".to_string())));

        // Test round
        let result = engine.evaluate("round(doc.value)", &doc);
        assert_eq!(result, Some(Value::Float(4.0)));
    }
}
