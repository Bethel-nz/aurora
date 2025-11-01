use crate::error::{AuroraError, Result};
use crate::types::{Document, Value};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use uuid::Uuid;

// --- API Payload Structs ---
// These define the shape of JSON the server expects.

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FilterOperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Contains,
}

#[derive(Deserialize)]
pub struct Filter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: JsonValue,
}

#[derive(Deserialize)]
pub struct SortOptions {
    pub field: String,
    pub ascending: bool,
}

#[derive(Deserialize)]
pub struct QueryPayload {
    pub filters: Option<Vec<Filter>>,
    pub sort: Option<SortOptions>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub select: Option<Vec<String>>,
}

// --- Data Conversion Functions ---

/// Converts an internal `Document` into a clean, client-friendly `serde_json::Value`.
pub fn document_to_json(doc: &Document) -> JsonValue {
    let mut map = serde_json::Map::new();
    // Always include the ID in the output
    map.insert("id".to_string(), json!(doc.id));

    for (key, value) in &doc.data {
        map.insert(key.clone(), value_to_json(value));
    }
    JsonValue::Object(map)
}

/// Recursively converts an internal `Value` into a `serde_json::Value`.
fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::String(s) => json!(s),
        Value::Int(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::Bool(b) => json!(b),
        Value::Uuid(u) => json!(u.to_string()),
        Value::Array(arr) => JsonValue::Array(arr.iter().map(value_to_json).collect()),
        Value::Object(obj) => {
            let map = obj
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
    }
}

/// Converts an incoming `serde_json::Value` (from a request body) into the `HashMap<String, Value>`
/// that our `insert_map` function expects.
pub fn json_to_insert_data(json: JsonValue) -> Result<HashMap<String, Value>> {
    let map = json.as_object().ok_or_else(|| {
        AuroraError::InvalidOperation("Request body must be a JSON object".into())
    })?;

    let mut result = HashMap::new();
    for (key, value) in map {
        if key.to_lowercase() == "id" {
            continue; // Ignore client-provided IDs on insert
        }
        result.insert(key.clone(), json_to_value(value)?);
    }
    Ok(result)
}

/// Recursively converts a `serde_json::Value` into an internal `Value`.
pub fn json_to_value(json_value: &JsonValue) -> Result<Value> {
    match json_value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(b) => Ok(Value::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(AuroraError::InvalidOperation("Invalid number value".into()))
            }
        }
        JsonValue::String(s) => {
            if let Ok(uuid) = Uuid::parse_str(s) {
                Ok(Value::Uuid(uuid))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        JsonValue::Array(arr) => {
            let mut values = Vec::new();
            for item in arr {
                values.push(json_to_value(item)?);
            }
            Ok(Value::Array(values))
        }
        JsonValue::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_value(v)?);
            }
            Ok(Value::Object(map))
        }
    }
}
