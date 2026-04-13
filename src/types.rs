//! # Aurora Data Types
//!
//! This module defines the core data structures used throughout Aurora DB, 
//! including the fundamental `Value` enum, document schema definitions, 
//! and configuration options.
//!
//! ## Key Types
//! - **Value**: The universal data type for all document fields (String, Int, UUID, etc.).
//! - **Document**: A collection-native document containing data and a system ID.
//! - **FieldType / FieldDefinition**: Structures for defining and validating collection schemas.
//! - **AuroraConfig**: The primary configuration struct for database initialization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use uuid::Uuid;

/// Validation constraint stored on a field definition.
/// Applied during insert/update to enforce data integrity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValidationConstraint {
    Format(String),
    Min(f64),
    Max(f64),
    MinLength(i64),
    MaxLength(i64),
    Pattern(String),
}

impl PartialEq for FieldValidationConstraint {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Format(a), Self::Format(b)) | (Self::Pattern(a), Self::Pattern(b)) => a == b,
            (Self::Min(a), Self::Min(b)) | (Self::Max(a), Self::Max(b)) => {
                a.to_bits() == b.to_bits()
            }
            (Self::MinLength(a), Self::MinLength(b)) | (Self::MaxLength(a), Self::MaxLength(b)) => {
                a == b
            }
            _ => false,
        }
    }
}

impl Eq for FieldValidationConstraint {}

impl Hash for FieldValidationConstraint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Format(s) | Self::Pattern(s) => s.hash(state),
            Self::Min(f) | Self::Max(f) => f.to_bits().hash(state),
            Self::MinLength(i) | Self::MaxLength(i) => i.hash(state),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarType {
    String,
    Int,
    Uuid,
    Bool,
    Float,
    DateTime,
    Object,
    Array,
    Any,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    Scalar(ScalarType),
    Object,
    Array(ScalarType),
    Nested(Box<HashMap<String, FieldDefinition>>),
    Any,
}

impl Hash for FieldType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            FieldType::Scalar(s) => s.hash(state),
            FieldType::Array(s) => s.hash(state),
            FieldType::Nested(m) => {
                let mut keys: Vec<_> = m.keys().collect();
                keys.sort();
                for k in keys {
                    k.hash(state);
                    m.get(k).unwrap().hash(state);
                }
            }
            _ => {}
        }
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarType::String => write!(f, "String"),
            ScalarType::Int => write!(f, "Int"),
            ScalarType::Uuid => write!(f, "Uuid"),
            ScalarType::Bool => write!(f, "Bool"),
            ScalarType::Float => write!(f, "Float"),
            ScalarType::DateTime => write!(f, "DateTime"),
            ScalarType::Object => write!(f, "Object"),
            ScalarType::Array => write!(f, "Array"),
            ScalarType::Any => write!(f, "Any"),
        }
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::Scalar(s) => write!(f, "{}", s),
            FieldType::Object => write!(f, "Object"),
            FieldType::Array(s) => write!(f, "Array<{}>", s),
            FieldType::Nested(_) => write!(f, "Nested"),
            FieldType::Any => write!(f, "Any"),
        }
    }
}

impl fmt::Display for FieldDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{} (indexed: {}, unique: {})",
            self.field_type,
            if self.nullable { "?" } else { "!" },
            self.indexed,
            self.unique,
        )
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string_pretty(self).unwrap_or_default()
        )
    }
}

impl fmt::Display for DurabilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DurabilityMode::None => write!(f, "None"),
            DurabilityMode::WAL => write!(f, "WAL"),
            DurabilityMode::Strict => write!(f, "Strict"),
            DurabilityMode::Synchronous => write!(f, "Synchronous"),
        }
    }
}

impl fmt::Display for ColdStoreMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColdStoreMode::HighThroughput => write!(f, "HighThroughput"),
            ColdStoreMode::LowSpace => write!(f, "LowSpace"),
        }
    }
}

impl fmt::Display for AuroraConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string_pretty(self).unwrap_or_default()
        )
    }
}

impl Default for FieldType {
    fn default() -> Self {
        FieldType::Any
    }
}

impl FieldType {
    pub const SCALAR_STRING: FieldType = FieldType::Scalar(ScalarType::String);
    pub const SCALAR_INT: FieldType = FieldType::Scalar(ScalarType::Int);
    pub const SCALAR_BOOL: FieldType = FieldType::Scalar(ScalarType::Bool);
    pub const SCALAR_FLOAT: FieldType = FieldType::Scalar(ScalarType::Float);
    pub const SCALAR_UUID: FieldType = FieldType::Scalar(ScalarType::Uuid);
    pub const SCALAR_OBJECT: FieldType = FieldType::Scalar(ScalarType::Object);
    pub const SCALAR_ARRAY: FieldType = FieldType::Scalar(ScalarType::Array);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Relation {
    pub to: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct FieldDefinition {
    pub field_type: FieldType,
    pub unique: bool,
    pub indexed: bool,
    pub nullable: bool,
    /// Validation constraints applied on insert/update.
    #[serde(default)]
    pub validations: Vec<FieldValidationConstraint>,
    /// Relationship metadata for foreign keys.
    pub relation: Option<Relation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    pub name: String,
    pub fields: HashMap<String, FieldDefinition>,
}

/// Represents a single document in an Aurora collection.
///
/// A document consists of a unique system ID (`_sid`) and a set of key-value pairs (`data`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// The unique internal system ID for this document.
    #[serde(rename = "_sid", alias = "id")]
    pub _sid: String,
    /// The document's fields and values.
    pub data: HashMap<String, Value>,
}

impl Document {
    /// Creates a new document with a unique ID and empty data.
    pub fn new() -> Self {
        Self {
            _sid: Uuid::now_v7().to_string(),
            data: HashMap::new(),
        }
    }

    /// Returns the system ID of the document.
    pub fn id(&self) -> &str {
        &self._sid
    }

    /// Gets a value from the document's data by field name.
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }

    /// Maps the document data into a user-defined struct that implements `Deserialize`.
    ///
    /// This automatically aliases the internal `_sid` to `id` if not present in the data,
    /// and also provides `_sid` explicitly.
    pub fn bind<T: serde::de::DeserializeOwned>(mut self) -> crate::error::Result<T> {
        self.data
            .insert("_sid".to_string(), Value::String(self._sid.clone()));

        if !self.data.contains_key("id") {
            self.data
                .insert("id".to_string(), Value::String(self._sid.clone()));
        }

        let json = serde_json::to_value(&self.data)?;
        Ok(serde_json::from_value(json)?)
    }
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{\n  \"_sid\": \"{}\",\n  \"data\": {{\n", self._sid)?;
        let mut first = true;
        for (k, v) in &self.data {
            if !first {
                write!(f, ",\n")?;
            }
            write!(f, "    \"{}\": {}", k, v)?;
            first = false;
        }
        write!(f, "\n  }}\n}}")
    }
}

/// Represents any value that can be stored in an Aurora document.
///
/// This enum covers all supported scalar and collection types in AQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// A null value.
    Null,
    /// A boolean value.
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A 64-bit floating point number.
    Float(f64),
    /// A UTF-8 string.
    String(String),
    /// A Universally Unique Identifier (UUID).
    Uuid(Uuid),
    /// A UTC date and time.
    DateTime(DateTime<Utc>),
    /// An ordered list of values.
    Array(Vec<Value>),
    /// A map of string keys to values.
    Object(HashMap<String, Value>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Int(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Int(b)) => *a == *b as f64,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Uuid(u), Value::String(s)) | (Value::String(s), Value::Uuid(u)) => {
                u.to_string() == *s
            }
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Bool(_), _) => Ordering::Less,
            (_, Value::Bool(_)) => Ordering::Greater,

            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.total_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Int(_) | Value::Float(_), _) => Ordering::Less,
            (_, Value::Int(_) | Value::Float(_)) => Ordering::Greater,

            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Uuid(u), Value::String(s)) => u.to_string().cmp(s),
            (Value::String(s), Value::Uuid(u)) => s.cmp(&u.to_string()),
            (Value::String(_), _) => Ordering::Less,
            (_, Value::String(_)) => Ordering::Greater,

            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Uuid(_), _) => Ordering::Less,
            (_, Value::Uuid(_)) => Ordering::Greater,

            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::DateTime(_), _) => Ordering::Less,
            (_, Value::DateTime(_)) => Ordering::Greater,

            (Value::Array(a), Value::Array(b)) => a.cmp(b),
            (Value::Array(_), _) => Ordering::Less,
            (_, Value::Array(_)) => Ordering::Greater,

            (Value::Object(_), Value::Object(_)) => Ordering::Equal, // Object ordering not strictly defined
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Uuid(u) => write!(f, "\"{}\"", u),
            Value::DateTime(dt) => write!(f, "\"{}\"", dt),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, val) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            Value::Object(obj) => {
                write!(f, "{{")?;
                let mut first = true;
                for (k, v) in obj {
                    if !first {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{}\": {}", k, v)?;
                    first = false;
                }
                write!(f, "}}")
            }
        }
    }
}

impl Value {
    /// Returns the value as a string slice, if it is a `Value::String`.
    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Returns the value as an `i64`, if it is a `Value::Int`.
    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    /// Returns the value as an `f64`, if it is a `Value::Float`.
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float(f) = self {
            Some(*f)
        } else {
            None
        }
    }

    /// Returns the value as a `bool`, if it is a `Value::Bool`.
    pub fn as_bool(&self) -> Option<bool> {
        if let Value::Bool(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    /// Returns the value as a reference to a `Vec<Value>`, if it is a `Value::Array`.
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        if let Value::Array(a) = self {
            Some(a)
        } else {
            None
        }
    }

    /// Returns the value as a reference to a `HashMap`, if it is a `Value::Object`.
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        if let Value::Object(o) = self {
            Some(o)
        } else {
            None
        }
    }

    /// Coerces the value to a target field type if possible.
    ///
    /// This is used for type normalization during ingestion.
    pub fn coerce_to(&self, target: &FieldType) -> Value {
        match target {
            FieldType::Scalar(ScalarType::String) => match self {
                Value::String(s) => Value::String(s.clone()),
                Value::Null => Value::Null,
                _ => Value::String(self.to_string()),
            },
            FieldType::Scalar(ScalarType::Int) => match self {
                Value::Int(i) => Value::Int(*i),
                Value::Float(f) => Value::Int(*f as i64),
                Value::String(s) => s.parse().map(Value::Int).unwrap_or(Value::Null),
                _ => Value::Null,
            },
            FieldType::Scalar(ScalarType::Float) => match self {
                Value::Float(f) => Value::Float(*f),
                Value::Int(i) => Value::Float(*i as f64),
                Value::String(s) => s.parse().map(Value::Float).unwrap_or(Value::Null),
                _ => Value::Null,
            },
            FieldType::Scalar(ScalarType::Uuid) => match self {
                Value::Uuid(u) => Value::Uuid(*u),
                Value::String(s) => Uuid::parse_str(s).map(Value::Uuid).unwrap_or(Value::Null),
                _ => Value::Null,
            },
            FieldType::Scalar(ScalarType::Bool) => match self {
                Value::Bool(b) => Value::Bool(*b),
                Value::String(s) => Value::Bool(s == "true"),
                _ => Value::Null,
            },
            FieldType::Scalar(ScalarType::DateTime) => match self {
                Value::DateTime(dt) => Value::DateTime(*dt),
                Value::String(s) => s
                    .parse::<DateTime<Utc>>()
                    .map(Value::DateTime)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            },
            _ => self.clone(),
        }
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DurabilityMode {
    None,
    WAL,
    Strict,
    /// Synchronous mode: every write blocks until it reaches durable storage.
    /// No buffering, no WAL — safest but slowest.
    Synchronous,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColdStoreMode {
    HighThroughput,
    LowSpace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuroraConfig {
    pub db_path: PathBuf,
    pub create_dirs: bool,
    pub hot_cache_size_mb: usize,
    pub hot_cache_cleanup_interval_secs: u64,
    pub eviction_policy: crate::storage::EvictionPolicy,
    pub cold_cache_capacity_mb: usize,
    pub cold_flush_interval_ms: Option<u64>,
    pub cold_mode: ColdStoreMode,
    pub auto_compact: bool,
    pub compact_interval_mins: u64,
    pub max_index_entries_per_field: usize,
    pub enable_write_buffering: bool,
    pub write_buffer_size: usize,
    pub write_buffer_flush_interval_ms: u64,
    pub durability_mode: DurabilityMode,
    pub enable_wal: bool,
    pub checkpoint_interval_ms: u64,
    pub audit_log_path: Option<String>,
    pub workers_enabled: bool,
    pub worker_threads: usize,
}

impl Default for AuroraConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("aurora_db"),
            create_dirs: true,
            hot_cache_size_mb: 256,
            hot_cache_cleanup_interval_secs: 60,
            eviction_policy: crate::storage::EvictionPolicy::LRU,
            cold_cache_capacity_mb: 1024,
            cold_flush_interval_ms: Some(5000),
            cold_mode: ColdStoreMode::HighThroughput,
            auto_compact: true,
            compact_interval_mins: 60,
            max_index_entries_per_field: 100_000,
            enable_write_buffering: true,
            write_buffer_size: 10_000,
            write_buffer_flush_interval_ms: 1000,
            durability_mode: DurabilityMode::WAL,
            enable_wal: true,
            checkpoint_interval_ms: 10000,
            audit_log_path: None,
            workers_enabled: false,
            worker_threads: 4,
        }
    }
}
