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

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub enum ScalarType {
    String,
    Int,
    Uuid,
    Bool,
    Float,
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
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap_or_default())
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
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap_or_default())
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct FieldDefinition {
    pub field_type: FieldType,
    pub unique: bool,
    pub indexed: bool,
    pub nullable: bool,
    /// Validation constraints applied on insert/update.
    #[serde(default)]
    pub validations: Vec<FieldValidationConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    pub name: String,
    pub fields: HashMap<String, FieldDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    #[serde(rename = "_sid")]
    pub _sid: String,
    pub data: HashMap<String, Value>,
}

impl Document {
    pub fn new() -> Self {
        Self {
            _sid: Uuid::now_v7().to_string(),
            data: HashMap::new(),
        }
    }
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Uuid(Uuid),
    DateTime(DateTime<Utc>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
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
            (Value::Int(_), _) => Ordering::Less,
            (_, Value::Int(_)) => Ordering::Greater,

            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Float(_), _) => Ordering::Less,
            (_, Value::Float(_)) => Ordering::Greater,

            (Value::String(a), Value::String(b)) => a.cmp(b),
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

            (Value::Object(_), Value::Object(_)) => Ordering::Equal, // Simplified for brevity
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
            Value::String(s) => write!(f, "{}", s),
            Value::Uuid(u) => write!(f, "{}", u),
            Value::DateTime(dt) => write!(f, "{}", dt),
            Value::Array(arr) => write!(f, "{}", serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string())),
            Value::Object(obj) => write!(f, "{}", serde_json::to_string(obj).unwrap_or_else(|_| "{}".to_string())),
        }
    }
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self { Some(s) } else { None }
    }
    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Int(i) = self { Some(*i) } else { None }
    }
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float(f) = self { Some(*f) } else { None }
    }
    pub fn as_bool(&self) -> Option<bool> {
        if let Value::Bool(b) = self { Some(*b) } else { None }
    }
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        if let Value::Array(a) = self { Some(a) } else { None }
    }
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        if let Value::Object(o) = self { Some(o) } else { None }
    }
}

impl From<String> for Value { fn from(v: String) -> Self { Value::String(v) } }
impl From<&str> for Value { fn from(v: &str) -> Self { Value::String(v.to_string()) } }
impl From<bool> for Value { fn from(v: bool) -> Self { Value::Bool(v) } }
impl From<f64> for Value { fn from(v: f64) -> Self { Value::Float(v) } }
impl From<i64> for Value { fn from(v: i64) -> Self { Value::Int(v) } }

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
