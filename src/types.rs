use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FieldType {
    String,
    Int,
    Uuid,
    Boolean,
    Float,
    Array,
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldDefinition {
    pub field_type: FieldType,
    pub unique: bool,
    pub indexed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    pub name: String,
    pub fields: HashMap<String, FieldDefinition>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub data: HashMap<String, Value>,
}

impl Default for Document {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            data: HashMap::new(),
        }
    }
}

impl Document {
    pub fn new() -> Self {
        Self::default()
    }
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ ")?;
        let mut first = true;
        for (key, value) in &self.data {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"{}\": {}", key, value)?;
            first = false;
        }
        write!(f, " }}")
    }
}

impl fmt::Debug for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Uuid(Uuid),
}

// Custom implementations for Hash, Eq, and PartialEq
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0.hash(state),
            Value::String(s) => s.hash(state),
            Value::Int(i) => i.hash(state),
            Value::Float(f) => {
                // Convert to bits to hash floating point numbers
                f.to_bits().hash(state)
            }
            Value::Bool(b) => b.hash(state),
            Value::Array(arr) => arr.hash(state),
            Value::Object(map) => {
                // Sort keys for consistent hashing
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for key in keys {
                    key.hash(state);
                    map.get(key).unwrap().hash(state);
                }
            }
            Value::Uuid(u) => u.hash(state),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

// Implement Display for Value
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                write!(f, "[{}]", items.join(", "))
            }
            Value::Object(obj) => {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, v))
                    .collect();
                write!(f, "{{{}}}", items.join(", "))
            }
            Value::Uuid(u) => write!(f, "\"{}\"", u),
            Value::Null => write!(f, "null"),
        }
    }
}

// Helper for deterministic ordering of different types
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Float(_) => 3,
        Value::String(_) => 4,
        Value::Uuid(_) => 5,
        Value::Array(_) => 6,
        Value::Object(_) => 7,
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_rank = type_rank(self);
        let other_rank = type_rank(other);

        if self_rank != other_rank {
            return Some(self_rank.cmp(&other_rank));
        }

        match (self, other) {
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Array(a), Value::Array(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
            (Value::Object(_), Value::Object(_)) => Some(Ordering::Equal),
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

// Add From implementations for common types
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
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

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(v: HashMap<String, Value>) -> Self {
        Value::Object(v)
    }
}

impl From<Uuid> for Value {
    fn from(v: Uuid) -> Self {
        Value::Uuid(v)
    }
}

/// Configuration for Aurora database
#[derive(Debug, Clone)]
pub struct AuroraConfig {
    // Database location settings
    pub db_path: PathBuf,
    pub create_dirs: bool, // Create parent directories if they don't exist

    // Hot store config
    pub hot_cache_size_mb: usize,
    pub hot_cache_cleanup_interval_secs: u64,

    // Cold store config
    pub cold_cache_capacity_mb: usize,
    pub cold_flush_interval_ms: Option<u64>,
    pub cold_mode: ColdStoreMode,

    // General config
    pub auto_compact: bool,
    pub compact_interval_mins: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum ColdStoreMode {
    HighThroughput,
    LowSpace,
}

impl Default for AuroraConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("aurora.db"),
            create_dirs: true,

            hot_cache_size_mb: 128,
            hot_cache_cleanup_interval_secs: 30,

            cold_cache_capacity_mb: 64,
            cold_flush_interval_ms: Some(100),
            cold_mode: ColdStoreMode::HighThroughput,

            auto_compact: true,
            compact_interval_mins: 60,
        }
    }
}

impl AuroraConfig {
    /// Create a new configuration with a specific database path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        let mut config = Self::default();
        config.db_path = path.as_ref().to_path_buf();
        config
    }
}
