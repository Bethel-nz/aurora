use std::collections::HashMap;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FieldType {
    String,
    Int,
    Uuid,
    Boolean,
    Float,
    Array,
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
    pub unique_fields: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub data: HashMap<String, Value>,
}

impl Document {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            data: HashMap::new(),
        }
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
            write!(f, "{}: {}", key, value)?;
            first = false;
        }
        write!(f, " }}")
    }
}

impl fmt::Debug for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use the same formatting as Display
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
            },
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
            },
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
            (Value::Float(a), Value::Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            },
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
            Value::String(s) => write!(f, "{}", s),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter()
                    .map(|v| v.to_string())
                    .collect();
                write!(f, "{}", items.join(", "))
            },
            Value::Object(obj) => {
                let items: Vec<String> = obj.iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect();
                write!(f, "{}", items.join(", "))
            },
            Value::Uuid(u) => write!(f, "{}", u),
            Value::Null => write!(f, ""),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Array(a), Value::Array(b)) => {
                // Compare arrays element by element
                for (x, y) in a.iter().zip(b.iter()) {
                    match x.partial_cmp(y) {
                        Some(Ordering::Equal) => continue,
                        other => return other,
                    }
                }
                // If all elements are equal, compare lengths
                Some(a.len().cmp(&b.len()))
            },
            (Value::Object(a), Value::Object(b)) => {
                // Compare objects by their sorted keys and values
                let mut a_keys: Vec<_> = a.keys().collect();
                let mut b_keys: Vec<_> = b.keys().collect();
                a_keys.sort();
                b_keys.sort();
                
                // First compare keys
                match a_keys.partial_cmp(&b_keys) {
                    Some(Ordering::Equal) => {
                        // If keys are equal, compare values
                        for key in a_keys {
                            match (a.get(key), b.get(key)) {
                                (Some(a_val), Some(b_val)) => {
                                    match a_val.partial_cmp(b_val) {
                                        Some(Ordering::Equal) => continue,
                                        other => return other,
                                    }
                                }
                                _ => unreachable!(), // Keys are identical, so this can't happen
                            }
                        }
                        Some(Ordering::Equal)
                    }
                    other => other,
                }
            },
            (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            // Different types are not comparable
            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

pub type InsertData = Vec<(&'static str, Value)>; 

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
    pub create_dirs: bool,      // Create parent directories if they don't exist
    
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