use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FieldType {
    String,
    Int,
    Uuid,
    Bool,
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

// Helper methods for Value type conversion and extraction
impl Value {
    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        if let Value::Bool(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float(f) = self {
            Some(*f)
        } else {
            None
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        if let Value::Array(arr) = self {
            Some(arr)
        } else {
            None
        }
    }

    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        if let Value::Object(obj) = self {
            Some(obj)
        } else {
            None
        }
    }

    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            Value::Uuid(u) => Some(*u),
            Value::String(s) => Uuid::parse_str(s).ok(),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<DateTime<Utc>> {
        match self {
            Value::String(s) => DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc)),
            _ => None,
        }
    }

    /// Generate a string from known value types.
    pub fn to_safe_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            Value::Int(i) => Some(i.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Uuid(u) => Some(u.to_string()),
            _ => None,
        }
    }

    /// Try conversion to i32
    pub fn as_i32(&self) -> Option<i32> {
        self.as_i64().and_then(|i| i.try_into().ok())
    }
}

//
// General extractor helpers for repeated access patterns
//

pub fn required_str<'a>(
    map: &'a HashMap<String, Value>,
    key: &str,
) -> Result<&'a str, Box<dyn Error>> {
    map.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("Missing or invalid '{}' (str)", key).into())
}

pub fn optional_str(map: &HashMap<String, Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

pub fn required_uuid(map: &HashMap<String, Value>, key: &str) -> Result<Uuid, Box<dyn Error>> {
    map.get(key)
        .and_then(|v| v.as_uuid())
        .ok_or_else(|| format!("Missing or invalid '{}' (uuid)", key).into())
}

pub fn optional_uuid(map: &HashMap<String, Value>, key: &str) -> Option<Uuid> {
    map.get(key).and_then(|v| v.as_uuid())
}

pub fn required_i64(map: &HashMap<String, Value>, key: &str) -> Result<i64, Box<dyn Error>> {
    map.get(key)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| format!("Missing or invalid '{}' (i64)", key).into())
}

pub fn optional_i64(map: &HashMap<String, Value>, key: &str) -> Option<i64> {
    map.get(key).and_then(|v| v.as_i64())
}

pub fn required_bool(map: &HashMap<String, Value>, key: &str) -> Result<bool, Box<dyn Error>> {
    map.get(key)
        .and_then(|v| v.as_bool())
        .ok_or_else(|| format!("Missing or invalid '{}' (bool)", key).into())
}

pub fn optional_bool(map: &HashMap<String, Value>, key: &str) -> Option<bool> {
    map.get(key).and_then(|v| v.as_bool())
}

pub fn required_datetime(
    map: &HashMap<String, Value>,
    key: &str,
) -> Result<DateTime<Utc>, Box<dyn Error>> {
    map.get(key)
        .and_then(|v| v.as_datetime())
        .ok_or_else(|| format!("Missing or invalid '{}' (datetime)", key).into())
}

pub fn optional_datetime(map: &HashMap<String, Value>, key: &str) -> Option<DateTime<Utc>> {
    map.get(key).and_then(|v| v.as_datetime())
}

/// Get a vector of Strings from a Value Array field
pub fn array_of_strings(map: &HashMap<String, Value>, key: &str) -> Vec<String> {
    map.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
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
    pub eviction_policy: crate::storage::EvictionPolicy,

    // Cold store config
    pub cold_cache_capacity_mb: usize,
    pub cold_flush_interval_ms: Option<u64>,
    pub cold_mode: ColdStoreMode,

    // General config
    pub auto_compact: bool,
    pub compact_interval_mins: u64,

    // Index config
    pub max_index_entries_per_field: usize, // Limit memory for indices

    // Write config
    pub enable_write_buffering: bool, // Background write buffering
    pub write_buffer_size: usize,     // Number of operations to buffer
    pub write_buffer_flush_interval_ms: u64, // Flush interval
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
            eviction_policy: crate::storage::EvictionPolicy::Hybrid,

            cold_cache_capacity_mb: 64,
            cold_flush_interval_ms: Some(100),
            cold_mode: ColdStoreMode::HighThroughput,

            auto_compact: true,
            compact_interval_mins: 60,

            max_index_entries_per_field: 100_000,

            enable_write_buffering: false,
            write_buffer_size: 1000,
            write_buffer_flush_interval_ms: 10,
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

    /// Configuration optimized for read-heavy workloads (news sites, blogs)
    pub fn read_optimized() -> Self {
        Self {
            hot_cache_size_mb: 512,
            eviction_policy: crate::storage::EvictionPolicy::LFU,
            cold_cache_capacity_mb: 256,
            cold_mode: ColdStoreMode::HighThroughput,
            ..Default::default()
        }
    }

    /// Configuration optimized for write-heavy workloads (analytics, logging)
    pub fn write_optimized() -> Self {
        Self {
            hot_cache_size_mb: 128,
            eviction_policy: crate::storage::EvictionPolicy::LRU,
            cold_cache_capacity_mb: 512,
            cold_flush_interval_ms: Some(50),
            enable_write_buffering: true,
            write_buffer_size: 10000,
            ..Default::default()
        }
    }

    /// Configuration for memory-constrained environments
    pub fn low_memory() -> Self {
        Self {
            hot_cache_size_mb: 32,
            eviction_policy: crate::storage::EvictionPolicy::LRU,
            cold_cache_capacity_mb: 32,
            cold_mode: ColdStoreMode::LowSpace,
            max_index_entries_per_field: 10_000,
            ..Default::default()
        }
    }

    /// Configuration for high-traffic real-time applications
    pub fn realtime() -> Self {
        Self {
            hot_cache_size_mb: 1024,
            eviction_policy: crate::storage::EvictionPolicy::Hybrid,
            cold_cache_capacity_mb: 512,
            cold_flush_interval_ms: Some(25),
            enable_write_buffering: true,
            write_buffer_size: 5000,
            auto_compact: false,
            max_index_entries_per_field: 500_000,
            ..Default::default()
        }
    }
}
