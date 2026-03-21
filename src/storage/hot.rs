//! Hot Store - Moka-based cache (Sync Version)
//! Optimized for high-concurrency database workloads.

use moka::Expiry;
use moka::sync::Cache;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub item_count: u64,
    pub memory_usage: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_ratio: f64,
    pub weighted_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EvictionPolicy {
    LRU,    // 5min default TTL
    LFU,    // 15min default TTL
    Hybrid, // 30min default TTL
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::LRU => write!(f, "LRU"),
            EvictionPolicy::LFU => write!(f, "LFU"),
            EvictionPolicy::Hybrid => write!(f, "Hybrid"),
        }
    }
}

impl EvictionPolicy {
    pub fn default_ttl(&self) -> Duration {
        match self {
            EvictionPolicy::LRU => Duration::from_secs(5 * 60),
            EvictionPolicy::LFU => Duration::from_secs(15 * 60),
            EvictionPolicy::Hybrid => Duration::from_secs(30 * 60),
        }
    }
}

/// Cached value with optional custom TTL
#[derive(Clone)]
pub struct CachedValue {
    pub data: Arc<Vec<u8>>,
    pub ttl: Option<Duration>,
}

/// Per-entry expiration policy
pub struct HotStoreExpiry {
    default_ttl: Duration,
}

// Note: moka::Expiry trait is the same for sync and future caches
impl Expiry<String, CachedValue> for HotStoreExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &CachedValue,
        _current_time: Instant,
    ) -> Option<Duration> {
        Some(value.ttl.unwrap_or(self.default_ttl))
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &CachedValue,
        _current_time: Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl.unwrap_or(self.default_ttl))
    }
}

pub struct HotStore {
    // Changed: Using moka::sync::Cache instead of moka::future::Cache
    cache: Cache<String, CachedValue>,
    hit_count: Arc<AtomicU64>,
    miss_count: Arc<AtomicU64>,
    max_size: u64,
    eviction_policy: EvictionPolicy,
    default_ttl: Duration,
}

impl Clone for HotStore {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            hit_count: Arc::clone(&self.hit_count),
            miss_count: Arc::clone(&self.miss_count),
            max_size: self.max_size,
            eviction_policy: self.eviction_policy,
            default_ttl: self.default_ttl,
        }
    }
}

impl Default for HotStore {
    fn default() -> Self {
        Self::new()
    }
}

impl HotStore {
    pub fn new() -> Self {
        Self::new_with_size_limit(128)
    }

    pub fn new_with_size_limit(max_size_mb: usize) -> Self {
        Self::with_config_and_eviction(max_size_mb, 0, EvictionPolicy::Hybrid)
    }

    pub fn with_config(cache_size_mb: usize, _cleanup_interval_secs: u64) -> Self {
        Self::with_config_and_eviction(
            cache_size_mb,
            _cleanup_interval_secs,
            EvictionPolicy::Hybrid,
        )
    }

    pub fn with_config_and_eviction(
        cache_size_mb: usize,
        _cleanup_interval_secs: u64,
        eviction_policy: EvictionPolicy,
    ) -> Self {
        let max_size = (cache_size_mb as u64).saturating_mul(1024 * 1024);
        let default_ttl = eviction_policy.default_ttl();

        let cache = Cache::builder()
            .max_capacity(max_size)
            .weigher(|_key: &String, value: &CachedValue| -> u32 {
                value.data.len().min(u32::MAX as usize) as u32
            })
            .expire_after(HotStoreExpiry { default_ttl })
            .build();

        Self {
            cache,
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
            max_size,
            eviction_policy,
            default_ttl,
        }
    }

    // Synchronous API (Fast path)

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.get_ref(key).map(|arc| (*arc).clone())
    }

    pub fn get_ref(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        match self.cache.get(key) {
            Some(value) => {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                Some(value.data)
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn set(&self, key: Arc<String>, value: Arc<Vec<u8>>, ttl: Option<Duration>) {
        let cached = CachedValue {
            data: value,
            ttl,
        };
        // Moka requires String keys, so we deref the Arc<String>. 
        // This is a small clone (just the key string), unavoidable with Moka, but cheap.
        self.cache.insert(key.to_string(), cached);
    }

    // Async API Compatibility
    // Since moka::sync is thread-safe and fast (memory only),
    // we can call it directly in async blocks without spawn_blocking
    // unless you have massive eviction callbacks.

    pub async fn get_async(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        // Direct call to underlying sync cache is safe here
        self.get_ref(key)
    }

    pub async fn set_async(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        self.set(Arc::new(key), Arc::new(value), ttl)
    }

    // Maintenance & Stats

    pub fn is_hot(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    pub fn delete(&self, key: &str) {
        self.cache.invalidate(key);
    }

    pub fn get_stats(&self) -> CacheStats {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;

        // Note: sync cache uses run_pending_tasks too for exact stats,
        // but it also does maintenance on reads/writes automatically.
        CacheStats {
            item_count: self.cache.entry_count(),
            memory_usage: self.cache.weighted_size(),
            hit_count: hits,
            miss_count: misses,
            hit_ratio: if total == 0 {
                0.0
            } else {
                hits as f64 / total as f64
            },
            weighted_size: self.cache.weighted_size(),
        }
    }

    pub fn clear(&self) {
        self.cache.invalidate_all();
    }

    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Explicitly run maintenance tasks (eviction, etc).
    /// Moka sync does this automatically on access, but this can be called
    /// by a background thread if the cache is idle.
    pub fn sync(&self) {
        self.cache.run_pending_tasks();
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    pub fn default_ttl(&self) -> Duration {
        self.default_ttl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_get_set() {
        let store = HotStore::new_with_size_limit(1);
        store.set(Arc::new("key1".to_string()), Arc::new(vec![1, 2, 3, 4]), None);

        let result = store.get("key1");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_cache_miss() {
        let store = HotStore::new();
        let result = store.get("nonexistent");
        assert!(result.is_none());
        assert_eq!(store.get_stats().miss_count, 1);
    }

    #[tokio::test]
    async fn test_async_operations() {
        let store = HotStore::new();
        // Verify the async wrappers work on the sync cache
        store
            .set_async("key1".to_string(), vec![1, 2, 3], None)
            .await;

        let result = store.get_async("key1").await;
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_is_hot() {
        let store = HotStore::new();
        store.set(Arc::new("key1".to_string()), Arc::new(vec![1]), None);
        assert!(store.is_hot("key1"));
        assert!(!store.is_hot("key2"));
    }
}