//! Hot Store - Moka-based cache with W-TinyLFU eviction and per-entry TTL

use moka::future::Cache;
use moka::Expiry;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    LRU,    // 5min default TTL
    LFU,    // 15min default TTL
    Hybrid, // 30min default TTL
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
struct HotStoreExpiry {
    default_ttl: Duration,
}

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

    pub fn with_config(cache_size_mb: usize, _cleanup_interval_secs: u64) -> Self {
        Self::with_config_and_eviction(cache_size_mb, _cleanup_interval_secs, EvictionPolicy::Hybrid)
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

    pub fn new_with_size_limit(max_size_mb: usize) -> Self {
        Self::with_config_and_eviction(max_size_mb, 0, EvictionPolicy::Hybrid)
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.get_ref(key).map(|arc| (*arc).clone())
    }

    pub fn get_ref(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        // HACK: block_on for sync API compat - callers should prefer get_async
        let result = futures::executor::block_on(self.cache.get(key));
        match result {
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

    pub async fn get_async(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        match self.cache.get(key).await {
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

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        // HACK: block_on for sync API compat - callers should prefer set_async
        futures::executor::block_on(self.set_async(key, value, ttl));
    }

    pub async fn set_async(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        let cached = CachedValue {
            data: Arc::new(value),
            ttl,
        };
        self.cache.insert(key, cached).await;
    }

    pub fn is_hot(&self, key: &str) -> bool {
        // contains_key is sync in moka::future::Cache
        self.cache.contains_key(key)
    }

    pub fn delete(&self, key: &str) {
        // HACK: block_on for sync API compat
        futures::executor::block_on(self.cache.invalidate(key));
    }

    pub fn get_stats(&self) -> CacheStats {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;

        CacheStats {
            item_count: self.cache.entry_count(),
            memory_usage: self.cache.weighted_size(),
            hit_count: hits,
            miss_count: misses,
            hit_ratio: if total == 0 { 0.0 } else { hits as f64 / total as f64 },
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
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    // HACK: No-op for API compat
    pub async fn start_cleanup_with_interval(self: Arc<Self>, _interval_secs: u64) {}

    pub fn sync(&self) {
        futures::executor::block_on(self.cache.run_pending_tasks());
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

    #[cfg(test)]
    fn new_for_testing(max_capacity: u64, default_ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .weigher(|_key: &String, value: &CachedValue| -> u32 {
                value.data.len().min(u32::MAX as usize) as u32
            })
            .expire_after(HotStoreExpiry { default_ttl })
            .build();

        Self {
            cache,
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
            max_size: max_capacity,
            eviction_policy: EvictionPolicy::Hybrid, // Not relevant for this specific constructor
            default_ttl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_basic_get_set() {
        let store = HotStore::new_with_size_limit(1);
        store.set("key1".to_string(), vec![1, 2, 3, 4], None);
        
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

    #[test]
    fn test_cache_hit_counting() {
        let store = HotStore::new();
        store.set("key1".to_string(), vec![1, 2, 3], None);
        store.get("key1");
        store.get("key1");
        store.get("key1");
        assert_eq!(store.get_stats().hit_count, 3);
        assert_eq!(store.get_stats().miss_count, 0);
        assert_eq!(store.hit_ratio(), 1.0);
    }

    #[test]
    fn test_delete() {
        let store = HotStore::new();
        store.set("key1".to_string(), vec![1, 2, 3], None);
        assert!(store.get("key1").is_some());
        
        store.delete("key1");
        store.sync(); // Block until pending tasks are finished.
        assert!(store.get("key1").is_none());
    }

    #[test]
    fn test_clear() {
        let store = HotStore::new();
        store.set("key1".to_string(), vec![1], None);
        store.set("key2".to_string(), vec![2], None);
        store.set("key3".to_string(), vec![3], None);
        
        store.clear();
        store.sync();
        
        assert!(store.get("key1").is_none());
        assert!(store.get("key2").is_none());
        assert!(store.get("key3").is_none());
        assert_eq!(store.get_stats().item_count, 0);
    }

    #[tokio::test]
    async fn test_async_operations() {
        let store = HotStore::new();
        store.set_async("key1".to_string(), vec![1, 2, 3], None).await;
        
        let result = store.get_async("key1").await;
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_custom_ttl() {
        let store = HotStore::new();
        store.set_async("short".to_string(), vec![1], Some(Duration::from_millis(50))).await;
        
        assert!(store.get_async("short").await.is_some());
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Moka evicts expired items on next access or during internal maintenance.
        // Calling get ensures the eviction logic for the expired item is triggered.
        assert!(store.get_async("short").await.is_none());
    }

    #[tokio::test]
    async fn test_default_ttl() {
        // Use a policy with a short default TTL for testing.
        let policy = EvictionPolicy::LRU;
        let mut store = HotStore::with_config_and_eviction(1, 0, policy);
        store.default_ttl = Duration::from_millis(50); // Override for testability

        store.set_async("default_ttl_key".to_string(), vec![1, 2], None).await; // No custom TTL
        
        assert!(store.get_async("default_ttl_key").await.is_some());
        
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Trigger maintenance/check
        assert!(store.get_async("default_ttl_key").await.is_none(), "Item should have expired via default TTL");
    }

    #[tokio::test]
    async fn test_weight_based_eviction() {
        // Create a cache with a max size of 10 bytes using the test constructor.
        let store = HotStore::new_for_testing(10, Duration::from_secs(60));

        // Insert an item of 6 bytes.
        store.set_async("item1".to_string(), vec![0; 6], None).await;
        store.cache.run_pending_tasks().await;
        assert!(store.get_async("item1").await.is_some(), "item1 should be in cache");
        assert_eq!(store.get_stats().memory_usage, 6);

        // Insert an item of 5 bytes. This should push the total weight to 11,
        // causing `item1` to be evicted.
        store.set_async("item2".to_string(), vec![0; 5], None).await;
        store.cache.run_pending_tasks().await;
        
        // After insertion and maintenance, check the contents.
        assert!(store.get_async("item1").await.is_none(), "item1 should have been evicted");
        assert!(store.get_async("item2").await.is_some(), "item2 should now be in the cache");
        assert_eq!(store.get_stats().memory_usage, 5);
    }
    
    #[tokio::test]
    async fn test_update_value() {
        let store = HotStore::new();
        
        // Insert initial value
        store.set_async("key".to_string(), vec![1], None).await;
        assert_eq!(*store.get_async("key").await.unwrap(), vec![1]);
        assert_eq!(store.get_stats().memory_usage, 1);

        // Insert new value for the same key
        store.set_async("key".to_string(), vec![2, 3], None).await;
        store.cache.run_pending_tasks().await;

        assert_eq!(*store.get_async("key").await.unwrap(), vec![2, 3]);
        assert_eq!(store.get_stats().item_count, 1, "Item count should be 1 after update");
        assert_eq!(store.get_stats().memory_usage, 2, "Memory usage should be updated");
    }

    #[tokio::test]
    async fn test_concurrency_stress() {
        let store = Arc::new(HotStore::new_with_size_limit(10));
        let num_tasks = 100;
        let num_ops_per_task = 50;

        let mut tasks = Vec::new();

        for i in 0..num_tasks {
            let store_clone = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let mut get_attempts = 0;
                for j in 0..num_ops_per_task {
                    let key = format!("key-{}-{}", i, j % 10);
                    // Mix of writes and reads
                    if j % 2 == 0 {
                        store_clone.set_async(key, vec![i as u8, j as u8], Some(Duration::from_secs(10))).await;
                    } else {
                        get_attempts += 1;
                        store_clone.get_async(&key).await;
                    }
                }
                get_attempts
            }));
        }

        let mut total_get_attempts = 0;
        for task in tasks {
            total_get_attempts += task.await.unwrap();
        }
        
        store.cache.run_pending_tasks().await;

        let stats = store.get_stats();
        println!("Final Stats: {:?}", stats);

        // The number of hits + misses should equal the total number of get operations.
        assert_eq!(stats.hit_count + stats.miss_count, total_get_attempts, "Hit/miss count should match total get operations");
        // Ensure cache is not empty, but don't assert an exact number as timing can vary.
        assert!(stats.item_count > 0, "Cache should not be empty after stress test");
    }
}
