use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::time::interval;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

#[derive(Debug)]
pub struct CacheStats {
     pub item_count: usize,
    pub memory_usage: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    LRU,
    LFU,
    Hybrid,
}

struct ValueMetadata {
    data: Arc<Vec<u8>>,
    expires_at: Option<SystemTime>,
    last_accessed: SystemTime,
}

pub struct HotStore {
    data: Arc<DashMap<String, ValueMetadata>>,
    access_count: Arc<DashMap<String, u64>>,
    hit_count: Arc<AtomicU64>,
    miss_count: Arc<AtomicU64>,
    max_size: usize,
    current_size: Arc<AtomicUsize>,
    eviction_policy: EvictionPolicy,
    eviction_lock: Arc<Mutex<()>>,
}

impl Clone for HotStore {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            access_count: Arc::clone(&self.access_count),
            hit_count: Arc::clone(&self.hit_count),
            miss_count: Arc::clone(&self.miss_count),
            max_size: self.max_size,
            current_size: Arc::clone(&self.current_size),
            eviction_policy: self.eviction_policy,
            eviction_lock: Arc::clone(&self.eviction_lock),
        }
    }
}

impl HotStore {
    pub fn new() -> Self {
        Self::new_with_size_limit(128)
    }

    pub fn with_config(
        cache_size_mb: usize,
        _cleanup_interval_secs: u64,
    ) -> Self {
        Self::with_config_and_eviction(cache_size_mb, _cleanup_interval_secs, EvictionPolicy::Hybrid)
    }

    pub fn with_config_and_eviction(
        cache_size_mb: usize,
        _cleanup_interval_secs: u64,
        eviction_policy: EvictionPolicy,
    ) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            access_count: Arc::new(DashMap::new()),
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
            max_size: cache_size_mb.checked_mul(1024 * 1024).unwrap_or(usize::MAX),
            current_size: Arc::new(AtomicUsize::new(0)),
            eviction_policy,
            eviction_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn new_with_size_limit(max_size_mb: usize) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            access_count: Arc::new(DashMap::new()),
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
            max_size: max_size_mb.checked_mul(1024 * 1024).unwrap_or(usize::MAX),
            current_size: Arc::new(AtomicUsize::new(0)),
            eviction_policy: EvictionPolicy::Hybrid,
            eviction_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.get_ref(key).map(|arc| (*arc).clone())
    }

    pub fn get_ref(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.increment_access(key);

        if let Some(mut entry) = self.data.get_mut(key) {
            if let Some(expires_at) = entry.expires_at {
                if SystemTime::now() > expires_at {
                    self.data.remove(key);
                    self.access_count.remove(key);
                    let size = entry.data.len();
                    self.current_size.fetch_sub(size, Ordering::Relaxed);
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }

            entry.last_accessed = SystemTime::now();
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(&entry.data))
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        let value_size = value.len();

        {
            let _guard = self.eviction_lock.lock().unwrap();
            let current_size = self.current_size.load(Ordering::Relaxed);
            if current_size + value_size > self.max_size {
                match self.eviction_policy {
                    EvictionPolicy::LRU => {
                        let bytes_to_free = (current_size + value_size) - self.max_size;
                        self.cleanup_by_lru(bytes_to_free);
                    }
                    EvictionPolicy::LFU => {
                        let bytes_to_free = (current_size + value_size) - self.max_size;
                        self.evict_least_frequently_used(bytes_to_free);
                    }
                    EvictionPolicy::Hybrid => self.evict_hybrid(value_size),
                }
            }
        }

        let expires_at = ttl.map(|duration| SystemTime::now() + duration);

        let metadata = ValueMetadata {
            data: Arc::new(value),
            expires_at,
            last_accessed: SystemTime::now(),
        };

        let old_value = self.data.insert(key, metadata);
        let old_size = old_value.map_or(0, |v| v.data.len());

        if value_size > old_size {
            self.current_size.fetch_add(value_size - old_size, Ordering::Relaxed);
        } else {
            self.current_size.fetch_sub(old_size - value_size, Ordering::Relaxed);
        }
    }

    fn increment_access(&self, key: &str) {
        self.access_count
            .entry(key.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    pub fn is_hot(&self, key: &str) -> bool {
        self.access_count
            .get(key)
            .map(|count| *count > 10)
            .unwrap_or(false)
    }

    fn cleanup_expired(&self) {
        let now = SystemTime::now();
        let mut total_freed = 0;
        
        let expired_keys: Vec<String> = self.data
            .iter()
            .filter_map(|entry| {
                if let Some(expires_at) = entry.expires_at {
                    if expires_at <= now {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
            
        for key in expired_keys {
            if let Some(entry) = self.data.remove(&key) {
                total_freed += entry.1.data.len();
            }
        }
        
        if total_freed > 0 {
            self.current_size.fetch_sub(total_freed, Ordering::Relaxed);
        }
    }

    fn cleanup_by_lru(&self, bytes_to_free: usize) {
        let mut entries: Vec<_> = self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.last_accessed))
            .collect();
        
        entries.sort_by_key(|e| e.1);
        
        let mut freed = 0;
        for (key, _) in entries {
            if freed >= bytes_to_free {
                break;
            }
            
            if let Some(removed) = self.data.remove(&key) {
                freed += removed.1.data.len();
                self.access_count.remove(&key);
            }
        }
        
        if freed > 0 {
            self.current_size.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    fn evict_least_frequently_used(&self, bytes_to_free: usize) {
        let mut entries: Vec<_> = self.access_count
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();

        entries.sort_by_key(|e| e.1);

        let mut freed = 0;
        for (key, _) in entries {
            if freed >= bytes_to_free {
                break;
            }

            if let Some(value) = self.data.remove(&key) {
                freed += value.1.data.len();
                self.access_count.remove(&key);
            }
        }

        if freed > 0 {
            self.current_size.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    fn evict_hybrid(&self, item_size: usize) {
        self.cleanup_expired();
        
        let current = self.current_size.load(Ordering::Relaxed);
        if current + item_size <= self.max_size {
            return;
        }

        let bytes_to_free = (current + item_size) - self.max_size;
        self.evict_least_frequently_used(bytes_to_free);

        let current = self.current_size.load(Ordering::Relaxed);
        if current + item_size > self.max_size {
            let more_bytes_to_free = (current + item_size) - self.max_size;
            self.cleanup_by_lru(more_bytes_to_free);
        }
    }

    pub async fn start_cleanup_with_interval(self: Arc<Self>, interval_secs: u64) {
        let mut interval = interval(Duration::from_secs(interval_secs));
        
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                self.cleanup_expired();
            }
        });
    }

    pub fn delete(&self, key: &str) {
        if let Some(entry) = self.data.remove(key) {
            let size = entry.1.data.len();
            self.current_size.fetch_sub(size, Ordering::Relaxed);
        }
    }

    pub fn get_stats(&self) -> CacheStats {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        
        let hit_ratio = if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        };
        
        CacheStats {
            item_count: self.data.len(),
            memory_usage: self.current_size.load(Ordering::Relaxed),
            hit_count: hits,
            miss_count: misses,
            hit_ratio,
        }
    }

    pub fn clear(&self) {
        self.data.clear();
        self.access_count.clear();
        self.current_size.store(0, Ordering::Relaxed);
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
}
