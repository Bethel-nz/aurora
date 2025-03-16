use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::interval;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

// Cache statistics structure
#[derive(Debug)]
pub struct CacheStats {
     pub item_count: usize,
    pub memory_usage: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_ratio: f64,
}

// Metadata for each cached item
struct ValueMetadata {
    data: Vec<u8>,
    expires_at: Option<SystemTime>,
    last_accessed: SystemTime,
}

pub struct HotStore {
    data: Arc<DashMap<String, ValueMetadata>>,
    access_count: Arc<DashMap<String, u64>>,
    hit_count: Arc<AtomicU64>,
    miss_count: Arc<AtomicU64>,
    max_size: usize,  // Maximum size in bytes
    current_size: Arc<AtomicUsize>,
}

// Implement Clone for HotStore
impl Clone for HotStore {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            access_count: Arc::clone(&self.access_count),
            hit_count: Arc::clone(&self.hit_count),
            miss_count: Arc::clone(&self.miss_count),
            max_size: self.max_size,
            current_size: Arc::clone(&self.current_size),
        }
    }
}

impl HotStore {
    pub fn new() -> Self {
        Self::new_with_size_limit(128) // Default to 128MB
    }

    pub fn with_config(
        cache_size_mb: usize,
        _cleanup_interval_secs: u64,
    ) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            access_count: Arc::new(DashMap::new()),
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
            max_size: cache_size_mb * 1024 * 1024,
            current_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn new_with_size_limit(max_size_mb: usize) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            access_count: Arc::new(DashMap::new()),
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
            max_size: max_size_mb * 1024 * 1024,
            current_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.increment_access(key);
        
        if let Some(mut entry) = self.data.get_mut(key) {
            // Check if expired
            if let Some(expires_at) = entry.expires_at {
                if SystemTime::now() > expires_at {
                    self.data.remove(key);
                    let size = entry.data.len();
                    self.current_size.fetch_sub(size, Ordering::Relaxed);
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            
            // Update last accessed time
            entry.last_accessed = SystemTime::now();
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            Some(entry.data.clone())
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    // Get with Arc to allow zero-copy when possible
    pub fn get_ref(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.increment_access(key);
        
        if let Some(mut entry) = self.data.get_mut(key) {
            // Check if expired
            if let Some(expires_at) = entry.expires_at {
                if SystemTime::now() > expires_at {
                    self.data.remove(key);
                    let size = entry.data.len();
                    self.current_size.fetch_sub(size, Ordering::Relaxed);
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            
            // Update last accessed time
            entry.last_accessed = SystemTime::now();
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            Some(Arc::new(entry.data.clone()))
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        let value_size = value.len();
        
        // Check if we need to evict items
        let new_size = self.current_size.load(Ordering::Relaxed)
            .saturating_add(value_size);
            
        if new_size > self.max_size {
            self.evict_least_used(new_size.saturating_sub(self.max_size));
        }
        
        let expires_at = ttl.map(|duration| SystemTime::now() + duration);
        
        // Update size tracking
        if let Some(old_value) = self.data.get(&key) {
            let old_size = old_value.data.len();
            self.current_size.fetch_sub(old_size, Ordering::Relaxed);
        }
        
        let metadata = ValueMetadata {
            data: value,
            expires_at,
            last_accessed: SystemTime::now(),
        };
        
        self.data.insert(key, metadata);
        self.current_size.fetch_add(value_size, Ordering::Relaxed);
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

    // Clean up expired items
    fn cleanup_expired(&self) {
        let now = SystemTime::now();
        let mut total_freed = 0;
        
        // Clean up expired items
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

    // Clean up using LRU when cache is full
    fn cleanup_by_lru(&self, bytes_to_free: usize) {
        let mut entries: Vec<_> = self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.last_accessed))
            .collect();
        
        // Sort by last accessed time (oldest first)
        entries.sort_by_key(|e| e.1);
        
        let mut freed = 0;
        for (key, _) in entries {
            if freed >= bytes_to_free {
                break;
            }
            
            if let Some(removed) = self.data.remove(&key) {
                freed += removed.1.data.len();
            }
        }
        
        if freed > 0 {
            self.current_size.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    // Evict least frequently used items
    fn evict_least_used(&self, bytes_to_free: usize) {
        // First try to clean up expired items
        self.cleanup_expired();
        
        // If we've freed enough, we're done
        if self.current_size.load(Ordering::Relaxed) + bytes_to_free <= self.max_size {
            return;
        }
        
        // Otherwise, evict by access count
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
            }
        }
        
        if freed > 0 {
            self.current_size.fetch_sub(freed, Ordering::Relaxed);
        }
        
        // If we still need to free more, use LRU approach
        if self.current_size.load(Ordering::Relaxed) + bytes_to_free > self.max_size {
            self.cleanup_by_lru(bytes_to_free.saturating_sub(freed));
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

    // Get statistics about the cache
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

    // Clear the cache
    pub fn clear(&self) {
        self.data.clear();
        self.access_count.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    // Get hit ratio
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
