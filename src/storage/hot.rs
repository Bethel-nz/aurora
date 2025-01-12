use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::interval;

// Simplified metadata struct - removed unused fields
struct ValueMetadata {
    data: Vec<u8>,
    expires_at: Option<SystemTime>,
}

pub struct HotStore {
    data: Arc<DashMap<String, ValueMetadata>>,
    access_count: Arc<DashMap<String, u64>>,
}

impl HotStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            access_count: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.increment_access(key);
        
        if let Some(value) = self.data.get(key) {
            // Check if expired
            if let Some(expires_at) = value.expires_at {
                if SystemTime::now() > expires_at {
                    self.data.remove(key);
                    return None;
                }
            }
            Some(value.data.clone())
        } else {
            None
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        let expires_at = ttl.map(|duration| SystemTime::now() + duration);
        
        let metadata = ValueMetadata {
            data: value,
            expires_at,
        };
        
        self.data.insert(key, metadata);
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
        self.data.retain(|_, value| {
            value.expires_at
                .map(|expires_at| expires_at > now)
                .unwrap_or(true)
        });
    }

    pub async fn start_cleanup_task(self: Arc<Self>) {
        let mut interval = interval(Duration::from_secs(60)); // Run every minute
        
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                self.cleanup_expired();
            }
        });
    }
}
