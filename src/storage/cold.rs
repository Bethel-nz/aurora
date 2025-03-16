use crate::error::{AuroraError, Result};
use sled::Db;
use crate::types::{AuroraConfig, ColdStoreMode};

pub struct ColdStore {
    db: Db,
    #[allow(dead_code)]
    db_path: String,
}

impl ColdStore {
    /// Creates a new ColdStore instance with default settings
    pub fn new(path: &str) -> Result<Self> {
        // Use the default configuration values
        let config = AuroraConfig::default();
        Self::with_config(
            path, 
            config.cold_cache_capacity_mb,
            config.cold_flush_interval_ms,
            config.cold_mode
        )
    }
    
    /// Creates a new ColdStore instance with custom configuration
    pub fn with_config(
        path: &str, 
        cache_capacity_mb: usize,
        flush_interval_ms: Option<u64>,
        mode: ColdStoreMode,
    ) -> Result<Self> {
        let db_path = if !path.ends_with(".db") {
            format!("{}.db", path)
        } else {
            path.to_string()
        };

        // Configure sled based on the provided config
        let mut sled_config = sled::Config::new()
            .path(&db_path)
            .cache_capacity((cache_capacity_mb * 1024 * 1024) as u64)
            .flush_every_ms(flush_interval_ms); // Don't need unwrap_or(0) - passing Option directly
            
        // Set the mode based on config
        sled_config = match mode {
            ColdStoreMode::HighThroughput => sled_config.mode(sled::Mode::HighThroughput),
            ColdStoreMode::LowSpace => sled_config.mode(sled::Mode::LowSpace),
        };

        let db = sled_config.open()?;

        Ok(Self { 
            db,
            db_path,
        })
    }


    /// Retrieves a value from cold storage
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key.as_bytes())?.map(|ivec| ivec.to_vec()))
    }

    /// Stores a value in cold storage
    pub fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.db.insert(key.as_bytes(), value)?;
        // Don't flush immediately - let the background flusher handle it
        Ok(())
    }

    /// Removes a value from cold storage
    pub fn delete(&self, key: &str) -> Result<()> {
        self.db.remove(key.as_bytes())?;
        // Don't flush immediately
        Ok(())
    }

    /// Returns an iterator over all key-value pairs
    pub fn scan(&self) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.iter().map(|result| {
            result.map_err(|e| AuroraError::Storage(e)).and_then(|(key, value)| {
                Ok((
                    String::from_utf8(key.to_vec())
                        .map_err(|_| AuroraError::Protocol("Invalid UTF-8 in key".to_string()))?,
                    value.to_vec(),
                ))
            })
        })
    }

    /// Returns an iterator over keys with a specific prefix
    pub fn scan_prefix(&self, prefix: &str) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.scan_prefix(prefix.as_bytes()).map(|result| {
            result.map_err(|e| AuroraError::Storage(e)).and_then(|(key, value)| {
                Ok((
                    String::from_utf8(key.to_vec())
                        .map_err(|_| AuroraError::Protocol("Invalid UTF-8 in key".to_string()))?,
                    value.to_vec(),
                ))
            })
        })
    }

    /// Batch operations for better performance
    pub fn batch_set(&self, pairs: Vec<(String, Vec<u8>)>) -> Result<()> {
        let batch = pairs
            .into_iter()
            .fold(sled::Batch::default(), |mut batch, (key, value)| {
                batch.insert(key.as_bytes(), value);
                batch
            });

        self.db.apply_batch(batch)?;
        self.db.flush()?;
        Ok(())
    }

    /// Compacts the database to reclaim space
    pub fn compact(&self) -> Result<()> {
        self.db.flush()?;
        // Note: In newer versions of sled, we would use compact_range
        // For now, just flush and let sled handle internal compaction
        Ok(())
    }

    /// Get database statistics
    pub fn get_stats(&self) -> Result<ColdStoreStats> {
        Ok(ColdStoreStats {
            size_on_disk: self.estimated_size(),
            tree_count: self.db.tree_names().len() as u64,
        })
    }

    /// Get estimated size on disk
    pub fn estimated_size(&self) -> u64 {
        self.db.size_on_disk().unwrap_or(0)
    }
}

impl Drop for ColdStore {
    fn drop(&mut self) {
        if let Err(e) = self.db.flush() {
            eprintln!("Error flushing database: {}", e);
        }
    }
}

/// Custom statistics struct since sled doesn't expose one
#[derive(Debug)]
pub struct ColdStoreStats {
    pub size_on_disk: u64,
    pub tree_count: u64,
}
