use crate::error::{AuroraError, Result};
use crate::types::{AuroraConfig, ColdStoreMode};
use sled::Db;

pub struct ColdStore {
    db: Db,
    #[allow(dead_code)]
    db_path: String,
}

impl ColdStore {
    pub fn new(path: &str) -> Result<Self> {
        let config = AuroraConfig::default();
        Self::with_config(
            path,
            config.cold_cache_capacity_mb,
            config.cold_flush_interval_ms,
            config.cold_mode,
        )
    }

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

        let mut sled_config = sled::Config::new()
            .path(&db_path)
            .cache_capacity((cache_capacity_mb * 1024 * 1024) as u64)
            .flush_every_ms(flush_interval_ms);

        sled_config = match mode {
            ColdStoreMode::HighThroughput => sled_config.mode(sled::Mode::HighThroughput),
            ColdStoreMode::LowSpace => sled_config.mode(sled::Mode::LowSpace),
        };

        let db = sled_config.open().map_err(|e| {
            let error_msg = e.to_string();

            // Check for Windows "Access is denied" error (file locked by another process)
            if error_msg.contains("Access is denied") || error_msg.contains("os error 5") {
                let lock_hint = if std::path::Path::new(&db_path).exists() {
                    "\n\nPossible solutions:\n\
                    1. Close any other Aurora instances using this database\n\
                    2. If no other instance is running, the previous instance may have crashed.\n\
                       Delete the lock file in the database directory and try again\n\
                    3. Run as administrator if permission is the issue"
                } else {
                    "\n\nThe database directory may require administrator privileges to create."
                };

                AuroraError::IoError(format!(
                    "Cannot open database at '{}': file is locked or access denied.{}",
                    db_path, lock_hint
                ))
            } else {
                AuroraError::Storage(e)
            }
        })?;

        Ok(Self { db, db_path })
    }

    /// Attempt to detect and remove stale lock files
    ///
    /// This is useful for recovering from crashes where the lock file wasn't properly cleaned up.
    /// Only call this if you're sure no other process is using the database.
    ///
    /// # Safety
    /// This function should only be used when you're certain the database is not in use by another process.
    /// Calling this while another Aurora instance is running could lead to data corruption.
    pub fn try_remove_stale_lock(db_path: &str) -> Result<bool> {
        use std::path::Path;

        let path = if !db_path.ends_with(".db") {
            format!("{}.db", db_path)
        } else {
            db_path.to_string()
        };

        let db_dir = Path::new(&path);
        if !db_dir.exists() {
            return Ok(false);
        }

        // Sled uses a .lock file in the db directory
        let lock_file = db_dir.join(".lock");
        if lock_file.exists() {
            std::fs::remove_file(&lock_file)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key.as_bytes())?.map(|ivec| ivec.to_vec()))
    }

    pub fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.db.insert(key.as_bytes(), value)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        self.db.remove(key.as_bytes())?;
        Ok(())
    }

    pub fn scan(&self) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.iter().map(|result| {
            result
                .map_err(AuroraError::Storage)
                .and_then(|(key, value)| {
                    Ok((
                        String::from_utf8(key.to_vec()).map_err(|_| {
                            AuroraError::Protocol("Invalid UTF-8 in key".to_string())
                        })?,
                        value.to_vec(),
                    ))
                })
        })
    }

    pub fn scan_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.scan_prefix(prefix.as_bytes()).map(|result| {
            result
                .map_err(AuroraError::Storage)
                .and_then(|(key, value)| {
                    Ok((
                        String::from_utf8(key.to_vec()).map_err(|_| {
                            AuroraError::Protocol("Invalid UTF-8 in key".to_string())
                        })?,
                        value.to_vec(),
                    ))
                })
        })
    }

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

    /// Flushes all buffered writes to disk to ensure durability.
    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    pub fn get_stats(&self) -> Result<ColdStoreStats> {
        Ok(ColdStoreStats {
            size_on_disk: self.estimated_size(),
            tree_count: self.db.tree_names().len() as u64,
        })
    }

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

#[derive(Debug)]
pub struct ColdStoreStats {
    pub size_on_disk: u64,
    pub tree_count: u64,
}
