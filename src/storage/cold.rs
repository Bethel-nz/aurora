use crate::error::{AqlError, ErrorCode, Result};
use crate::types::{AuroraConfig, ColdStoreMode};
use sled::Db;
use std::sync::Arc;

const ZSTD_MAGIC: u8 = 0x5A; // 'Z' in hex

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

            if error_msg.contains("Access is denied") || error_msg.contains("os error 5") {
                AqlError::new(
                    ErrorCode::IoError,
                    format!(
                        "Cannot open database at '{}': file is locked or access denied.",
                        db_path,
                    ),
                )
            } else {
                AqlError::from(e)
            }
        })?;

        Ok(Self { db, db_path })
    }

    pub fn open_tree(&self, name: &str) -> Result<sled::Tree> {
        Ok(self.db.open_tree(name)?)
    }

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

        let lock_file = db_dir.join(".lock");
        if lock_file.exists() {
            std::fs::remove_file(&lock_file)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let val = self.db.get(key.as_bytes())?;
        match val {
            Some(ivec) => {
                let bytes = ivec.as_ref();
                if bytes.starts_with(&[ZSTD_MAGIC]) {
                    let decompressed = zstd::decode_all(&bytes[1..]).map_err(|e| {
                        AqlError::new(ErrorCode::SerializationError, format!("Decompression failed: {}", e))
                    })?;
                    Ok(Some(decompressed))
                } else {
                    Ok(Some(bytes.to_vec()))
                }
            }
            None => Ok(None),
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        let mut compressed = vec![ZSTD_MAGIC];
        zstd::stream::copy_encode(&value[..], &mut compressed, 3).map_err(|e| {
            AqlError::new(ErrorCode::SerializationError, format!("Compression failed: {}", e))
        })?;
        
        self.db.insert(key.as_bytes(), compressed)?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        self.db.remove(key.as_bytes())?;
        Ok(())
    }

    pub fn scan(&self) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.iter().map(|result| {
            result.map_err(AqlError::from).and_then(|(key, value)| {
                let bytes = value.as_ref();
                let data = if bytes.starts_with(&[ZSTD_MAGIC]) {
                    zstd::decode_all(&bytes[1..]).map_err(|e| {
                        AqlError::new(ErrorCode::SerializationError, format!("Decompression failed: {}", e))
                    })?
                } else {
                    bytes.to_vec()
                };

                Ok((
                    String::from_utf8(key.to_vec()).map_err(|_| {
                        AqlError::new(ErrorCode::ProtocolError, "Invalid UTF-8 in key".to_string())
                    })?,
                    data,
                ))
            })
        })
    }

    pub fn scan_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.scan_prefix(prefix.as_bytes()).map(|result| {
            result.map_err(AqlError::from).and_then(|(key, value)| {
                let bytes = value.as_ref();
                let data = if bytes.starts_with(&[ZSTD_MAGIC]) {
                    zstd::decode_all(&bytes[1..]).map_err(|e| {
                        AqlError::new(ErrorCode::SerializationError, format!("Decompression failed: {}", e))
                    })?
                } else {
                    bytes.to_vec()
                };

                Ok((
                    String::from_utf8(key.to_vec()).map_err(|_| {
                        AqlError::new(ErrorCode::ProtocolError, "Invalid UTF-8 in key".to_string())
                    })?,
                    data,
                ))
            })
        })
    }

    pub fn batch_set(&self, pairs: Vec<(String, Vec<u8>)>) -> Result<()> {
        let batch = pairs
            .into_iter()
            .fold(sled::Batch::default(), |mut batch, (key, value)| {
                let mut compressed = vec![ZSTD_MAGIC];
                let _ = zstd::stream::copy_encode(&value[..], &mut compressed, 3);
                batch.insert(key.as_bytes(), compressed);
                batch
            });

        self.db.apply_batch(batch)?;
        Ok(())
    }

    pub fn batch_set_arc(&self, pairs: Vec<(Arc<String>, Arc<Vec<u8>>)>) -> Result<()> {
        let batch = pairs
            .into_iter()
            .fold(sled::Batch::default(), |mut batch, (key, value)| {
                let mut compressed = vec![ZSTD_MAGIC];
                let _ = zstd::stream::copy_encode(value.as_slice(), &mut compressed, 3);
                batch.insert(key.as_bytes(), compressed);
                batch
            });

        self.db.apply_batch(batch)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        // Sled compaction is handled by its internal GC, but we can call checksum or other things
        // or just let it flush.
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
