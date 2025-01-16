use crate::error::{AuroraError, Result};
use sled::Db;

pub struct ColdStore {
    db: Db,
    #[allow(dead_code)]
    db_path: String,
}

impl ColdStore {
    /// Creates a new ColdStore instance
    pub fn new(path: &str) -> Result<Self> {
        let db_path = if !path.ends_with(".db") {
            format!("{}.db", path)
        } else {
            path.to_string()
        };

        // Configure sled for better performance
        let config = sled::Config::new()
            .path(&db_path)
            .cache_capacity(1024 * 1024 * 128) // 128MB cache
            .flush_every_ms(Some(1000)) // Flush every second
            .mode(sled::Mode::HighThroughput); // Optimize for throughput

        let db = config.open()?;

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
        Ok(())
    }
}

impl Drop for ColdStore {
    fn drop(&mut self) {
        if let Err(e) = self.db.flush() {
            eprintln!("Error flushing database: {}", e);
        }
    }
}
