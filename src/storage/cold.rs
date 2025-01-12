use crate::error::{AuroraError, Result};
use sled::Db;
use std::path::Path;

pub struct ColdStore {
    db: Db,
}

impl ColdStore {
    /// Creates a new ColdStore instance
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Retrieves a value from cold storage
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key.as_bytes())?.map(|ivec| ivec.to_vec()))
    }

    /// Stores a value in cold storage
    pub fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.db.insert(key.as_bytes(), value)?;
        // Ensure durability by flushing to disk
        self.db.flush()?;
        Ok(())
    }

    /// Removes a value from cold storage
    pub fn delete(&self, key: &str) -> Result<()> {
        self.db.remove(key.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    /// Returns an iterator over all key-value pairs
    pub fn scan(&self) -> impl Iterator<Item = Result<(String, Vec<u8>)>> + '_ {
        self.db.iter().map(|result| {
            result.map_err(AuroraError::from).and_then(|(key, value)| {
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
        // Ensure all data is flushed when the store is dropped
        if let Err(e) = self.db.flush() {
            eprintln!("Error flushing cold store on drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_basic_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = ColdStore::new(temp_dir.path())?;

        // Test set and get
        store.set("key1".to_string(), b"value1".to_vec())?;
        assert_eq!(store.get("key1")?, Some(b"value1".to_vec()));

        // Test delete
        store.delete("key1")?;
        assert_eq!(store.get("key1")?, None);

        Ok(())
    }

    #[test]
    fn test_batch_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = ColdStore::new(temp_dir.path())?;

        let pairs = vec![
            ("key1".to_string(), b"value1".to_vec()),
            ("key2".to_string(), b"value2".to_vec()),
        ];

        store.batch_set(pairs)?;

        assert_eq!(store.get("key1")?, Some(b"value1".to_vec()));
        assert_eq!(store.get("key2")?, Some(b"value2".to_vec()));

        Ok(())
    }

    #[test]
    fn test_scan() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = ColdStore::new(temp_dir.path())?;

        store.set("key1".to_string(), b"value1".to_vec())?;
        store.set("key2".to_string(), b"value2".to_vec())?;

        let mut items: Vec<_> = store.scan().collect::<Result<Vec<_>>>()?;
        items.sort_by(|(a, _), (b, _)| a.cmp(b));

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, "key1");
        assert_eq!(items[1].0, "key2");

        Ok(())
    }
}
