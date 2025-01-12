use crate::error::Result;
use crate::storage::{ColdStore, HotStore};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncReadExt;

pub struct Aurora {
    hot: HotStore,
    cold: ColdStore,
}

// #[derive(Serialize, Deserialize)]
// struct ObjectMetadata {
//     content_type: String,
//     size: usize,
//     created_at: DateTime<Utc>,
//     etag: String, // Like MD5 hash
// }

impl Aurora {
    pub fn new(db_path: &str) -> Result<Self> {
        Ok(Self {
            hot: HotStore::new(),
            cold: ColdStore::new(db_path)?,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // First check hot storage
        if let Some(value) = self.hot.get(key) {
            return Ok(Some(value));
        }

        // If not in hot storage, check cold storage
        if let Some(value) = self.cold.get(key)? {
            // Optionally promote to hot storage
            self.hot.set(key.to_string(), value.clone(), None);
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn put(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        // Always write to cold storage for persistence
        self.cold.set(key.clone(), value.clone())?;

        // Optionally cache in hot storage
        self.hot.set(key, value, ttl);

        Ok(())
    }

    pub fn put_blob(&self, key: String, file_path: &Path) -> Result<()> {
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut blob_data = Vec::new();
        blob_data.extend_from_slice(b"BLOB:");
        blob_data.extend_from_slice(&buffer);

        self.put(key, blob_data, None)
    }

    // pub fn put_object(&self, key: String, data: Vec<u8>, metadata: ObjectMetadata) -> Result<()> {
    //     // Store object data
    //     self.store_blob_bytes(format!("obj:{}", key), &data)?;

    //     // Store metadata separately
    //     self.put(format!("meta:{}", key), serde_json::to_vec(&metadata)?)?;

    //     Ok(())
    // }

    // pub fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
    //     let mut objects = Vec::new();
    //     for key_result in self.cold.scan() {
    //         let (key, _) = key_result?;
    //         let key_str = String::from_utf8(key.to_vec())?;
    //         if key_str.starts_with(prefix) {
    //             objects.push(key_str);
    //         }
    //     }
    //     Ok(objects)
    // }

    // // Get object metadata
    // pub fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>> {
    //     if let Some(meta_bytes) = self.get(&format!("meta:{}", key))? {
    //         Ok(Some(serde_json::from_slice(&meta_bytes)?))
    //     } else {
    //         Ok(None)
    //     }
    // }

    // // Delete object and its metadata
    // pub fn delete_object(&self, key: &str) -> Result<()> {
    //     self.cold.delete(&format!("obj:{}", key))?;
    //     self.cold.delete(&format!("meta:{}", key))?;
    //     Ok(())
    // }
    pub async fn store_large_blob(
        &self,
        key: String,
        mut reader: impl AsyncReadExt + Unpin,
        chunk_size: usize,
    ) -> Result<()> {
        let mut buffer = Vec::new();
        let mut chunk = vec![0; chunk_size];

        buffer.extend_from_slice(b"BLOB:");

        loop {
            let n = reader.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);
        }

        self.put(key, buffer, None)
    }

    pub fn get_blob(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if let Some(data) = self.get(key)? {
            // Check if it's a blob
            if data.starts_with(b"BLOB:") {
                return Ok(Some(data[5..].to_vec())); // Skip the BLOB: marker
            }
        }
        Ok(None)
    }

    pub fn store_blob_bytes(&self, key: String, data: &[u8]) -> Result<()> {
        let mut blob_data = Vec::new();
        blob_data.extend_from_slice(b"BLOB:");
        blob_data.extend_from_slice(data);
        self.put(key, blob_data, None)
    }

    pub fn store_chunked_blob(&self, key: String, data: &[u8], chunk_size: usize) -> Result<()> {
        let total_chunks = (data.len() + chunk_size - 1) / chunk_size;
        let metadata = format!("{}:{}", data.len(), total_chunks);
        self.put(format!("{}:meta", key), metadata.as_bytes().to_vec(), None)?;

        // Store chunks
        for (i, chunk) in data.chunks(chunk_size).enumerate() {
            let chunk_key = format!("{}:chunk:{}", key, i);
            self.store_blob_bytes(chunk_key, chunk)?;
        }

        Ok(())
    }

    pub fn get_chunked_blob(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Get metadata
        if let Some(meta) = self.get(&format!("{}:meta", key))? {
            let meta_str = String::from_utf8_lossy(&meta);
            let parts: Vec<&str> = meta_str.split(':').collect();
            let total_size: usize = parts[0].parse().unwrap();
            let total_chunks: usize = parts[1].parse().unwrap();

            let mut result = Vec::with_capacity(total_size);

            // Retrieve all chunks
            for i in 0..total_chunks {
                let chunk_key = format!("{}:chunk:{}", key, i);
                if let Some(chunk) = self.get_blob(&chunk_key)? {
                    result.extend_from_slice(&chunk);
                }
            }

            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}
