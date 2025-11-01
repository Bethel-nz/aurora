use crate::error::{AuroraError, Result};
use crate::storage::ColdStore;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

#[derive(Debug, Clone)]
pub struct WriteOp {
    pub key: String,
    pub value: Vec<u8>,
}

pub struct WriteBuffer {
    sender: mpsc::UnboundedSender<WriteOp>,
    is_alive: Arc<AtomicBool>,
}

impl WriteBuffer {
    pub fn new(
        cold: Arc<ColdStore>,
        buffer_size: usize,
        flush_interval_ms: u64,
    ) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<WriteOp>();
        let is_alive = Arc::new(AtomicBool::new(true));
        let task_is_alive = Arc::clone(&is_alive);

        tokio::spawn(async move {
            struct TaskGuard(Arc<AtomicBool>);
            impl Drop for TaskGuard {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            let _guard = TaskGuard(task_is_alive);

            let mut batch = Vec::with_capacity(buffer_size);
            let mut flush_timer = interval(Duration::from_millis(flush_interval_ms));

            loop {
                tokio::select! {
                    Some(op) = receiver.recv() => {
                        batch.push((op.key, op.value));

                        if batch.len() >= buffer_size {
                            if let Err(e) = cold.batch_set(batch.drain(..).collect()) {
                                eprintln!("Write buffer flush error: {}", e);
                            }
                        }
                    }

                    _ = flush_timer.tick() => {
                        if !batch.is_empty() {
                            if let Err(e) = cold.batch_set(batch.drain(..).collect()) {
                                eprintln!("Write buffer periodic flush error: {}", e);
                            }
                        }
                    }

                    else => break,
                }
            }

            if !batch.is_empty() {
                if let Err(e) = cold.batch_set(batch) {
                    eprintln!("Write buffer final flush error: {}", e);
                }
            }
        });

        Self { sender, is_alive }
    }

    pub fn write(&self, key: String, value: Vec<u8>) -> Result<()> {
        if !self.is_alive.load(Ordering::SeqCst) {
            return Err(AuroraError::InvalidOperation(
                "Write buffer is not active.".into(),
            ));
        }
        self.sender
            .send(WriteOp { key, value })
            .map_err(|_| {
                AuroraError::InvalidOperation("Write buffer channel closed unexpectedly.".into())
            })?;
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.is_alive.load(Ordering::SeqCst)
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_buffer() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.db");
        let cold = Arc::new(ColdStore::new(db_path.to_str().unwrap())?);

        let buffer = WriteBuffer::new(Arc::clone(&cold), 100, 50);

        buffer.write("key1".to_string(), b"value1".to_vec())?;
        buffer.write("key2".to_string(), b"value2".to_vec())?;
        buffer.write("key3".to_string(), b"value3".to_vec())?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cold.get("key1")?, Some(b"value1".to_vec()));
        assert_eq!(cold.get("key2")?, Some(b"value2".to_vec()));
        assert_eq!(cold.get("key3")?, Some(b"value3".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_write_buffer_batch_flush() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test.db");
        let cold = Arc::new(ColdStore::new(db_path.to_str().unwrap())?);

        let buffer = WriteBuffer::new(Arc::clone(&cold), 5, 1000);

        for i in 0..10 {
            buffer.write(format!("key{}", i), format!("value{}", i).into_bytes())?;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        for i in 0..10 {
            assert_eq!(
                cold.get(&format!("key{}", i))?,
                Some(format!("value{}", i).into_bytes())
            );
        }

        Ok(())
    }
}
