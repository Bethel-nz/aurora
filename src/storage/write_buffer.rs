use crate::error::{AuroraError, Result};
use crate::storage::ColdStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub enum WriteOp {
    Write { key: String, value: Vec<u8> },
    Flush(mpsc::SyncSender<Result<()>>),
    Shutdown,
}

pub struct WriteBuffer {
    sender: mpsc::SyncSender<WriteOp>,
    is_alive: Arc<AtomicBool>,
}

impl WriteBuffer {
    pub fn new(cold: Arc<ColdStore>, buffer_size: usize, flush_interval_ms: u64) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<WriteOp>(1000);
        let is_alive = Arc::new(AtomicBool::new(true));
        let task_is_alive = Arc::clone(&is_alive);

        // Use a real OS thread instead of tokio::spawn
        // This allows Drop to safely block without async context issues
        std::thread::spawn(move || {
            struct TaskGuard(Arc<AtomicBool>);
            impl Drop for TaskGuard {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            let _guard = TaskGuard(task_is_alive);

            let mut batch = Vec::with_capacity(buffer_size);
            let mut last_flush = Instant::now();
            let flush_duration = Duration::from_millis(flush_interval_ms);

            loop {
                // Try to receive with a timeout to allow periodic flushes
                let timeout = flush_duration.saturating_sub(last_flush.elapsed());
                let op = receiver.recv_timeout(timeout);

                match op {
                    Ok(WriteOp::Write { key, value }) => {
                        batch.push((key, value));

                        // Flush if batch is full
                        if batch.len() >= buffer_size {
                            if let Err(e) = cold.batch_set(batch.drain(..).collect()) {
                                eprintln!("Write buffer flush error: {}", e);
                            }
                            last_flush = Instant::now();
                        }
                    }
                    Ok(WriteOp::Flush(response)) => {
                        let result = if !batch.is_empty() {
                            cold.batch_set(batch.drain(..).collect())
                        } else {
                            Ok(())
                        };
                        let _ = response.send(result);
                        last_flush = Instant::now();
                    }
                    Ok(WriteOp::Shutdown) => {
                        // Final flush before shutdown
                        if !batch.is_empty() {
                            if let Err(e) = cold.batch_set(batch) {
                                eprintln!("Write buffer shutdown flush error: {}", e);
                            }
                        }
                        break; // Exit gracefully
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Periodic flush
                        if !batch.is_empty() && last_flush.elapsed() >= flush_duration {
                            if let Err(e) = cold.batch_set(batch.drain(..).collect()) {
                                eprintln!("Write buffer periodic flush error: {}", e);
                            }
                            last_flush = Instant::now();
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed, flush and exit
                        if !batch.is_empty() {
                            if let Err(e) = cold.batch_set(batch) {
                                eprintln!("Write buffer final flush error: {}", e);
                            }
                        }
                        break;
                    }
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
        self.sender.send(WriteOp::Write { key, value }).map_err(|_| {
            AuroraError::InvalidOperation("Write buffer channel closed unexpectedly.".into())
        })?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .send(WriteOp::Flush(tx))
            .map_err(|_| AuroraError::InvalidOperation("Write buffer closed".into()))?;

        rx.recv()
            .map_err(|_| AuroraError::InvalidOperation("Flush response lost".into()))?
    }

    pub fn is_active(&self) -> bool {
        self.is_alive.load(Ordering::SeqCst)
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        // Signal graceful shutdown
        // The background thread (not tokio task) will perform final flush
        // We can safely send because we're using std::sync::mpsc
        let _ = self.sender.send(WriteOp::Shutdown);

        // Give the background thread a moment to flush
        // The thread will exit cleanly after flushing
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
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
