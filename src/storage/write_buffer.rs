use crate::error::{AqlError, Result};
use crate::storage::ColdStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub enum WriteOp {
    Write { key: Arc<String>, value: Arc<Vec<u8>> },
    Flush(mpsc::SyncSender<Result<()>>),
    Shutdown,
}

pub struct WriteBuffer {
    sender: mpsc::SyncSender<WriteOp>,
    is_alive: Arc<AtomicBool>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl WriteBuffer {
    pub fn new(cold: Arc<ColdStore>, buffer_size: usize, flush_interval_ms: u64) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<WriteOp>(1000);
        let is_alive = Arc::new(AtomicBool::new(true));
        let task_is_alive = Arc::clone(&is_alive);

        // Use a real OS thread instead of tokio::spawn
        // This allows Drop to safely block without async context issues
        let handle = std::thread::spawn(move || {
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
                            let batch_to_write = std::mem::take(&mut batch);
                            if let Err(e) = cold.batch_set_arc(batch_to_write) {
                                eprintln!("Write buffer flush error: {}", e);
                            }
                            last_flush = Instant::now();
                        }
                    }
                    Ok(WriteOp::Flush(response)) => {
                        let result = if !batch.is_empty() {
                            let batch_to_write = std::mem::take(&mut batch);
                            cold.batch_set_arc(batch_to_write)
                        } else {
                            Ok(())
                        };
                        let _ = response.send(result);
                        last_flush = Instant::now();
                    }
                    Ok(WriteOp::Shutdown) => {
                        // Final flush before shutdown
                        if !batch.is_empty() {
                            let batch_to_write = std::mem::take(&mut batch);
                            if let Err(e) = cold.batch_set_arc(batch_to_write) {
                                eprintln!("Write buffer shutdown flush error: {}", e);
                            }
                        }
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Periodic flush
                        if !batch.is_empty() && last_flush.elapsed() >= flush_duration {
                            let batch_to_write = std::mem::take(&mut batch);
                            
                            // Use a match to handle the error cleanly
                            match cold.batch_set_arc(batch_to_write) {
                                Ok(_) => last_flush = Instant::now(),
                                Err(_) => {
                                    eprintln!("Write buffer periodic flush error: Disk Full. Pausing writes.");
                                    // PAUSE OR RETURN ERROR
                                    std::thread::sleep(Duration::from_millis(100)); 
                                }
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed, flush and exit
                        if !batch.is_empty() {
                            let batch_to_write = std::mem::take(&mut batch);
                            if let Err(e) = cold.batch_set_arc(batch_to_write) {
                                eprintln!("Write buffer final flush error: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        });

        Self {
            sender,
            is_alive,
            thread_handle: Some(handle),
        }
    }

    pub fn write(&self, key: Arc<String>, value: Arc<Vec<u8>>) -> Result<()> {
        if !self.is_alive.load(Ordering::SeqCst) {
            return Err(AqlError::invalid_operation(
                "Write buffer is not active.".to_string(),
            ));
        }
        self.sender
            .send(WriteOp::Write { key, value })
            .map_err(|_| {
                AqlError::invalid_operation("Write buffer channel closed unexpectedly.".to_string())
            })?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .send(WriteOp::Flush(tx))
            .map_err(|_| AqlError::invalid_operation("Write buffer closed".to_string()))?;

        rx.recv()
            .map_err(|_| AqlError::invalid_operation("Flush response lost".to_string()))?
    }

    pub fn is_active(&self) -> bool {
        self.is_alive.load(Ordering::SeqCst)
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        let _ = self.sender.send(WriteOp::Shutdown);

        // HACK: Join thread to prevent Windows zombie process (causing LNK1104 on next build)
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
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

        buffer.write(Arc::new("key1".to_string()), Arc::new(b"value1".to_vec()))?;
        buffer.write(Arc::new("key2".to_string()), Arc::new(b"value2".to_vec()))?;
        buffer.write(Arc::new("key3".to_string()), Arc::new(b"value3".to_vec()))?;

        // Explicitly flush to ensure data is written
        buffer.flush()?;

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
            buffer.write(Arc::new(format!("key{}", i)), Arc::new(format!("value{}", i).into_bytes()))?;
        }

        // Wait for flush interval (1000ms) plus some buffer
        tokio::time::sleep(Duration::from_millis(1500)).await;

        for i in 0..10 {
            assert_eq!(
                cold.get(&format!("key{}", i))?,
                Some(format!("value{}", i).into_bytes())
            );
        }

        Ok(())
    }
}