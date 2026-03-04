use super::job::{Job, JobStatus};
use crate::error::{AqlError, ErrorCode, Result};
use crate::storage::ColdStore;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Persistent job queue
pub struct JobQueue {
    // In-memory index for all jobs (status tracking)
    jobs: Arc<DashMap<String, Job>>,
    // Fast-path channel for pending jobs (avoid scanning)
    pending_tx: mpsc::UnboundedSender<String>,
    pending_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<String>>>,
    // Persistent storage
    storage: Arc<ColdStore>,
    // Collection name for jobs
    collection: String,
}

impl JobQueue {
    pub fn new(storage_path: String) -> Result<Self> {
        let storage = Arc::new(ColdStore::new(&storage_path)?);
        let jobs = Arc::new(DashMap::new());
        let (pending_tx, pending_rx) = mpsc::unbounded_channel();

        let queue = Self {
            jobs,
            pending_tx,
            pending_rx: Arc::new(tokio::sync::Mutex::new(pending_rx)),
            storage,
            collection: "__aurora_jobs".to_string(),
        };

        // Load existing jobs from storage
        queue.load_jobs()?;

        Ok(queue)
    }

    /// Load jobs from persistent storage into memory
    fn load_jobs(&self) -> Result<()> {
        let prefix = format!("{}:", self.collection);

        for entry in self.storage.scan_prefix(&prefix) {
            if let Ok((key, value)) = entry
                && let Ok(job) = bincode::deserialize::<Job>(&value)
            {
                if !matches!(job.status, JobStatus::Completed) {
                    let job_id = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
                    self.jobs.insert(job_id.clone(), job);
                    
                    // Re-enqueue pending jobs into fast-path
                    let _ = self.pending_tx.send(job_id);
                }
            }
        }

        Ok(())
    }

    /// Enqueue a new job
    pub async fn enqueue(&self, job: Job) -> Result<String> {
        let job_id = job.id.clone();
        let key = format!("{}:{}", self.collection, job_id);

        // 1. ATOMIC FAST-PATH: Add to in-memory index immediately
        self.jobs.insert(job_id.clone(), job.clone());

        // 2. PERSISTENCE: Write to storage (Crucial for durability)
        // Optimization: In high-throughput scenarios, we could batch these.
        let serialized = bincode::serialize(&job)
            .map_err(|e| AqlError::new(ErrorCode::SerializationError, e.to_string()))?;
        self.storage.set(key, serialized)?;

        // 3. NOTIFY: Wake up waiting workers instantly via channel
        let _ = self.pending_tx.send(job_id.clone());

        Ok(job_id)
    }

    /// Dequeue next job (O(1) via channel instead of O(N) scan)
    pub async fn dequeue(&self) -> Result<Option<Job>> {
        let mut rx = self.pending_rx.lock().await;
        
        while let Some(job_id) = rx.recv().await {
            // Check for shutdown sentinel
            if job_id == "__SHUTDOWN__" {
                return Ok(None);
            }

            // Check if job is still valid and pending
            if let Some(mut job) = self.jobs.get_mut(&job_id) {
                if matches!(job.status, JobStatus::Pending | JobStatus::Failed { .. }) && job.should_run() {
                    // Mark as running
                    job.mark_running();
                    let job_clone = job.clone();
                    drop(job);
                    
                    // Persist state change (Pending -> Running)
                    self.update_job(&job_id, job_clone.clone()).await?;
                    
                    return Ok(Some(job_clone));
                }
            }
        }
        
        Ok(None)
    }

    /// Update job status
    pub async fn update_job(&self, job_id: &str, job: Job) -> Result<()> {
        let key = format!("{}:{}", self.collection, job_id);

        // Update in-memory index first (O(1))
        self.jobs.insert(job_id.to_string(), job.clone());

        // Persist to storage
        let serialized = bincode::serialize(&job)
            .map_err(|e| AqlError::new(ErrorCode::SerializationError, e.to_string()))?;
        self.storage.set(key, serialized)?;

        Ok(())
    }

    /// Shutdown the queue (wakes up all waiting workers)
    pub async fn shutdown(&self) {
        // We use a sentinel string because the receiver is locked in dequeue().
        // Sending this will wake up the worker waiting on rx.recv().await.
        let _ = self.pending_tx.send("__SHUTDOWN__".to_string());
    }

    // Dummy method for backward compatibility with executor.rs notify calls
    pub fn notify_all(&self) {}
    pub async fn notified(&self) {
        // Not used anymore due to channel-based dequeue
    }

    /// Get job by ID
    pub async fn get(&self, job_id: &str) -> Result<Option<Job>> {
        Ok(self.jobs.get(job_id).map(|j| j.clone()))
    }

    /// Get job status
    pub async fn get_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        Ok(self.jobs.get(job_id).map(|j| j.status.clone()))
    }

    /// Remove completed jobs (cleanup)
    pub async fn cleanup_completed(&self) -> Result<usize> {
        let mut removed = 0;
        let to_remove: Vec<String> = self.jobs.iter()
            .filter(|entry| matches!(entry.value().status, JobStatus::Completed))
            .map(|entry| entry.key().clone())
            .collect();

        for job_id in to_remove {
            let key = format!("{}:{}", self.collection, job_id);
            let _ = self.storage.delete(&key);
            self.jobs.remove(&job_id);
            removed += 1;
        }
        Ok(removed)
    }

    /// Get queue statistics
    pub async fn stats(&self) -> Result<super::QueueStats> {
        let mut stats = super::QueueStats {
            pending: 0, running: 0, completed: 0, failed: 0, dead_letter: 0,
        };
        for entry in self.jobs.iter() {
            match &entry.value().status {
                JobStatus::Pending => stats.pending += 1,
                JobStatus::Running => stats.running += 1,
                JobStatus::Completed => stats.completed += 1,
                JobStatus::Failed { .. } => stats.failed += 1,
                JobStatus::DeadLetter { .. } => stats.dead_letter += 1,
            }
        }
        Ok(stats)
    }

    /// Find zombie jobs
    pub async fn find_zombie_jobs(&self) -> Vec<String> {
        self.jobs.iter()
            .filter(|entry| entry.value().is_heartbeat_expired())
            .map(|entry| entry.key().clone())
            .collect()
    }
}
