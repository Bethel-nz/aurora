// Durable Workers - Background job processing with persistence
//
// Provides a reliable background job system with:
// - Job persistence across restarts
// - Retry logic with exponential backoff
// - Job scheduling and delayed execution
// - Dead letter queue for failed jobs
// - Job status tracking

pub mod job;
pub mod queue;
pub mod executor;

pub use job::{Job, JobStatus, JobPriority};
pub use queue::JobQueue;
pub use executor::{WorkerExecutor, WorkerConfig};

use crate::error::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Main worker system
pub struct WorkerSystem {
    queue: Arc<JobQueue>,
    executor: Arc<RwLock<WorkerExecutor>>,
}

impl WorkerSystem {
    pub fn new(config: WorkerConfig) -> Result<Self> {
        let queue = Arc::new(JobQueue::new(config.storage_path.clone())?);
        let executor = Arc::new(RwLock::new(WorkerExecutor::new(
            Arc::clone(&queue),
            config,
        )));

        Ok(Self { queue, executor })
    }

    /// Start the worker system
    pub async fn start(&self) -> Result<()> {
        self.executor.write().await.start().await
    }

    /// Stop the worker system gracefully
    pub async fn stop(&self) -> Result<()> {
        self.executor.write().await.stop().await
    }

    /// Enqueue a new job
    pub async fn enqueue(&self, job: Job) -> Result<String> {
        self.queue.enqueue(job).await
    }

    /// Get job status
    pub async fn get_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        self.queue.get_status(job_id).await
    }

    /// Get queue statistics
    pub async fn stats(&self) -> Result<QueueStats> {
        self.queue.stats().await
    }
}

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub pending: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub dead_letter: usize,
}
