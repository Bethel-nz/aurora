// Durable Workers - Background job processing with persistence
//
// Provides a reliable background job system with:
// - Job persistence across restarts
// - Retry logic with exponential backoff
// - Job scheduling and delayed execution
// - Dead letter queue for failed jobs
// - Job status tracking

pub mod executor;
pub mod job;
pub mod queue;

pub use executor::{WorkerConfig, WorkerExecutor};
pub use job::{Job, JobPriority, JobStatus};
pub use queue::JobQueue;

use crate::error::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Main worker system for durable background job processing
///
/// Provides a reliable background job system with job persistence, retry logic,
/// scheduling, and status tracking. Perfect for email sending, image processing,
/// report generation, and any task that should survive restarts.
///
/// # Features
/// - Job persistence across restarts
/// - Exponential backoff retry logic
/// - Job scheduling and delayed execution
/// - Dead letter queue for failed jobs
/// - Priority-based job execution
/// - Concurrent job processing
/// - Job status tracking
///
/// # Examples
///
/// ```
/// use aurora_db::workers::{WorkerSystem, WorkerConfig, Job, JobPriority};
/// use serde_json::json;
///
/// // Create worker system
/// let config = WorkerConfig {
///     storage_path: "./workers.db".to_string(),
///     max_concurrent_jobs: 10,
///     poll_interval_ms: 100,
/// };
///
/// let workers = WorkerSystem::new(config)?;
///
/// // Register job handlers
/// workers.register_handler("send_email", |job| async move {
///     let to = job.payload.get("to").and_then(|v| v.as_str()).unwrap();
///     let subject = job.payload.get("subject").and_then(|v| v.as_str()).unwrap();
///
///     // Send email
///     send_email(to, subject).await?;
///     Ok(())
/// }).await;
///
/// // Start processing jobs
/// workers.start().await?;
///
/// // Enqueue a job
/// let job = Job::new("send_email")
///     .add_field("to", json!("user@example.com"))
///     .add_field("subject", json!("Welcome!"))
///     .with_priority(JobPriority::High);
///
/// let job_id = workers.enqueue(job).await?;
/// println!("Enqueued job: {}", job_id);
/// ```
pub struct WorkerSystem {
    queue: Arc<JobQueue>,
    executor: Arc<RwLock<WorkerExecutor>>,
}

impl WorkerSystem {
    /// Create a new worker system
    ///
    /// # Arguments
    /// * `config` - Worker configuration including storage path and concurrency settings
    ///
    /// # Examples
    ///
    /// ```
    /// let config = WorkerConfig {
    ///     storage_path: "./jobs.db".to_string(),
    ///     max_concurrent_jobs: 5,
    ///     poll_interval_ms: 100,
    /// };
    ///
    /// let workers = WorkerSystem::new(config)?;
    /// ```
    pub fn new(config: WorkerConfig) -> Result<Self> {
        let queue = Arc::new(JobQueue::new(config.storage_path.clone())?);
        let executor = Arc::new(RwLock::new(WorkerExecutor::new(Arc::clone(&queue), config)));

        Ok(Self { queue, executor })
    }

    /// Start the worker system
    ///
    /// Begins processing jobs from the queue. Jobs are executed concurrently
    /// based on the `max_concurrent_jobs` configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// let workers = WorkerSystem::new(config)?;
    ///
    /// // Register handlers first
    /// workers.register_handler("task", handler).await;
    ///
    /// // Then start processing
    /// workers.start().await?;
    /// ```
    pub async fn start(&self) -> Result<()> {
        self.executor.write().await.start().await
    }

    /// Stop the worker system gracefully
    ///
    /// Waits for currently running jobs to complete before shutting down.
    /// No new jobs will be picked up after calling this method.
    ///
    /// # Examples
    ///
    /// ```
    /// // Graceful shutdown
    /// workers.stop().await?;
    /// ```
    pub async fn stop(&self) -> Result<()> {
        self.executor.write().await.stop().await
    }

    /// Enqueue a new job for processing
    ///
    /// Adds a job to the queue. It will be executed by a worker when available,
    /// respecting priority and scheduling constraints.
    ///
    /// # Arguments
    /// * `job` - The job to enqueue
    ///
    /// # Returns
    /// The unique job ID for tracking status
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::json;
    ///
    /// // Simple job
    /// let job = Job::new("send_welcome_email")
    ///     .add_field("user_id", json!("123"))
    ///     .add_field("email", json!("user@example.com"));
    ///
    /// let job_id = workers.enqueue(job).await?;
    ///
    /// // High priority job
    /// let urgent = Job::new("process_payment")
    ///     .add_field("amount", json!(99.99))
    ///     .with_priority(JobPriority::Critical)
    ///     .with_timeout(30); // 30 seconds
    ///
    /// workers.enqueue(urgent).await?;
    ///
    /// // Scheduled job (runs in 1 hour)
    /// let scheduled = Job::new("send_reminder")
    ///     .add_field("message", json!("Meeting in 1 hour"))
    ///     .scheduled_at(chrono::Utc::now() + chrono::Duration::hours(1));
    ///
    /// workers.enqueue(scheduled).await?;
    /// ```
    pub async fn enqueue(&self, job: Job) -> Result<String> {
        self.queue.enqueue(job).await
    }

    /// Get job status
    ///
    /// Retrieves the current status of a job by its ID.
    ///
    /// # Arguments
    /// * `job_id` - The job ID returned from `enqueue()`
    ///
    /// # Returns
    /// - `Some(JobStatus)` if job exists
    /// - `None` if job not found
    ///
    /// # Examples
    ///
    /// ```
    /// let job_id = workers.enqueue(job).await?;
    ///
    /// // Check status later
    /// if let Some(status) = workers.get_status(&job_id).await? {
    ///     match status {
    ///         JobStatus::Pending => println!("Waiting to run"),
    ///         JobStatus::Running => println!("Currently executing"),
    ///         JobStatus::Completed => println!("Done!"),
    ///         JobStatus::Failed { error, retries } => {
    ///             println!("Failed: {} (retries: {})", error, retries);
    ///         },
    ///         JobStatus::DeadLetter { error } => {
    ///             println!("Permanently failed: {}", error);
    ///         },
    ///     }
    /// }
    /// ```
    pub async fn get_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        self.queue.get_status(job_id).await
    }

    /// Get queue statistics
    ///
    /// Returns counts of jobs in various states for monitoring.
    ///
    /// # Returns
    /// `QueueStats` with pending, running, completed, failed, and dead letter counts
    ///
    /// # Examples
    ///
    /// ```
    /// let stats = workers.stats().await?;
    ///
    /// println!("Queue status:");
    /// println!("  Pending: {}", stats.pending);
    /// println!("  Running: {}", stats.running);
    /// println!("  Completed: {}", stats.completed);
    /// println!("  Failed: {}", stats.failed);
    /// println!("  Dead letter: {}", stats.dead_letter);
    ///
    /// // Alert on high failure rate
    /// let total = stats.completed + stats.failed;
    /// if total > 0 {
    ///     let failure_rate = stats.failed as f64 / total as f64;
    ///     if failure_rate > 0.10 {
    ///         alert!("High job failure rate: {:.1}%", failure_rate * 100.0);
    ///     }
    /// }
    /// ```
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
