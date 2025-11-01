use super::job::Job;
use super::queue::JobQueue;
use crate::error::Result;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout};

/// Job handler function type
pub type JobHandler = Arc<
    dyn Fn(Job) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync,
>;

/// Worker configuration
#[derive(Clone)]
pub struct WorkerConfig {
    pub storage_path: String,
    pub concurrency: usize,
    pub poll_interval_ms: u64,
    pub cleanup_interval_seconds: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            storage_path: "./aurora_workers".to_string(),
            concurrency: 4,
            poll_interval_ms: 100,
            cleanup_interval_seconds: 3600, // 1 hour
        }
    }
}

/// Worker executor that processes jobs
pub struct WorkerExecutor {
    queue: Arc<JobQueue>,
    handlers: Arc<RwLock<HashMap<String, JobHandler>>>,
    config: WorkerConfig,
    running: Arc<RwLock<bool>>,
    worker_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

impl WorkerExecutor {
    pub fn new(queue: Arc<JobQueue>, config: WorkerConfig) -> Self {
        Self {
            queue,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            config,
            running: Arc::new(RwLock::new(false)),
            worker_handles: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a job handler
    pub async fn register_handler<F, Fut>(&self, job_type: impl Into<String>, handler: F)
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler = Arc::new(move |job: Job| -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
            Box::pin(handler(job))
        });

        self.handlers.write().await.insert(job_type.into(), handler);
    }

    /// Start the worker executor
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        // Spawn worker tasks
        let mut handles = self.worker_handles.write().await;
        for worker_id in 0..self.config.concurrency {
            let handle = self.spawn_worker(worker_id);
            handles.push(handle);
        }

        // Spawn cleanup task
        let cleanup_handle = self.spawn_cleanup_task();
        handles.push(cleanup_handle);

        Ok(())
    }

    /// Stop the worker executor
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        drop(running);

        // Cancel all worker tasks
        let mut handles = self.worker_handles.write().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        Ok(())
    }

    /// Spawn a worker task
    fn spawn_worker(&self, worker_id: usize) -> JoinHandle<()> {
        let queue = Arc::clone(&self.queue);
        let handlers = Arc::clone(&self.handlers);
        let running = Arc::clone(&self.running);
        let poll_interval = self.config.poll_interval_ms;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(poll_interval));

            loop {
                interval.tick().await;

                if !*running.read().await {
                    break;
                }

                // Try to dequeue a job
                match queue.dequeue().await {
                    Ok(Some(mut job)) => {
                        println!("[Worker {}] Processing job: {} ({})", worker_id, job.id, job.job_type);

                        // Get handler
                        let handlers = handlers.read().await;
                        let handler = handlers.get(&job.job_type);

                        if let Some(handler) = handler {
                            let handler = Arc::clone(handler);
                            drop(handlers);

                            // Execute job with timeout
                            let result = if let Some(timeout_secs) = job.timeout_seconds {
                                timeout(
                                    Duration::from_secs(timeout_secs),
                                    handler(job.clone()),
                                )
                                .await
                            } else {
                                Ok(handler(job.clone()).await)
                            };

                            match result {
                                Ok(Ok(())) => {
                                    job.mark_completed();
                                    println!("[Worker {}] Job completed: {}", worker_id, job.id);
                                }
                                Ok(Err(e)) => {
                                    job.mark_failed(e.to_string());
                                    println!("[Worker {}] Job failed: {} - {}", worker_id, job.id, e);
                                }
                                Err(_) => {
                                    job.mark_failed("Timeout".to_string());
                                    println!("[Worker {}] Job timeout: {}", worker_id, job.id);
                                }
                            }

                            // Update job status
                            let job_id = job.id.clone();
                            let _ = queue.update_job(&job_id, job).await;
                        } else {
                            let job_type = job.job_type.clone();
                            job.mark_failed("No handler registered".to_string());
                            let job_id = job.id.clone();
                            let _ = queue.update_job(&job_id, job).await;
                            println!("[Worker {}] No handler for job type: {}", worker_id, job_type);
                        }
                    }
                    Ok(None) => {
                        // No jobs available
                    }
                    Err(e) => {
                        eprintln!("[Worker {}] Error dequeuing job: {}", worker_id, e);
                    }
                }
            }

            println!("[Worker {}] Stopped", worker_id);
        })
    }

    /// Spawn cleanup task
    fn spawn_cleanup_task(&self) -> JoinHandle<()> {
        let queue = Arc::clone(&self.queue);
        let running = Arc::clone(&self.running);
        let cleanup_interval = self.config.cleanup_interval_seconds;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(cleanup_interval));

            loop {
                interval.tick().await;

                if !*running.read().await {
                    break;
                }

                match queue.cleanup_completed().await {
                    Ok(count) => {
                        if count > 0 {
                            println!("[Cleanup] Removed {} completed jobs", count);
                        }
                    }
                    Err(e) => {
                        eprintln!("[Cleanup] Error: {}", e);
                    }
                }
            }

            println!("[Cleanup] Stopped");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workers::job::{Job, JobStatus};
    use tempfile::TempDir;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_worker_execution() {
        let temp_dir = TempDir::new().unwrap();
        let config = WorkerConfig {
            storage_path: temp_dir.path().to_str().unwrap().to_string(),
            concurrency: 2,
            poll_interval_ms: 50,
            cleanup_interval_seconds: 10, // Short interval for testing
        };

        let queue = Arc::new(JobQueue::new(config.storage_path.clone()).unwrap());
        let executor = WorkerExecutor::new(Arc::clone(&queue), config);

        // Register a test handler
        executor
            .register_handler("test", |_job| async { Ok(()) })
            .await;

        // Start executor
        executor.start().await.unwrap();

        // Enqueue a job
        let job = Job::new("test");
        let job_id = queue.enqueue(job).await.unwrap();

        // Wait for job to complete
        sleep(Duration::from_millis(300)).await;

        // Check job status - it might be completed or already cleaned up
        let status = queue.get_status(&job_id).await.unwrap();
        // Either completed or None (cleaned up) is ok
        assert!(matches!(status, Some(JobStatus::Completed) | None));

        executor.stop().await.unwrap();
    }
}
