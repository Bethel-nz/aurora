use super::job::Job;
use super::queue::JobQueue;
use crate::error::Result;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout};

/// Job handler function type
pub type JobHandler =
    Arc<dyn Fn(Job) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

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
            poll_interval_ms: 10,           // Faster polling fallback
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
    shutdown_tx: Option<broadcast::Sender<()>>,
}

impl WorkerExecutor {
    pub fn new(queue: Arc<JobQueue>, config: WorkerConfig) -> Self {
        Self {
            queue,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            config,
            running: Arc::new(RwLock::new(false)),
            worker_handles: Arc::new(RwLock::new(Vec::new())),
            shutdown_tx: None,
        }
    }

    /// Register a job handler
    pub async fn register_handler<F, Fut>(&self, job_type: impl Into<String>, handler: F)
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler = Arc::new(
            move |job: Job| -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
                Box::pin(handler(job))
            },
        );

        self.handlers.write().await.insert(job_type.into(), handler);
    }

    /// Start the worker executor
    pub async fn start(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        let (tx, _) = broadcast::channel(1);
        self.shutdown_tx = Some(tx.clone());

        // Spawn worker tasks
        let mut handles = self.worker_handles.write().await;
        for worker_id in 0..self.config.concurrency {
            let handle = self.spawn_worker(worker_id, tx.subscribe());
            handles.push(handle);
        }

        // Spawn cleanup task
        let cleanup_handle = self.spawn_cleanup_task(tx.subscribe());
        handles.push(cleanup_handle);

        // Spawn reaper task for zombie job recovery
        let reaper_handle = self.spawn_reaper(tx.subscribe());
        handles.push(reaper_handle);

        Ok(())
    }

    /// Stop the worker executor
    pub async fn stop(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        *running = false;
        drop(running);

        // Send shutdown signal to all tasks
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Also notify queue to wake up any workers stuck in dequeue
        self.queue.shutdown().await;

        // Wait for all worker tasks to finish with a timeout
        let mut handles = self.worker_handles.write().await;
        for handle in handles.drain(..) {
            // We give each task a small window to exit gracefully
            let _ = timeout(Duration::from_millis(500), handle).await;
        }

        Ok(())
    }

    /// Spawn a worker task
    fn spawn_worker(
        &self,
        worker_id: usize,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        let queue = Arc::clone(&self.queue);
        let handlers = Arc::clone(&self.handlers);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            loop {
                // Check if we should stop
                if !*running.read().await {
                    break;
                }

                // HIGH-PERFORMANCE DEQUEUE (Channel-based, O(1))
                // Use select to allow immediate interruption during dequeue
                let job_opt = tokio::select! {
                    res = queue.dequeue() => {
                        match res {
                            Ok(Some(job)) => Some(job),
                            Ok(None) => return, // Channel closed
                            Err(e) => {
                                eprintln!("[Worker {}] Dequeue Error: {}", worker_id, e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                None
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                };

                if let Some(mut job) = job_opt {
                    // Get handler
                    let handlers_guard = handlers.read().await;
                    let handler = handlers_guard.get(&job.job_type).cloned();
                    drop(handlers_guard);

                    if let Some(handler) = handler {
                        let result = if let Some(timeout_secs) = job.timeout_seconds {
                            timeout(Duration::from_secs(timeout_secs), handler(job.clone())).await
                        } else {
                            Ok(handler(job.clone()).await)
                        };

                        match result {
                            Ok(Ok(())) => {
                                job.mark_completed();
                            }
                            Ok(Err(e)) => {
                                job.mark_failed(e.to_string());
                            }
                            Err(_) => {
                                job.mark_failed("Timeout".to_string());
                            }
                        }

                        let job_id = job.id.clone();
                        let _ = queue.update_job(&job_id, job).await;
                    } else {
                        job.mark_failed("No handler registered".to_string());
                        let job_id = job.id.clone();
                        let _ = queue.update_job(&job_id, job).await;
                    }
                }
            }
        })
    }

    /// Spawn cleanup task
    fn spawn_cleanup_task(&self, mut shutdown_rx: broadcast::Receiver<()>) -> JoinHandle<()> {
        let queue = Arc::clone(&self.queue);
        let cleanup_interval = self.config.cleanup_interval_seconds;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(cleanup_interval));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let _ = queue.cleanup_completed().await;
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        })
    }

    /// Spawn the Reaper task for zombie job recovery
    fn spawn_reaper(&self, mut shutdown_rx: broadcast::Receiver<()>) -> JoinHandle<()> {
        let queue = Arc::clone(&self.queue);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let zombies = queue.find_zombie_jobs().await;
                        for job_id in zombies {
                            if let Ok(Some(mut job)) = queue.get(&job_id).await {
                                job.status = super::job::JobStatus::Pending;
                                job.retry_count += 1;
                                job.touch();
                                let _ = queue.update_job(&job_id, job).await;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        })
    }
}
