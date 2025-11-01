use super::job::{Job, JobStatus};
use crate::error::{AuroraError, Result};
use crate::storage::ColdStore;
use dashmap::DashMap;
use std::sync::Arc;

/// Persistent job queue
pub struct JobQueue {
    // In-memory index for fast lookups
    jobs: Arc<DashMap<String, Job>>,
    // Persistent storage
    storage: Arc<ColdStore>,
    // Collection name for jobs
    collection: String,
}

impl JobQueue {
    pub fn new(storage_path: String) -> Result<Self> {
        let storage = Arc::new(ColdStore::new(&storage_path)?);
        let jobs = Arc::new(DashMap::new());

        let queue = Self {
            jobs,
            storage,
            collection: "__aurora_jobs".to_string(),
        };

        // Load existing jobs from storage
        queue.load_jobs()?;

        Ok(queue)
    }

    /// Load jobs from persistent storage into memory
    fn load_jobs(&self) -> Result<()> {
        // Scan all jobs from storage
        let prefix = format!("{}:", self.collection);

        for entry in self.storage.scan_prefix(&prefix) {
            if let Ok((key, value)) = entry {
                if let Ok(job) = bincode::deserialize::<Job>(&value) {
                    // Only load jobs that aren't completed
                    if !matches!(job.status, JobStatus::Completed) {
                        let job_id = key.strip_prefix(&prefix).unwrap_or(&key).to_string();
                        self.jobs.insert(job_id, job);
                    }
                }
            }
        }

        Ok(())
    }

    /// Enqueue a new job
    pub async fn enqueue(&self, job: Job) -> Result<String> {
        let job_id = job.id.clone();
        let key = format!("{}:{}", self.collection, job_id);

        // Persist to storage
        let serialized =
            bincode::serialize(&job).map_err(|e| AuroraError::SerializationError(e.to_string()))?;
        self.storage.set(key, serialized)?;

        // Add to in-memory index
        self.jobs.insert(job_id.clone(), job);

        Ok(job_id)
    }

    /// Dequeue next job (highest priority, oldest first)
    pub async fn dequeue(&self) -> Result<Option<Job>> {
        let mut best_job: Option<(String, Job)> = None;

        for entry in self.jobs.iter() {
            let job = entry.value();

            // Skip jobs that shouldn't run yet
            if !job.should_run() {
                continue;
            }

            // Skip non-pending jobs
            if !matches!(job.status, JobStatus::Pending | JobStatus::Failed { .. }) {
                continue;
            }

            // Find highest priority job
            match &best_job {
                None => {
                    best_job = Some((entry.key().clone(), job.clone()));
                }
                Some((_, current_best)) => {
                    // Higher priority wins
                    if job.priority > current_best.priority {
                        best_job = Some((entry.key().clone(), job.clone()));
                    }
                    // Same priority, older job wins
                    else if job.priority == current_best.priority
                        && job.created_at < current_best.created_at
                    {
                        best_job = Some((entry.key().clone(), job.clone()));
                    }
                }
            }
        }

        if let Some((job_id, mut job)) = best_job {
            // Mark as running
            job.mark_running();
            self.update_job(&job_id, job.clone()).await?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    /// Update job status
    pub async fn update_job(&self, job_id: &str, job: Job) -> Result<()> {
        let key = format!("{}:{}", self.collection, job_id);

        // Persist to storage
        let serialized =
            bincode::serialize(&job).map_err(|e| AuroraError::SerializationError(e.to_string()))?;
        self.storage.set(key, serialized)?;

        // Update in-memory index
        self.jobs.insert(job_id.to_string(), job);

        Ok(())
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

        let to_remove: Vec<String> = self
            .jobs
            .iter()
            .filter(|entry| matches!(entry.value().status, JobStatus::Completed))
            .map(|entry| entry.key().clone())
            .collect();

        for job_id in to_remove {
            let key = format!("{}:{}", self.collection, job_id);
            self.storage.delete(&key)?;
            self.jobs.remove(&job_id);
            removed += 1;
        }

        Ok(removed)
    }

    /// Get queue statistics
    pub async fn stats(&self) -> Result<super::QueueStats> {
        let mut stats = super::QueueStats {
            pending: 0,
            running: 0,
            completed: 0,
            failed: 0,
            dead_letter: 0,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workers::job::{Job, JobPriority};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_queue_enqueue_dequeue() {
        let temp_dir = TempDir::new().unwrap();
        let queue = JobQueue::new(temp_dir.path().to_str().unwrap().to_string()).unwrap();

        let job = Job::new("test");
        let job_id = queue.enqueue(job).await.unwrap();

        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().id, job_id);
    }

    #[tokio::test]
    async fn test_queue_priority_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let queue = JobQueue::new(temp_dir.path().to_str().unwrap().to_string()).unwrap();

        queue
            .enqueue(Job::new("low").with_priority(JobPriority::Low))
            .await
            .unwrap();
        queue
            .enqueue(Job::new("high").with_priority(JobPriority::High))
            .await
            .unwrap();
        queue
            .enqueue(Job::new("critical").with_priority(JobPriority::Critical))
            .await
            .unwrap();

        let first = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(first.job_type, "critical");

        let second = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(second.job_type, "high");

        let third = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(third.job_type, "low");
    }

    #[tokio::test]
    async fn test_queue_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();

        // Create queue and add job
        {
            let queue = JobQueue::new(path.clone()).unwrap();
            queue.enqueue(Job::new("persistent")).await.unwrap();
        }

        // Reopen queue and verify job is still there
        {
            let queue = JobQueue::new(path).unwrap();
            let stats = queue.stats().await.unwrap();
            assert_eq!(stats.pending, 1);
        }
    }
}
