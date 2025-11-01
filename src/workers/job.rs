use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Job status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed { error: String, retries: u32 },
    DeadLetter { error: String },
}

/// Job priority
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobPriority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// A durable background job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub job_type: String,
    pub payload: HashMap<String, serde_json::Value>,
    pub priority: JobPriority,
    pub status: JobStatus,
    pub max_retries: u32,
    pub retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub timeout_seconds: Option<u64>,
}

impl Job {
    /// Create a new job
    pub fn new(job_type: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            job_type: job_type.into(),
            payload: HashMap::new(),
            priority: JobPriority::Normal,
            status: JobStatus::Pending,
            max_retries: 3,
            retry_count: 0,
            created_at: Utc::now(),
            scheduled_at: None,
            started_at: None,
            completed_at: None,
            timeout_seconds: Some(300), // 5 minutes default
        }
    }

    /// Set job payload
    pub fn with_payload(mut self, payload: HashMap<String, serde_json::Value>) -> Self {
        self.payload = payload;
        self
    }

    /// Add a single payload field
    pub fn add_field(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.payload.insert(key.into(), value);
        self
    }

    /// Set job priority
    pub fn with_priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set max retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Schedule job for later execution
    pub fn scheduled_at(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(at);
        self
    }

    /// Set job timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Check if job should be executed (based on schedule)
    pub fn should_run(&self) -> bool {
        match self.status {
            JobStatus::Pending => {
                if let Some(scheduled) = self.scheduled_at {
                    Utc::now() >= scheduled
                } else {
                    true
                }
            }
            _ => false,
        }
    }

    /// Mark job as running
    pub fn mark_running(&mut self) {
        self.status = JobStatus::Running;
        self.started_at = Some(Utc::now());
    }

    /// Mark job as completed
    pub fn mark_completed(&mut self) {
        self.status = JobStatus::Completed;
        self.completed_at = Some(Utc::now());
    }

    /// Mark job as failed
    pub fn mark_failed(&mut self, error: String) {
        self.retry_count += 1;

        if self.retry_count >= self.max_retries {
            self.status = JobStatus::DeadLetter { error };
        } else {
            self.status = JobStatus::Failed {
                error,
                retries: self.retry_count,
            };
        }
    }

    /// Calculate next retry delay (exponential backoff)
    pub fn next_retry_delay(&self) -> chrono::Duration {
        let base_delay = 5; // 5 seconds
        let delay_seconds = base_delay * 2_i64.pow(self.retry_count);
        chrono::Duration::seconds(delay_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_creation() {
        let job = Job::new("email")
            .add_field("to", serde_json::json!("user@example.com"))
            .add_field("subject", serde_json::json!("Hello"))
            .with_priority(JobPriority::High)
            .with_max_retries(5);

        assert_eq!(job.job_type, "email");
        assert_eq!(job.priority, JobPriority::High);
        assert_eq!(job.max_retries, 5);
        assert_eq!(job.status, JobStatus::Pending);
    }

    #[test]
    fn test_job_should_run() {
        let job = Job::new("test");
        assert!(job.should_run());

        let future_job = Job::new("test")
            .scheduled_at(Utc::now() + chrono::Duration::hours(1));
        assert!(!future_job.should_run());

        let past_job = Job::new("test")
            .scheduled_at(Utc::now() - chrono::Duration::hours(1));
        assert!(past_job.should_run());
    }

    #[test]
    fn test_job_retry_logic() {
        let mut job = Job::new("test").with_max_retries(3);

        job.mark_failed("Error 1".to_string());
        assert!(matches!(job.status, JobStatus::Failed { .. }));
        assert_eq!(job.retry_count, 1);

        job.mark_failed("Error 2".to_string());
        assert_eq!(job.retry_count, 2);

        job.mark_failed("Error 3".to_string());
        assert!(matches!(job.status, JobStatus::DeadLetter { .. }));
        assert_eq!(job.retry_count, 3);
    }

    #[test]
    fn test_exponential_backoff() {
        let mut job = Job::new("test");

        assert_eq!(job.next_retry_delay().num_seconds(), 5);

        job.retry_count = 1;
        assert_eq!(job.next_retry_delay().num_seconds(), 10);

        job.retry_count = 2;
        assert_eq!(job.next_retry_delay().num_seconds(), 20);

        job.retry_count = 3;
        assert_eq!(job.next_retry_delay().num_seconds(), 40);
    }
}
