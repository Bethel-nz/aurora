use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Job status tracking the lifecycle of a background job
///
/// Jobs progress through states: Pending → Running → Completed/Failed → DeadLetter
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    /// Job is queued and waiting to be processed
    Pending,
    /// Job is currently being executed by a worker
    Running,
    /// Job completed successfully
    Completed,
    /// Job failed but can be retried
    Failed { error: String, retries: u32 },
    /// Job permanently failed after max retries
    DeadLetter { error: String },
}

/// Job priority for execution order
///
/// Higher priority jobs are executed first. Critical > High > Normal > Low.
///
/// # Examples
///
/// ```
/// use aurora_db::workers::{Job, JobPriority};
///
/// // Time-sensitive payment
/// let payment = Job::new("process_payment")
///     .with_priority(JobPriority::Critical);
///
/// // User-facing task
/// let notification = Job::new("send_notification")
///     .with_priority(JobPriority::High);
///
/// // Background cleanup
/// let cleanup = Job::new("cleanup_temp_files")
///     .with_priority(JobPriority::Low);
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobPriority {
    /// Low priority (value: 1) - background tasks, cleanup
    Low = 1,
    /// Normal priority (value: 2) - default for most jobs
    Normal = 2,
    /// High priority (value: 3) - user-facing operations
    High = 3,
    /// Critical priority (value: 4) - payments, security
    Critical = 4,
}

/// A durable background job
///
/// Represents a unit of work that can be queued, scheduled, retried, and tracked.
/// Jobs are persisted to disk and survive restarts. Use the builder pattern to
/// configure job properties.
///
/// # Examples
///
/// ```
/// use aurora_db::workers::{Job, JobPriority};
/// use serde_json::json;
///
/// // Simple job
/// let job = Job::new("send_email")
///     .add_field("to", json!("user@example.com"))
///     .add_field("subject", json!("Welcome!"));
///
/// // Job with all options
/// let job = Job::new("process_video")
///     .add_field("video_id", json!("vid-123"))
///     .add_field("resolution", json!("1080p"))
///     .with_priority(JobPriority::High)
///     .with_max_retries(5)
///     .with_timeout(600) // 10 minutes
///     .scheduled_at(chrono::Utc::now() + chrono::Duration::hours(1));
/// ```
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
    ///
    /// Creates a job with default settings:
    /// - Priority: Normal
    /// - Max retries: 3
    /// - Timeout: 5 minutes
    /// - Status: Pending
    ///
    /// # Arguments
    /// * `job_type` - Type identifier for routing to the correct handler
    ///
    /// # Examples
    ///
    /// ```
    /// // Create different job types
    /// let email = Job::new("send_email");
    /// let image = Job::new("resize_image");
    /// let report = Job::new("generate_report");
    /// ```
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

    /// Set job payload from a HashMap
    ///
    /// Replaces the entire payload with the provided HashMap.
    /// For adding individual fields, use `add_field()` instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use serde_json::json;
    ///
    /// let mut payload = HashMap::new();
    /// payload.insert("user_id".to_string(), json!("123"));
    /// payload.insert("amount".to_string(), json!(99.99));
    ///
    /// let job = Job::new("process_payment")
    ///     .with_payload(payload);
    /// ```
    pub fn with_payload(mut self, payload: HashMap<String, serde_json::Value>) -> Self {
        self.payload = payload;
        self
    }

    /// Add a single field to the job payload
    ///
    /// Use this for building the payload incrementally.
    /// Can be chained multiple times.
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::json;
    ///
    /// let job = Job::new("send_email")
    ///     .add_field("to", json!("user@example.com"))
    ///     .add_field("subject", json!("Welcome!"))
    ///     .add_field("template", json!("welcome_email"))
    ///     .add_field("vars", json!({"name": "Alice"}));
    /// ```
    pub fn add_field(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.payload.insert(key.into(), value);
        self
    }

    /// Set job priority
    ///
    /// Higher priority jobs are executed before lower priority ones.
    /// Default is `JobPriority::Normal`.
    ///
    /// # Examples
    ///
    /// ```
    /// // Critical - payments, security operations
    /// let payment = Job::new("charge_card")
    ///     .with_priority(JobPriority::Critical);
    ///
    /// // High - user-facing operations
    /// let notification = Job::new("push_notification")
    ///     .with_priority(JobPriority::High);
    ///
    /// // Low - background cleanup, analytics
    /// let cleanup = Job::new("clean_old_logs")
    ///     .with_priority(JobPriority::Low);
    /// ```
    pub fn with_priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set maximum retry attempts
    ///
    /// If a job fails, it will be retried up to this many times with
    /// exponential backoff. Default is 3 retries.
    ///
    /// # Examples
    ///
    /// ```
    /// // Network operation - retry more
    /// let api_call = Job::new("fetch_api_data")
    ///     .with_max_retries(5);
    ///
    /// // Critical operation - don't retry
    /// let one_time = Job::new("send_invoice")
    ///     .with_max_retries(0);
    ///
    /// // Flaky operation - retry extensively
    /// let external = Job::new("third_party_webhook")
    ///     .with_max_retries(10);
    /// ```
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Schedule job for later execution
    ///
    /// Job will not be executed until the specified time.
    /// Useful for reminders, scheduled reports, delayed notifications.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::{Utc, Duration};
    ///
    /// // Run in 1 hour
    /// let reminder = Job::new("send_reminder")
    ///     .add_field("message", json!("Meeting starts soon"))
    ///     .scheduled_at(Utc::now() + Duration::hours(1));
    ///
    /// // Run at specific time
    /// let report = Job::new("daily_report")
    ///     .scheduled_at(Utc::now().date_naive().and_hms_opt(9, 0, 0).unwrap());
    ///
    /// // Delayed retry pattern
    /// let retry = Job::new("retry_failed_upload")
    ///     .scheduled_at(Utc::now() + Duration::minutes(30));
    /// ```
    pub fn scheduled_at(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(at);
        self
    }

    /// Set job execution timeout
    ///
    /// Job will be terminated if it runs longer than this.
    /// Default is 300 seconds (5 minutes).
    ///
    /// # Examples
    ///
    /// ```
    /// // Quick task
    /// let quick = Job::new("send_sms")
    ///     .with_timeout(30); // 30 seconds
    ///
    /// // Long-running task
    /// let video = Job::new("transcode_video")
    ///     .with_timeout(3600); // 1 hour
    ///
    /// // Very long task
    /// let batch = Job::new("process_millions_of_records")
    ///     .with_timeout(7200); // 2 hours
    /// ```
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

        let future_job = Job::new("test").scheduled_at(Utc::now() + chrono::Duration::hours(1));
        assert!(!future_job.should_run());

        let past_job = Job::new("test").scheduled_at(Utc::now() - chrono::Duration::hours(1));
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
