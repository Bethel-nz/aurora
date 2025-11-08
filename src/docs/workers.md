# Aurora DB Durable Workers

This guide covers Aurora's durable worker system for background job processing with automatic retries and persistence.

## Overview

Durable workers provide a reliable way to execute background tasks asynchronously. Unlike PubSub events (which are ephemeral), worker jobs are persisted and survive database restarts.

## Key Features

- **Persistent**: Jobs are stored in the database and survive restarts
- **Automatic Retries**: Failed jobs are automatically retried with exponential backoff
- **Priority Queues**: Jobs can have different priorities
- **Scheduled Execution**: Jobs can be scheduled for future execution
- **ACID Guarantees**: Job state changes are transactional

## Basic Usage

### Creating and Starting a Worker

```rust
use aurora_db::workers::{WorkerExecutor, JobHandler, Job, JobResult};
use async_trait::async_trait;

// Define a job handler
struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    async fn handle(&self, job: &Job) -> JobResult {
        // Extract job payload
        let email = job.payload.get("email")
            .and_then(|v| v.as_str())
            .ok_or("Missing email")?;

        let subject = job.payload.get("subject")
            .and_then(|v| v.as_str())
            .ok_or("Missing subject")?;

        // Send email
        send_email(email, subject).await?;

        Ok(())
    }
}

// Start worker executor
let mut executor = WorkerExecutor::new(db.clone(), 4); // 4 concurrent workers
executor.register_handler("send_email", Box::new(EmailHandler));
executor.start().await?;
```

### Enqueueing Jobs

```rust
use std::collections::HashMap;
use aurora_db::workers::Priority;

// Simple job
db.enqueue_job(
    "send_email",
    {
        let mut payload = HashMap::new();
        payload.insert("email".to_string(), json!("user@example.com"));
        payload.insert("subject".to_string(), json!("Welcome!"));
        payload
    },
    None, // Run immediately
    Priority::Normal,
).await?;

// High priority job
db.enqueue_job(
    "send_urgent_alert",
    payload,
    None,
    Priority::High,
).await?;

// Scheduled job (run in 1 hour)
let scheduled_time = chrono::Utc::now() + chrono::Duration::hours(1);
db.enqueue_job(
    "send_reminder",
    payload,
    Some(scheduled_time),
    Priority::Normal,
).await?;
```

## Job Priorities

Jobs are processed in priority order:

```rust
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}
```

Higher priority jobs are always processed before lower priority jobs.

## Retry Logic

Failed jobs are automatically retried with exponential backoff:

```rust
pub struct Job {
    pub id: String,
    pub job_type: String,
    pub payload: HashMap<String, serde_json::Value>,
    pub max_retries: u32,      // Default: 3
    pub retry_count: u32,       // Current attempt
    pub scheduled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub status: JobStatus,
    pub priority: Priority,
}
```

### Retry Schedule

- **Attempt 1**: Immediate
- **Attempt 2**: 5 seconds delay
- **Attempt 3**: 25 seconds delay (5 * 5)
- **Attempt 4**: 125 seconds delay (25 * 5)

After `max_retries`, the job moves to `Failed` status.

### Custom Retry Configuration

```rust
let mut job = Job::new("risky_operation", payload);
job.max_retries = 5; // Allow 5 retries instead of default 3
db.enqueue_job_custom(job).await?;
```

## Job Status Lifecycle

```rust
pub enum JobStatus {
    Pending,    // Waiting to be processed
    Running,    // Currently being processed
    Completed,  // Successfully finished
    Failed,     // All retries exhausted
    Scheduled,  // Waiting for scheduled time
}
```

## Advanced Examples

### 1. Image Processing Pipeline

```rust
struct ImageProcessor;

#[async_trait]
impl JobHandler for ImageProcessor {
    async fn handle(&self, job: &Job) -> JobResult {
        let image_url = job.payload.get("url")
            .and_then(|v| v.as_str())
            .ok_or("Missing image URL")?;

        // Download image
        let image_data = download_image(image_url).await?;

        // Generate thumbnails
        let thumbnail = create_thumbnail(&image_data, 200, 200)?;
        let preview = create_thumbnail(&image_data, 800, 600)?;

        // Upload to storage
        let thumbnail_url = upload_to_s3(&thumbnail).await?;
        let preview_url = upload_to_s3(&preview).await?;

        // Update database with results
        db.insert_into("processed_images", vec![
            ("original_url", Value::String(image_url.to_string())),
            ("thumbnail_url", Value::String(thumbnail_url)),
            ("preview_url", Value::String(preview_url)),
        ]).await?;

        Ok(())
    }
}

// Enqueue image processing
db.enqueue_job(
    "process_image",
    {
        let mut payload = HashMap::new();
        payload.insert("url".to_string(), json!("https://example.com/photo.jpg"));
        payload
    },
    None,
    Priority::Normal,
).await?;
```

### 2. Batch Data Export

```rust
struct DataExporter;

#[async_trait]
impl JobHandler for DataExporter {
    async fn handle(&self, job: &Job) -> JobResult {
        let collection = job.payload.get("collection")
            .and_then(|v| v.as_str())
            .ok_or("Missing collection name")?;

        let format = job.payload.get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("json");

        // Fetch all documents
        let documents = db.query(collection)
            .collect()
            .await?;

        // Export based on format
        let export_path = match format {
            "json" => db.export_as_json(collection, &format!("./exports/{}.json", collection))?,
            "csv" => db.export_as_csv(collection, &format!("./exports/{}.csv", collection))?,
            _ => return Err("Unsupported format".into()),
        };

        // Send notification
        db.enqueue_job(
            "send_email",
            {
                let mut payload = HashMap::new();
                payload.insert("email".to_string(),
                    job.payload.get("notify_email").unwrap().clone());
                payload.insert("subject".to_string(),
                    json!("Export complete"));
                payload.insert("body".to_string(),
                    json!(format!("Your export is ready: {}", export_path)));
                payload
            },
            None,
            Priority::High,
        ).await?;

        Ok(())
    }
}
```

### 3. Scheduled Maintenance Tasks

```rust
// Schedule daily cleanup (run at 2 AM)
let next_run = chrono::Utc::now()
    .date()
    .and_hms_opt(2, 0, 0)
    .unwrap()
    + chrono::Duration::days(1);

db.enqueue_job(
    "cleanup_old_logs",
    {
        let mut payload = HashMap::new();
        payload.insert("days_to_keep".to_string(), json!(30));
        payload
    },
    Some(next_run),
    Priority::Low,
).await?;
```

### 4. Webhook Delivery

```rust
struct WebhookHandler;

#[async_trait]
impl JobHandler for WebhookHandler {
    async fn handle(&self, job: &Job) -> JobResult {
        let url = job.payload.get("url")
            .and_then(|v| v.as_str())
            .ok_or("Missing webhook URL")?;

        let payload = job.payload.get("payload")
            .ok_or("Missing webhook payload")?;

        // Send HTTP POST request
        let client = reqwest::Client::new();
        let response = client
            .post(url)
            .json(payload)
            .timeout(Duration::from_secs(30))
            .send()
            .await?;

        // Check response status
        if !response.status().is_success() {
            return Err(format!("Webhook failed with status: {}", response.status()).into());
        }

        Ok(())
    }
}

// Enqueue webhook (will retry on failure)
db.enqueue_job(
    "send_webhook",
    {
        let mut payload = HashMap::new();
        payload.insert("url".to_string(), json!("https://api.example.com/webhook"));
        payload.insert("payload".to_string(), json!({
            "event": "user.created",
            "user_id": "12345"
        }));
        payload
    },
    None,
    Priority::High,
).await?;
```

## Monitoring Jobs

### Check Job Status

```rust
// Get job by ID
let job = db.get_job(&job_id).await?;
println!("Job status: {:?}", job.status);
println!("Retry count: {}/{}", job.retry_count, job.max_retries);
```

### List Pending Jobs

```rust
// Query all pending jobs
let pending = db.query("_jobs")
    .filter(|f| f.eq("status", "Pending"))
    .collect()
    .await?;

println!("Pending jobs: {}", pending.len());
```

### Failed Jobs

```rust
// Get failed jobs for manual intervention
let failed = db.query("_jobs")
    .filter(|f| f.eq("status", "Failed"))
    .collect()
    .await?;

for job in failed {
    log::error!("Job failed: {:?}", job);
}
```

## Worker Configuration

### Concurrency

Control how many jobs run simultaneously:

```rust
// 1 worker = sequential processing
let executor = WorkerExecutor::new(db.clone(), 1);

// 10 workers = up to 10 jobs in parallel
let executor = WorkerExecutor::new(db.clone(), 10);
```

### Multiple Worker Types

Run different workers with different configurations:

```rust
// Fast workers for quick tasks
let mut fast_executor = WorkerExecutor::new(db.clone(), 8);
fast_executor.register_handler("send_email", Box::new(EmailHandler));
fast_executor.start().await?;

// Slow workers for heavy tasks
let mut slow_executor = WorkerExecutor::new(db.clone(), 2);
slow_executor.register_handler("process_video", Box::new(VideoProcessor));
slow_executor.start().await?;
```

## Best Practices

1. **Idempotency**: Jobs may be retried. Ensure handlers can safely run multiple times.

2. **Timeout Handling**: Long-running jobs should have internal timeouts to avoid blocking workers.

3. **Payload Size**: Keep payloads small. Store large data separately and reference by ID.

4. **Error Handling**: Return meaningful errors to help with debugging failed jobs.

5. **Monitoring**: Regularly check for failed jobs and alert on accumulation.

```rust
// Good: Idempotent handler
async fn handle(&self, job: &Job) -> JobResult {
    let order_id = job.payload.get("order_id")?;

    // Check if already processed
    if is_order_processed(order_id).await? {
        return Ok(()); // Already done, skip
    }

    // Process order
    process_order(order_id).await?;

    // Mark as processed
    mark_order_processed(order_id).await?;

    Ok(())
}
```

## Limitations

- **Single Instance**: Workers run on the same instance as the database. For distributed workers, use external queue systems.
- **No Dependencies**: Jobs are independent. For job chains, enqueue subsequent jobs from handlers.
- **Memory Bound**: Job queue size is limited by available memory.

For real-time event processing without persistence, see the [PubSub System](./pubsub.md) documentation.
