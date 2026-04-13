# Aurora DB Durable Workers Guide

Aurora includes a built-in, persistent job queue system. This allows you to offload time-consuming tasks to the background while ensuring they survive system restarts and are retried automatically on failure.

## 1. Core Architecture

The worker system consists of three main components:
-   **Job Queue**: A persistent collection in Aurora that stores pending, active, and failed jobs.
-   **Handlers**: Logic defined in Rust that knows how to process a specific "job type."
-   **Executor**: A background system that pulls jobs from the queue based on priority and schedules them for execution across a thread pool.

## 2. Enabling the System (Rust)

To use workers, you must enable them in your initial configuration.

```rust
let config = AuroraConfig {
    workers_enabled: true,   // Critical: enables the system
    worker_threads: 4,       // Parallel processing threads
    ..Default::default()
};
let db = Aurora::with_config(config).await?;
```

## 3. Registering Handlers

A handler is any struct that implements the `JobHandler` trait.

```rust
use aurora_db::workers::{Job, JobHandler, JobResult};
use async_trait::async_trait;

struct ImageProcessor;

#[async_trait]
impl JobHandler for ImageProcessor {
    async fn handle(&self, job: &Job) -> JobResult {
        let path = job.payload["path"].as_str().unwrap();
        println!("Processing image at {}...", path);
        // ... heavy processing logic ...
        Ok(())
    }
}

// Register the handler with the database's worker system
db.workers().unwrap().register_handler("process_image", Box::new(ImageProcessor));
```

## 4. Enqueuing Jobs

### Via AQL
```graphql
mutation {
    enqueueJob(
        type: "process_image",
        payload: { path: "/uploads/cat.jpg" },
        priority: NORMAL,
        retries: 5
    ) {
        jobId
    }
}
```

### Via Rust API
```rust
db.enqueue_job("process_image", object!({ "path": "/uploads/dog.png" }), 0).await?;
```

## 5. Automation Handlers (`on` events)

Aurora can automatically enqueue jobs in response to database events. This is similar to "Triggers" in traditional databases but executes asynchronously in the background.

```graphql
# When a new product is added, automatically generate its thumbnails
define handler "auto_thumbnail" {
    on: "insert:products",
    action: {
        enqueueJob(
            type: "process_image",
            payload: { 
                product_id: "${id}",
                path: "${image_url}"
            }
        )
    }
}
```

## 6. Priority & Retries

### Priority Levels
Jobs are processed in order of priority:
1.  **CRITICAL**: Processed immediately, ahead of all others.
2.  **HIGH**: Priority tasks.
3.  **NORMAL**: Default level.
4.  **LOW**: Background/maintenance tasks.

### Error Handling & Retries
-   If a handler returns `Ok(())`, the job is marked as `COMPLETED`.
-   If it returns `Err(e)`, Aurora increments the retry counter.
-   If `retries` < `max_retries`, the job is scheduled for a **Backoff Retry** (the delay increases after each failure).
-   If `retries` reaches `max_retries`, the job is marked as `FAILED`.

## 7. Monitoring Jobs

Since jobs are just documents in a special internal collection, you can query them using AQL:

```graphql
query {
    _jobs(where: { status: { eq: "FAILED" } }) {
        id
        type
        error
        last_retry
    }
}
```

## 8. Best Practices

1.  **Keep Payloads Small**: Don't store large blobs in the job payload. Store a file path or document ID instead.
2.  **Idempotency**: Ensure your handlers can safely run multiple times. If a job is retried after a partial failure, it should not cause inconsistent state.
3.  **Graceful Shutdown**: The Aurora worker system automatically waits for active jobs to finish (up to a timeout) when the database is closed.
4.  **Avoid Long-Running Sync Code**: Since the handlers are `async`, avoid `std::thread::sleep` or blocking I/O. Use `tokio::time::sleep` or `spawn_blocking` for heavy CPU tasks.
