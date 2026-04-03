# Aurora DB Durable Workers

This guide covers Aurora's durable worker system for background job processing.

## Overview

Durable workers allow you to offload heavy tasks (email sending, image processing) to the background. Jobs are persisted, retried automatically on failure, and can be scheduled for the future.

## Enabling the Worker System

By default, the background worker system is disabled to save resources. To use workers or handlers, you must explicitly enable them in your `AuroraConfig`.

```rust
let config = AuroraConfig {
    workers_enabled: true,   // Arm the system
    worker_threads: 8,       // Number of concurrent processing threads
    ..AuroraConfig::default()
};

let db = Aurora::with_config(config).await?;
```

*   **`workers_enabled`**: If `false`, `enqueueJob` and `define handler` will fail with an initialization error.
*   **`worker_threads`**: Defaults to 4. Higher values allow for more simultaneous background task processing.

## Enqueuing Jobs

You can enqueue jobs directly from AQL using the `enqueueJob` mutation.

```graphql
mutation {
    enqueueJob(
        type: "send_email",
        payload: {
            to: "user@example.com",
            subject: "Welcome!"
        },
        priority: HIGH
    ) {
        jobId
        status
    }
}
```

### Scheduled Jobs

Schedule a job to run in the future:

```graphql
mutation {
    enqueueJob(
        type: "cleanup_logs",
        payload: { days: 30 },
        runAt: "2023-12-31T23:59:59Z"
    ) {
        jobId
    }
}
```

### Options
*   `type`: The name of the job handler (must match a registered Rust handler).
*   `payload`: JSON object with job data.
*   `priority`: `LOW`, `NORMAL`, `HIGH`, `CRITICAL`.
*   `runAt`: ISO 8601 timestamp for delayed execution.
*   `retries`: Number of max retries (default: 3).

## Defining Handlers (Automation)

For simple automation, you can define handlers directly in AQL using `define handler`.

```graphql
# Automatically send a welcome email when a user is inserted
define handler "welcome_new_user" {
    on: "insert:users",
    action: {
        enqueueJob(
            type: "send_email",
            payload: {
                user_id: "${id}",
                template: "welcome"
            }
        )
    }
}
```

## Defining Workers (Rust Backend)

To process the jobs, you need to register Rust handlers in your application code.

```rust
use aurora_db::workers::{WorkerExecutor, JobHandler, Job, JobResult};

struct EmailHandler;

#[async_trait]
impl JobHandler for EmailHandler {
    async fn handle(&self, job: &Job) -> JobResult {
        let to = job.payload.get("to").unwrap();
        // Send email logic...
        Ok(())
    }
}

// In your main setup:
let mut executor = WorkerExecutor::new(db.clone(), 4);
executor.register_handler("send_email", Box::new(EmailHandler));
executor.start().await?;
```