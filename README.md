# axum-tasks

A lightweight background task queue for Axum applications with automatic persistence and monitoring.

## Features

- **Minimal dependencies**: Only requires `tokio` and `flume`
- **Crash-proof**: Tasks persist before queueing and survive restarts
- **Type-safe**: Derive macros ensure compile-time correctness
- **Auto-recovery**: Incomplete tasks resume automatically on startup
- **Built-in monitoring**: Health checks and metrics endpoints included
- **Configurable persistence**: Save state to files, databases, or any backend

## Quick Start

```toml
[dependencies]
axum-tasks = "0.1"
axum = "0.7"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

Define a task:

```rust
use axum_tasks::{Task, TaskResult};
use serde::{Deserialize, Serialize};

#[derive(Task, Debug, Clone, Serialize, Deserialize)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
}

impl SendEmail {
    async fn execute(&self) -> TaskResult {
        // Your logic here
        send_email(&self.to, &self.subject).await?;
        TaskResult::Success
    }
}
```

Set up your application:

```rust
use axum_tasks::{AppTasks, HasTasks, admin_routes, spawn_task_workers};

#[derive(Clone, HasTasks)]
pub struct AppState {
    pub tasks: AppTasks,
}

#[tokio::main]
async fn main() {
    let app_state = AppState {
        tasks: AppTasks::with_queue(LocalQueue::new()).with_auto_persist(|states| {
            tokio::spawn(async move {
                let json = serde_json::to_string_pretty(states).unwrap();
                tokio::fs::write("tasks.json", json).await.ok();
            });
        }),
    };

    // Start workers
    let shutdown = tokio_util::sync::CancellationToken::new();
    spawn_task_workers(app_state.tasks.clone(), shutdown.clone(), None).await;

    // Build router
    let app = axum::Router::new()
        .route("/send", axum::routing::post(queue_email))
        .nest("/admin", admin_routes::<AppState>())
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn queue_email(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Json(email): axum::Json<SendEmail>,
) -> Result<String, String> {
    state.tasks.queue(email).await.map_err(|e| e.to_string())
}
```

## Admin Endpoints

The library automatically provides:

- `GET /admin/health` - System health and queue status
- `GET /admin/tasks` - List all tasks with optional filtering
- `GET /admin/metrics` - Performance metrics and throughput data
- `POST /admin/cleanup` - Remove completed tasks older than specified time

## Task Results

```rust
impl MyTask {
    async fn execute(&self) -> TaskResult {
        match perform_work().await {
            Ok(_) => TaskResult::Success,
            Err(e) if e.is_retryable() => TaskResult::RetryableError(e.to_string()),
            Err(e) => TaskResult::PermanentError(e.to_string()),
        }
    }
}
```

Retryable errors use exponential backoff. Permanent errors are marked as failed immediately.

## Configuration

Worker count defaults to `max(4, num_cpus / 2)`. Override with:

```rust
spawn_task_workers(tasks, shutdown, Some(8)).await;
```

Persistence is user-controlled. The library provides task state, you choose how to save it:

```rust
AppTasks::with_queue(LocalQueue::new()).with_auto_persist(|task_states| {
    // Save to PostgreSQL, Redis, file system, etc.
    save_to_your_backend(task_states);
});
```

## Multi-Node Support

Multi-node (distributed) operation is supported with the following backends:
- **Redis** (enable with `--features redis`)
- **Postgres** (enable with `--features postgres`)

To run in distributed (multi-node) mode, use Redis as the queue backend. You can run any number of producer (API) and worker nodes, all pointing to the same Redis instance:

- **Producer node:**
  - Run `examples/multi_producer_node.rs` to expose the HTTP API and enqueue tasks.
- **Worker node(s):**
  - Run `examples/multi_worker_node.rs` on one or more machines/containers to process tasks from the queue.

All nodes must use the same Redis URL and queue name. This allows you to scale producers and workers independently for high availability and throughput.

## Design

- **Single field**: Only `tasks: AppTasks` needed in your state
- **Type erasure**: Multiple task types work in one queue via serialization
- **Crash recovery**: Tasks are persisted before queueing
- **Minimal overhead**: Built specifically for Axum with essential dependencies only

## License

Licensed under the MIT License.