# axum-tasks

A lightweight background task queue for Axum applications with automatic persistence and monitoring.

## Features

- **Minimal dependencies**: Only requires `tokio` and `flume`
- **Persistence hook**: Task states are emitted before queueing; the caller must durably commit the synchronous callback and reconcile uncertain task effects after a crash
- **Type-safe**: Derive macros ensure compile-time correctness
- **Best-effort local recovery**: `load_state` requeues incomplete tasks, subject to queue capacity; non-idempotent effects need application-level reconciliation
- **Built-in monitoring**: Health checks and metrics endpoints included
- **Configurable persistence**: Save state to files, databases, or any backend

## Quick Start

```toml
[dependencies]
axum-tasks = "0.1"
axum = "0.7"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1"
async-trait = "0.1" # required by the Task derive
```

Define a task:

```rust
use axum_tasks::{Task, TaskOutput};
use serde::{Deserialize, Serialize};

#[derive(Task, Debug, Clone, Serialize, Deserialize)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
}

impl SendEmail {
    async fn execute(&self) -> TaskOutput {
        // Return a retryable or permanent error from your mail adapter.
        TaskOutput::Success(serde_json::json!({"to": self.to}))
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
        tasks: AppTasks::new().with_auto_persist(|states| {
            // This callback is synchronous; persist a cloned snapshot before
            // returning (and propagate failures in a production adapter).
            let json = serde_json::to_vec(states).expect("serialize state");
            std::fs::write("tasks.json", json).expect("persist state");
        }),
    };

    // Start workers
    let shutdown = axum_tasks::CancellationToken::new();
    spawn_task_workers(app_state.tasks.clone(), shutdown.clone(), None);

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
    state.tasks.queue(email).await.map_err(|e| format!("{e:?}"))
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
    async fn execute(&self) -> TaskOutput {
        match perform_work().await {
            Ok(data) => TaskOutput::Success(data),
            Err(e) if e.is_retryable() => TaskOutput::RetryableError(e.to_string()),
            Err(e) => TaskOutput::PermanentError(e.to_string()),
        }
    }
}
```

The `Task` derive consumes `TaskOutput`: success includes JSON data, while retryable and permanent errors carry their reason. Retryable errors use exponential backoff. Permanent errors are marked as failed immediately.

## Configuration

Worker count defaults to `max(4, num_cpus / 2)`. Override with:

```rust
spawn_task_workers(tasks, shutdown, Some(8));
```

Persistence is user-controlled. The library provides task state, you choose how to save it:

```rust
AppTasks::new().with_auto_persist(|task_states| {
    // Persist synchronously; handle errors before claiming durable admission.
    save_to_your_backend(task_states);
});
```

## Design

- **Single field**: Only `tasks: AppTasks` needed in your state
- **Type erasure**: Multiple task types work in one queue via serialization
- **Crash recovery**: A synchronous persistence callback records state before queueing; requeue is best-effort and effects still require reconciliation
- **Minimal overhead**: Built specifically for Axum with essential dependencies only

## License

Licensed under the MIT License.