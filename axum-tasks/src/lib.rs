//! # Axum Background Tasks
//!
//! Production-ready background task system for Axum applications.
//!
//! ## Quick Start
//!
//! ```no_run
//! use axum::{Router, extract::State, routing::post};
//! use axum_tasks::{AppTasks, HasTasks, Task, TaskOutput, admin_routes, spawn_task_workers};
//! use serde::{Deserialize, Serialize};
//! use axum_tasks::CancellationToken;
//!
//! #[derive(Task, Serialize, Deserialize)]
//! #[task(description = "Processing data", retry = true)]
//! struct DataProcessing { data_id: String }
//!
//! impl DataProcessing {
//!     async fn execute(&self) -> TaskOutput {
//!         TaskOutput::Success(serde_json::json!({"id": self.data_id}))
//!     }
//! }
//!
//! #[derive(Clone, HasTasks)]
//! struct AppState { tasks: AppTasks }
//!
//! async fn process_handler(State(state): State<AppState>) -> String {
//!     state.tasks.queue(DataProcessing { data_id: "example".into() })
//!         .await.expect("queue task")
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let state = AppState { tasks: AppTasks::new() };
//!     let shutdown = CancellationToken::new();
//!     spawn_task_workers(state.tasks.clone(), shutdown, None);
//!     let app = Router::new()
//!         .route("/process", post(process_handler))
//!         .nest("/admin", admin_routes::<AppState>())
//!         .with_state(state);
//!     let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
//!     axum::serve(listener, app).await.unwrap();
//! }
//! ```

mod app_tasks;
mod metrics;
mod routes;
mod types;
mod worker;

pub use app_tasks::{AppTasks, TaskQueueError};
pub use metrics::TaskMetrics;
pub use routes::admin_routes;
pub use types::*;
pub use worker::spawn_task_workers;

pub use async_trait::async_trait;
pub use axum_tasks_derive::{HasTasks, Task};
pub use inventory;
pub use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait TaskHandler: Send + Sync {
    async fn handle(&self, app_tasks: &AppTasks, job_id: &str) -> TaskResult;
    fn description(&self) -> String;
    fn is_retryable(&self, error: &str) -> bool;
}

pub trait HasTasks {
    fn tasks(&self) -> &AppTasks;
    fn tasks_mut(&mut self) -> &mut AppTasks;
}

pub type TaskFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<TaskResult, TaskResult>> + Send + 'a>>;
pub type TaskHandlerT = for<'a> fn(&'a [u8], &'a AppTasks, &str) -> TaskFuture<'a>;

pub struct TaskRegistration {
    pub name: &'static str,
    pub handler: TaskHandlerT,
}

inventory::collect!(TaskRegistration);

pub fn init_task_system() -> (AppTasks, CancellationToken) {
    let app_tasks = AppTasks::new();
    let shutdown_token = CancellationToken::new();

    (app_tasks, shutdown_token)
}

pub fn init_task_system_with_persistence<F>(
    persistence_callback: F,
) -> (AppTasks, CancellationToken)
where
    F: Fn(&std::collections::HashMap<String, TaskState>) + Send + Sync + 'static,
{
    let app_tasks = AppTasks::new().with_auto_persist(persistence_callback);
    let shutdown_token = CancellationToken::new();

    (app_tasks, shutdown_token)
}
