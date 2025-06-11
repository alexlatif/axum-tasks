//! # Axum Background Tasks
//!
//! Production-ready background task system for Axum applications.
//!
//! ## Quick Start
//!
//! ```rust
//! use axum::{Router, routing::post};
//! use axum_background_tasks::{AppTasks, Task, TaskResult, admin_routes, HasTasks};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Task, Serialize, Deserialize)]
//! #[task(description = "Processing data", retry = true)]
//! struct DataProcessing {
//!     data_id: String,
//! }
//!
//! impl DataProcessing {
//!     async fn execute(&self) -> TaskResult {
//!         // Your task logic here
//!         TaskResult::Success
//!     }
//! }
//!
//! #[derive(HasTasks)]
//! struct AppState {
//!     tasks: AppTasks,
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let app_state = AppState {
//!         tasks: AppTasks::with_queue(LocalQueue::new()),
//!     };
//!     
//!     let app = Router::new()
//!         .route("/process", post(process_handler))
//!         .nest("/admin", admin_routes::<AppState>())
//!         .with_state(app_state);
//!         
//!     // Start workers and server
//! }
//! ```

mod app_tasks;
mod metrics;
mod routes;
mod types;
mod worker;

pub use app_tasks::{AppTasks, TaskQueue, TaskQueueError, LocalQueue};
pub use metrics::TaskMetrics;
pub use routes::admin_routes;
pub use types::*;
pub use worker::spawn_task_workers;

pub use async_trait::async_trait;
pub use axum_tasks_derive::{HasTasks, Task};
pub use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait TaskHandler: Send + Sync {
    async fn handle(&self) -> TaskResult;
    fn description(&self) -> String;
    fn is_retryable(&self, error: &str) -> bool;
}

pub trait HasTasks {
    fn tasks(&self) -> &AppTasks<LocalQueue>;
    fn tasks_mut(&mut self) -> &mut AppTasks<LocalQueue>;
}

type TaskHandlerT = fn(
    &[u8],
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<TaskResult, TaskResult>> + Send>,
>;
pub struct TaskRegistration {
    pub name: &'static str,
    pub handler: TaskHandlerT,
}

inventory::collect!(TaskRegistration);

pub fn init_task_system() -> (AppTasks, CancellationToken) {
    let app_tasks = AppTasks::with_queue(LocalQueue::new());
    let shutdown_token = CancellationToken::new();

    (app_tasks, shutdown_token)
}

pub fn init_task_system_with_persistence<F>(
    persistence_callback: F,
) -> (AppTasks, CancellationToken)
where
    F: Fn(&std::collections::HashMap<String, TaskState>) + Send + Sync + 'static,
{
    let app_tasks = AppTasks::with_queue(LocalQueue::new()).with_auto_persist(persistence_callback);
    let shutdown_token = CancellationToken::new();

    (app_tasks, shutdown_token)
}

#[cfg(feature = "redis")]
pub mod redis_queue;

#[cfg(feature = "postgres")]
pub mod postgres_queue;
