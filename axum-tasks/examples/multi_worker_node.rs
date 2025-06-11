// multi_worker_node.rs
// This is the worker node for distributed task queueing with Redis.
use axum_tasks::{AppTasks, Task, TaskHandler, TaskResult, spawn_task_workers};
use axum_tasks::redis_queue::RedisQueue;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Task, Debug, Clone, Serialize, Deserialize)]
#[task(description = "Send email", retry = true)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
}

impl SendEmail {
    async fn execute(&self) -> TaskResult {
        info!("[worker] Sending email to {}: {}", self.to, self.subject);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        TaskResult::Success
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let redis_queue = RedisQueue::new("redis://localhost:6379", "axum_tasks_queue").await;
    let tasks = AppTasks::with_queue(redis_queue);
    let shutdown_token = axum_tasks::CancellationToken::new();
    info!("[worker] Starting distributed workers...");
    spawn_task_workers(tasks, shutdown_token, Some(4)).await;
    Ok(())
}
