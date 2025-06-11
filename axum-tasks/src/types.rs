use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const MAX_QUEUE_SIZE: usize = 10_000;
pub const MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum TaskResult {
    Success,
    RetryableError(String),
    PermanentError(String),
}

impl fmt::Display for TaskResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskResult::Success => write!(f, "Success"),
            TaskResult::RetryableError(err) => write!(f, "Retryable Error: {}", err),
            TaskResult::PermanentError(err) => write!(f, "Permanent Error: {}", err),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Queued,
    InProgress,
    Completed,
    Failed,
    Retrying,
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskStatus::Queued => write!(f, "queued"),
            TaskStatus::InProgress => write!(f, "in_progress"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Failed => write!(f, "failed"),
            TaskStatus::Retrying => write!(f, "retrying"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskState {
    pub id: String,
    pub task_name: String,
    pub task_data: serde_json::Value,
    pub status: TaskStatus,
    pub retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    pub error_message: Option<String>,
    pub worker_id: Option<usize>,
}

impl TaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, TaskStatus::Completed | TaskStatus::Failed)
    }

    pub fn can_retry(&self) -> bool {
        self.retry_count < MAX_RETRIES
            && matches!(self.status, TaskStatus::Failed)
            && self
                .error_message
                .as_ref()
                .is_some_and(|msg| !msg.contains("permanent"))
    }

    pub fn age(&self) -> chrono::Duration {
        Utc::now().signed_duration_since(self.created_at)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedTask {
    pub id: String,
    pub task_name: String,
    pub task_data: Vec<u8>,
    pub retry_count: u32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub queue_depth: u64,
    pub max_queue_size: usize,
    pub timestamp: DateTime<Utc>,
    pub accepting_tasks: bool,
}

impl HealthStatus {
    pub fn healthy(queue_depth: u64) -> Self {
        Self {
            status: "healthy".to_string(),
            queue_depth,
            max_queue_size: MAX_QUEUE_SIZE,
            timestamp: Utc::now(),
            accepting_tasks: queue_depth < (MAX_QUEUE_SIZE as u64 / 2),
        }
    }

    pub fn degraded(queue_depth: u64) -> Self {
        Self {
            status: "degraded".to_string(),
            queue_depth,
            max_queue_size: MAX_QUEUE_SIZE,
            timestamp: Utc::now(),
            accepting_tasks: queue_depth < MAX_QUEUE_SIZE as u64,
        }
    }

    pub fn unhealthy(queue_depth: u64) -> Self {
        Self {
            status: "unhealthy".to_string(),
            queue_depth,
            max_queue_size: MAX_QUEUE_SIZE,
            timestamp: Utc::now(),
            accepting_tasks: false,
        }
    }
}
