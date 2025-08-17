use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

pub const MAX_QUEUE_SIZE: usize = 10_000;
pub const MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone, PartialEq)]
pub enum TaskResult {
    Success,
    RetryableError(String),
    PermanentError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskOutput {
    Success(serde_json::Value),
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

impl From<TaskOutput> for TaskResult {
    fn from(output: TaskOutput) -> Self {
        match output {
            TaskOutput::Success(_) => TaskResult::Success,
            TaskOutput::RetryableError(e) => TaskResult::RetryableError(e),
            TaskOutput::PermanentError(e) => TaskResult::PermanentError(e),
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
pub struct JobMetrics {
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub retry_count: u32,
    pub error_message: Option<String>,
    pub duration_ms: Option<u64>,
    pub worker_id: Option<usize>,
}

impl From<&TaskState> for JobMetrics {
    fn from(state: &TaskState) -> Self {
        JobMetrics {
            status: state.status.clone(),
            created_at: state.created_at,
            started_at: state.started_at,
            retry_count: state.retry_count,
            error_message: state.error_message.clone(),
            duration_ms: state.duration_ms,
            worker_id: state.worker_id,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueuedTask {
    pub id: String,
    pub task_name: String,
    pub task_data: Vec<u8>,
    pub retry_count: u32,
    pub created_at: std::time::Instant,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedJobResult {
    pub job_id: String,
    pub completed_at: DateTime<Utc>,
    pub success: bool,
    pub data: serde_json::Value, // The actual response/output data
    pub error: Option<String>,   // Error message if failed
    pub ttl: Option<Duration>,   // Optional TTL for auto-cleanup
}
