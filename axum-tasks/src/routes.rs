use crate::{HasTasks, TaskStatus};
use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub fn admin_routes<T>() -> Router<T>
where
    T: HasTasks + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/tasks", get(list_tasks::<T>))
        .route("/tasks/{task_id}", get(get_task_status::<T>))
        .route("/tasks/by-status/{status}", get(get_tasks_by_status::<T>))
        .route("/cleanup", post(cleanup_old_tasks::<T>))
        .route("/health", get(health_check::<T>))
        .route("/metrics", get(get_metrics::<T>))
}

#[derive(Debug, Deserialize)]
struct ListTasksQuery {
    status: Option<String>,
    limit: Option<usize>,
}

async fn list_tasks<T>(
    State(state): State<T>,
    Query(query): Query<ListTasksQuery>,
) -> Result<Json<TaskListResponse>, ApiErrorResponse>
where
    T: HasTasks,
{
    let status = if let Some(status_str) = query.status {
        Some(parse_task_status(&status_str)?)
    } else {
        None
    };

    let tasks = state.tasks().list_tasks(status.clone(), query.limit).await;

    Ok(Json(TaskListResponse {
        tasks: tasks.clone(),
        total: tasks.len(),
        filtered: status.is_some(),
    }))
}

async fn get_task_status<T>(
    State(state): State<T>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::TaskState>, ApiErrorResponse>
where
    T: HasTasks,
{
    match state.tasks().get_task(&task_id).await {
        Some(task) => Ok(Json(task)),
        None => Err(error_stack::report!(ApiError::NotFound(task_id.clone()))
            .attach_printable("Task does not exist in the current state")
            .into()),
    }
}

async fn get_tasks_by_status<T>(
    State(state): State<T>,
    Path(status): Path<String>,
) -> Result<Json<Vec<crate::TaskState>>, ApiErrorResponse>
where
    T: HasTasks,
{
    let status = parse_task_status(&status)?;
    let tasks = state.tasks().get_tasks_by_status(status).await;
    Ok(Json(tasks))
}

#[derive(Debug, Deserialize)]
struct CleanupRequest {
    older_than_hours: Option<u64>,
    older_than: Option<DateTime<Utc>>,
}

async fn cleanup_old_tasks<T>(
    State(state): State<T>,
    Json(request): Json<CleanupRequest>,
) -> Result<Json<CleanupResponse>, ApiErrorResponse>
where
    T: HasTasks,
{
    let cutoff = if let Some(timestamp) = request.older_than {
        timestamp
    } else {
        let hours = request.older_than_hours.unwrap_or(24); // Default 24 hours
        Utc::now() - chrono::Duration::hours(hours as i64)
    };

    let removed = state.tasks().cleanup_old_tasks(cutoff).await;

    Ok(Json(CleanupResponse {
        removed_count: removed,
        cutoff_time: cutoff,
    }))
}

async fn health_check<T>(State(state): State<T>) -> Json<crate::types::HealthStatus>
where
    T: HasTasks,
{
    Json(state.tasks().health_status())
}

async fn get_metrics<T>(State(state): State<T>) -> Json<crate::metrics::MetricsSnapshot>
where
    T: HasTasks,
{
    Json(state.tasks().metrics())
}

#[derive(Debug, Serialize)]
struct TaskListResponse {
    tasks: Vec<crate::TaskState>,
    total: usize,
    filtered: bool,
}

#[derive(Debug, Serialize)]
struct CleanupResponse {
    removed_count: usize,
    cutoff_time: DateTime<Utc>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    InvalidStatus(String),
    BadRequest(String),
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::NotFound(msg) => write!(f, "Task not found: {}", msg),
            ApiError::InvalidStatus(msg) => write!(f, "Invalid task status: {}", msg),
            ApiError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            ApiError::Internal(msg) => write!(f, "Internal server error: {}", msg),
        }
    }
}

impl error_stack::Context for ApiError {}

#[derive(Debug)]
pub struct ApiErrorResponse(error_stack::Report<ApiError>);

impl From<error_stack::Report<ApiError>> for ApiErrorResponse {
    fn from(report: error_stack::Report<ApiError>) -> Self {
        Self(report)
    }
}

impl From<ApiError> for ApiErrorResponse {
    fn from(error: ApiError) -> Self {
        Self(error_stack::report!(error))
    }
}

impl axum::response::IntoResponse for ApiErrorResponse {
    fn into_response(self) -> axum::response::Response {
        let context = self.0.current_context();

        let (status, error_message) = match context {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ApiError::InvalidStatus(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        let main_error_msg = context.to_string();
        let additional_context: Vec<String> = format!("{}", self.0)
            .lines()
            .map(str::trim)
            .filter(|line| {
                !line.is_empty()
                    && !line.starts_with("at ")
                    && *line != main_error_msg
                    && !line.contains("src/")
            })
            .map(String::from)
            .collect();

        let body = if additional_context.is_empty() {
            Json(serde_json::json!({"error": error_message}))
        } else {
            Json(serde_json::json!({
                "error": error_message,
                "context": additional_context
            }))
        };

        (status, body).into_response()
    }
}

fn parse_task_status(status_str: &str) -> Result<TaskStatus, ApiErrorResponse> {
    match status_str.to_lowercase().as_str() {
        "queued" => Ok(TaskStatus::Queued),
        "in_progress" | "inprogress" | "running" => Ok(TaskStatus::InProgress),
        "completed" | "success" | "done" => Ok(TaskStatus::Completed),
        "failed" | "error" => Ok(TaskStatus::Failed),
        "retrying" | "retry" => Ok(TaskStatus::Retrying),
        _ => Err(
            error_stack::report!(ApiError::InvalidStatus(status_str.to_string()))
                .attach_printable(
                    "Valid statuses: queued, in_progress, completed, failed, retrying",
                )
                .into(),
        ),
    }
}
