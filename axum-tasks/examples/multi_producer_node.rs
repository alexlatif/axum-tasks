// multi_producer_node.rs
// This is the API/producer node for distributed task queueing with Redis.
use axum_tasks::{AppTasks, Task, TaskHandler, TaskResult, admin_routes};
use axum_tasks::redis_queue::RedisQueue;
use axum::{Router, extract::State, response::Json, routing::{get, post}};
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
        info!("Sending email to {}: {}", self.to, self.subject);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        TaskResult::Success
    }
}

#[derive(Clone)]
pub struct AppState {
    pub tasks: AppTasks<RedisQueue>,
}

impl AppState {
    async fn new() -> Self {
        let redis_queue = RedisQueue::new("redis://localhost:6379", "axum_tasks_queue").await;
        let tasks = AppTasks::with_queue(redis_queue);
        Self { tasks }
    }
}

#[derive(Deserialize)]
struct EmailRequest {
    to: String,
    subject: String,
}

#[derive(Serialize)]
struct EmailResponse {
    task_id: String,
    message: String,
}

async fn send_email(
    State(state): State<AppState>,
    Json(request): Json<EmailRequest>,
) -> Result<Json<EmailResponse>, String> {
    let task_id = state.tasks.queue(SendEmail {
        to: request.to,
        subject: request.subject,
    }).await.map_err(|e| e.to_string())?;
    Ok(Json(EmailResponse {
        task_id,
        message: "Email queued".to_string(),
    }))
}

async fn home() -> &'static str {
    "Distributed Task Producer (Redis)\n\nPOST /send_email - Queue an email\nGET /admin/health - System health\nGET /admin/tasks - List all tasks\nGET /admin/metrics - System metrics"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let app_state = AppState::new().await;
    let app = Router::new()
        .route("/", get(home))
        .route("/send_email", post(send_email))
        .nest("/admin", admin_routes::<AppState>())
        .with_state(app_state);
    info!("🚀 Producer API starting on http://127.0.0.1:3000 (Redis-backed queue)");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down gracefully...");
        })
        .await?;
    Ok(())
}
