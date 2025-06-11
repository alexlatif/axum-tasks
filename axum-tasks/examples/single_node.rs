// This example demonstrates single-node (local) queue usage.
use axum::{Router, extract::State, response::Json, routing::{get, post}};
use axum_tasks::{AppTasks, CancellationToken, HasTasks, Task, TaskHandler, TaskResult, admin_routes, LocalQueue, spawn_task_workers};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::info;

#[derive(Task, Debug, Clone, Serialize, Deserialize)]
#[task(description = "Processing some data", retry = true)]
pub struct DataProcessor {
    pub data: String,
    pub user_id: u32,
}

impl DataProcessor {
    async fn execute(&self) -> TaskResult {
        info!("Processing data: {} for user {}", self.data, self.user_id);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        if self.data.len() % 3 == 0 {
            TaskResult::RetryableError("Temporary processing error".to_string())
        } else {
            TaskResult::Success
        }
    }
}

#[derive(Clone, Serialize, Deserialize, HasTasks)]
pub struct AppState {
    pub tasks: AppTasks<LocalQueue>,
}

impl AppState {
    fn new() -> Self {
        let tasks = AppTasks::with_queue(LocalQueue::new()).with_auto_persist(|task_states| {
            let json = serde_json::to_string_pretty(task_states).unwrap();
            tokio::spawn(async move {
                if let Err(e) = fs::write("tasks_state.json", json).await {
                    eprintln!("Failed to save task state: {}", e);
                }
            });
        });
        Self { tasks }
    }
    async fn load() -> Self {
        let app_state = Self::new();
        if let Ok(json) = fs::read_to_string("tasks_state.json").await {
            if let Ok(task_states) = serde_json::from_str(&json) {
                app_state.tasks.load_state(task_states).await;
                info!("Loaded existing task state from file");
            }
        }
        app_state
    }
}

#[derive(Deserialize)]
struct ProcessRequest {
    data: String,
    user_id: u32,
}

#[derive(Serialize)]
struct ProcessResponse {
    task_id: String,
    message: String,
}

async fn process_data(
    State(state): State<AppState>,
    Json(request): Json<ProcessRequest>,
) -> Result<Json<ProcessResponse>, String> {
    let task_id = state
        .tasks
        .queue(DataProcessor {
            data: request.data,
            user_id: request.user_id,
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(Json(ProcessResponse {
        task_id,
        message: "Processing started".to_string(),
    }))
}

async fn home() -> &'static str {
    "Background Task Demo\n\nPOST /process - Queue a task\nGET /admin/health - System health\nGET /admin/tasks - List all tasks\nGET /admin/metrics - System metrics"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let app_state = AppState::load().await;
    let shutdown_token = CancellationToken::new();
    let tasks = app_state.tasks.clone();
    let worker_shutdown = shutdown_token.clone();
    tokio::spawn(async move {
        spawn_task_workers(tasks, worker_shutdown, Some(6)).await;
    });
    let app = Router::new()
        .route("/", get(home))
        .route("/process", post(process_data))
        .nest("/admin", admin_routes::<AppState>())
        .with_state(app_state);
    info!("🚀 Server starting on http://127.0.0.1:3000");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down gracefully...");
            shutdown_token.cancel();
        })
        .await?;
    Ok(())
}
