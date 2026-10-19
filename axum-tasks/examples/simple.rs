use axum::{
    Router,
    extract::State,
    response::Json,
    routing::{get, post},
};
use axum_tasks::{
    AppTasks, CancellationToken, HasTasks, Task, TaskOutput, admin_routes, spawn_task_workers,
};
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
    pub async fn execute(&self) -> TaskOutput {
        info!("Processing data: {} for user {}", self.data, self.user_id);

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Randomly succeed or fail
        if self.data.len().is_multiple_of(3) {
            TaskOutput::RetryableError("Temporary processing error".to_string())
        } else {
            // auotsaves
            TaskOutput::Success(serde_json::json!({
                "message": format!("Processed data: {}", self.data),
                "user_id": self.user_id,
            }))
        }
    }
}

#[derive(Clone, Serialize, Deserialize, HasTasks)]
pub struct AppState {
    pub tasks: AppTasks,
}

impl AppState {
    fn new() -> Self {
        let tasks = AppTasks::new().with_auto_persist(|task_states| {
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

        if let Ok(json) = fs::read_to_string("tasks_state.json").await
            && let Ok(task_states) = serde_json::from_str(&json)
        {
            app_state.tasks.load_state(task_states).await;
            info!("Loaded existing task state from file");
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
    "Background Task Demo\n\n\
     POST /process - Queue a task\n\
     GET /admin/health - System health\n\
     GET /admin/tasks - List all tasks\n\
     GET /admin/metrics - System metrics"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    tracing_subscriber::fmt::init();

    // Load app state (with auto task recovery)
    let app_state = AppState::load().await;

    // Start background workers
    let shutdown_token = CancellationToken::new();
    let tasks = app_state.tasks.clone();
    spawn_task_workers(tasks, shutdown_token.clone(), Some(4));
    // Build routes
    let app = Router::new()
        .route("/", get(home))
        .route("/process", post(process_data))
        // Built-in admin panel - zero code!
        .nest("/admin", admin_routes::<AppState>())
        .with_state(app_state);

    info!("🚀 Server starting on http://127.0.0.1:3000");
    info!("📊 Admin panel: http://127.0.0.1:3000/admin/health");
    info!("📈 Task metrics: http://127.0.0.1:3000/admin/metrics");
    info!("📋 Task list: http://127.0.0.1:3000/admin/tasks");

    // Start server with graceful shutdown
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

//=============================================================================
// DEMO USAGE
//=============================================================================

/*
# Start the server:
cargo run --example simple_queue

# Queue some tasks:
curl -X POST http://127.0.0.1:3000/process \
  -H "Content-Type: application/json" \
  -d '{"data": "hello world", "user_id": 123}'

curl -X POST http://127.0.0.1:3000/process \
  -H "Content-Type: application/json" \
  -d '{"data": "foo", "user_id": 456}'

# Check system health:
curl http://127.0.0.1:3000/admin/health

# View all tasks:
curl http://127.0.0.1:3000/admin/tasks

# View metrics:
curl http://127.0.0.1:3000/admin/metrics

# Filter tasks by status:
curl "http://127.0.0.1:3000/admin/tasks?status=completed&limit=10"

# Cleanup old tasks (older than 1 hour):
curl -X POST http://127.0.0.1:3000/admin/cleanup \
  -H "Content-Type: application/json" \
  -d '{"older_than_hours": 1}'

# Restart the server - tasks automatically resume!
*/
