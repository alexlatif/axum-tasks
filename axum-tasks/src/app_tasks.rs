use crate::{
    CachedJobResult, JobMetrics, TaskMetrics, TaskState, TaskStatus,
    types::{HealthStatus, MAX_QUEUE_SIZE, QueuedTask},
};
use chrono::{DateTime, Utc};
use error_stack::ResultExt;
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppTasks {
    // Internal queue
    sender: Sender<QueuedTask>,
    receiver: Receiver<QueuedTask>,
    metrics: Arc<TaskMetrics>,
    task_states: Arc<tokio::sync::RwLock<HashMap<String, TaskState>>>,
    results_cache: Arc<RwLock<HashMap<String, CachedJobResult>>>,
    persistence_callback: Option<Arc<dyn Fn(&HashMap<String, TaskState>) + Send + Sync>>,
    is_shutting_down: Arc<std::sync::atomic::AtomicBool>,
}

impl AppTasks {
    pub fn new() -> Self {
        let (sender, receiver) = flume::bounded(MAX_QUEUE_SIZE);

        Self {
            sender,
            receiver,
            metrics: Arc::new(TaskMetrics::new()),
            task_states: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            results_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence_callback: None,
            is_shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Enable automatic persistence when task states change
    pub fn with_auto_persist<F>(mut self, callback: F) -> Self
    where
        F: Fn(&HashMap<String, TaskState>) + Send + Sync + 'static,
    {
        self.persistence_callback = Some(Arc::new(callback));
        self
    }

    pub async fn queue<T>(&self, task: T) -> Result<String, error_stack::Report<TaskQueueError>>
    where
        T: crate::TaskHandler + serde::Serialize + Send + Sync + 'static,
    {
        // Check if system is shutting down
        if self
            .is_shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(error_stack::report!(TaskQueueError)
                .attach_printable("System is shutting down")
                .attach_printable("No new tasks accepted during shutdown"));
        }

        // Check capacity
        let queue_depth = self.metrics.get_queue_depth();
        if queue_depth >= MAX_QUEUE_SIZE as u64 {
            return Err(error_stack::report!(TaskQueueError)
                .attach_printable("Queue is full")
                .attach_printable(format!("Current depth: {}/{}", queue_depth, MAX_QUEUE_SIZE)));
        }

        let task_id = uuid::Uuid::new_v4().to_string();
        let task_name = std::any::type_name::<T>()
            .split("::")
            .last()
            .unwrap_or("Unknown")
            .to_string();

        let task_data = serde_json::to_vec(&task)
            .change_context(TaskQueueError)
            .attach_printable("Failed to serialize task")?;

        // 1. FIRST: Persist state (crash-proof)
        let task_state = TaskState {
            id: task_id.clone(),
            task_name: task_name.clone(),
            task_data: serde_json::to_value(&task)
                .change_context(TaskQueueError)
                .attach_printable("Failed to serialize task for state")?,
            status: TaskStatus::Queued,
            retry_count: 0,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: None,
            worker_id: None,
        };

        // Add to persistent state
        {
            let mut states = self.task_states.write().await;
            states.insert(task_id.clone(), task_state);

            // Auto-persist if callback set
            if let Some(callback) = &self.persistence_callback {
                callback(&states);
            }
        }

        // 2. THEN: Queue for processing
        let queued_task = QueuedTask {
            id: task_id.clone(),
            task_name,
            task_data,
            retry_count: 0,
            created_at: std::time::Instant::now(),
        };

        match tokio::time::timeout(
            Duration::from_millis(100),
            self.sender.send_async(queued_task),
        )
        .await
        {
            Ok(Ok(_)) => {
                self.metrics.record_queued();
                Ok(task_id)
            }
            _ => {
                // Remove from state if queueing failed
                self.task_states.write().await.remove(&task_id);
                Err(error_stack::report!(TaskQueueError)
                    .attach_printable("Failed to send task to queue")
                    .attach_printable("Timeout or channel disconnected")
                    .attach_printable(format!("Task ID: {}", task_id)))
            }
        }
    }

    pub async fn load_state(&self, states: HashMap<String, TaskState>) {
        let mut task_states = self.task_states.write().await;
        task_states.clear();
        task_states.extend(states);

        // Re-queue incomplete tasks for recovery
        for (task_id, task_state) in &*task_states {
            if matches!(
                task_state.status,
                TaskStatus::Queued | TaskStatus::InProgress
            ) {
                if let Ok(task_data) = serde_json::to_vec(&task_state.task_data) {
                    let queued_task = QueuedTask {
                        id: task_id.clone(),
                        task_name: task_state.task_name.clone(),
                        task_data,
                        retry_count: task_state.retry_count,
                        created_at: std::time::Instant::now(),
                    };

                    // Best effort recovery - don't block startup if queue is full
                    let _ = self.sender.try_send(queued_task);
                }
            }
        }

        tracing::info!(
            "Loaded {} task states, {} incomplete tasks requeued",
            task_states.len(),
            task_states.values().filter(|t| !t.is_terminal()).count()
        );
    }

    pub async fn get_state(&self) -> HashMap<String, TaskState> {
        self.task_states.read().await.clone()
    }

    pub async fn get_status(&self, job_id: &str) -> Option<TaskStatus> {
        let states = self.task_states.read().await;
        states.get(job_id).map(|state| state.status.clone())
    }

    pub async fn get_task(&self, task_id: &str) -> Option<TaskState> {
        self.task_states.read().await.get(task_id).cloned()
    }

    pub async fn get_result(&self, job_id: &str) -> Option<CachedJobResult> {
        let results = self.results_cache.read().await;
        results.get(job_id).cloned()
    }

    pub async fn get_job_metrics(&self, job_id: &str) -> Option<JobMetrics> {
        let states = self.task_states.read().await;
        states.get(job_id).map(JobMetrics::from)
    }

    pub async fn list_tasks(
        &self,
        status: Option<TaskStatus>,
        limit: Option<usize>,
    ) -> Vec<TaskState> {
        let states = self.task_states.read().await;
        let mut tasks: Vec<TaskState> = states
            .values()
            .filter(|task| status.as_ref().is_none_or(|s| &task.status == s))
            .cloned()
            .collect();

        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        if let Some(limit) = limit {
            tasks.truncate(limit);
        }

        tasks
    }

    pub async fn get_tasks_by_status(&self, status: TaskStatus) -> Vec<TaskState> {
        self.task_states
            .read()
            .await
            .values()
            .filter(|task| task.status == status)
            .cloned()
            .collect()
    }

    pub async fn store_success(
        &self,
        job_id: String,
        data: serde_json::Value,
        ttl: Option<Duration>,
    ) {
        let cached_result = CachedJobResult {
            job_id: job_id.clone(),
            completed_at: Utc::now(),
            success: true,
            data,
            error: None,
            ttl,
        };

        let mut results = self.results_cache.write().await;
        results.insert(job_id.clone(), cached_result);

        if let Some(ttl) = ttl {
            let cache = self.results_cache.clone();
            let id = job_id.clone();
            tokio::spawn(async move {
                tokio::time::sleep(ttl).await;
                let mut results = cache.write().await;
                results.remove(&id);
            });
        }
    }

    pub async fn store_failure(&self, job_id: String, error: String, ttl: Option<Duration>) {
        let cached_result = CachedJobResult {
            job_id: job_id.clone(),
            completed_at: Utc::now(),
            success: false,
            data: serde_json::json!({}),
            error: Some(error),
            ttl,
        };

        let mut results = self.results_cache.write().await;
        results.insert(job_id.clone(), cached_result);

        if let Some(ttl) = ttl {
            let cache = self.results_cache.clone();
            let id = job_id.clone();
            tokio::spawn(async move {
                tokio::time::sleep(ttl).await;
                let mut results = cache.write().await;
                results.remove(&id);
            });
        }
    }

    pub async fn cleanup_old_tasks(&self, older_than: DateTime<Utc>) -> usize {
        let mut states = self.task_states.write().await;
        let initial_count = states.len();

        states.retain(|_, task| {
            match task.status {
                TaskStatus::Completed | TaskStatus::Failed => task
                    .completed_at
                    .is_none_or(|completed| completed >= older_than),
                _ => true, // Keep in-progress tasks
            }
        });

        let removed = initial_count - states.len();

        // Auto-persist if tasks were removed
        if removed > 0 {
            if let Some(callback) = &self.persistence_callback {
                callback(&states);
            }
            tracing::info!("Cleaned up {} old tasks", removed);
        }

        removed
    }

    pub(crate) fn sender(&self) -> &Sender<QueuedTask> {
        &self.sender
    }

    pub(crate) fn receiver(&self) -> &Receiver<QueuedTask> {
        &self.receiver
    }

    pub fn get_task_metrics(&self) -> crate::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn queue_depth(&self) -> u64 {
        self.metrics.get_queue_depth()
    }

    pub fn is_healthy(&self) -> bool {
        let queue_depth = self.queue_depth();
        queue_depth < (MAX_QUEUE_SIZE as u64 / 2)
    }

    pub fn health_status(&self) -> crate::types::HealthStatus {
        let queue_depth = self.queue_depth();

        if self.is_shutting_down() || queue_depth >= MAX_QUEUE_SIZE as u64 {
            HealthStatus::unhealthy(queue_depth)
        } else if queue_depth >= (MAX_QUEUE_SIZE as u64 * 3 / 4) {
            crate::types::HealthStatus::degraded(queue_depth)
        } else {
            crate::types::HealthStatus::healthy(queue_depth)
        }
    }

    pub fn shutdown(&self) {
        self.is_shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("Task system shutdown initiated - no new tasks will be accepted");
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    //=========================================================================
    // INTERNAL METHODS (used by worker system)
    //=========================================================================

    pub(crate) fn metrics_ref(&self) -> &Arc<TaskMetrics> {
        &self.metrics
    }

    pub(crate) async fn update_task_status(
        &self,
        task_id: &str,
        status: TaskStatus,
        worker_id: Option<usize>,
        duration_ms: Option<u64>,
        error_message: Option<String>,
    ) {
        let mut states = self.task_states.write().await;
        if let Some(task) = states.get_mut(task_id) {
            let old_status = task.status.clone();

            task.status = status.clone();
            task.worker_id = worker_id;
            task.error_message = error_message;

            if let Some(duration) = duration_ms {
                task.duration_ms = Some(duration);
                self.metrics.record_processing_time(duration);
            }

            match status {
                TaskStatus::InProgress => {
                    task.started_at = Some(Utc::now());
                }
                TaskStatus::Completed | TaskStatus::Failed => {
                    task.completed_at = Some(Utc::now());
                }
                TaskStatus::Retrying => {
                    task.retry_count += 1;
                    task.started_at = None; // Reset for retry
                    self.metrics.record_retried();
                }
                _ => {}
            }

            // Log status changes
            match (&old_status, &status) {
                (TaskStatus::Queued, TaskStatus::InProgress) => {
                    tracing::debug!(task_id = %task_id, worker_id = ?worker_id, "Task started");
                }
                (TaskStatus::InProgress, TaskStatus::Completed) => {
                    tracing::info!(
                        task_id = %task_id,
                        duration_ms = ?duration_ms,
                        "Task completed successfully"
                    );
                }
                (TaskStatus::InProgress, TaskStatus::Failed) => {
                    tracing::warn!(
                        task_id = %task_id,
                        error = ?task.error_message,
                        retry_count = task.retry_count,
                        "Task failed"
                    );
                }
                _ => {}
            }

            // Auto-persist if callback set
            if let Some(callback) = &self.persistence_callback {
                callback(&states);
            }
        }
    }
}

impl Default for AppTasks {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for AppTasks {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Only serialize the task states - channels can't be serialized
        #[derive(Serialize)]
        struct AppTasksSnapshot {
            task_states: HashMap<String, TaskState>,
        }

        let states = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.task_states.read().await.clone() })
        });

        let snapshot = AppTasksSnapshot {
            task_states: states,
        };
        snapshot.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AppTasks {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct AppTasksSnapshot {
            task_states: HashMap<String, TaskState>,
        }

        let snapshot = AppTasksSnapshot::deserialize(deserializer)?;

        let app_tasks = AppTasks::new();

        let states = snapshot.task_states;
        let app_tasks_clone = app_tasks.clone();
        tokio::spawn(async move {
            app_tasks_clone.load_state(states).await;
        });

        Ok(app_tasks)
    }
}

#[derive(Debug)]
pub struct TaskQueueError;

impl std::fmt::Display for TaskQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task queue operation failed")
    }
}

impl error_stack::Context for TaskQueueError {}
