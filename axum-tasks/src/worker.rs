use crate::{
    AppTasks, TaskResult, TaskStatus,
    types::{MAX_RETRIES, QueuedTask},
};
use flume::Receiver;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Spawns background task workers. Returns immediately.
/// Workers process tasks until shutdown, completing their current task before exiting.
pub fn spawn_task_workers(
    app_tasks: AppTasks,
    shutdown: CancellationToken,
    worker_count: Option<usize>,
) {
    let num_workers = worker_count.unwrap_or_else(|| std::cmp::max(4, num_cpus::get() / 2));
    tracing::info!("Starting {} task workers", num_workers);
    for worker_id in 0..num_workers {
        let receiver = app_tasks.receiver().clone();
        let app_tasks = app_tasks.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            worker_loop(worker_id, receiver, app_tasks, shutdown).await;
        });
    }
}

async fn worker_loop(
    worker_id: usize,
    receiver: Receiver<QueuedTask>,
    app_tasks: AppTasks,
    shutdown: CancellationToken,
) {
    tracing::debug!("Worker {} started", worker_id);
    loop {
        tokio::select! {
            task_result = receiver.recv_async() => {
                match task_result {
                    Ok(task) => process_task(worker_id, task, &app_tasks).await,
                    Err(_) => break,
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
    tracing::debug!("Worker {} stopped", worker_id);
}

async fn process_task(worker_id: usize, task: QueuedTask, app_tasks: &AppTasks) {
    let start_time = std::time::Instant::now();

    tracing::debug!(
        task_id = %task.id,
        task_name = %task.task_name,
        worker_id = worker_id,
        retry_count = task.retry_count,
        "Processing task"
    );

    let queue_wait_time = task.created_at.elapsed();
    if queue_wait_time > Duration::from_secs(300) {
        tracing::warn!(
            task_id = %task.id,
            wait_time_ms = queue_wait_time.as_millis(),
            "Task waited unusually long in queue - system may be overloaded"
        );
    }

    // Claim and install the cancellation token atomically with the state
    // check. A cancelled delayed retry (or a duplicate queue message) cannot
    // overwrite a terminal or already-running task.
    let Some(cancel_token) = app_tasks
        .claim_task(&task.id, worker_id, task.retry_count)
        .await
    else {
        tracing::info!(task_id = %task.id, "Skipping stale or cancelled task");
        return;
    };
    let timeout = app_tasks.task_timeout();

    // Execute with timeout and cancellation
    let result = tokio::select! {
        res = execute_task_from_registry(&task.task_name, &task.task_data, app_tasks, &task.id) => res,
        _ = cancel_token.cancelled() => {
            tracing::warn!(task_id = %task.id, worker_id = worker_id, "Task cancelled");
            let duration_ms = start_time.elapsed().as_millis() as u64;
            app_tasks.update_task_status(
                &task.id,
                TaskStatus::Cancelled,
                Some(worker_id),
                Some(duration_ms),
                Some("Cancelled by user".to_string()),
            ).await;
            app_tasks.remove_cancellation_token(&task.id).await;
            return;
        }
        _ = tokio::time::sleep(timeout) => {
            tracing::error!(
                task_id = %task.id,
                worker_id = worker_id,
                timeout_secs = timeout.as_secs(),
                "Task timed out"
            );
            let duration_ms = start_time.elapsed().as_millis() as u64;
            if app_tasks.update_task_status(
                &task.id,
                TaskStatus::Failed,
                Some(worker_id),
                Some(duration_ms),
                Some(format!("Task timed out after {}s", timeout.as_secs())),
            ).await {
                app_tasks.metrics_ref().record_failed();
            }
            app_tasks.remove_cancellation_token(&task.id).await;
            return;
        }
    };

    // Clean up cancellation token
    app_tasks.remove_cancellation_token(&task.id).await;

    let duration = start_time.elapsed();
    let duration_ms = duration.as_millis() as u64;

    match result {
        TaskResult::Success => {
            if app_tasks
                .update_task_status(
                    &task.id,
                    TaskStatus::Completed,
                    Some(worker_id),
                    Some(duration_ms),
                    None,
                )
                .await
            {
                app_tasks.metrics_ref().record_completed();
            }

            tracing::info!(
                task_id = %task.id,
                duration_ms = duration_ms,
                "Task completed successfully"
            );
        }

        TaskResult::RetryableError(error) => {
            if task.retry_count < MAX_RETRIES {
                let delay = calculate_retry_delay(task.retry_count);

                tracing::warn!(
                    task_id = %task.id,
                    error = %error,
                    retry_count = task.retry_count,
                    delay_ms = delay.as_millis(),
                    "Task failed, scheduling retry"
                );

                if app_tasks
                    .update_task_status(
                        &task.id,
                        TaskStatus::Retrying,
                        Some(worker_id),
                        Some(duration_ms),
                        Some(error.clone()),
                    )
                    .await
                {
                    schedule_retry(task, delay, app_tasks).await;
                }
            } else {
                if app_tasks
                    .update_task_status(
                        &task.id,
                        TaskStatus::Failed,
                        Some(worker_id),
                        Some(duration_ms),
                        Some(format!("Max retries exceeded: {}", error)),
                    )
                    .await
                {
                    app_tasks.metrics_ref().record_failed();
                }

                tracing::error!(
                    task_id = %task.id,
                    error = %error,
                    retry_count = task.retry_count,
                    "Task failed permanently after max retries"
                );
            }
        }

        TaskResult::PermanentError(error) => {
            if app_tasks
                .update_task_status(
                    &task.id,
                    TaskStatus::Failed,
                    Some(worker_id),
                    Some(duration_ms),
                    Some(error.clone()),
                )
                .await
            {
                app_tasks.metrics_ref().record_failed();
            }

            tracing::error!(
                task_id = %task.id,
                error = %error,
                "Task failed permanently"
            );
        }
    }
}

async fn execute_task_from_registry(
    task_name: &str,
    task_data: &[u8],
    app_tasks: &AppTasks,
    job_id: &str,
) -> TaskResult {
    // Use the global task registry to find and execute the task
    for registration in inventory::iter::<crate::TaskRegistration> {
        if registration.name == task_name {
            match (registration.handler)(task_data, app_tasks, job_id).await {
                Ok(result) => return result,
                Err(error_result) => return error_result,
            }
        }
    }

    TaskResult::PermanentError(format!("Unknown task type: {}", task_name))
}

async fn schedule_retry(mut task: QueuedTask, delay: Duration, app_tasks: &AppTasks) {
    task.retry_count += 1;

    let sender = app_tasks.sender().clone();

    tokio::spawn(async move {
        tokio::time::sleep(delay).await;

        if let Err(e) = sender.send_async(task.clone()).await {
            tracing::error!(
                task_id = %task.id,
                error = %e,
                "Failed to requeue task for retry"
            );
        }
    });
}

fn calculate_retry_delay(retry_count: u32) -> Duration {
    // Exponential backoff: 2^retry_count seconds, capped at 5 minutes
    let base_delay = 2_u64.pow(retry_count);
    let delay_seconds = std::cmp::min(base_delay, 300); // Cap at 5 minutes
    Duration::from_secs(delay_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static EXECUTIONS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Serialize)]
    struct Probe;
    inventory::submit! {
        crate::TaskRegistration {
            name: "Probe",
            handler: |_data, _tasks, _id| Box::pin(async {
                EXECUTIONS.fetch_add(1, Ordering::SeqCst);
                Ok(TaskResult::Success)
            }),
        }
    }

    #[crate::async_trait]
    impl crate::TaskHandler for Probe {
        async fn handle(&self, _: &AppTasks, _: &str) -> TaskResult {
            TaskResult::Success
        }
        fn description(&self) -> String {
            "probe".into()
        }
        fn is_retryable(&self, _: &str) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn cancelled_retry_never_executes_or_loses_terminal_status() {
        let tasks = AppTasks::new();
        let id = tasks.queue(Probe).await.unwrap();
        let queued = tasks.receiver().try_recv().unwrap();
        tasks
            .update_task_status(
                &id,
                TaskStatus::Retrying,
                Some(0),
                None,
                Some("temporary".into()),
            )
            .await;
        assert!(
            tasks.cancel_task(&id).await,
            "retry-waiting tasks must be cancellable"
        );
        assert_eq!(tasks.get_status(&id).await, Some(TaskStatus::Cancelled));
        assert_eq!(tasks.queue_depth(), 0);
        let before = EXECUTIONS.load(Ordering::SeqCst);
        process_task(0, queued, &tasks).await;
        assert_eq!(EXECUTIONS.load(Ordering::SeqCst), before);
        assert_eq!(tasks.get_status(&id).await, Some(TaskStatus::Cancelled));
    }

    #[tokio::test]
    async fn cancel_running_task_fences_late_success_and_duplicate_queue() {
        let tasks = AppTasks::new();
        let id = tasks.queue(Probe).await.unwrap();
        let queued = tasks.receiver().try_recv().unwrap();
        let token = tasks.claim_task(&id, 0, 0).await.unwrap();
        assert!(tasks.cancel_task(&id).await);
        assert!(token.is_cancelled());
        tasks
            .update_task_status(&id, TaskStatus::Completed, Some(0), Some(1), None)
            .await;
        let before = EXECUTIONS.load(Ordering::SeqCst);
        process_task(1, queued, &tasks).await;
        assert_eq!(EXECUTIONS.load(Ordering::SeqCst), before);
        assert_eq!(tasks.get_status(&id).await, Some(TaskStatus::Cancelled));
        assert_eq!(tasks.queue_depth(), 0);
    }

    #[tokio::test]
    async fn stale_retry_generation_cannot_claim_new_queued_work() {
        let tasks = AppTasks::new();
        let id = tasks.queue(Probe).await.unwrap();
        let old = tasks.receiver().try_recv().unwrap();
        tasks
            .update_task_status(
                &id,
                TaskStatus::Retrying,
                Some(0),
                None,
                Some("temporary".into()),
            )
            .await;
        assert!(tasks.claim_task(&id, 0, old.retry_count).await.is_none());
        let next = QueuedTask {
            retry_count: 1,
            ..old
        };
        assert!(tasks.claim_task(&id, 0, next.retry_count).await.is_some());
    }

    #[tokio::test]
    async fn delayed_retry_does_not_revive_a_cancelled_task() {
        let tasks = AppTasks::new();
        let id = tasks.queue(Probe).await.unwrap();
        let queued = tasks.receiver().try_recv().unwrap();
        tasks
            .update_task_status(
                &id,
                TaskStatus::Retrying,
                Some(0),
                None,
                Some("temporary".into()),
            )
            .await;
        schedule_retry(queued, Duration::from_millis(1), &tasks).await;
        assert!(tasks.cancel_task(&id).await);
        let delayed = tokio::time::timeout(Duration::from_secs(1), tasks.receiver().recv_async())
            .await
            .unwrap()
            .unwrap();
        let before = EXECUTIONS.load(Ordering::SeqCst);
        process_task(1, delayed, &tasks).await;
        assert_eq!(EXECUTIONS.load(Ordering::SeqCst), before);
        assert_eq!(tasks.get_status(&id).await, Some(TaskStatus::Cancelled));
    }
}
