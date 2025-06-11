use crate::{AppTasks, TaskResult, TaskStatus, types::{MAX_RETRIES, QueuedTask}};
use crate::TaskQueue;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub async fn spawn_task_workers<Q: TaskQueue + Clone + 'static>(
    app_tasks: AppTasks<Q>,
    shutdown_token: CancellationToken,
    worker_count: Option<usize>,
) {
    let mut handles = Vec::new();
    // Default to half the CPU cores, but at least 4 workers
    let num_workers = worker_count.unwrap_or_else(|| std::cmp::max(4, num_cpus::get() / 2));
    tracing::info!("Starting {} task workers", num_workers);

    for worker_id in 0..num_workers {
        let app_tasks = app_tasks.clone();
        let token = shutdown_token.clone();

        let handle = tokio::spawn(async move {
            worker_loop(worker_id, app_tasks, token).await;
        });
        handles.push(handle);
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down workers...");
        }
        _ = shutdown_token.cancelled() => {
            tracing::info!("Shutdown token cancelled, stopping workers...");
        }
    }

    shutdown_token.cancel();

    let shutdown_timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    for (i, handle) in handles.into_iter().enumerate() {
        let remaining_time = shutdown_timeout.saturating_sub(start.elapsed());

        match tokio::time::timeout(remaining_time, handle).await {
            Ok(Ok(_)) => {
                tracing::debug!("Worker {} stopped gracefully", i);
            }
            Ok(Err(e)) => {
                tracing::error!("Worker {} panicked: {}", i, e);
            }
            Err(_) => {
                tracing::warn!("Worker {} did not stop within timeout", i);
            }
        }
    }

    tracing::info!("All workers stopped");
}

async fn worker_loop<Q: TaskQueue + Clone + 'static>(
    worker_id: usize,
    app_tasks: AppTasks<Q>,
    shutdown_token: CancellationToken,
) {
    tracing::debug!("Worker {} started", worker_id);

    let queue = app_tasks.queue_ref();
    while !shutdown_token.is_cancelled() {
        tokio::select! {
            task_result = queue.dequeue() => {
                match task_result {
                    Ok(Some(task)) => {
                        process_task(worker_id, task, &app_tasks).await;
                    }
                    Ok(None) => {
                        tracing::debug!("Worker {} queue closed", worker_id);
                        break;
                    }
                    Err(_) => {
                        tracing::debug!("Worker {} queue error", worker_id);
                        break;
                    }
                }
            }
            _ = shutdown_token.cancelled() => {
                tracing::debug!("Worker {} received shutdown signal", worker_id);
                break;
            }
        }
    }

    tracing::debug!("Worker {} stopped", worker_id);
}

async fn process_task<Q: TaskQueue + 'static>(worker_id: usize, task: QueuedTask, app_tasks: &AppTasks<Q>) {
    let start_time = std::time::Instant::now();

    tracing::debug!(
        task_id = %task.id,
        task_name = %task.task_name,
        worker_id = worker_id,
        retry_count = task.retry_count,
        "Processing task"
    );

    let queue_wait_time = chrono::Utc::now().signed_duration_since(task.created_at);
    if queue_wait_time > chrono::Duration::seconds(300) {
        tracing::warn!(
            task_id = %task.id,
            wait_time_ms = queue_wait_time.num_milliseconds(),
            "Task waited unusually long in queue - system may be overloaded"
        );
    }

    app_tasks
        .update_task_status(
            &task.id,
            TaskStatus::InProgress,
            Some(worker_id),
            None,
            None,
        )
        .await;

    let result = execute_task_from_registry(&task.task_name, &task.task_data).await;
    let duration = start_time.elapsed();
    let duration_ms = duration.as_millis() as u64;

    match result {
        TaskResult::Success => {
            app_tasks
                .update_task_status(
                    &task.id,
                    TaskStatus::Completed,
                    Some(worker_id),
                    Some(duration_ms),
                    None,
                )
                .await;

            app_tasks.metrics_ref().record_completed();

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

                app_tasks
                    .update_task_status(
                        &task.id,
                        TaskStatus::Retrying,
                        Some(worker_id),
                        Some(duration_ms),
                        Some(error.clone()),
                    )
                    .await;

                schedule_retry(task, delay, app_tasks).await;
            } else {
                app_tasks
                    .update_task_status(
                        &task.id,
                        TaskStatus::Failed,
                        Some(worker_id),
                        Some(duration_ms),
                        Some(format!("Max retries exceeded: {}", error)),
                    )
                    .await;

                app_tasks.metrics_ref().record_failed();

                tracing::error!(
                    task_id = %task.id,
                    error = %error,
                    retry_count = task.retry_count,
                    "Task failed permanently after max retries"
                );
            }
        }

        TaskResult::PermanentError(error) => {
            app_tasks
                .update_task_status(
                    &task.id,
                    TaskStatus::Failed,
                    Some(worker_id),
                    Some(duration_ms),
                    Some(error.clone()),
                )
                .await;

            app_tasks.metrics_ref().record_failed();

            tracing::error!(
                task_id = %task.id,
                error = %error,
                "Task failed permanently"
            );
        }
    }
}

async fn execute_task_from_registry(task_name: &str, task_data: &[u8]) -> TaskResult {
    // Use the global task registry to find and execute the task
    for registration in inventory::iter::<crate::TaskRegistration> {
        if registration.name == task_name {
            match (registration.handler)(task_data).await {
                Ok(result) => return result,
                Err(error_result) => return error_result,
            }
        }
    }

    TaskResult::PermanentError(format!("Unknown task type: {}", task_name))
}

async fn schedule_retry<Q: TaskQueue + 'static>(mut task: QueuedTask, delay: Duration, app_tasks: &AppTasks<Q>) {
    task.retry_count += 1;
    task.created_at = chrono::Utc::now();
    let queue = app_tasks.queue_ref();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        if let Err(e) = queue.enqueue(task.clone()).await {
            tracing::error!(
                task_id = %task.id,
                error = ?e,
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
