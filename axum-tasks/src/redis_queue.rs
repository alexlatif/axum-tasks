// RedisQueue implementation stub for distributed task processing
// Feature-gated with `redis`
#[cfg(feature = "redis")]
use redis::{AsyncCommands, aio::ConnectionManager, Client};
#[cfg(feature = "redis")]
use std::sync::Arc;
#[cfg(feature = "redis")]
use tokio::sync::Mutex;

#[cfg(feature = "redis")]
pub struct RedisQueue {
    conn: Arc<Mutex<ConnectionManager>>,
    queue_key: String,
    in_progress_key: String,
}

#[cfg(feature = "redis")]
impl RedisQueue {
    pub async fn new(url: &str, queue: &str) -> Self {
        let client = Client::open(url).expect("Invalid Redis URL");
        let conn = client.get_connection_manager().await.expect("Failed to connect to Redis");
        let queue_key = queue.to_string();
        let in_progress_key = format!("{}_inprogress", queue);
        Self {
            conn: Arc::new(Mutex::new(conn)),
            queue_key,
            in_progress_key,
        }
    }
}

#[cfg(feature = "redis")]
#[async_trait::async_trait]
impl crate::app_tasks::TaskQueue for RedisQueue {
    async fn enqueue(&self, task: crate::types::QueuedTask) -> Result<String, crate::app_tasks::TaskQueueError> {
        let id = task.id.clone();
        let data = bincode::serialize(&task).map_err(|_| crate::app_tasks::TaskQueueError)?;
        let mut conn = self.conn.lock().await;
        let _: () = conn.rpush(&self.queue_key, data).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        Ok(id)
    }
    async fn dequeue(&self) -> Result<Option<crate::types::QueuedTask>, crate::app_tasks::TaskQueueError> {
        let mut conn = self.conn.lock().await;
        // BRPOPLPUSH: atomically pop from main queue, push to in-progress
        let data: Option<Vec<u8>> = conn.brpoplpush(&self.queue_key, &self.in_progress_key, 5.0).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        if let Some(data) = data {
            let task: crate::types::QueuedTask = bincode::deserialize(&data).map_err(|_| crate::app_tasks::TaskQueueError)?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }
    async fn ack(&self, task_id: &str) -> Result<(), crate::app_tasks::TaskQueueError> {
        let mut conn = self.conn.lock().await;
        // Remove from in-progress by scanning for the task with matching id
        let tasks: Vec<Vec<u8>> = conn.lrange(&self.in_progress_key, 0, -1).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        for data in tasks.iter() {
            if let Ok(task) = bincode::deserialize::<crate::types::QueuedTask>(data) {
                if task.id == task_id {
                    let _: () = conn.lrem(&self.in_progress_key, 1, data).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
                    break;
                }
            }
        }
        Ok(())
    }
    async fn nack(&self, task_id: &str, _retry_after: Option<std::time::Duration>) -> Result<(), crate::app_tasks::TaskQueueError> {
        let mut conn = self.conn.lock().await;
        // Move back to main queue
        let tasks: Vec<Vec<u8>> = conn.lrange(&self.in_progress_key, 0, -1).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        for data in tasks.iter() {
            if let Ok(task) = bincode::deserialize::<crate::types::QueuedTask>(data) {
                if task.id == task_id {
                    let _: () = conn.lrem(&self.in_progress_key, 1, data).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
                    let _: () = conn.rpush(&self.queue_key, data).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
                    break;
                }
            }
        }
        Ok(())
    }
}
