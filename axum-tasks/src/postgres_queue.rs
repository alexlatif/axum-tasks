// postgres_queue.rs
// Feature-gated with `postgres`
#[cfg(feature = "postgres")]
use tokio_postgres::{NoTls, Client as PgClient};
#[cfg(feature = "postgres")]
use std::sync::Arc;
#[cfg(feature = "postgres")]
use tokio::sync::Mutex;

#[cfg(feature = "postgres")]
pub struct PostgresQueue {
    client: Arc<Mutex<PgClient>>,
    queue_table: String,
}

#[cfg(feature = "postgres")]
impl PostgresQueue {
    pub async fn new(url: &str, queue_table: &str) -> Self {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await.expect("Failed to connect to Postgres");
        // Spawn the connection driver
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });
        let client = Arc::new(Mutex::new(client));
        let queue_table = queue_table.to_string();
        // Ensure table exists
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                task BYTEA NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            queue_table
        );
        client.lock().await.execute(&create_table, &[]).await.expect("Failed to create queue table");
        Self { client, queue_table }
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl crate::app_tasks::TaskQueue for PostgresQueue {
    async fn enqueue(&self, task: crate::types::QueuedTask) -> Result<String, crate::app_tasks::TaskQueueError> {
        let id = task.id.clone();
        let data = bincode::serialize(&task).map_err(|_| crate::app_tasks::TaskQueueError)?;
        let sql = format!("INSERT INTO {} (id, task, status) VALUES ($1, $2, 'queued') ON CONFLICT (id) DO NOTHING", self.queue_table);
        self.client.lock().await.execute(&sql, &[&id, &data]).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        Ok(id)
    }
    async fn dequeue(&self) -> Result<Option<crate::types::QueuedTask>, crate::app_tasks::TaskQueueError> {
        // Atomically select and update a queued task to in_progress
        let sql = format!(
            "UPDATE {} SET status = 'in_progress', updated_at = NOW() \
             WHERE id = (SELECT id FROM {} WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED) \
             RETURNING id, task",
            self.queue_table, self.queue_table
        );
        if let Some(row) = self.client.lock().await.query_opt(&sql, &[]).await.map_err(|_| crate::app_tasks::TaskQueueError)? {
            let data: Vec<u8> = row.get("task");
            let task: crate::types::QueuedTask = bincode::deserialize(&data).map_err(|_| crate::app_tasks::TaskQueueError)?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }
    async fn ack(&self, task_id: &str) -> Result<(), crate::app_tasks::TaskQueueError> {
        let sql = format!("DELETE FROM {} WHERE id = $1", self.queue_table);
        self.client.lock().await.execute(&sql, &[&task_id]).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        Ok(())
    }
    async fn nack(&self, task_id: &str, _retry_after: Option<std::time::Duration>) -> Result<(), crate::app_tasks::TaskQueueError> {
        let sql = format!("UPDATE {} SET status = 'queued', updated_at = NOW() WHERE id = $1", self.queue_table);
        self.client.lock().await.execute(&sql, &[&task_id]).await.map_err(|_| crate::app_tasks::TaskQueueError)?;
        Ok(())
    }
}
