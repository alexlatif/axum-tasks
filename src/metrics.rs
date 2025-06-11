use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe metrics for task processing
#[derive(Debug, Default)]
pub struct TaskMetrics {
    /// Total tasks queued since startup
    tasks_queued: AtomicU64,
    /// Total tasks completed successfully
    tasks_completed: AtomicU64,
    /// Total tasks that failed permanently
    tasks_failed: AtomicU64,
    /// Total tasks retried
    tasks_retried: AtomicU64,
    /// Current queue depth
    queue_depth: AtomicU64,
    /// Total processing time in milliseconds
    total_processing_time_ms: AtomicU64,
    /// Peak queue depth seen
    peak_queue_depth: AtomicU64,
    /// When metrics were last reset
    reset_at: AtomicU64, // Unix timestamp
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            reset_at: AtomicU64::new(chrono::Utc::now().timestamp() as u64),
            ..Default::default()
        }
    }

    /// Record a task being queued
    pub fn record_queued(&self) {
        self.tasks_queued.fetch_add(1, Ordering::Relaxed);
        let new_depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;

        // Update peak if necessary
        self.update_peak_depth(new_depth);
    }

    /// Record a task completing successfully
    pub fn record_completed(&self) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a task failing permanently
    pub fn record_failed(&self) {
        self.tasks_failed.fetch_add(1, Ordering::Relaxed);
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a task being retried
    pub fn record_retried(&self) {
        self.tasks_retried.fetch_add(1, Ordering::Relaxed);
        // Don't change queue depth for retries
    }

    /// Record task processing time
    pub fn record_processing_time(&self, duration_ms: u64) {
        self.total_processing_time_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Get current queue depth
    pub fn get_queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Get snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let queued = self.tasks_queued.load(Ordering::Relaxed);
        let completed = self.tasks_completed.load(Ordering::Relaxed);
        let failed = self.tasks_failed.load(Ordering::Relaxed);
        let retried = self.tasks_retried.load(Ordering::Relaxed);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let total_time = self.total_processing_time_ms.load(Ordering::Relaxed);
        let peak_depth = self.peak_queue_depth.load(Ordering::Relaxed);
        let reset_timestamp = self.reset_at.load(Ordering::Relaxed);

        let processed = completed + failed;
        let success_rate = if processed > 0 {
            (completed as f64 / processed as f64) * 100.0
        } else {
            0.0
        };

        let avg_processing_time = if completed > 0 {
            total_time as f64 / completed as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            tasks_queued: queued,
            tasks_completed: completed,
            tasks_failed: failed,
            tasks_retried: retried,
            tasks_in_progress: queue_depth,
            peak_queue_depth: peak_depth,
            success_rate_percent: success_rate,
            average_processing_time_ms: avg_processing_time,
            total_processing_time_ms: total_time,
            uptime_seconds: chrono::Utc::now().timestamp() as u64 - reset_timestamp,
            timestamp: Utc::now(),
        }
    }

    /// Reset all metrics (useful for testing or periodic resets)
    pub fn reset(&self) {
        self.tasks_queued.store(0, Ordering::Relaxed);
        self.tasks_completed.store(0, Ordering::Relaxed);
        self.tasks_failed.store(0, Ordering::Relaxed);
        self.tasks_retried.store(0, Ordering::Relaxed);
        self.total_processing_time_ms.store(0, Ordering::Relaxed);
        self.peak_queue_depth
            .store(self.queue_depth.load(Ordering::Relaxed), Ordering::Relaxed);
        self.reset_at
            .store(chrono::Utc::now().timestamp() as u64, Ordering::Relaxed);
    }

    fn update_peak_depth(&self, current_depth: u64) {
        self.peak_queue_depth
            .fetch_max(current_depth, Ordering::Relaxed);
    }
}

/// Immutable snapshot of metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Total tasks queued since last reset
    pub tasks_queued: u64,
    /// Total tasks completed successfully
    pub tasks_completed: u64,
    /// Total tasks that failed permanently
    pub tasks_failed: u64,
    /// Total retry attempts
    pub tasks_retried: u64,
    /// Current tasks being processed
    pub tasks_in_progress: u64,
    /// Highest queue depth reached
    pub peak_queue_depth: u64,
    /// Success rate as percentage
    pub success_rate_percent: f64,
    /// Average processing time per task in milliseconds
    pub average_processing_time_ms: f64,
    /// Total processing time across all tasks
    pub total_processing_time_ms: u64,
    /// Uptime in seconds since last reset
    pub uptime_seconds: u64,
    /// When this snapshot was taken
    pub timestamp: DateTime<Utc>,
}

impl MetricsSnapshot {
    /// Get throughput in tasks per second
    pub fn throughput_per_second(&self) -> f64 {
        if self.uptime_seconds > 0 {
            (self.tasks_completed + self.tasks_failed) as f64 / self.uptime_seconds as f64
        } else {
            0.0
        }
    }

    /// Get failure rate as percentage
    pub fn failure_rate_percent(&self) -> f64 {
        100.0 - self.success_rate_percent
    }

    /// Check if system is healthy based on metrics
    pub fn is_healthy(&self) -> bool {
        self.success_rate_percent >= 95.0
            && self.tasks_in_progress < (crate::types::MAX_QUEUE_SIZE as u64 / 2)
    }
}
