use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct HeartbeatInfo {
    pub known_version: u64,
    pub reported_state: i32,
    pub generation_id: String,
    pub received_at: Instant,
}

pub struct HeartbeatBatcher {
    pending: Mutex<HashMap<String, HeartbeatInfo>>,
    flush_interval: Duration,
}

impl HeartbeatBatcher {
    pub fn new(flush_interval: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            flush_interval,
        }
    }

    pub fn record_heartbeat(&self, node_id: String, info: HeartbeatInfo) {
        let mut pending = self.pending.lock().unwrap();
        pending.insert(node_id, info);
    }

    pub fn take_batch(&self) -> HashMap<String, HeartbeatInfo> {
        let mut pending = self.pending.lock().unwrap();
        std::mem::take(&mut *pending)
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

pub async fn run_heartbeat_flush_loop<F, Fut, E>(batcher: Arc<HeartbeatBatcher>, flush_fn: F)
where
    F: Fn(HashMap<String, HeartbeatInfo>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let mut interval = tokio::time::interval(batcher.flush_interval());

    loop {
        interval.tick().await;

        let batch = batcher.take_batch();
        if batch.is_empty() {
            continue;
        }

        if let Err(e) = flush_fn(batch).await {
            tracing::warn!("Failed to flush heartbeat batch: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_batcher() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        assert_eq!(batcher.flush_interval(), Duration::from_secs(1));
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_record_heartbeat() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        let info = HeartbeatInfo {
            known_version: 42,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: Instant::now(),
        };

        batcher.record_heartbeat("node-1".to_string(), info);
        assert_eq!(batcher.pending_count(), 1);
    }

    #[test]
    fn test_record_heartbeat_overwrites_existing() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        let info1 = HeartbeatInfo {
            known_version: 10,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: Instant::now(),
        };
        batcher.record_heartbeat("node-1".to_string(), info1);

        let info2 = HeartbeatInfo {
            known_version: 20,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: Instant::now(),
        };
        batcher.record_heartbeat("node-1".to_string(), info2);

        assert_eq!(batcher.pending_count(), 1);
        let batch = batcher.take_batch();
        assert_eq!(batch.get("node-1").unwrap().known_version, 20);
    }

    #[test]
    fn test_take_batch_clears_pending() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        let info = HeartbeatInfo {
            known_version: 42,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: Instant::now(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 1);
        assert!(batch.contains_key("node-1"));
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_take_batch_returns_empty_when_no_pending() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        let batch = batcher.take_batch();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_multiple_nodes() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        for i in 1..=5 {
            let info = HeartbeatInfo {
                known_version: i as u64,
                reported_state: 1,
                generation_id: format!("gen-{}", i),
                received_at: Instant::now(),
            };
            batcher.record_heartbeat(format!("node-{}", i), info);
        }

        assert_eq!(batcher.pending_count(), 5);
        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 5);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_processes_batch() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(50)));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let total_nodes = Arc::new(AtomicUsize::new(0));

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);
        let total_nodes_clone = Arc::clone(&total_nodes);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(batcher_clone, |batch| {
                let flush_count = Arc::clone(&flush_count_clone);
                let total_nodes = Arc::clone(&total_nodes_clone);
                async move {
                    flush_count.fetch_add(1, Ordering::SeqCst);
                    total_nodes.fetch_add(batch.len(), Ordering::SeqCst);
                    Ok::<(), String>(())
                }
            })
            .await;
        });

        for i in 1..=3 {
            let info = HeartbeatInfo {
                known_version: i as u64,
                reported_state: 1,
                generation_id: format!("gen-{}", i),
                received_at: Instant::now(),
            };
            batcher.record_heartbeat(format!("node-{}", i), info);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        handle.abort();

        assert!(flush_count.load(Ordering::SeqCst) >= 1);
        assert_eq!(total_nodes.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_skips_empty_batch() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(50)));
        let flush_count = Arc::new(AtomicUsize::new(0));

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(batcher_clone, |_batch| {
                let flush_count = Arc::clone(&flush_count_clone);
                async move {
                    flush_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), String>(())
                }
            })
            .await;
        });

        tokio::time::sleep(Duration::from_millis(150)).await;

        handle.abort();

        assert_eq!(flush_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_handles_errors() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(50)));
        let flush_count = Arc::new(AtomicUsize::new(0));

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(batcher_clone, |_batch| {
                let flush_count = Arc::clone(&flush_count_clone);
                async move {
                    flush_count.fetch_add(1, Ordering::SeqCst);
                    Err::<(), String>("test error".to_string())
                }
            })
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: Instant::now(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let info2 = HeartbeatInfo {
            known_version: 2,
            reported_state: 1,
            generation_id: "gen-2".to_string(),
            received_at: Instant::now(),
        };
        batcher.record_heartbeat("node-2".to_string(), info2);

        tokio::time::sleep(Duration::from_millis(100)).await;

        handle.abort();

        assert!(flush_count.load(Ordering::SeqCst) >= 2);
    }
}
