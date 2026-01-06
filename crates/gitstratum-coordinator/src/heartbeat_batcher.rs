use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::sync::watch;

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 50;
const DEFAULT_MAX_BACKOFF_MS: u64 = 500;

#[derive(Debug, Clone)]
pub struct HeartbeatInfo {
    pub known_version: u64,
    pub reported_state: i32,
    pub generation_id: String,
    pub received_at: i64,
}

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl HeartbeatInfo {
    pub fn new(known_version: u64, reported_state: i32, generation_id: String) -> Self {
        Self {
            known_version,
            reported_state,
            generation_id,
            received_at: unix_timestamp_ms(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlushConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff: Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS),
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
        }
    }
}

pub struct HeartbeatBatcher {
    pending: Mutex<HashMap<String, HeartbeatInfo>>,
    flush_interval: Duration,
    flush_config: FlushConfig,
}

impl HeartbeatBatcher {
    pub fn new(flush_interval: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            flush_interval,
            flush_config: FlushConfig::default(),
        }
    }

    pub fn with_flush_config(flush_interval: Duration, flush_config: FlushConfig) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            flush_interval,
            flush_config,
        }
    }

    pub fn record_heartbeat(&self, node_id: String, info: HeartbeatInfo) {
        let mut pending = self.pending.lock();
        pending.insert(node_id, info);
    }

    pub fn take_batch(&self) -> HashMap<String, HeartbeatInfo> {
        let mut pending = self.pending.lock();
        std::mem::take(&mut *pending)
    }

    pub fn merge_batch(&self, batch: HashMap<String, HeartbeatInfo>) {
        if batch.is_empty() {
            return;
        }
        let mut pending = self.pending.lock();
        for (node_id, info) in batch {
            pending.entry(node_id).or_insert(info);
        }
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    pub fn flush_config(&self) -> &FlushConfig {
        &self.flush_config
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }
}

pub async fn run_heartbeat_flush_loop<F, Fut, L, E>(
    batcher: Arc<HeartbeatBatcher>,
    flush_fn: F,
    is_leader_fn: L,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    F: Fn(HashMap<String, HeartbeatInfo>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
    L: Fn() -> bool,
    E: std::fmt::Display,
{
    let flush_config = batcher.flush_config().clone();
    let flush_interval = batcher.flush_interval();
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + flush_interval, flush_interval);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                flush_batch_with_retry(&batcher, &flush_fn, &is_leader_fn, &flush_config).await;
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("Heartbeat flush loop received shutdown signal, flushing pending batch");
                    flush_batch_with_retry(&batcher, &flush_fn, &is_leader_fn, &flush_config).await;
                    tracing::info!("Heartbeat flush loop shutdown complete");
                    return;
                }
            }
        }
    }
}

async fn flush_batch_with_retry<F, Fut, L, E>(
    batcher: &Arc<HeartbeatBatcher>,
    flush_fn: &F,
    is_leader_fn: &L,
    flush_config: &FlushConfig,
) where
    F: Fn(HashMap<String, HeartbeatInfo>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
    L: Fn() -> bool,
    E: std::fmt::Display,
{
    if !is_leader_fn() {
        tracing::trace!("Not leader, skipping heartbeat batch flush");
        return;
    }

    let batch = batcher.take_batch();
    if batch.is_empty() {
        return;
    }

    let batch_size = batch.len();
    let mut backoff = flush_config.initial_backoff;

    for attempt in 0..=flush_config.max_retries {
        if attempt > 0 {
            tracing::debug!(
                attempt = attempt,
                backoff_ms = backoff.as_millis(),
                "Retrying heartbeat batch flush"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(flush_config.max_backoff);
        }

        match flush_fn(batch.clone()).await {
            Ok(()) => {
                tracing::debug!(
                    batch_size = batch_size,
                    "Heartbeat batch flushed successfully"
                );
                return;
            }
            Err(e) => {
                if attempt == flush_config.max_retries {
                    tracing::warn!(
                        batch_size = batch_size,
                        attempts = attempt + 1,
                        error = %e,
                        "Failed to flush heartbeat batch after all retries, re-queuing"
                    );
                    batcher.merge_batch(batch);
                    return;
                }
                tracing::debug!(
                    attempt = attempt,
                    error = %e,
                    "Flush attempt failed, will retry"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_new_batcher() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        assert_eq!(batcher.flush_interval(), Duration::from_secs(1));
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_with_flush_config() {
        let flush_config = FlushConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };
        let batcher = HeartbeatBatcher::with_flush_config(Duration::from_secs(2), flush_config);
        assert_eq!(batcher.flush_interval(), Duration::from_secs(2));
        assert_eq!(batcher.flush_config().max_retries, 5);
        assert_eq!(
            batcher.flush_config().initial_backoff,
            Duration::from_millis(100)
        );
        assert_eq!(batcher.flush_config().max_backoff, Duration::from_secs(1));
    }

    #[test]
    fn test_record_heartbeat() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        let info = HeartbeatInfo {
            known_version: 42,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: unix_timestamp_ms(),
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
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info1);

        let info2 = HeartbeatInfo {
            known_version: 20,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: unix_timestamp_ms(),
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
            received_at: unix_timestamp_ms(),
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
    fn test_merge_batch_empty() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        batcher.merge_batch(HashMap::new());
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_merge_batch_into_empty() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        let mut batch = HashMap::new();
        batch.insert(
            "node-1".to_string(),
            HeartbeatInfo {
                known_version: 10,
                reported_state: 1,
                generation_id: "gen-1".to_string(),
                received_at: unix_timestamp_ms(),
            },
        );

        batcher.merge_batch(batch);
        assert_eq!(batcher.pending_count(), 1);
        let taken = batcher.take_batch();
        assert_eq!(taken.get("node-1").unwrap().known_version, 10);
    }

    #[test]
    fn test_merge_batch_preserves_newer_pending() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        batcher.record_heartbeat(
            "node-1".to_string(),
            HeartbeatInfo {
                known_version: 20,
                reported_state: 1,
                generation_id: "gen-new".to_string(),
                received_at: unix_timestamp_ms(),
            },
        );

        let mut old_batch = HashMap::new();
        old_batch.insert(
            "node-1".to_string(),
            HeartbeatInfo {
                known_version: 10,
                reported_state: 1,
                generation_id: "gen-old".to_string(),
                received_at: unix_timestamp_ms(),
            },
        );

        batcher.merge_batch(old_batch);

        assert_eq!(batcher.pending_count(), 1);
        let taken = batcher.take_batch();
        assert_eq!(taken.get("node-1").unwrap().known_version, 20);
    }

    #[test]
    fn test_merge_batch_adds_missing_nodes() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        batcher.record_heartbeat(
            "node-1".to_string(),
            HeartbeatInfo {
                known_version: 10,
                reported_state: 1,
                generation_id: "gen-1".to_string(),
                received_at: unix_timestamp_ms(),
            },
        );

        let mut batch = HashMap::new();
        batch.insert(
            "node-2".to_string(),
            HeartbeatInfo {
                known_version: 20,
                reported_state: 1,
                generation_id: "gen-2".to_string(),
                received_at: unix_timestamp_ms(),
            },
        );

        batcher.merge_batch(batch);

        assert_eq!(batcher.pending_count(), 2);
        let taken = batcher.take_batch();
        assert!(taken.contains_key("node-1"));
        assert!(taken.contains_key("node-2"));
    }

    #[test]
    fn test_multiple_nodes() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));

        for i in 1..=5 {
            let info = HeartbeatInfo {
                known_version: i as u64,
                reported_state: 1,
                generation_id: format!("gen-{}", i),
                received_at: unix_timestamp_ms(),
            };
            batcher.record_heartbeat(format!("node-{}", i), info);
        }

        assert_eq!(batcher.pending_count(), 5);
        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 5);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_flush_config_default() {
        let config = FlushConfig::default();
        assert_eq!(config.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(
            config.initial_backoff,
            Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS)
        );
        assert_eq!(
            config.max_backoff,
            Duration::from_millis(DEFAULT_MAX_BACKOFF_MS)
        );
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_processes_batch() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(50)));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let total_nodes = Arc::new(AtomicUsize::new(0));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);
        let total_nodes_clone = Arc::clone(&total_nodes);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |batch| {
                    let flush_count = Arc::clone(&flush_count_clone);
                    let total_nodes = Arc::clone(&total_nodes_clone);
                    async move {
                        flush_count.fetch_add(1, Ordering::SeqCst);
                        total_nodes.fetch_add(batch.len(), Ordering::SeqCst);
                        Ok::<(), String>(())
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        for i in 1..=3 {
            let info = HeartbeatInfo {
                known_version: i as u64,
                reported_state: 1,
                generation_id: format!("gen-{}", i),
                received_at: unix_timestamp_ms(),
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
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let flush_count = Arc::clone(&flush_count_clone);
                    async move {
                        flush_count.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), String>(())
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(150)).await;

        handle.abort();

        assert_eq!(flush_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_skips_when_not_leader() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(50)));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let flush_count = Arc::clone(&flush_count_clone);
                    async move {
                        flush_count.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), String>(())
                    }
                },
                || false,
                shutdown_rx,
            )
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(150)).await;

        handle.abort();

        assert_eq!(flush_count.load(Ordering::SeqCst), 0);
        assert_eq!(batcher.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_requeues_on_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let flush_config = FlushConfig {
            max_retries: 1,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(20),
        };
        let batcher = Arc::new(HeartbeatBatcher::with_flush_config(
            Duration::from_millis(50),
            flush_config,
        ));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let flush_count = Arc::clone(&flush_count_clone);
                    async move {
                        flush_count.fetch_add(1, Ordering::SeqCst);
                        Err::<(), String>("test error".to_string())
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(150)).await;

        handle.abort();

        assert!(flush_count.load(Ordering::SeqCst) >= 2);
        assert_eq!(batcher.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_retry_succeeds() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let flush_config = FlushConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
        };
        let batcher = Arc::new(HeartbeatBatcher::with_flush_config(
            Duration::from_millis(50),
            flush_config,
        ));
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let attempt_count_clone = Arc::clone(&attempt_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let attempt_count = Arc::clone(&attempt_count_clone);
                    async move {
                        let count = attempt_count.fetch_add(1, Ordering::SeqCst);
                        if count < 2 {
                            Err::<(), String>("transient error".to_string())
                        } else {
                            Ok(())
                        }
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(200)).await;

        let _ = shutdown_tx.send(true);

        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_graceful_shutdown() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_secs(10)));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let flush_count = Arc::clone(&flush_count_clone);
                    async move {
                        flush_count.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), String>(())
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(flush_count.load(Ordering::SeqCst), 0);

        let _ = shutdown_tx.send(true);

        let result = tokio::time::timeout(Duration::from_millis(200), handle).await;
        assert!(result.is_ok());

        assert_eq!(flush_count.load(Ordering::SeqCst), 1);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_run_heartbeat_flush_loop_continues_after_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let flush_config = FlushConfig {
            max_retries: 0,
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
        };
        let batcher = Arc::new(HeartbeatBatcher::with_flush_config(
            Duration::from_millis(50),
            flush_config,
        ));
        let flush_count = Arc::new(AtomicUsize::new(0));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let flush_count = Arc::clone(&flush_count_clone);
                    async move {
                        flush_count.fetch_add(1, Ordering::SeqCst);
                        Err::<(), String>("persistent error".to_string())
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(200)).await;

        handle.abort();

        assert!(flush_count.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn test_flush_batch_with_retry_backoff_increases() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Mutex as StdMutex;

        let flush_config = FlushConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
        };
        let batcher = Arc::new(HeartbeatBatcher::with_flush_config(
            Duration::from_millis(50),
            flush_config,
        ));
        let attempt_times: Arc<StdMutex<Vec<Instant>>> = Arc::new(StdMutex::new(Vec::new()));
        let attempt_count = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let batcher_clone = Arc::clone(&batcher);
        let attempt_times_clone = Arc::clone(&attempt_times);
        let attempt_count_clone = Arc::clone(&attempt_count);

        let handle = tokio::spawn(async move {
            run_heartbeat_flush_loop(
                batcher_clone,
                |_batch| {
                    let attempt_times = Arc::clone(&attempt_times_clone);
                    let attempt_count = Arc::clone(&attempt_count_clone);
                    async move {
                        attempt_times.lock().unwrap().push(Instant::now());
                        attempt_count.fetch_add(1, Ordering::SeqCst);
                        Err::<(), String>("error".to_string())
                    }
                },
                || true,
                shutdown_rx,
            )
            .await;
        });

        let info = HeartbeatInfo {
            known_version: 1,
            reported_state: 1,
            generation_id: "gen-1".to_string(),
            received_at: unix_timestamp_ms(),
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        tokio::time::sleep(Duration::from_millis(400)).await;

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        let times = attempt_times.lock().unwrap();
        assert!(times.len() >= 4);

        if times.len() >= 4 {
            let gap1 = times[1].duration_since(times[0]);
            let gap2 = times[2].duration_since(times[1]);
            let gap3 = times[3].duration_since(times[2]);

            assert!(gap2 >= gap1);
            assert!(gap3 >= gap2 || gap3.as_millis() >= 80);
        }
    }
}
