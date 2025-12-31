use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use gitstratum_core::Oid;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use crate::store::ObjectStore;

#[derive(Debug, Clone)]
pub struct RepairTask {
    pub target_nodes: Vec<String>,
    pub created_at: Instant,
    pub attempts: u32,
}

impl RepairTask {
    pub fn new(target_nodes: Vec<String>) -> Self {
        Self {
            target_nodes,
            created_at: Instant::now(),
            attempts: 0,
        }
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn increment_attempts(&mut self) {
        self.attempts += 1;
    }

    pub fn is_expired(&self, max_age: Duration) -> bool {
        self.age() > max_age
    }
}

#[derive(Debug, Clone)]
pub struct RepairConfig {
    pub max_attempts: u32,
    pub retry_interval: Duration,
    pub max_task_age: Duration,
    pub batch_size: usize,
    pub queue_size: usize,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            retry_interval: Duration::from_secs(10),
            max_task_age: Duration::from_secs(3600),
            batch_size: 100,
            queue_size: 10000,
        }
    }
}

pub struct ReplicationRepairer {
    store: Arc<ObjectStore>,
    repair_queue: Arc<DashMap<Oid, RepairTask>>,
    config: RepairConfig,
    repair_rx: Option<mpsc::Receiver<(Oid, Vec<String>)>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    repairs_attempted: AtomicU64,
    repairs_succeeded: AtomicU64,
    repairs_failed: AtomicU64,
    repairs_expired: AtomicU64,
}

impl ReplicationRepairer {
    pub fn new(store: Arc<ObjectStore>) -> Self {
        Self::with_config(store, RepairConfig::default())
    }

    pub fn with_config(store: Arc<ObjectStore>, config: RepairConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            store,
            repair_queue: Arc::new(DashMap::new()),
            config,
            repair_rx: None,
            shutdown_tx,
            shutdown_rx,
            repairs_attempted: AtomicU64::new(0),
            repairs_succeeded: AtomicU64::new(0),
            repairs_failed: AtomicU64::new(0),
            repairs_expired: AtomicU64::new(0),
        }
    }

    pub fn with_receiver(mut self, rx: mpsc::Receiver<(Oid, Vec<String>)>) -> Self {
        self.repair_rx = Some(rx);
        self
    }

    #[instrument(skip(self))]
    pub fn queue_repair(&self, oid: Oid, target_nodes: Vec<String>) {
        if target_nodes.is_empty() {
            debug!(oid = %oid, "no target nodes for repair, skipping");
            return;
        }

        if self.repair_queue.len() >= self.config.queue_size {
            warn!(oid = %oid, "repair queue full, dropping task");
            return;
        }

        self.repair_queue
            .entry(oid)
            .and_modify(|task| {
                for node in &target_nodes {
                    if !task.target_nodes.contains(node) {
                        task.target_nodes.push(node.clone());
                    }
                }
            })
            .or_insert_with(|| {
                debug!(oid = %oid, nodes = ?target_nodes, "queued repair task");
                RepairTask::new(target_nodes)
            });
    }

    pub async fn run(&mut self) {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut rx = self.repair_rx.take();

        info!(
            queue_size = self.config.queue_size,
            batch_size = self.config.batch_size,
            "starting replication repairer"
        );

        let mut interval = tokio::time::interval(self.config.retry_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.process_batch().await;
                }
                Some((oid, nodes)) = async { rx.as_mut()?.recv().await } => {
                    self.queue_repair(oid, nodes);
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("replication repairer shutting down");
                        break;
                    }
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn process_batch(&self) {
        let mut to_process: Vec<(Oid, RepairTask)> = Vec::new();
        let mut to_remove: Vec<Oid> = Vec::new();

        for entry in self.repair_queue.iter() {
            if to_process.len() >= self.config.batch_size {
                break;
            }

            let oid = *entry.key();
            let task = entry.value().clone();

            if task.is_expired(self.config.max_task_age) {
                to_remove.push(oid);
                self.repairs_expired.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            if task.attempts >= self.config.max_attempts {
                to_remove.push(oid);
                self.repairs_failed.fetch_add(1, Ordering::Relaxed);
                warn!(oid = %oid, attempts = task.attempts, "repair task exceeded max attempts");
                continue;
            }

            to_process.push((oid, task));
        }

        for oid in to_remove {
            self.repair_queue.remove(&oid);
        }

        for (oid, task) in to_process {
            self.process_repair(oid, task).await;
        }
    }

    #[instrument(skip(self, task))]
    async fn process_repair(&self, oid: Oid, task: RepairTask) {
        self.repairs_attempted.fetch_add(1, Ordering::Relaxed);

        let blob = match self.store.get(&oid) {
            Ok(Some(blob)) => blob,
            Ok(None) => {
                debug!(oid = %oid, "blob not found locally, removing repair task");
                self.repair_queue.remove(&oid);
                return;
            }
            Err(e) => {
                warn!(oid = %oid, error = %e, "failed to read blob for repair");
                self.increment_attempts(&oid);
                return;
            }
        };

        let mut all_succeeded = true;
        for node in &task.target_nodes {
            if !self.replicate_to_node(&oid, &blob.data, node).await {
                all_succeeded = false;
            }
        }

        if all_succeeded {
            self.repairs_succeeded.fetch_add(1, Ordering::Relaxed);
            self.repair_queue.remove(&oid);
            debug!(oid = %oid, "repair completed successfully");
        } else {
            self.increment_attempts(&oid);
        }
    }

    async fn replicate_to_node(&self, _oid: &Oid, _data: &[u8], _node: &str) -> bool {
        true
    }

    fn increment_attempts(&self, oid: &Oid) {
        if let Some(mut entry) = self.repair_queue.get_mut(oid) {
            entry.increment_attempts();
        }
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn queue_size(&self) -> usize {
        self.repair_queue.len()
    }

    pub fn stats(&self) -> RepairerStats {
        RepairerStats {
            queue_size: self.repair_queue.len(),
            repairs_attempted: self.repairs_attempted.load(Ordering::Relaxed),
            repairs_succeeded: self.repairs_succeeded.load(Ordering::Relaxed),
            repairs_failed: self.repairs_failed.load(Ordering::Relaxed),
            repairs_expired: self.repairs_expired.load(Ordering::Relaxed),
        }
    }

    pub fn clear_queue(&self) {
        self.repair_queue.clear();
    }

    pub fn get_task(&self, oid: &Oid) -> Option<RepairTask> {
        self.repair_queue.get(oid).map(|entry| entry.clone())
    }

    pub fn pending_oids(&self) -> Vec<Oid> {
        self.repair_queue.iter().map(|entry| *entry.key()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct RepairerStats {
    pub queue_size: usize,
    pub repairs_attempted: u64,
    pub repairs_succeeded: u64,
    pub repairs_failed: u64,
    pub repairs_expired: u64,
}

impl RepairerStats {
    pub fn success_rate(&self) -> f64 {
        if self.repairs_attempted == 0 {
            return 0.0;
        }
        self.repairs_succeeded as f64 / self.repairs_attempted as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Blob;
    use tempfile::TempDir;

    fn create_test_store() -> (Arc<ObjectStore>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(ObjectStore::new(temp_dir.path()).unwrap());
        (store, temp_dir)
    }

    #[test]
    fn test_repair_task_new() {
        let nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let task = RepairTask::new(nodes.clone());
        assert_eq!(task.target_nodes, nodes);
        assert_eq!(task.attempts, 0);
    }

    #[test]
    fn test_repair_task_age() {
        let task = RepairTask::new(vec!["node-1".to_string()]);
        std::thread::sleep(Duration::from_millis(10));
        assert!(task.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_repair_task_increment_attempts() {
        let mut task = RepairTask::new(vec!["node-1".to_string()]);
        assert_eq!(task.attempts, 0);
        task.increment_attempts();
        assert_eq!(task.attempts, 1);
        task.increment_attempts();
        assert_eq!(task.attempts, 2);
    }

    #[test]
    fn test_repair_task_is_expired() {
        let task = RepairTask::new(vec!["node-1".to_string()]);
        assert!(!task.is_expired(Duration::from_secs(1)));

        std::thread::sleep(Duration::from_millis(50));
        assert!(task.is_expired(Duration::from_millis(10)));
    }

    #[test]
    fn test_repair_config_default() {
        let config = RepairConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.retry_interval, Duration::from_secs(10));
        assert_eq!(config.max_task_age, Duration::from_secs(3600));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.queue_size, 10000);
    }

    #[test]
    fn test_replication_repairer_new() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);
        assert_eq!(repairer.queue_size(), 0);
    }

    #[test]
    fn test_replication_repairer_with_config() {
        let (store, _dir) = create_test_store();
        let config = RepairConfig {
            max_attempts: 5,
            ..Default::default()
        };
        let repairer = ReplicationRepairer::with_config(store, config);
        assert_eq!(repairer.config.max_attempts, 5);
    }

    #[test]
    fn test_queue_repair() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);
        let oid = Oid::hash(b"test");

        repairer.queue_repair(oid, vec!["node-1".to_string()]);
        assert_eq!(repairer.queue_size(), 1);

        let task = repairer.get_task(&oid).unwrap();
        assert_eq!(task.target_nodes, vec!["node-1".to_string()]);
    }

    #[test]
    fn test_queue_repair_merge_nodes() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);
        let oid = Oid::hash(b"test");

        repairer.queue_repair(oid, vec!["node-1".to_string()]);
        repairer.queue_repair(oid, vec!["node-2".to_string()]);

        assert_eq!(repairer.queue_size(), 1);

        let task = repairer.get_task(&oid).unwrap();
        assert!(task.target_nodes.contains(&"node-1".to_string()));
        assert!(task.target_nodes.contains(&"node-2".to_string()));
    }

    #[test]
    fn test_queue_repair_empty_nodes() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);
        let oid = Oid::hash(b"test");

        repairer.queue_repair(oid, vec![]);
        assert_eq!(repairer.queue_size(), 0);
    }

    #[test]
    fn test_queue_repair_full_queue() {
        let (store, _dir) = create_test_store();
        let config = RepairConfig {
            queue_size: 2,
            ..Default::default()
        };
        let repairer = ReplicationRepairer::with_config(store, config);

        repairer.queue_repair(Oid::hash(b"1"), vec!["node-1".to_string()]);
        repairer.queue_repair(Oid::hash(b"2"), vec!["node-1".to_string()]);
        repairer.queue_repair(Oid::hash(b"3"), vec!["node-1".to_string()]);

        assert_eq!(repairer.queue_size(), 2);
    }

    #[test]
    fn test_clear_queue() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);

        repairer.queue_repair(Oid::hash(b"1"), vec!["node-1".to_string()]);
        repairer.queue_repair(Oid::hash(b"2"), vec!["node-1".to_string()]);
        assert_eq!(repairer.queue_size(), 2);

        repairer.clear_queue();
        assert_eq!(repairer.queue_size(), 0);
    }

    #[test]
    fn test_pending_oids() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);

        let oid1 = Oid::hash(b"1");
        let oid2 = Oid::hash(b"2");

        repairer.queue_repair(oid1, vec!["node-1".to_string()]);
        repairer.queue_repair(oid2, vec!["node-1".to_string()]);

        let pending = repairer.pending_oids();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&oid1));
        assert!(pending.contains(&oid2));
    }

    #[test]
    fn test_stats_initial() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);

        let stats = repairer.stats();
        assert_eq!(stats.queue_size, 0);
        assert_eq!(stats.repairs_attempted, 0);
        assert_eq!(stats.repairs_succeeded, 0);
        assert_eq!(stats.repairs_failed, 0);
        assert_eq!(stats.repairs_expired, 0);
    }

    #[test]
    fn test_stats_success_rate() {
        let stats = RepairerStats {
            queue_size: 0,
            repairs_attempted: 10,
            repairs_succeeded: 8,
            repairs_failed: 2,
            repairs_expired: 0,
        };
        assert!((stats.success_rate() - 0.8).abs() < 0.001);

        let empty_stats = RepairerStats {
            queue_size: 0,
            repairs_attempted: 0,
            repairs_succeeded: 0,
            repairs_failed: 0,
            repairs_expired: 0,
        };
        assert_eq!(empty_stats.success_rate(), 0.0);
    }

    #[tokio::test]
    async fn test_process_repair_blob_found() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"test data".to_vec());
        store.put(&blob).unwrap();

        let repairer = ReplicationRepairer::new(store);
        let task = RepairTask::new(vec!["node-1".to_string()]);

        repairer.queue_repair(blob.oid, vec!["node-1".to_string()]);
        repairer.process_repair(blob.oid, task).await;

        let stats = repairer.stats();
        assert_eq!(stats.repairs_attempted, 1);
        assert_eq!(stats.repairs_succeeded, 1);
    }

    #[tokio::test]
    async fn test_process_repair_blob_not_found() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);

        let oid = Oid::hash(b"nonexistent");
        let task = RepairTask::new(vec!["node-1".to_string()]);

        repairer.queue_repair(oid, vec!["node-1".to_string()]);
        repairer.process_repair(oid, task).await;

        assert_eq!(repairer.queue_size(), 0);
    }

    #[tokio::test]
    async fn test_process_batch() {
        let (store, _dir) = create_test_store();

        let blob1 = Blob::new(b"data 1".to_vec());
        let blob2 = Blob::new(b"data 2".to_vec());
        store.put(&blob1).unwrap();
        store.put(&blob2).unwrap();

        let repairer = ReplicationRepairer::new(store);

        repairer.queue_repair(blob1.oid, vec!["node-1".to_string()]);
        repairer.queue_repair(blob2.oid, vec!["node-2".to_string()]);

        repairer.process_batch().await;

        assert_eq!(repairer.queue_size(), 0);
        let stats = repairer.stats();
        assert_eq!(stats.repairs_attempted, 2);
        assert_eq!(stats.repairs_succeeded, 2);
    }

    #[tokio::test]
    async fn test_with_receiver() {
        let (store, _dir) = create_test_store();
        let (tx, rx) = mpsc::channel(10);

        let mut repairer = ReplicationRepairer::new(store).with_receiver(rx);

        tx.send((Oid::hash(b"test"), vec!["node-1".to_string()]))
            .await
            .unwrap();
        drop(tx);

        let handle = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(100), repairer.run())
                .await
                .ok();
        });

        handle.await.unwrap();
    }

    #[test]
    fn test_shutdown() {
        let (store, _dir) = create_test_store();
        let repairer = ReplicationRepairer::new(store);
        repairer.shutdown();
    }

    #[test]
    fn test_repair_task_debug() {
        let task = RepairTask::new(vec!["node-1".to_string()]);
        let debug = format!("{:?}", task);
        assert!(debug.contains("RepairTask"));
    }

    #[test]
    fn test_repair_config_debug() {
        let config = RepairConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RepairConfig"));
    }

    #[test]
    fn test_repairer_stats_debug() {
        let stats = RepairerStats {
            queue_size: 10,
            repairs_attempted: 100,
            repairs_succeeded: 90,
            repairs_failed: 10,
            repairs_expired: 5,
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("RepairerStats"));
    }
}
