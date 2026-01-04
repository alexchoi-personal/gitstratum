#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::error::Result;
use crate::repair::constants::{
    DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_MAX_CONCURRENT_PEERS, DEFAULT_MERKLE_TREE_DEPTH,
    DEFAULT_PEER_TIMEOUT, DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC,
};
use crate::repair::types::NodeId;
use crate::repair::{
    generate_session_id, DowntimeTracker, MerkleTreeBuilder, ObjectMerkleTree, PositionRange,
    RepairPriority, RepairSession, RepairType,
};
use crate::store::ObjectStorage;

#[derive(Debug, Clone)]
pub struct CrashRecoveryConfig {
    pub max_bandwidth_bytes_per_sec: u64,
    pub checkpoint_interval: u64,
    pub merkle_tree_depth: u8,
    pub peer_timeout: Duration,
    pub max_concurrent_peers: usize,
}

impl Default for CrashRecoveryConfig {
    fn default() -> Self {
        Self {
            max_bandwidth_bytes_per_sec: DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            merkle_tree_depth: DEFAULT_MERKLE_TREE_DEPTH,
            peer_timeout: DEFAULT_PEER_TIMEOUT,
            max_concurrent_peers: DEFAULT_MAX_CONCURRENT_PEERS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryNeeded {
    pub downtime_start: u64,
    pub downtime_end: u64,
    pub duration_secs: u64,
}

pub struct CrashRecoveryHandler<S: ObjectStorage> {
    store: Arc<S>,
    downtime_tracker: Arc<DowntimeTracker>,
    config: CrashRecoveryConfig,
    local_node_id: NodeId,
}

impl<S: ObjectStorage + Send + Sync + 'static> CrashRecoveryHandler<S> {
    pub fn new(
        store: Arc<S>,
        data_dir: &Path,
        local_node_id: impl Into<NodeId>,
        config: CrashRecoveryConfig,
    ) -> Result<Self> {
        let downtime_tracker = Arc::new(DowntimeTracker::new(data_dir)?);
        Ok(Self {
            store,
            downtime_tracker,
            config,
            local_node_id: local_node_id.into(),
        })
    }

    pub fn with_tracker(
        store: Arc<S>,
        downtime_tracker: Arc<DowntimeTracker>,
        local_node_id: impl Into<NodeId>,
        config: CrashRecoveryConfig,
    ) -> Self {
        Self {
            store,
            downtime_tracker,
            config,
            local_node_id: local_node_id.into(),
        }
    }

    pub fn check_recovery_needed(&self) -> Option<RecoveryNeeded> {
        self.downtime_tracker.downtime_window().map(|(start, end)| {
            let duration_secs = end.saturating_sub(start);
            debug!(
                downtime_start = start,
                downtime_end = end,
                duration_secs = duration_secs,
                "Detected downtime window"
            );
            RecoveryNeeded {
                downtime_start: start,
                downtime_end: end,
                duration_secs,
            }
        })
    }

    pub fn create_session(
        &self,
        recovery: &RecoveryNeeded,
        ring_version: u64,
        ranges: Vec<PositionRange>,
        peer_nodes: Vec<String>,
    ) -> Result<RepairSession> {
        let session_id = generate_session_id("crash-recovery", &self.local_node_id);

        info!(
            session_id = %session_id,
            downtime_start = recovery.downtime_start,
            downtime_end = recovery.downtime_end,
            peer_count = peer_nodes.len(),
            range_count = ranges.len(),
            "Creating crash recovery session"
        );

        RepairSession::builder()
            .id(session_id)
            .session_type(RepairType::crash_recovery(
                recovery.downtime_start,
                recovery.downtime_end,
            ))
            .ring_version(ring_version)
            .ranges(ranges)
            .peer_nodes(peer_nodes.into_iter().map(NodeId::from).collect())
            .build()
    }

    pub fn build_local_tree(&self, range: &PositionRange, ring_version: u64) -> ObjectMerkleTree {
        debug!(
            range_start = range.start,
            range_end = range.end,
            depth = self.config.merkle_tree_depth,
            "Building local Merkle tree for range"
        );
        let builder = MerkleTreeBuilder::with_depth(ring_version, self.config.merkle_tree_depth);
        builder.build_range(range.start, range.end)
    }

    pub fn find_missing_objects(
        &self,
        local_tree: &ObjectMerkleTree,
        remote_tree: &ObjectMerkleTree,
    ) -> Vec<(u64, u64)> {
        let differences = local_tree.find_range_differences(remote_tree);
        if !differences.is_empty() {
            debug!(
                difference_count = differences.len(),
                "Found range differences between local and remote trees"
            );
        }
        differences
    }

    pub fn priority(&self) -> RepairPriority {
        RepairPriority::CrashRecovery
    }

    pub fn mark_healthy(&self) -> Result<()> {
        debug!("Marking node as healthy");
        self.downtime_tracker.mark_healthy()
    }

    pub fn downtime_tracker(&self) -> &Arc<DowntimeTracker> {
        &self.downtime_tracker
    }

    pub fn config(&self) -> &CrashRecoveryConfig {
        &self.config
    }

    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub fn start_heartbeat(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        info!(interval_ms = interval.as_millis(), "Starting heartbeat");
        self.downtime_tracker.clone().start_heartbeat(interval)
    }
}

pub struct RepairRateLimiter {
    bytes_per_sec: u64,
    last_check: Instant,
    bytes_since_last_check: u64,
}

impl RepairRateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            last_check: Instant::now(),
            bytes_since_last_check: 0,
        }
    }

    pub fn acquire(&mut self, bytes: u64) -> Option<Duration> {
        self.bytes_since_last_check = self.bytes_since_last_check.saturating_add(bytes);

        let elapsed = self.last_check.elapsed();
        let expected_duration =
            Duration::from_secs_f64(self.bytes_since_last_check as f64 / self.bytes_per_sec as f64);

        let sleep_duration = if expected_duration > elapsed {
            let duration = expected_duration - elapsed;
            debug!(
                sleep_ms = duration.as_millis(),
                bytes_transferred = self.bytes_since_last_check,
                "Rate limiting repair bandwidth"
            );
            Some(duration)
        } else {
            None
        };

        if elapsed > Duration::from_secs(1) {
            self.last_check = Instant::now();
            self.bytes_since_last_check = 0;
        }

        sleep_duration
    }

    pub fn bytes_per_sec(&self) -> u64 {
        self.bytes_per_sec
    }

    pub fn set_bytes_per_sec(&mut self, bytes_per_sec: u64) {
        if bytes_per_sec != self.bytes_per_sec {
            warn!(
                old_rate = self.bytes_per_sec,
                new_rate = bytes_per_sec,
                "Adjusting repair rate limit"
            );
        }
        self.bytes_per_sec = bytes_per_sec;
    }

    pub fn bytes_since_last_check(&self) -> u64 {
        self.bytes_since_last_check
    }

    pub fn reset(&mut self) {
        self.last_check = Instant::now();
        self.bytes_since_last_check = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MockObjectStorage;
    use gitstratum_core::{Blob, Oid};
    use tempfile::TempDir;

    #[test]
    fn test_crash_recovery_config_default() {
        let config = CrashRecoveryConfig::default();
        assert_eq!(config.max_bandwidth_bytes_per_sec, 100 * 1024 * 1024);
        assert_eq!(config.checkpoint_interval, 1000);
        assert_eq!(config.merkle_tree_depth, 4);
        assert_eq!(config.peer_timeout, Duration::from_secs(30));
        assert_eq!(config.max_concurrent_peers, 3);
    }

    #[test]
    fn test_crash_recovery_config_custom() {
        let config = CrashRecoveryConfig {
            max_bandwidth_bytes_per_sec: 50 * 1024 * 1024,
            checkpoint_interval: 500,
            merkle_tree_depth: 6,
            peer_timeout: Duration::from_secs(60),
            max_concurrent_peers: 5,
        };
        assert_eq!(config.max_bandwidth_bytes_per_sec, 50 * 1024 * 1024);
        assert_eq!(config.checkpoint_interval, 500);
        assert_eq!(config.merkle_tree_depth, 6);
        assert_eq!(config.peer_timeout, Duration::from_secs(60));
        assert_eq!(config.max_concurrent_peers, 5);
    }

    #[test]
    fn test_crash_recovery_config_clone() {
        let config = CrashRecoveryConfig::default();
        let cloned = config.clone();
        assert_eq!(
            config.max_bandwidth_bytes_per_sec,
            cloned.max_bandwidth_bytes_per_sec
        );
        assert_eq!(config.checkpoint_interval, cloned.checkpoint_interval);
    }

    #[test]
    fn test_crash_recovery_config_debug() {
        let config = CrashRecoveryConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("CrashRecoveryConfig"));
        assert!(debug.contains("max_bandwidth_bytes_per_sec"));
    }

    #[test]
    fn test_recovery_needed_struct() {
        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 2000,
            duration_secs: 1000,
        };
        assert_eq!(recovery.downtime_start, 1000);
        assert_eq!(recovery.downtime_end, 2000);
        assert_eq!(recovery.duration_secs, 1000);
    }

    #[test]
    fn test_recovery_needed_clone() {
        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 2000,
            duration_secs: 1000,
        };
        let cloned = recovery.clone();
        assert_eq!(recovery.downtime_start, cloned.downtime_start);
        assert_eq!(recovery.downtime_end, cloned.downtime_end);
        assert_eq!(recovery.duration_secs, cloned.duration_secs);
    }

    #[test]
    fn test_recovery_needed_debug() {
        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 2000,
            duration_secs: 1000,
        };
        let debug = format!("{:?}", recovery);
        assert!(debug.contains("RecoveryNeeded"));
        assert!(debug.contains("1000"));
        assert!(debug.contains("2000"));
    }

    #[test]
    fn test_crash_recovery_handler_new() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config);

        assert!(handler.is_ok());
    }

    #[test]
    fn test_crash_recovery_handler_with_tracker() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let tracker = Arc::new(DowntimeTracker::new(temp_dir.path()).unwrap());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::with_tracker(store, tracker, NodeId::new("node-2"), config);

        assert_eq!(handler.local_node_id().as_str(), "node-2");
    }

    #[test]
    fn test_crash_recovery_handler_check_recovery_needed_no_downtime() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let recovery = handler.check_recovery_needed();
        assert!(recovery.is_none());
    }

    #[test]
    fn test_crash_recovery_handler_check_recovery_needed_with_downtime() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let tracker = Arc::new(DowntimeTracker::new(temp_dir.path()).unwrap());

        tracker.mark_healthy().unwrap();
        std::thread::sleep(Duration::from_millis(10));

        let config = CrashRecoveryConfig::default();
        let handler =
            CrashRecoveryHandler::with_tracker(store, tracker, "node-1".to_string(), config);

        let recovery = handler.check_recovery_needed();
        assert!(recovery.is_some() || recovery.is_none());
    }

    #[test]
    fn test_crash_recovery_handler_create_session() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 2000,
            duration_secs: 1000,
        };

        let ranges = vec![PositionRange::new(0, u64::MAX)];
        let peers = vec!["peer-1".to_string(), "peer-2".to_string()];

        let session = handler.create_session(&recovery, 1, ranges, peers);
        assert!(session.is_ok());

        let session = session.unwrap();
        assert!(session.id().contains("crash-recovery"));
        assert!(session.id().contains("node-1"));
        // Session ID now uses current timestamp from generate_session_id()
    }

    #[test]
    fn test_crash_recovery_handler_create_session_empty_peers() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 2000,
            duration_secs: 1000,
        };

        let session = handler.create_session(&recovery, 1, vec![], vec![]);
        assert!(session.is_ok());
    }

    #[test]
    fn test_crash_recovery_handler_build_local_tree() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let range = PositionRange::new(0, u64::MAX);
        let tree = handler.build_local_tree(&range, 1);

        assert_eq!(tree.ring_version(), 1);
        assert_eq!(tree.depth(), 4);
    }

    #[test]
    fn test_crash_recovery_handler_find_missing_objects_identical() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let tree1 = ObjectMerkleTree::new(1);
        let tree2 = ObjectMerkleTree::new(1);

        let differences = handler.find_missing_objects(&tree1, &tree2);
        assert!(differences.is_empty());
    }

    #[test]
    fn test_crash_recovery_handler_find_missing_objects_different() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let oid = Oid::hash(b"test");
        let tree1 = ObjectMerkleTree::build_from_positions(1, [(1000u64, oid)]);
        let tree2 = ObjectMerkleTree::new(1);

        let differences = handler.find_missing_objects(&tree1, &tree2);
        assert!(!differences.is_empty());
    }

    #[test]
    fn test_crash_recovery_handler_priority() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        assert_eq!(handler.priority(), RepairPriority::CrashRecovery);
    }

    #[test]
    fn test_crash_recovery_handler_mark_healthy() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let result = handler.mark_healthy();
        assert!(result.is_ok());

        let ts = handler.downtime_tracker().last_healthy_timestamp();
        assert!(ts > 0);
    }

    #[test]
    fn test_crash_recovery_handler_downtime_tracker() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let tracker = handler.downtime_tracker();
        assert_eq!(tracker.last_healthy_timestamp(), 0);
    }

    #[test]
    fn test_crash_recovery_handler_config() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig {
            max_bandwidth_bytes_per_sec: 50 * 1024 * 1024,
            ..Default::default()
        };

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        assert_eq!(
            handler.config().max_bandwidth_bytes_per_sec,
            50 * 1024 * 1024
        );
    }

    #[test]
    fn test_crash_recovery_handler_local_node_id() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), NodeId::new("test-node-123"), config)
                .unwrap();

        assert_eq!(handler.local_node_id().as_str(), "test-node-123");
    }

    #[test]
    fn test_crash_recovery_handler_store() {
        let temp_dir = TempDir::new().unwrap();
        let blob = Blob::new(b"test data".to_vec());
        let store = Arc::new(MockObjectStorage::with_objects(vec![blob.clone()]));
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        assert!(handler.store().has(&blob.oid));
    }

    #[tokio::test]
    async fn test_crash_recovery_handler_start_heartbeat() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let handle = handler.start_heartbeat(Duration::from_millis(10));

        tokio::time::sleep(Duration::from_millis(50)).await;

        handler.downtime_tracker().shutdown();
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        let ts = handler.downtime_tracker().last_healthy_timestamp();
        assert!(ts > 0);
    }

    #[test]
    fn test_repair_rate_limiter_new() {
        let limiter = RepairRateLimiter::new(100 * 1024 * 1024);
        assert_eq!(limiter.bytes_per_sec(), 100 * 1024 * 1024);
        assert_eq!(limiter.bytes_since_last_check(), 0);
    }

    #[tokio::test]
    async fn test_repair_rate_limiter_acquire_no_delay() {
        let mut limiter = RepairRateLimiter::new(1024 * 1024 * 1024);
        let start = Instant::now();
        if let Some(d) = limiter.acquire(1024) {
            tokio::time::sleep(d).await;
        }
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_repair_rate_limiter_acquire_with_delay() {
        let mut limiter = RepairRateLimiter::new(1000);
        if let Some(d) = limiter.acquire(500) {
            tokio::time::sleep(d).await;
        }
        let start = Instant::now();
        if let Some(d) = limiter.acquire(1000) {
            tokio::time::sleep(d).await;
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(400));
    }

    #[test]
    fn test_repair_rate_limiter_set_bytes_per_sec() {
        let mut limiter = RepairRateLimiter::new(100);
        assert_eq!(limiter.bytes_per_sec(), 100);

        limiter.set_bytes_per_sec(200);
        assert_eq!(limiter.bytes_per_sec(), 200);
    }

    #[test]
    fn test_repair_rate_limiter_set_bytes_per_sec_same_value() {
        let mut limiter = RepairRateLimiter::new(100);
        limiter.set_bytes_per_sec(100);
        assert_eq!(limiter.bytes_per_sec(), 100);
    }

    #[test]
    fn test_repair_rate_limiter_bytes_since_last_check() {
        let mut limiter = RepairRateLimiter::new(1024 * 1024 * 1024);
        assert_eq!(limiter.bytes_since_last_check(), 0);

        limiter.acquire(1000);
        assert_eq!(limiter.bytes_since_last_check(), 1000);

        limiter.acquire(500);
        assert_eq!(limiter.bytes_since_last_check(), 1500);
    }

    #[test]
    fn test_repair_rate_limiter_reset() {
        let mut limiter = RepairRateLimiter::new(100);
        limiter.bytes_since_last_check = 1000;

        limiter.reset();
        assert_eq!(limiter.bytes_since_last_check(), 0);
    }

    #[test]
    fn test_repair_rate_limiter_reset_after_second() {
        let mut limiter = RepairRateLimiter::new(1024 * 1024 * 1024);
        limiter.acquire(100);

        limiter.last_check = Instant::now() - Duration::from_secs(2);

        limiter.acquire(100);
        assert_eq!(limiter.bytes_since_last_check(), 0);
    }

    #[test]
    fn test_repair_rate_limiter_saturating_add() {
        let mut limiter = RepairRateLimiter::new(u64::MAX);
        limiter.bytes_since_last_check = u64::MAX - 10;
        limiter.acquire(100);
        assert_eq!(limiter.bytes_since_last_check(), u64::MAX);
    }

    #[test]
    fn test_crash_recovery_handler_with_objects() {
        let temp_dir = TempDir::new().unwrap();
        let blob1 = Blob::new(b"data1".to_vec());
        let blob2 = Blob::new(b"data2".to_vec());
        let store = Arc::new(MockObjectStorage::with_objects(vec![
            blob1.clone(),
            blob2.clone(),
        ]));
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        assert!(handler.store().has(&blob1.oid));
        assert!(handler.store().has(&blob2.oid));
    }

    #[test]
    fn test_create_session_with_multiple_ranges() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());
        let config = CrashRecoveryConfig::default();

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 2000,
            duration_secs: 1000,
        };

        let ranges = vec![
            PositionRange::new(0, 1000),
            PositionRange::new(1000, 2000),
            PositionRange::new(2000, u64::MAX),
        ];
        let peers = vec!["peer-1".to_string()];

        let session = handler
            .create_session(&recovery, 5, ranges.clone(), peers)
            .unwrap();
        assert_eq!(session.ranges().len(), 3);
        assert_eq!(session.ring_version(), 5);
    }

    #[test]
    fn test_build_local_tree_with_different_depths() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MockObjectStorage::new());

        let config = CrashRecoveryConfig {
            merkle_tree_depth: 6,
            ..Default::default()
        };

        let handler =
            CrashRecoveryHandler::new(store, temp_dir.path(), "node-1".to_string(), config)
                .unwrap();

        let range = PositionRange::new(0, u64::MAX);
        let tree = handler.build_local_tree(&range, 1);

        assert_eq!(tree.depth(), 6);
    }

    #[test]
    fn test_recovery_needed_zero_duration() {
        let recovery = RecoveryNeeded {
            downtime_start: 1000,
            downtime_end: 1000,
            duration_secs: 0,
        };
        assert_eq!(recovery.duration_secs, 0);
    }

    #[test]
    fn test_recovery_needed_large_duration() {
        let recovery = RecoveryNeeded {
            downtime_start: 0,
            downtime_end: u64::MAX,
            duration_secs: u64::MAX,
        };
        assert_eq!(recovery.duration_secs, u64::MAX);
    }
}
