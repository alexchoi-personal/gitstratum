use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio_stream::StreamExt;

use crate::error::{ObjectStoreError, Result};
use crate::repair::constants::{
    DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_PEER_TIMEOUT, DEFAULT_REBALANCE_BATCH_SIZE,
    DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC,
};
use crate::repair::types::NodeId;
use crate::repair::{
    MerkleTreeBuilder, ObjectMerkleTree, PositionRange, RebalanceDirection, RepairPriority,
    RepairSession, RepairType,
};
use crate::store::{ObjectStorage, ObjectStore, StorageStats};

#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    pub max_bandwidth_bytes_per_sec: u64,
    pub batch_size: usize,
    pub checkpoint_interval: u64,
    pub verify_crc: bool,
    pub peer_timeout: Duration,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            max_bandwidth_bytes_per_sec: DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC,
            batch_size: DEFAULT_REBALANCE_BATCH_SIZE,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            verify_crc: true,
            peer_timeout: DEFAULT_PEER_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceState {
    Idle,
    Preparing,
    Transferring,
    Verifying,
    Completed,
    Failed,
}

impl RebalanceState {
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            RebalanceState::Preparing | RebalanceState::Transferring | RebalanceState::Verifying
        )
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, RebalanceState::Completed | RebalanceState::Failed)
    }
}

#[derive(Debug, Default, Clone)]
pub struct RebalanceStats {
    pub rebalances_started: u64,
    pub rebalances_completed: u64,
    pub rebalances_failed: u64,
    pub objects_transferred: u64,
    pub bytes_transferred: u64,
    pub ranges_processed: u64,
}

#[derive(Debug, Clone)]
pub struct RangeTransfer {
    pub range: PositionRange,
    pub direction: RebalanceDirection,
    pub peer_node: NodeId,
    pub estimated_objects: u64,
}

impl RangeTransfer {
    pub fn incoming(range: PositionRange, peer_node: impl Into<NodeId>) -> Self {
        Self {
            range,
            direction: RebalanceDirection::Incoming,
            peer_node: peer_node.into(),
            estimated_objects: 0,
        }
    }

    pub fn outgoing(range: PositionRange, peer_node: impl Into<NodeId>) -> Self {
        Self {
            range,
            direction: RebalanceDirection::Outgoing,
            peer_node: peer_node.into(),
            estimated_objects: 0,
        }
    }

    pub fn with_estimate(mut self, estimated_objects: u64) -> Self {
        self.estimated_objects = estimated_objects;
        self
    }
}

pub struct RebalanceHandler {
    store: Arc<ObjectStore>,
    config: RebalanceConfig,
    local_node_id: NodeId,
    state: RwLock<RebalanceState>,
    current_ring_version: AtomicU64,
    pending_transfers: RwLock<Vec<RangeTransfer>>,
    stats: RwLock<RebalanceStats>,
    paused: AtomicBool,
}

impl RebalanceHandler {
    pub fn new(
        store: Arc<ObjectStore>,
        local_node_id: impl Into<NodeId>,
        config: RebalanceConfig,
    ) -> Self {
        Self {
            store,
            config,
            local_node_id: local_node_id.into(),
            state: RwLock::new(RebalanceState::Idle),
            current_ring_version: AtomicU64::new(0),
            pending_transfers: RwLock::new(Vec::new()),
            stats: RwLock::new(RebalanceStats::default()),
            paused: AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }

    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn state(&self) -> RebalanceState {
        *self.state.read()
    }

    pub fn set_state(&self, state: RebalanceState) {
        *self.state.write() = state;
    }

    pub fn ring_version(&self) -> u64 {
        self.current_ring_version.load(Ordering::SeqCst)
    }

    pub fn set_ring_version(&self, version: u64) {
        self.current_ring_version.store(version, Ordering::SeqCst);
    }

    pub fn stats(&self) -> RebalanceStats {
        self.stats.read().clone()
    }

    pub fn storage_stats(&self) -> StorageStats {
        self.store.stats()
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn needs_rebalance(&self, new_version: u64) -> bool {
        let current = self.ring_version();
        new_version > current
    }

    pub fn queue_transfers(&self, transfers: Vec<RangeTransfer>) {
        let mut pending = self.pending_transfers.write();
        pending.extend(transfers);
    }

    pub fn pending_transfers(&self) -> Vec<RangeTransfer> {
        self.pending_transfers.read().clone()
    }

    pub fn take_next_transfer(&self) -> Option<RangeTransfer> {
        let mut pending = self.pending_transfers.write();
        if pending.is_empty() {
            None
        } else {
            Some(pending.remove(0))
        }
    }

    pub fn clear_pending(&self) {
        self.pending_transfers.write().clear();
    }

    pub fn pending_count(&self) -> usize {
        self.pending_transfers.read().len()
    }

    pub fn create_session(
        &self,
        transfer: &RangeTransfer,
        ring_version: u64,
    ) -> Result<RepairSession> {
        let session_id = format!(
            "rebalance-{}-{:?}-{}-{}",
            self.local_node_id,
            transfer.direction,
            transfer.range.start,
            crate::util::time::current_timestamp()
        );

        let repair_type = match transfer.direction {
            RebalanceDirection::Incoming => RepairType::rebalance_incoming(),
            RebalanceDirection::Outgoing => RepairType::rebalance_outgoing(),
        };

        RepairSession::builder()
            .id(session_id)
            .session_type(repair_type)
            .ring_version(ring_version)
            .range(transfer.range)
            .peer_node(transfer.peer_node.clone())
            .build()
            .map_err(|e| ObjectStoreError::Internal(e.to_string()))
    }

    pub fn priority(&self) -> RepairPriority {
        RepairPriority::Rebalance
    }

    pub async fn build_tree_for_range(&self, range: &PositionRange) -> ObjectMerkleTree {
        let mut builder = MerkleTreeBuilder::with_depth(0, 4);

        let mut stream = self.store.iter_range(range.start, range.end);
        while let Some(result) = stream.next().await {
            if let Ok((oid, _blob)) = result {
                let position = oid_to_position(&oid);
                builder.add(position, oid);
            }
        }

        builder.build()
    }

    pub fn record_transfer_completed(&self, objects: u64, bytes: u64) {
        let mut stats = self.stats.write();
        stats.objects_transferred += objects;
        stats.bytes_transferred += bytes;
        stats.ranges_processed += 1;
    }

    pub fn record_rebalance_started(&self) {
        let mut stats = self.stats.write();
        stats.rebalances_started += 1;
    }

    pub fn record_rebalance_completed(&self) {
        let mut stats = self.stats.write();
        stats.rebalances_completed += 1;
    }

    pub fn record_rebalance_failed(&self) {
        let mut stats = self.stats.write();
        stats.rebalances_failed += 1;
    }

    pub fn start(&self, new_ring_version: u64) {
        self.set_state(RebalanceState::Preparing);
        self.set_ring_version(new_ring_version);
        self.record_rebalance_started();
    }

    pub fn complete(&self) {
        self.set_state(RebalanceState::Completed);
        self.record_rebalance_completed();
        self.clear_pending();
    }

    pub fn fail(&self) {
        self.set_state(RebalanceState::Failed);
        self.record_rebalance_failed();
    }

    pub fn reset(&self) {
        self.set_state(RebalanceState::Idle);
        self.clear_pending();
    }
}

fn oid_to_position(oid: &gitstratum_core::Oid) -> u64 {
    u64::from_le_bytes(oid.as_bytes()[..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_handler() -> (RebalanceHandler, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        #[cfg(feature = "bucketstore")]
        let store = {
            use gitstratum_storage::BucketStoreConfig;
            let config = BucketStoreConfig {
                data_dir: temp_dir.path().to_path_buf(),
                max_data_file_size: 1024 * 1024,
                bucket_count: 64,
                bucket_cache_size: 16,
                io_queue_depth: 4,
                io_queue_count: 1,
                compaction: gitstratum_storage::config::CompactionConfig::default(),
            };
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { crate::store::BucketObjectStore::new(config).await.unwrap() })
        };

        #[cfg(not(feature = "bucketstore"))]
        let store = crate::store::RocksDbStore::new(temp_dir.path()).unwrap();

        let handler = RebalanceHandler::new(
            Arc::new(store),
            "node-1".to_string(),
            RebalanceConfig::default(),
        );
        (handler, temp_dir)
    }

    #[test]
    fn test_rebalance_config_default() {
        let config = RebalanceConfig::default();
        assert_eq!(config.max_bandwidth_bytes_per_sec, 100 * 1024 * 1024);
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.checkpoint_interval, 1000);
        assert!(config.verify_crc);
        assert_eq!(config.peer_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_rebalance_config_clone() {
        let config = RebalanceConfig::default();
        let cloned = config.clone();
        assert_eq!(
            config.max_bandwidth_bytes_per_sec,
            cloned.max_bandwidth_bytes_per_sec
        );
        assert_eq!(config.batch_size, cloned.batch_size);
    }

    #[test]
    fn test_rebalance_config_debug() {
        let config = RebalanceConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RebalanceConfig"));
    }

    #[test]
    fn test_rebalance_state_is_active_idle() {
        assert!(!RebalanceState::Idle.is_active());
    }

    #[test]
    fn test_rebalance_state_is_active_preparing() {
        assert!(RebalanceState::Preparing.is_active());
    }

    #[test]
    fn test_rebalance_state_is_active_transferring() {
        assert!(RebalanceState::Transferring.is_active());
    }

    #[test]
    fn test_rebalance_state_is_active_verifying() {
        assert!(RebalanceState::Verifying.is_active());
    }

    #[test]
    fn test_rebalance_state_is_active_completed() {
        assert!(!RebalanceState::Completed.is_active());
    }

    #[test]
    fn test_rebalance_state_is_active_failed() {
        assert!(!RebalanceState::Failed.is_active());
    }

    #[test]
    fn test_rebalance_state_is_terminal_idle() {
        assert!(!RebalanceState::Idle.is_terminal());
    }

    #[test]
    fn test_rebalance_state_is_terminal_preparing() {
        assert!(!RebalanceState::Preparing.is_terminal());
    }

    #[test]
    fn test_rebalance_state_is_terminal_transferring() {
        assert!(!RebalanceState::Transferring.is_terminal());
    }

    #[test]
    fn test_rebalance_state_is_terminal_verifying() {
        assert!(!RebalanceState::Verifying.is_terminal());
    }

    #[test]
    fn test_rebalance_state_is_terminal_completed() {
        assert!(RebalanceState::Completed.is_terminal());
    }

    #[test]
    fn test_rebalance_state_is_terminal_failed() {
        assert!(RebalanceState::Failed.is_terminal());
    }

    #[test]
    fn test_rebalance_state_debug() {
        let state = RebalanceState::Preparing;
        let debug = format!("{:?}", state);
        assert!(debug.contains("Preparing"));
    }

    #[test]
    fn test_rebalance_state_clone() {
        let state = RebalanceState::Transferring;
        let cloned = state.clone();
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_rebalance_state_copy() {
        let state = RebalanceState::Verifying;
        let copied: RebalanceState = state;
        assert_eq!(state, copied);
    }

    #[test]
    fn test_rebalance_stats_default() {
        let stats = RebalanceStats::default();
        assert_eq!(stats.rebalances_started, 0);
        assert_eq!(stats.rebalances_completed, 0);
        assert_eq!(stats.rebalances_failed, 0);
        assert_eq!(stats.objects_transferred, 0);
        assert_eq!(stats.bytes_transferred, 0);
        assert_eq!(stats.ranges_processed, 0);
    }

    #[test]
    fn test_rebalance_stats_clone() {
        let mut stats = RebalanceStats::default();
        stats.rebalances_started = 5;
        stats.objects_transferred = 100;
        let cloned = stats.clone();
        assert_eq!(stats.rebalances_started, cloned.rebalances_started);
        assert_eq!(stats.objects_transferred, cloned.objects_transferred);
    }

    #[test]
    fn test_rebalance_stats_debug() {
        let stats = RebalanceStats::default();
        let debug = format!("{:?}", stats);
        assert!(debug.contains("RebalanceStats"));
    }

    #[test]
    fn test_range_transfer_incoming() {
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        assert_eq!(transfer.range.start, 0);
        assert_eq!(transfer.range.end, 100);
        assert_eq!(transfer.direction, RebalanceDirection::Incoming);
        assert_eq!(transfer.peer_node, NodeId::new("peer-1"));
        assert_eq!(transfer.estimated_objects, 0);
    }

    #[test]
    fn test_range_transfer_outgoing() {
        let range = PositionRange::new(200, 400);
        let transfer = RangeTransfer::outgoing(range, "peer-2".to_string());
        assert_eq!(transfer.range.start, 200);
        assert_eq!(transfer.range.end, 400);
        assert_eq!(transfer.direction, RebalanceDirection::Outgoing);
        assert_eq!(transfer.peer_node, NodeId::new("peer-2"));
        assert_eq!(transfer.estimated_objects, 0);
    }

    #[test]
    fn test_range_transfer_with_estimate() {
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string()).with_estimate(500);
        assert_eq!(transfer.estimated_objects, 500);
    }

    #[test]
    fn test_range_transfer_clone() {
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        let cloned = transfer.clone();
        assert_eq!(transfer.range.start, cloned.range.start);
        assert_eq!(transfer.peer_node, cloned.peer_node);
    }

    #[test]
    fn test_range_transfer_debug() {
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        let debug = format!("{:?}", transfer);
        assert!(debug.contains("RangeTransfer"));
    }

    #[test]
    fn test_handler_new() {
        let (handler, _dir) = create_test_handler();
        assert_eq!(handler.local_node_id().as_str(), "node-1");
        assert_eq!(handler.state(), RebalanceState::Idle);
        assert_eq!(handler.ring_version(), 0);
        assert!(!handler.is_paused());
    }

    #[test]
    fn test_handler_config() {
        let (handler, _dir) = create_test_handler();
        let config = handler.config();
        assert_eq!(config.batch_size, 1000);
    }

    #[test]
    fn test_handler_set_state() {
        let (handler, _dir) = create_test_handler();
        handler.set_state(RebalanceState::Preparing);
        assert_eq!(handler.state(), RebalanceState::Preparing);
        handler.set_state(RebalanceState::Transferring);
        assert_eq!(handler.state(), RebalanceState::Transferring);
    }

    #[test]
    fn test_handler_ring_version() {
        let (handler, _dir) = create_test_handler();
        assert_eq!(handler.ring_version(), 0);
        handler.set_ring_version(42);
        assert_eq!(handler.ring_version(), 42);
    }

    #[test]
    fn test_handler_stats() {
        let (handler, _dir) = create_test_handler();
        let stats = handler.stats();
        assert_eq!(stats.rebalances_started, 0);
    }

    #[test]
    fn test_handler_storage_stats() {
        let (handler, _dir) = create_test_handler();
        let stats = handler.storage_stats();
        assert_eq!(stats.total_blobs, 0);
    }

    #[test]
    fn test_handler_pause_resume() {
        let (handler, _dir) = create_test_handler();
        assert!(!handler.is_paused());
        handler.pause();
        assert!(handler.is_paused());
        handler.resume();
        assert!(!handler.is_paused());
    }

    #[test]
    fn test_handler_needs_rebalance_higher_version() {
        let (handler, _dir) = create_test_handler();
        handler.set_ring_version(5);
        assert!(handler.needs_rebalance(10));
    }

    #[test]
    fn test_handler_needs_rebalance_same_version() {
        let (handler, _dir) = create_test_handler();
        handler.set_ring_version(5);
        assert!(!handler.needs_rebalance(5));
    }

    #[test]
    fn test_handler_needs_rebalance_lower_version() {
        let (handler, _dir) = create_test_handler();
        handler.set_ring_version(10);
        assert!(!handler.needs_rebalance(5));
    }

    #[test]
    fn test_handler_queue_transfers() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        handler.queue_transfers(vec![transfer]);
        assert_eq!(handler.pending_count(), 1);
    }

    #[test]
    fn test_handler_pending_transfers() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        handler.queue_transfers(vec![transfer]);
        let pending = handler.pending_transfers();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].peer_node, NodeId::new("peer-1"));
    }

    #[test]
    fn test_handler_take_next_transfer() {
        let (handler, _dir) = create_test_handler();
        let range1 = PositionRange::new(0, 100);
        let range2 = PositionRange::new(100, 200);
        let transfer1 = RangeTransfer::incoming(range1, "peer-1".to_string());
        let transfer2 = RangeTransfer::outgoing(range2, "peer-2".to_string());
        handler.queue_transfers(vec![transfer1, transfer2]);

        let first = handler.take_next_transfer().unwrap();
        assert_eq!(first.peer_node, NodeId::new("peer-1"));
        assert_eq!(handler.pending_count(), 1);

        let second = handler.take_next_transfer().unwrap();
        assert_eq!(second.peer_node, NodeId::new("peer-2"));
        assert_eq!(handler.pending_count(), 0);
    }

    #[test]
    fn test_handler_take_next_transfer_empty() {
        let (handler, _dir) = create_test_handler();
        assert!(handler.take_next_transfer().is_none());
    }

    #[test]
    fn test_handler_clear_pending() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        handler.queue_transfers(vec![transfer]);
        assert_eq!(handler.pending_count(), 1);
        handler.clear_pending();
        assert_eq!(handler.pending_count(), 0);
    }

    #[test]
    fn test_handler_pending_count_empty() {
        let (handler, _dir) = create_test_handler();
        assert_eq!(handler.pending_count(), 0);
    }

    #[test]
    fn test_handler_create_session_incoming() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        let session = handler.create_session(&transfer, 5).unwrap();
        assert!(session
            .id()
            .as_str()
            .starts_with("rebalance-node-1-Incoming"));
        assert_eq!(session.ring_version(), 5);
    }

    #[test]
    fn test_handler_create_session_outgoing() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::outgoing(range, "peer-2".to_string());
        let session = handler.create_session(&transfer, 10).unwrap();
        assert!(session
            .id()
            .as_str()
            .starts_with("rebalance-node-1-Outgoing"));
        assert_eq!(session.ring_version(), 10);
    }

    #[test]
    fn test_handler_priority() {
        let (handler, _dir) = create_test_handler();
        assert_eq!(handler.priority(), RepairPriority::Rebalance);
    }

    #[tokio::test]
    async fn test_handler_build_tree_for_range_empty() {
        let temp_dir = TempDir::new().unwrap();

        #[cfg(feature = "bucketstore")]
        let store = {
            use gitstratum_storage::BucketStoreConfig;
            let config = BucketStoreConfig {
                data_dir: temp_dir.path().to_path_buf(),
                max_data_file_size: 1024 * 1024,
                bucket_count: 64,
                bucket_cache_size: 16,
                io_queue_depth: 4,
                io_queue_count: 1,
                compaction: gitstratum_storage::config::CompactionConfig::default(),
            };
            crate::store::BucketObjectStore::new(config).await.unwrap()
        };

        #[cfg(not(feature = "bucketstore"))]
        let store = crate::store::RocksDbStore::new(temp_dir.path()).unwrap();

        let handler = RebalanceHandler::new(
            Arc::new(store),
            "node-1".to_string(),
            RebalanceConfig::default(),
        );

        let range = PositionRange::new(0, u64::MAX);
        let tree = handler.build_tree_for_range(&range).await;
        assert_eq!(tree.object_count(), 0);
    }

    #[test]
    fn test_handler_record_transfer_completed() {
        let (handler, _dir) = create_test_handler();
        handler.record_transfer_completed(100, 1024 * 1024);
        let stats = handler.stats();
        assert_eq!(stats.objects_transferred, 100);
        assert_eq!(stats.bytes_transferred, 1024 * 1024);
        assert_eq!(stats.ranges_processed, 1);
    }

    #[test]
    fn test_handler_record_rebalance_started() {
        let (handler, _dir) = create_test_handler();
        handler.record_rebalance_started();
        let stats = handler.stats();
        assert_eq!(stats.rebalances_started, 1);
    }

    #[test]
    fn test_handler_record_rebalance_completed() {
        let (handler, _dir) = create_test_handler();
        handler.record_rebalance_completed();
        let stats = handler.stats();
        assert_eq!(stats.rebalances_completed, 1);
    }

    #[test]
    fn test_handler_record_rebalance_failed() {
        let (handler, _dir) = create_test_handler();
        handler.record_rebalance_failed();
        let stats = handler.stats();
        assert_eq!(stats.rebalances_failed, 1);
    }

    #[test]
    fn test_handler_start() {
        let (handler, _dir) = create_test_handler();
        handler.start(42);
        assert_eq!(handler.state(), RebalanceState::Preparing);
        assert_eq!(handler.ring_version(), 42);
        assert_eq!(handler.stats().rebalances_started, 1);
    }

    #[test]
    fn test_handler_complete() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        handler.queue_transfers(vec![transfer]);
        handler.start(1);
        handler.complete();
        assert_eq!(handler.state(), RebalanceState::Completed);
        assert_eq!(handler.stats().rebalances_completed, 1);
        assert_eq!(handler.pending_count(), 0);
    }

    #[test]
    fn test_handler_fail() {
        let (handler, _dir) = create_test_handler();
        handler.start(1);
        handler.fail();
        assert_eq!(handler.state(), RebalanceState::Failed);
        assert_eq!(handler.stats().rebalances_failed, 1);
    }

    #[test]
    fn test_handler_reset() {
        let (handler, _dir) = create_test_handler();
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string());
        handler.queue_transfers(vec![transfer]);
        handler.start(10);
        handler.reset();
        assert_eq!(handler.state(), RebalanceState::Idle);
        assert_eq!(handler.pending_count(), 0);
    }

    #[test]
    fn test_handler_multiple_rebalances() {
        let (handler, _dir) = create_test_handler();

        handler.start(1);
        handler.complete();

        handler.start(2);
        handler.fail();

        handler.reset();
        handler.start(3);
        handler.complete();

        let stats = handler.stats();
        assert_eq!(stats.rebalances_started, 3);
        assert_eq!(stats.rebalances_completed, 2);
        assert_eq!(stats.rebalances_failed, 1);
    }

    #[test]
    fn test_handler_transfer_stats_accumulate() {
        let (handler, _dir) = create_test_handler();
        handler.record_transfer_completed(50, 1000);
        handler.record_transfer_completed(30, 500);
        handler.record_transfer_completed(20, 500);

        let stats = handler.stats();
        assert_eq!(stats.objects_transferred, 100);
        assert_eq!(stats.bytes_transferred, 2000);
        assert_eq!(stats.ranges_processed, 3);
    }

    #[test]
    fn test_handler_queue_multiple_transfers() {
        let (handler, _dir) = create_test_handler();
        let transfers: Vec<RangeTransfer> = (0..5)
            .map(|i| {
                let range = PositionRange::new(i * 100, (i + 1) * 100);
                RangeTransfer::incoming(range, format!("peer-{}", i))
            })
            .collect();
        handler.queue_transfers(transfers);
        assert_eq!(handler.pending_count(), 5);
    }

    #[test]
    fn test_oid_to_position() {
        let oid = gitstratum_core::Oid::hash(b"test");
        let position = oid_to_position(&oid);
        let bytes = oid.as_bytes();
        let expected = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        assert_eq!(position, expected);
    }

    #[test]
    fn test_rebalance_state_all_variants() {
        let states = [
            RebalanceState::Idle,
            RebalanceState::Preparing,
            RebalanceState::Transferring,
            RebalanceState::Verifying,
            RebalanceState::Completed,
            RebalanceState::Failed,
        ];

        for state in states {
            let _ = format!("{:?}", state);
        }
    }

    #[test]
    fn test_range_transfer_chain_with_estimate() {
        let range = PositionRange::new(0, 100);
        let transfer = RangeTransfer::incoming(range, "peer-1".to_string())
            .with_estimate(100)
            .with_estimate(200);
        assert_eq!(transfer.estimated_objects, 200);
    }
}
