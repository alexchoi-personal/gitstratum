use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gitstratum_core::Oid;
use parking_lot::{Mutex, RwLock};

use crate::error::Result;
use crate::repair::{
    MerkleTreeBuilder, ObjectMerkleTree, PositionRange, RepairPriority, RepairSession, RepairType,
};
use crate::store::ObjectStorage;

#[derive(Debug, Clone)]
pub struct AntiEntropyConfig {
    pub scan_interval: Duration,
    pub sample_size: usize,
    pub merkle_tree_depth: u8,
    pub max_queue_size: usize,
    pub enable_sampling: bool,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(3600),
            sample_size: 1000,
            merkle_tree_depth: 4,
            max_queue_size: 10000,
            enable_sampling: true,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct AntiEntropyStats {
    pub scans_completed: u64,
    pub ranges_checked: u64,
    pub inconsistencies_found: u64,
    pub objects_repaired: u64,
    pub last_scan_duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RepairItem {
    pub oid: Oid,
    pub position: u64,
    pub source_peer: String,
    pub priority: RepairPriority,
    pub queued_at: u64,
}

impl RepairItem {
    pub fn new(oid: Oid, position: u64, source_peer: String) -> Self {
        Self {
            oid,
            position,
            source_peer,
            priority: RepairPriority::AntiEntropy,
            queued_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    pub fn with_priority(mut self, priority: RepairPriority) -> Self {
        self.priority = priority;
        self
    }
}

pub struct RepairQueue {
    items: Mutex<VecDeque<RepairItem>>,
    max_size: usize,
    total_queued: AtomicU64,
    total_processed: AtomicU64,
}

impl RepairQueue {
    pub fn new(max_size: usize) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            max_size,
            total_queued: AtomicU64::new(0),
            total_processed: AtomicU64::new(0),
        }
    }

    pub fn push(&self, item: RepairItem) -> bool {
        let mut items = self.items.lock();
        if items.len() >= self.max_size {
            return false;
        }
        items.push_back(item);
        self.total_queued.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn push_front(&self, item: RepairItem) -> bool {
        let mut items = self.items.lock();
        if items.len() >= self.max_size {
            return false;
        }
        items.push_front(item);
        self.total_queued.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn pop(&self) -> Option<RepairItem> {
        let mut items = self.items.lock();
        let item = items.pop_front();
        if item.is_some() {
            self.total_processed.fetch_add(1, Ordering::Relaxed);
        }
        item
    }

    pub fn len(&self) -> usize {
        self.items.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.lock().is_empty()
    }

    pub fn clear(&self) {
        self.items.lock().clear();
    }

    pub fn total_queued(&self) -> u64 {
        self.total_queued.load(Ordering::Relaxed)
    }

    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }
}

impl Default for RepairQueue {
    fn default() -> Self {
        Self::new(10000)
    }
}

pub struct AntiEntropyRepairer<S: ObjectStorage> {
    store: Arc<S>,
    config: AntiEntropyConfig,
    local_node_id: String,
    repair_queue: Arc<RepairQueue>,
    stats: RwLock<AntiEntropyStats>,
    running: AtomicBool,
    owned_ranges: RwLock<Vec<PositionRange>>,
}

impl<S: ObjectStorage + Send + Sync + 'static> AntiEntropyRepairer<S> {
    pub fn new(store: Arc<S>, local_node_id: String, config: AntiEntropyConfig) -> Self {
        let repair_queue = Arc::new(RepairQueue::new(config.max_queue_size));
        Self {
            store,
            config,
            local_node_id,
            repair_queue,
            stats: RwLock::new(AntiEntropyStats::default()),
            running: AtomicBool::new(false),
            owned_ranges: RwLock::new(Vec::new()),
        }
    }

    pub fn config(&self) -> &AntiEntropyConfig {
        &self.config
    }

    pub fn stats(&self) -> AntiEntropyStats {
        self.stats.read().clone()
    }

    pub fn repair_queue(&self) -> Arc<RepairQueue> {
        self.repair_queue.clone()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn set_owned_ranges(&self, ranges: Vec<PositionRange>) {
        *self.owned_ranges.write() = ranges;
    }

    pub fn owned_ranges(&self) -> Vec<PositionRange> {
        self.owned_ranges.read().clone()
    }

    pub fn build_tree_for_range(&self, range: &PositionRange) -> ObjectMerkleTree {
        let builder = MerkleTreeBuilder::with_depth(0, self.config.merkle_tree_depth);
        builder.build_range(range.start, range.end)
    }

    pub fn create_session(
        &self,
        ring_version: u64,
        range: PositionRange,
        peer_nodes: Vec<String>,
    ) -> Result<RepairSession> {
        let session_id = format!(
            "anti-entropy-{}-{}-{}",
            self.local_node_id,
            range.start,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );

        RepairSession::builder()
            .id(session_id)
            .session_type(RepairType::AntiEntropy)
            .ring_version(ring_version)
            .range(range)
            .peer_nodes(peer_nodes)
            .build()
            .map_err(|e| crate::error::ObjectStoreError::Internal(e.to_string()))
    }

    pub fn priority(&self) -> RepairPriority {
        RepairPriority::AntiEntropy
    }

    pub fn queue_repair(&self, item: RepairItem) -> bool {
        self.repair_queue.push(item)
    }

    pub fn queue_repairs(&self, items: impl IntoIterator<Item = RepairItem>) -> usize {
        let mut count = 0;
        for item in items {
            if self.repair_queue.push(item) {
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn record_scan_completed(
        &self,
        duration_ms: u64,
        ranges_checked: u64,
        inconsistencies: u64,
    ) {
        let mut stats = self.stats.write();
        stats.scans_completed += 1;
        stats.ranges_checked += ranges_checked;
        stats.inconsistencies_found += inconsistencies;
        stats.last_scan_duration_ms = duration_ms;
    }

    pub fn record_object_repaired(&self) {
        let mut stats = self.stats.write();
        stats.objects_repaired += 1;
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use gitstratum_core::Blob;

    struct MockObjectStorage;

    #[async_trait]
    impl ObjectStorage for MockObjectStorage {
        async fn get(&self, _oid: &Oid) -> crate::error::Result<Option<Blob>> {
            Ok(None)
        }

        async fn put(&self, _blob: &Blob) -> crate::error::Result<()> {
            Ok(())
        }

        async fn delete(&self, _oid: &Oid) -> crate::error::Result<bool> {
            Ok(false)
        }

        fn has(&self, _oid: &Oid) -> bool {
            false
        }

        fn stats(&self) -> crate::store::StorageStats {
            crate::store::StorageStats {
                total_blobs: 0,
                total_bytes: 0,
                used_bytes: 0,
                available_bytes: 0,
                io_utilization: 0.0,
            }
        }
    }

    #[test]
    fn test_anti_entropy_config_default() {
        let config = AntiEntropyConfig::default();
        assert_eq!(config.scan_interval, Duration::from_secs(3600));
        assert_eq!(config.sample_size, 1000);
        assert_eq!(config.merkle_tree_depth, 4);
        assert_eq!(config.max_queue_size, 10000);
        assert!(config.enable_sampling);
    }

    #[test]
    fn test_anti_entropy_config_custom() {
        let config = AntiEntropyConfig {
            scan_interval: Duration::from_secs(7200),
            sample_size: 500,
            merkle_tree_depth: 8,
            max_queue_size: 5000,
            enable_sampling: false,
        };
        assert_eq!(config.scan_interval, Duration::from_secs(7200));
        assert_eq!(config.sample_size, 500);
        assert_eq!(config.merkle_tree_depth, 8);
        assert_eq!(config.max_queue_size, 5000);
        assert!(!config.enable_sampling);
    }

    #[test]
    fn test_anti_entropy_config_clone() {
        let config = AntiEntropyConfig::default();
        let cloned = config.clone();
        assert_eq!(config.scan_interval, cloned.scan_interval);
        assert_eq!(config.sample_size, cloned.sample_size);
    }

    #[test]
    fn test_anti_entropy_config_debug() {
        let config = AntiEntropyConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("AntiEntropyConfig"));
    }

    #[test]
    fn test_anti_entropy_stats_default() {
        let stats = AntiEntropyStats::default();
        assert_eq!(stats.scans_completed, 0);
        assert_eq!(stats.ranges_checked, 0);
        assert_eq!(stats.inconsistencies_found, 0);
        assert_eq!(stats.objects_repaired, 0);
        assert_eq!(stats.last_scan_duration_ms, 0);
    }

    #[test]
    fn test_anti_entropy_stats_clone() {
        let stats = AntiEntropyStats {
            scans_completed: 5,
            ranges_checked: 10,
            inconsistencies_found: 2,
            objects_repaired: 1,
            last_scan_duration_ms: 1000,
        };
        let cloned = stats.clone();
        assert_eq!(stats.scans_completed, cloned.scans_completed);
        assert_eq!(stats.ranges_checked, cloned.ranges_checked);
    }

    #[test]
    fn test_anti_entropy_stats_debug() {
        let stats = AntiEntropyStats::default();
        let debug = format!("{:?}", stats);
        assert!(debug.contains("AntiEntropyStats"));
    }

    #[test]
    fn test_repair_item_new() {
        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string());
        assert_eq!(item.oid, oid);
        assert_eq!(item.position, 1000);
        assert_eq!(item.source_peer, "peer-1");
        assert_eq!(item.priority, RepairPriority::AntiEntropy);
        assert!(item.queued_at > 0);
    }

    #[test]
    fn test_repair_item_with_priority() {
        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string())
            .with_priority(RepairPriority::FailedWrite);
        assert_eq!(item.priority, RepairPriority::FailedWrite);
    }

    #[test]
    fn test_repair_item_clone() {
        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string());
        let cloned = item.clone();
        assert_eq!(item.oid, cloned.oid);
        assert_eq!(item.position, cloned.position);
        assert_eq!(item.source_peer, cloned.source_peer);
    }

    #[test]
    fn test_repair_item_debug() {
        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string());
        let debug = format!("{:?}", item);
        assert!(debug.contains("RepairItem"));
    }

    #[test]
    fn test_repair_queue_new() {
        let queue = RepairQueue::new(100);
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
        assert_eq!(queue.total_queued(), 0);
        assert_eq!(queue.total_processed(), 0);
    }

    #[test]
    fn test_repair_queue_default() {
        let queue = RepairQueue::default();
        assert_eq!(queue.max_size, 10000);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_repair_queue_push() {
        let queue = RepairQueue::new(100);
        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string());
        assert!(queue.push(item));
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());
        assert_eq!(queue.total_queued(), 1);
    }

    #[test]
    fn test_repair_queue_push_front() {
        let queue = RepairQueue::new(100);
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");
        let item1 = RepairItem::new(oid1, 1000, "peer-1".to_string());
        let item2 = RepairItem::new(oid2, 2000, "peer-2".to_string());

        queue.push(item1);
        queue.push_front(item2.clone());

        let popped = queue.pop().unwrap();
        assert_eq!(popped.oid, oid2);
    }

    #[test]
    fn test_repair_queue_pop() {
        let queue = RepairQueue::new(100);
        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string());

        queue.push(item.clone());
        let popped = queue.pop();

        assert!(popped.is_some());
        assert_eq!(popped.unwrap().oid, oid);
        assert!(queue.is_empty());
        assert_eq!(queue.total_processed(), 1);
    }

    #[test]
    fn test_repair_queue_pop_empty() {
        let queue = RepairQueue::new(100);
        assert!(queue.pop().is_none());
        assert_eq!(queue.total_processed(), 0);
    }

    #[test]
    fn test_repair_queue_clear() {
        let queue = RepairQueue::new(100);
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");

        queue.push(RepairItem::new(oid1, 1000, "peer-1".to_string()));
        queue.push(RepairItem::new(oid2, 2000, "peer-2".to_string()));

        assert_eq!(queue.len(), 2);
        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(queue.total_queued(), 2);
    }

    #[test]
    fn test_repair_queue_max_size() {
        let queue = RepairQueue::new(2);
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");
        let oid3 = Oid::hash(b"test3");

        assert!(queue.push(RepairItem::new(oid1, 1000, "peer-1".to_string())));
        assert!(queue.push(RepairItem::new(oid2, 2000, "peer-2".to_string())));
        assert!(!queue.push(RepairItem::new(oid3, 3000, "peer-3".to_string())));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn test_repair_queue_push_front_max_size() {
        let queue = RepairQueue::new(2);
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");
        let oid3 = Oid::hash(b"test3");

        queue.push(RepairItem::new(oid1, 1000, "peer-1".to_string()));
        queue.push(RepairItem::new(oid2, 2000, "peer-2".to_string()));
        assert!(!queue.push_front(RepairItem::new(oid3, 3000, "peer-3".to_string())));
    }

    #[test]
    fn test_repair_queue_total_counters() {
        let queue = RepairQueue::new(100);
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");

        queue.push(RepairItem::new(oid1, 1000, "peer-1".to_string()));
        queue.push(RepairItem::new(oid2, 2000, "peer-2".to_string()));

        assert_eq!(queue.total_queued(), 2);
        assert_eq!(queue.total_processed(), 0);

        queue.pop();
        assert_eq!(queue.total_queued(), 2);
        assert_eq!(queue.total_processed(), 1);

        queue.pop();
        assert_eq!(queue.total_queued(), 2);
        assert_eq!(queue.total_processed(), 2);
    }

    #[test]
    fn test_repair_queue_fifo_order() {
        let queue = RepairQueue::new(100);
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");
        let oid3 = Oid::hash(b"test3");

        queue.push(RepairItem::new(oid1, 1000, "peer-1".to_string()));
        queue.push(RepairItem::new(oid2, 2000, "peer-2".to_string()));
        queue.push(RepairItem::new(oid3, 3000, "peer-3".to_string()));

        assert_eq!(queue.pop().unwrap().oid, oid1);
        assert_eq!(queue.pop().unwrap().oid, oid2);
        assert_eq!(queue.pop().unwrap().oid, oid3);
    }

    #[test]
    fn test_anti_entropy_repairer_new() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        assert_eq!(repairer.local_node_id(), "node-1");
        assert!(!repairer.is_running());
    }

    #[test]
    fn test_anti_entropy_repairer_config() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig {
            scan_interval: Duration::from_secs(7200),
            sample_size: 500,
            merkle_tree_depth: 8,
            max_queue_size: 5000,
            enable_sampling: false,
        };
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config.clone());

        assert_eq!(repairer.config().scan_interval, Duration::from_secs(7200));
        assert_eq!(repairer.config().sample_size, 500);
    }

    #[test]
    fn test_anti_entropy_repairer_stats() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let stats = repairer.stats();
        assert_eq!(stats.scans_completed, 0);
        assert_eq!(stats.ranges_checked, 0);
    }

    #[test]
    fn test_anti_entropy_repairer_repair_queue() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let queue = repairer.repair_queue();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_anti_entropy_repairer_start_stop() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        assert!(!repairer.is_running());
        repairer.start();
        assert!(repairer.is_running());
        repairer.stop();
        assert!(!repairer.is_running());
    }

    #[test]
    fn test_anti_entropy_repairer_owned_ranges() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        assert!(repairer.owned_ranges().is_empty());

        let ranges = vec![PositionRange::new(0, 100), PositionRange::new(200, 300)];
        repairer.set_owned_ranges(ranges.clone());

        let owned = repairer.owned_ranges();
        assert_eq!(owned.len(), 2);
        assert_eq!(owned[0].start, 0);
        assert_eq!(owned[0].end, 100);
    }

    #[test]
    fn test_anti_entropy_repairer_priority() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        assert_eq!(repairer.priority(), RepairPriority::AntiEntropy);
    }

    #[test]
    fn test_anti_entropy_repairer_queue_repair() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let oid = Oid::hash(b"test");
        let item = RepairItem::new(oid, 1000, "peer-1".to_string());

        assert!(repairer.queue_repair(item));
        assert_eq!(repairer.repair_queue().len(), 1);
    }

    #[test]
    fn test_anti_entropy_repairer_queue_repairs() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let items = vec![
            RepairItem::new(Oid::hash(b"test1"), 1000, "peer-1".to_string()),
            RepairItem::new(Oid::hash(b"test2"), 2000, "peer-2".to_string()),
            RepairItem::new(Oid::hash(b"test3"), 3000, "peer-3".to_string()),
        ];

        let count = repairer.queue_repairs(items);
        assert_eq!(count, 3);
        assert_eq!(repairer.repair_queue().len(), 3);
    }

    #[test]
    fn test_anti_entropy_repairer_queue_repairs_max_size() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig {
            max_queue_size: 2,
            ..Default::default()
        };
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let items = vec![
            RepairItem::new(Oid::hash(b"test1"), 1000, "peer-1".to_string()),
            RepairItem::new(Oid::hash(b"test2"), 2000, "peer-2".to_string()),
            RepairItem::new(Oid::hash(b"test3"), 3000, "peer-3".to_string()),
        ];

        let count = repairer.queue_repairs(items);
        assert_eq!(count, 2);
        assert_eq!(repairer.repair_queue().len(), 2);
    }

    #[test]
    fn test_anti_entropy_repairer_record_scan_completed() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        repairer.record_scan_completed(1000, 10, 2);

        let stats = repairer.stats();
        assert_eq!(stats.scans_completed, 1);
        assert_eq!(stats.ranges_checked, 10);
        assert_eq!(stats.inconsistencies_found, 2);
        assert_eq!(stats.last_scan_duration_ms, 1000);
    }

    #[test]
    fn test_anti_entropy_repairer_record_scan_completed_accumulates() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        repairer.record_scan_completed(1000, 10, 2);
        repairer.record_scan_completed(2000, 20, 5);

        let stats = repairer.stats();
        assert_eq!(stats.scans_completed, 2);
        assert_eq!(stats.ranges_checked, 30);
        assert_eq!(stats.inconsistencies_found, 7);
        assert_eq!(stats.last_scan_duration_ms, 2000);
    }

    #[test]
    fn test_anti_entropy_repairer_record_object_repaired() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        repairer.record_object_repaired();
        repairer.record_object_repaired();

        let stats = repairer.stats();
        assert_eq!(stats.objects_repaired, 2);
    }

    #[test]
    fn test_anti_entropy_repairer_create_session() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let range = PositionRange::new(0, 1000);
        let peers = vec!["peer-1".to_string(), "peer-2".to_string()];

        let session = repairer.create_session(1, range, peers).unwrap();

        assert!(session.id().starts_with("anti-entropy-node-1-"));
        assert_eq!(session.session_type(), RepairType::AntiEntropy);
        assert_eq!(session.ring_version(), 1);
        assert_eq!(session.ranges().len(), 1);
        assert_eq!(session.peer_nodes().len(), 2);
    }

    #[test]
    fn test_anti_entropy_repairer_build_tree_for_range() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig {
            merkle_tree_depth: 2,
            ..Default::default()
        };
        let repairer = AntiEntropyRepairer::new(store, "node-1".to_string(), config);

        let range = PositionRange::new(0, 1000);
        let tree = repairer.build_tree_for_range(&range);

        assert_eq!(tree.depth(), 2);
    }

    #[test]
    fn test_anti_entropy_repairer_store() {
        let store = Arc::new(MockObjectStorage);
        let config = AntiEntropyConfig::default();
        let repairer = AntiEntropyRepairer::new(store.clone(), "node-1".to_string(), config);

        let retrieved_store = repairer.store();
        assert!(Arc::ptr_eq(&store, retrieved_store));
    }
}
