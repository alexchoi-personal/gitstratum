use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use gitstratum_core::Oid;
use gitstratum_hashring::{ConsistentHashRing, NodeInfo};

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct ReadConfig {
    pub prefer_local: bool,
    pub max_retries: usize,
    pub timeout_ms: u64,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            prefer_local: true,
            max_retries: 2,
            timeout_ms: 3000,
        }
    }
}

pub struct ReplicationReader {
    ring: Arc<ConsistentHashRing>,
    config: ReadConfig,
    local_node_id: Option<String>,
    reads_attempted: AtomicU64,
    reads_succeeded: AtomicU64,
    reads_failed: AtomicU64,
    local_reads: AtomicU64,
}

impl ReplicationReader {
    pub fn new(ring: Arc<ConsistentHashRing>, config: ReadConfig) -> Self {
        Self {
            ring,
            config,
            local_node_id: None,
            reads_attempted: AtomicU64::new(0),
            reads_succeeded: AtomicU64::new(0),
            reads_failed: AtomicU64::new(0),
            local_reads: AtomicU64::new(0),
        }
    }

    pub fn with_local_node(mut self, node_id: String) -> Self {
        self.local_node_id = Some(node_id);
        self
    }

    pub fn get_read_nodes(&self, oid: &Oid) -> Result<Vec<NodeInfo>> {
        let nodes = self.ring.nodes_for_oid(oid)?;

        if self.config.prefer_local {
            if let Some(ref local_id) = self.local_node_id {
                let mut sorted: Vec<_> = nodes.into_iter().collect();
                sorted.sort_by_key(|n| if n.id.as_str() == local_id { 0 } else { 1 });
                return Ok(sorted);
            }
        }

        Ok(nodes)
    }

    pub fn record_read_result(&self, success: bool, was_local: bool) {
        self.reads_attempted.fetch_add(1, Ordering::Relaxed);

        if success {
            self.reads_succeeded.fetch_add(1, Ordering::Relaxed);
            if was_local {
                self.local_reads.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.reads_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn is_local_node(&self, node: &NodeInfo) -> bool {
        if let Some(ref local_id) = self.local_node_id {
            node.id.as_str() == local_id
        } else {
            false
        }
    }

    pub fn stats(&self) -> ReplicationReaderStats {
        let succeeded = self.reads_succeeded.load(Ordering::Relaxed);
        let local = self.local_reads.load(Ordering::Relaxed);
        let locality_rate = if succeeded > 0 {
            local as f64 / succeeded as f64
        } else {
            0.0
        };

        ReplicationReaderStats {
            reads_attempted: self.reads_attempted.load(Ordering::Relaxed),
            reads_succeeded: succeeded,
            reads_failed: self.reads_failed.load(Ordering::Relaxed),
            local_reads: local,
            locality_rate,
        }
    }

    pub fn max_retries(&self) -> usize {
        self.config.max_retries
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationReaderStats {
    pub reads_attempted: u64,
    pub reads_succeeded: u64,
    pub reads_failed: u64,
    pub local_reads: u64,
    pub locality_rate: f64,
}

pub struct ReadStrategy {
    config: ReadConfig,
}

impl ReadStrategy {
    pub fn new(config: ReadConfig) -> Self {
        Self { config }
    }

    pub fn order_nodes(&self, nodes: Vec<NodeInfo>, local_node_id: Option<&str>) -> Vec<NodeInfo> {
        let mut sorted = nodes;

        if self.config.prefer_local {
            if let Some(local_id) = local_node_id {
                sorted.sort_by_key(|n| if n.id.as_str() == local_id { 0 } else { 1 });
            }
        }

        sorted
    }

    pub fn should_retry(&self, attempt: usize) -> bool {
        attempt < self.config.max_retries
    }
}

impl Default for ReadStrategy {
    fn default() -> Self {
        Self::new(ReadConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_hashring::HashRingBuilder;

    fn create_test_ring() -> Arc<ConsistentHashRing> {
        Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .add_node(NodeInfo::new("node-1", "127.0.0.1", 9001))
                .add_node(NodeInfo::new("node-2", "127.0.0.1", 9002))
                .add_node(NodeInfo::new("node-3", "127.0.0.1", 9003))
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_read_config_default() {
        let config = ReadConfig::default();
        assert!(config.prefer_local);
        assert_eq!(config.max_retries, 2);
        assert_eq!(config.timeout_ms, 3000);
    }

    #[test]
    fn test_replication_reader_new() {
        let ring = create_test_ring();
        let config = ReadConfig::default();
        let reader = ReplicationReader::new(ring, config);
        assert_eq!(reader.max_retries(), 2);
    }

    #[test]
    fn test_with_local_node() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default())
            .with_local_node("node-1".to_string());

        let node = NodeInfo::new("node-1", "127.0.0.1", 9001);
        assert!(reader.is_local_node(&node));

        let other_node = NodeInfo::new("node-2", "127.0.0.1", 9002);
        assert!(!reader.is_local_node(&other_node));
    }

    #[test]
    fn test_get_read_nodes() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default());
        let oid = Oid::hash(b"test");

        let nodes = reader.get_read_nodes(&oid).unwrap();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_get_read_nodes_local_preferred() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default())
            .with_local_node("node-2".to_string());
        let oid = Oid::hash(b"test");

        let nodes = reader.get_read_nodes(&oid).unwrap();
        assert_eq!(nodes[0].id.as_str(), "node-2");
    }

    #[test]
    fn test_record_read_success() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default());

        reader.record_read_result(true, false);

        let stats = reader.stats();
        assert_eq!(stats.reads_attempted, 1);
        assert_eq!(stats.reads_succeeded, 1);
        assert_eq!(stats.reads_failed, 0);
    }

    #[test]
    fn test_record_read_local_success() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default());

        reader.record_read_result(true, true);

        let stats = reader.stats();
        assert_eq!(stats.local_reads, 1);
        assert_eq!(stats.locality_rate, 1.0);
    }

    #[test]
    fn test_record_read_failure() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default());

        reader.record_read_result(false, false);

        let stats = reader.stats();
        assert_eq!(stats.reads_attempted, 1);
        assert_eq!(stats.reads_succeeded, 0);
        assert_eq!(stats.reads_failed, 1);
    }

    #[test]
    fn test_read_strategy_order_nodes() {
        let strategy = ReadStrategy::default();
        let nodes = vec![
            NodeInfo::new("node-1", "127.0.0.1", 9001),
            NodeInfo::new("node-2", "127.0.0.1", 9002),
            NodeInfo::new("node-3", "127.0.0.1", 9003),
        ];

        let ordered = strategy.order_nodes(nodes, Some("node-2"));
        assert_eq!(ordered[0].id.as_str(), "node-2");
    }

    #[test]
    fn test_read_strategy_should_retry() {
        let strategy = ReadStrategy::default();
        assert!(strategy.should_retry(0));
        assert!(strategy.should_retry(1));
        assert!(!strategy.should_retry(2));
    }

    #[test]
    fn test_locality_rate_no_reads() {
        let ring = create_test_ring();
        let reader = ReplicationReader::new(ring, ReadConfig::default());

        let stats = reader.stats();
        assert_eq!(stats.locality_rate, 0.0);
    }
}
