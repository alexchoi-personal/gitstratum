use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use gitstratum_core::{Blob, Oid};
use gitstratum_hashring::{ConsistentHashRing, NodeInfo};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone)]
pub struct WriteConfig {
    pub replication_factor: usize,
    pub min_success: usize,
    pub timeout_ms: u64,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            min_success: 2,
            timeout_ms: 5000,
        }
    }
}

pub struct ReplicationWriter {
    ring: Arc<ConsistentHashRing>,
    config: WriteConfig,
    writes_attempted: AtomicU64,
    writes_succeeded: AtomicU64,
    writes_failed: AtomicU64,
}

impl ReplicationWriter {
    pub fn new(ring: Arc<ConsistentHashRing>, config: WriteConfig) -> Self {
        Self {
            ring,
            config,
            writes_attempted: AtomicU64::new(0),
            writes_succeeded: AtomicU64::new(0),
            writes_failed: AtomicU64::new(0),
        }
    }

    pub fn get_target_nodes(&self, oid: &Oid) -> Result<Vec<NodeInfo>> {
        let nodes = self.ring.nodes_for_oid(oid)?;
        Ok(nodes
            .into_iter()
            .take(self.config.replication_factor)
            .collect())
    }

    pub fn record_write_result(&self, success_count: usize) -> Result<()> {
        self.writes_attempted.fetch_add(1, Ordering::Relaxed);

        if success_count >= self.config.min_success {
            self.writes_succeeded.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            self.writes_failed.fetch_add(1, Ordering::Relaxed);
            Err(ObjectStoreError::InsufficientReplicas {
                required: self.config.min_success,
                achieved: success_count,
            })
        }
    }

    pub fn stats(&self) -> ReplicationWriterStats {
        ReplicationWriterStats {
            writes_attempted: self.writes_attempted.load(Ordering::Relaxed),
            writes_succeeded: self.writes_succeeded.load(Ordering::Relaxed),
            writes_failed: self.writes_failed.load(Ordering::Relaxed),
        }
    }

    pub fn replication_factor(&self) -> usize {
        self.config.replication_factor
    }

    pub fn min_success(&self) -> usize {
        self.config.min_success
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationWriterStats {
    pub writes_attempted: u64,
    pub writes_succeeded: u64,
    pub writes_failed: u64,
}

pub struct BatchWriter {
    writer: Arc<ReplicationWriter>,
    pending: Vec<Blob>,
    max_batch_size: usize,
}

impl BatchWriter {
    pub fn new(writer: Arc<ReplicationWriter>, max_batch_size: usize) -> Self {
        Self {
            writer,
            pending: Vec::new(),
            max_batch_size,
        }
    }

    pub fn add(&mut self, blob: Blob) -> Option<Vec<Blob>> {
        self.pending.push(blob);
        if self.pending.len() >= self.max_batch_size {
            Some(self.take_batch())
        } else {
            None
        }
    }

    pub fn take_batch(&mut self) -> Vec<Blob> {
        std::mem::take(&mut self.pending)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn clear(&mut self) {
        self.pending.clear();
    }

    pub fn group_by_node(&self) -> Result<Vec<(NodeInfo, Vec<Blob>)>> {
        use std::collections::HashMap;
        let mut groups: HashMap<String, (NodeInfo, Vec<Blob>)> = HashMap::new();

        for blob in &self.pending {
            let nodes = self.writer.get_target_nodes(&blob.oid)?;
            for node in nodes {
                let endpoint = node.endpoint();
                groups
                    .entry(endpoint)
                    .or_insert_with(|| (node, Vec::new()))
                    .1
                    .push(blob.clone());
            }
        }

        Ok(groups.into_values().collect())
    }
}

#[derive(Debug, Clone)]
pub struct NodeClient {
    pub node_id: String,
    pub endpoint: String,
}

impl NodeClient {
    pub fn new(node_id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            endpoint: endpoint.into(),
        }
    }

    pub fn from_node_info(node: &NodeInfo) -> Self {
        Self {
            node_id: node.id.to_string(),
            endpoint: node.endpoint(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WriteResult {
    QuorumAchieved {
        sync_replicas: usize,
        async_replicas: usize,
    },
}

impl WriteResult {
    pub fn total_replicas(&self) -> usize {
        match self {
            WriteResult::QuorumAchieved {
                sync_replicas,
                async_replicas,
            } => sync_replicas + async_replicas,
        }
    }

    pub fn sync_count(&self) -> usize {
        match self {
            WriteResult::QuorumAchieved { sync_replicas, .. } => *sync_replicas,
        }
    }

    pub fn async_count(&self) -> usize {
        match self {
            WriteResult::QuorumAchieved { async_replicas, .. } => *async_replicas,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuorumWriteConfig {
    pub quorum_size: usize,
    pub replication_factor: usize,
    pub timeout_ms: u64,
    pub async_replication: bool,
}

impl Default for QuorumWriteConfig {
    fn default() -> Self {
        Self {
            quorum_size: 2,
            replication_factor: 3,
            timeout_ms: 5000,
            async_replication: true,
        }
    }
}

pub struct QuorumWriter {
    nodes: Vec<NodeClient>,
    quorum_size: usize,
    replication_factor: usize,
    config: QuorumWriteConfig,
    ring: Arc<ConsistentHashRing>,
    writes_attempted: AtomicU64,
    writes_succeeded: AtomicU64,
    writes_failed: AtomicU64,
    async_writes_queued: AtomicU64,
    repair_tx: Option<mpsc::Sender<(Oid, Vec<String>)>>,
}

impl QuorumWriter {
    pub fn new(nodes: Vec<NodeClient>, replication_factor: usize) -> Self {
        let quorum_size = (replication_factor / 2) + 1;
        Self {
            nodes,
            quorum_size,
            replication_factor,
            config: QuorumWriteConfig {
                quorum_size,
                replication_factor,
                ..Default::default()
            },
            ring: Arc::new(gitstratum_hashring::ConsistentHashRing::new(
                16,
                replication_factor,
            )),
            writes_attempted: AtomicU64::new(0),
            writes_succeeded: AtomicU64::new(0),
            writes_failed: AtomicU64::new(0),
            async_writes_queued: AtomicU64::new(0),
            repair_tx: None,
        }
    }

    pub fn with_ring(mut self, ring: Arc<ConsistentHashRing>) -> Self {
        self.ring = ring;
        self
    }

    pub fn with_config(mut self, config: QuorumWriteConfig) -> Self {
        self.quorum_size = config.quorum_size;
        self.replication_factor = config.replication_factor;
        self.config = config;
        self
    }

    pub fn with_repair_channel(mut self, tx: mpsc::Sender<(Oid, Vec<String>)>) -> Self {
        self.repair_tx = Some(tx);
        self
    }

    pub fn select_nodes(&self, oid: &Oid) -> Vec<&NodeClient> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        match self.ring.nodes_for_oid(oid) {
            Ok(ring_nodes) => {
                let node_ids: std::collections::HashSet<_> =
                    ring_nodes.iter().map(|n| n.id.as_str()).collect();

                self.nodes
                    .iter()
                    .filter(|n| node_ids.contains(n.node_id.as_str()))
                    .take(self.replication_factor)
                    .collect()
            }
            Err(_) => {
                let hash = u64::from_be_bytes([
                    oid.as_bytes()[0],
                    oid.as_bytes()[1],
                    oid.as_bytes()[2],
                    oid.as_bytes()[3],
                    oid.as_bytes()[4],
                    oid.as_bytes()[5],
                    oid.as_bytes()[6],
                    oid.as_bytes()[7],
                ]);

                let start_idx = (hash as usize) % self.nodes.len();
                let mut selected = Vec::with_capacity(self.replication_factor);

                for i in 0..self.replication_factor.min(self.nodes.len()) {
                    let idx = (start_idx + i) % self.nodes.len();
                    selected.push(&self.nodes[idx]);
                }

                selected
            }
        }
    }

    pub async fn write(&self, oid: &Oid, data: &[u8]) -> Result<WriteResult> {
        let start = Instant::now();
        self.writes_attempted.fetch_add(1, Ordering::Relaxed);

        let target_nodes = self.select_nodes(oid);
        if target_nodes.is_empty() {
            self.writes_failed.fetch_add(1, Ordering::Relaxed);
            return Err(ObjectStoreError::NoAvailableNodes);
        }

        debug!(
            oid = %oid,
            target_count = target_nodes.len(),
            quorum = self.quorum_size,
            "starting quorum write"
        );

        let mut sync_success = 0usize;
        let mut failed_nodes: Vec<String> = Vec::new();

        for node in target_nodes.iter().take(self.quorum_size) {
            if self.simulate_write_to_node(node, oid, data).await {
                sync_success += 1;
            } else {
                failed_nodes.push(node.node_id.clone());
            }
        }

        if sync_success < self.quorum_size {
            self.writes_failed.fetch_add(1, Ordering::Relaxed);
            warn!(
                oid = %oid,
                achieved = sync_success,
                required = self.quorum_size,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "quorum write failed"
            );
            return Err(ObjectStoreError::InsufficientReplicas {
                required: self.quorum_size,
                achieved: sync_success,
            });
        }

        let mut async_success = 0usize;
        let remaining_nodes: Vec<_> = target_nodes
            .iter()
            .skip(self.quorum_size)
            .cloned()
            .collect();

        if self.config.async_replication && !remaining_nodes.is_empty() {
            self.async_writes_queued
                .fetch_add(remaining_nodes.len() as u64, Ordering::Relaxed);
            async_success = remaining_nodes.len();

            if !failed_nodes.is_empty() {
                if let Some(ref tx) = self.repair_tx {
                    let _ = tx.try_send((*oid, failed_nodes.clone()));
                }
            }
        }

        self.writes_succeeded.fetch_add(1, Ordering::Relaxed);

        debug!(
            oid = %oid,
            sync_replicas = sync_success,
            async_replicas = async_success,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "quorum write succeeded"
        );

        Ok(WriteResult::QuorumAchieved {
            sync_replicas: sync_success,
            async_replicas: async_success,
        })
    }

    async fn simulate_write_to_node(&self, _node: &NodeClient, _oid: &Oid, _data: &[u8]) -> bool {
        true
    }

    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn stats(&self) -> QuorumWriterStats {
        QuorumWriterStats {
            writes_attempted: self.writes_attempted.load(Ordering::Relaxed),
            writes_succeeded: self.writes_succeeded.load(Ordering::Relaxed),
            writes_failed: self.writes_failed.load(Ordering::Relaxed),
            async_writes_queued: self.async_writes_queued.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuorumWriterStats {
    pub writes_attempted: u64,
    pub writes_succeeded: u64,
    pub writes_failed: u64,
    pub async_writes_queued: u64,
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

    fn create_test_blob(data: &[u8]) -> Blob {
        Blob::new(data.to_vec())
    }

    #[test]
    fn test_write_config_default() {
        let config = WriteConfig::default();
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.min_success, 2);
        assert_eq!(config.timeout_ms, 5000);
    }

    #[test]
    fn test_replication_writer_new() {
        let ring = create_test_ring();
        let config = WriteConfig::default();
        let writer = ReplicationWriter::new(ring, config);
        assert_eq!(writer.replication_factor(), 3);
        assert_eq!(writer.min_success(), 2);
    }

    #[test]
    fn test_get_target_nodes() {
        let ring = create_test_ring();
        let writer = ReplicationWriter::new(ring, WriteConfig::default());
        let oid = Oid::hash(b"test");

        let nodes = writer.get_target_nodes(&oid).unwrap();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_record_write_success() {
        let ring = create_test_ring();
        let writer = ReplicationWriter::new(ring, WriteConfig::default());

        let result = writer.record_write_result(2);
        assert!(result.is_ok());

        let stats = writer.stats();
        assert_eq!(stats.writes_attempted, 1);
        assert_eq!(stats.writes_succeeded, 1);
        assert_eq!(stats.writes_failed, 0);
    }

    #[test]
    fn test_record_write_failure() {
        let ring = create_test_ring();
        let writer = ReplicationWriter::new(ring, WriteConfig::default());

        let result = writer.record_write_result(1);
        assert!(result.is_err());

        let stats = writer.stats();
        assert_eq!(stats.writes_attempted, 1);
        assert_eq!(stats.writes_succeeded, 0);
        assert_eq!(stats.writes_failed, 1);
    }

    #[test]
    fn test_batch_writer_add() {
        let ring = create_test_ring();
        let writer = Arc::new(ReplicationWriter::new(ring, WriteConfig::default()));
        let mut batch_writer = BatchWriter::new(writer, 3);

        assert!(batch_writer.add(create_test_blob(b"1")).is_none());
        assert!(batch_writer.add(create_test_blob(b"2")).is_none());
        let batch = batch_writer.add(create_test_blob(b"3"));
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 3);
    }

    #[test]
    fn test_batch_writer_take_batch() {
        let ring = create_test_ring();
        let writer = Arc::new(ReplicationWriter::new(ring, WriteConfig::default()));
        let mut batch_writer = BatchWriter::new(writer, 10);

        batch_writer.add(create_test_blob(b"1"));
        batch_writer.add(create_test_blob(b"2"));

        let batch = batch_writer.take_batch();
        assert_eq!(batch.len(), 2);
        assert!(batch_writer.is_empty());
    }

    #[test]
    fn test_batch_writer_clear() {
        let ring = create_test_ring();
        let writer = Arc::new(ReplicationWriter::new(ring, WriteConfig::default()));
        let mut batch_writer = BatchWriter::new(writer, 10);

        batch_writer.add(create_test_blob(b"1"));
        assert!(!batch_writer.is_empty());

        batch_writer.clear();
        assert!(batch_writer.is_empty());
    }

    #[test]
    fn test_batch_writer_group_by_node() {
        let ring = create_test_ring();
        let writer = Arc::new(ReplicationWriter::new(ring, WriteConfig::default()));
        let mut batch_writer = BatchWriter::new(writer, 10);

        batch_writer.add(create_test_blob(b"1"));
        batch_writer.add(create_test_blob(b"2"));

        let groups = batch_writer.group_by_node().unwrap();
        assert!(!groups.is_empty());
    }

    #[test]
    fn test_node_client_new() {
        let client = NodeClient::new("node-1", "127.0.0.1:9001");
        assert_eq!(client.node_id, "node-1");
        assert_eq!(client.endpoint, "127.0.0.1:9001");
    }

    #[test]
    fn test_node_client_from_node_info() {
        let node_info = NodeInfo::new("node-1", "127.0.0.1", 9001);
        let client = NodeClient::from_node_info(&node_info);
        assert_eq!(client.node_id, "node-1");
        assert_eq!(client.endpoint, "127.0.0.1:9001");
    }

    #[test]
    fn test_write_result_quorum_achieved() {
        let result = WriteResult::QuorumAchieved {
            sync_replicas: 2,
            async_replicas: 1,
        };
        assert_eq!(result.total_replicas(), 3);
        assert_eq!(result.sync_count(), 2);
        assert_eq!(result.async_count(), 1);
    }

    #[test]
    fn test_quorum_write_config_default() {
        let config = QuorumWriteConfig::default();
        assert_eq!(config.quorum_size, 2);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.timeout_ms, 5000);
        assert!(config.async_replication);
    }

    #[test]
    fn test_quorum_writer_new() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
        ];
        let writer = QuorumWriter::new(nodes, 3);
        assert_eq!(writer.quorum_size(), 2);
        assert_eq!(writer.replication_factor(), 3);
        assert_eq!(writer.node_count(), 3);
    }

    #[test]
    fn test_quorum_writer_with_config() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
        ];
        let config = QuorumWriteConfig {
            quorum_size: 1,
            replication_factor: 2,
            timeout_ms: 1000,
            async_replication: false,
        };
        let writer = QuorumWriter::new(nodes, 2).with_config(config);
        assert_eq!(writer.quorum_size(), 1);
        assert_eq!(writer.replication_factor(), 2);
    }

    #[test]
    fn test_quorum_writer_select_nodes() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
        ];
        let writer = QuorumWriter::new(nodes, 3);
        let oid = Oid::hash(b"test");
        let selected = writer.select_nodes(&oid);
        assert!(!selected.is_empty());
        assert!(selected.len() <= 3);
    }

    #[test]
    fn test_quorum_writer_select_nodes_empty() {
        let writer = QuorumWriter::new(vec![], 3);
        let oid = Oid::hash(b"test");
        let selected = writer.select_nodes(&oid);
        assert!(selected.is_empty());
    }

    #[tokio::test]
    async fn test_quorum_writer_write_success() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
        ];
        let writer = QuorumWriter::new(nodes, 3);
        let oid = Oid::hash(b"test");
        let data = b"test data";

        let result = writer.write(&oid, data).await;
        assert!(result.is_ok());

        let write_result = result.unwrap();
        assert!(write_result.sync_count() >= 2);

        let stats = writer.stats();
        assert_eq!(stats.writes_attempted, 1);
        assert_eq!(stats.writes_succeeded, 1);
        assert_eq!(stats.writes_failed, 0);
    }

    #[tokio::test]
    async fn test_quorum_writer_write_no_nodes() {
        let writer = QuorumWriter::new(vec![], 3);
        let oid = Oid::hash(b"test");
        let data = b"test data";

        let result = writer.write(&oid, data).await;
        assert!(result.is_err());

        let stats = writer.stats();
        assert_eq!(stats.writes_attempted, 1);
        assert_eq!(stats.writes_failed, 1);
    }

    #[test]
    fn test_quorum_writer_with_ring() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
        ];
        let ring = create_test_ring();
        let writer = QuorumWriter::new(nodes, 3).with_ring(ring);
        assert_eq!(writer.node_count(), 3);
    }

    #[tokio::test]
    async fn test_quorum_writer_with_repair_channel() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
        ];
        let (tx, _rx) = mpsc::channel(10);
        let writer = QuorumWriter::new(nodes, 2).with_repair_channel(tx);

        let oid = Oid::hash(b"test");
        let result = writer.write(&oid, b"data").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_quorum_writer_stats_initial() {
        let nodes = vec![NodeClient::new("node-1", "127.0.0.1:9001")];
        let writer = QuorumWriter::new(nodes, 1);
        let stats = writer.stats();
        assert_eq!(stats.writes_attempted, 0);
        assert_eq!(stats.writes_succeeded, 0);
        assert_eq!(stats.writes_failed, 0);
        assert_eq!(stats.async_writes_queued, 0);
    }

    #[test]
    fn test_node_client_debug() {
        let client = NodeClient::new("node-1", "127.0.0.1:9001");
        let debug = format!("{:?}", client);
        assert!(debug.contains("NodeClient"));
        assert!(debug.contains("node-1"));
    }

    #[test]
    fn test_write_result_debug() {
        let result = WriteResult::QuorumAchieved {
            sync_replicas: 2,
            async_replicas: 1,
        };
        let debug = format!("{:?}", result);
        assert!(debug.contains("QuorumAchieved"));
    }

    #[test]
    fn test_quorum_write_config_debug() {
        let config = QuorumWriteConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("QuorumWriteConfig"));
    }

    #[test]
    fn test_quorum_writer_stats_debug() {
        let stats = QuorumWriterStats {
            writes_attempted: 10,
            writes_succeeded: 8,
            writes_failed: 2,
            async_writes_queued: 5,
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("QuorumWriterStats"));
    }

    #[test]
    fn test_batch_writer_pending_count() {
        let ring = create_test_ring();
        let writer = Arc::new(ReplicationWriter::new(ring, WriteConfig::default()));
        let mut batch_writer = BatchWriter::new(writer, 10);

        assert_eq!(batch_writer.pending_count(), 0);

        batch_writer.add(create_test_blob(b"1"));
        assert_eq!(batch_writer.pending_count(), 1);

        batch_writer.add(create_test_blob(b"2"));
        assert_eq!(batch_writer.pending_count(), 2);
    }

    #[test]
    fn test_write_config_clone() {
        let config = WriteConfig {
            replication_factor: 5,
            min_success: 3,
            timeout_ms: 10000,
        };
        let cloned = config.clone();
        assert_eq!(cloned.replication_factor, 5);
        assert_eq!(cloned.min_success, 3);
        assert_eq!(cloned.timeout_ms, 10000);
    }

    #[test]
    fn test_write_config_debug() {
        let config = WriteConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("WriteConfig"));
    }

    #[test]
    fn test_replication_writer_stats_clone() {
        let stats = ReplicationWriterStats {
            writes_attempted: 5,
            writes_succeeded: 4,
            writes_failed: 1,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.writes_attempted, 5);
        assert_eq!(cloned.writes_succeeded, 4);
        assert_eq!(cloned.writes_failed, 1);
    }

    #[test]
    fn test_replication_writer_stats_debug() {
        let stats = ReplicationWriterStats {
            writes_attempted: 5,
            writes_succeeded: 4,
            writes_failed: 1,
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("ReplicationWriterStats"));
    }

    #[test]
    fn test_record_write_result_error_details() {
        let ring = create_test_ring();
        let config = WriteConfig {
            replication_factor: 3,
            min_success: 2,
            timeout_ms: 5000,
        };
        let writer = ReplicationWriter::new(ring, config);

        let result = writer.record_write_result(1);
        assert!(result.is_err());

        match result.unwrap_err() {
            ObjectStoreError::InsufficientReplicas { required, achieved } => {
                assert_eq!(required, 2);
                assert_eq!(achieved, 1);
            }
            _ => panic!("Expected InsufficientReplicas error"),
        }
    }

    #[test]
    fn test_quorum_writer_select_nodes_fallback_path() {
        let nodes = vec![
            NodeClient::new("different-node-1", "127.0.0.1:9001"),
            NodeClient::new("different-node-2", "127.0.0.1:9002"),
            NodeClient::new("different-node-3", "127.0.0.1:9003"),
        ];
        let ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .add_node(NodeInfo::new("node-a", "127.0.0.1", 8001))
                .add_node(NodeInfo::new("node-b", "127.0.0.1", 8002))
                .add_node(NodeInfo::new("node-c", "127.0.0.1", 8003))
                .build()
                .unwrap(),
        );

        let writer = QuorumWriter::new(nodes, 3).with_ring(ring);
        let oid = Oid::hash(b"test");
        let selected = writer.select_nodes(&oid);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_quorum_writer_select_nodes_with_empty_ring_fallback() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
        ];
        let empty_ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .build()
                .unwrap(),
        );

        let writer = QuorumWriter::new(nodes, 3).with_ring(empty_ring);
        let oid = Oid::hash(b"test");
        let selected = writer.select_nodes(&oid);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_quorum_writer_select_nodes_fallback_wraps_around() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
        ];
        let empty_ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .build()
                .unwrap(),
        );

        let writer = QuorumWriter::new(nodes, 3).with_ring(empty_ring);
        let oid = Oid::hash(b"test data");
        let selected = writer.select_nodes(&oid);
        assert_eq!(selected.len(), 2);
    }

    #[tokio::test]
    async fn test_quorum_writer_async_replication_with_remaining_nodes() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
            NodeClient::new("node-4", "127.0.0.1:9004"),
            NodeClient::new("node-5", "127.0.0.1:9005"),
        ];
        let config = QuorumWriteConfig {
            quorum_size: 2,
            replication_factor: 5,
            timeout_ms: 5000,
            async_replication: true,
        };
        let empty_ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(5)
                .build()
                .unwrap(),
        );

        let writer = QuorumWriter::new(nodes, 5)
            .with_ring(empty_ring)
            .with_config(config);

        let oid = Oid::hash(b"test");
        let result = writer.write(&oid, b"data").await;
        assert!(result.is_ok());

        let write_result = result.unwrap();
        assert_eq!(write_result.sync_count(), 2);
        assert!(write_result.async_count() > 0);

        let stats = writer.stats();
        assert!(stats.async_writes_queued > 0);
    }

    #[tokio::test]
    async fn test_quorum_writer_no_async_replication_when_disabled() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
        ];
        let config = QuorumWriteConfig {
            quorum_size: 2,
            replication_factor: 3,
            timeout_ms: 5000,
            async_replication: false,
        };
        let empty_ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .build()
                .unwrap(),
        );

        let writer = QuorumWriter::new(nodes, 3)
            .with_ring(empty_ring)
            .with_config(config);

        let oid = Oid::hash(b"test");
        let result = writer.write(&oid, b"data").await;
        assert!(result.is_ok());

        let write_result = result.unwrap();
        assert_eq!(write_result.sync_count(), 2);
        assert_eq!(write_result.async_count(), 0);

        let stats = writer.stats();
        assert_eq!(stats.async_writes_queued, 0);
    }

    #[test]
    fn test_quorum_write_config_clone() {
        let config = QuorumWriteConfig {
            quorum_size: 3,
            replication_factor: 5,
            timeout_ms: 10000,
            async_replication: false,
        };
        let cloned = config.clone();
        assert_eq!(cloned.quorum_size, 3);
        assert_eq!(cloned.replication_factor, 5);
        assert_eq!(cloned.timeout_ms, 10000);
        assert!(!cloned.async_replication);
    }

    #[test]
    fn test_quorum_writer_stats_clone() {
        let stats = QuorumWriterStats {
            writes_attempted: 10,
            writes_succeeded: 8,
            writes_failed: 2,
            async_writes_queued: 5,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.writes_attempted, 10);
        assert_eq!(cloned.writes_succeeded, 8);
        assert_eq!(cloned.writes_failed, 2);
        assert_eq!(cloned.async_writes_queued, 5);
    }

    #[test]
    fn test_write_result_clone() {
        let result = WriteResult::QuorumAchieved {
            sync_replicas: 2,
            async_replicas: 1,
        };
        let cloned = result.clone();
        assert_eq!(cloned.sync_count(), 2);
        assert_eq!(cloned.async_count(), 1);
    }

    #[test]
    fn test_node_client_clone() {
        let client = NodeClient::new("node-1", "127.0.0.1:9001");
        let cloned = client.clone();
        assert_eq!(cloned.node_id, "node-1");
        assert_eq!(cloned.endpoint, "127.0.0.1:9001");
    }

    #[test]
    fn test_get_target_nodes_with_fewer_nodes_than_replication_factor() {
        let ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(2)
                .add_node(NodeInfo::new("node-1", "127.0.0.1", 9001))
                .add_node(NodeInfo::new("node-2", "127.0.0.1", 9002))
                .build()
                .unwrap(),
        );
        let config = WriteConfig {
            replication_factor: 5,
            min_success: 2,
            timeout_ms: 5000,
        };
        let writer = ReplicationWriter::new(ring, config);
        let oid = Oid::hash(b"test");

        let nodes = writer.get_target_nodes(&oid).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[tokio::test]
    async fn test_quorum_writer_multiple_writes_stats_accumulate() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
        ];
        let writer = QuorumWriter::new(nodes, 2);

        for i in 0..5 {
            let oid = Oid::hash(format!("test-{}", i).as_bytes());
            let _ = writer.write(&oid, b"data").await;
        }

        let stats = writer.stats();
        assert_eq!(stats.writes_attempted, 5);
        assert_eq!(stats.writes_succeeded, 5);
        assert_eq!(stats.writes_failed, 0);
    }

    #[test]
    fn test_record_write_result_exactly_at_min_success() {
        let ring = create_test_ring();
        let config = WriteConfig {
            replication_factor: 3,
            min_success: 2,
            timeout_ms: 5000,
        };
        let writer = ReplicationWriter::new(ring, config);

        let result = writer.record_write_result(2);
        assert!(result.is_ok());

        let stats = writer.stats();
        assert_eq!(stats.writes_succeeded, 1);
        assert_eq!(stats.writes_failed, 0);
    }

    #[test]
    fn test_record_write_result_above_min_success() {
        let ring = create_test_ring();
        let config = WriteConfig {
            replication_factor: 3,
            min_success: 2,
            timeout_ms: 5000,
        };
        let writer = ReplicationWriter::new(ring, config);

        let result = writer.record_write_result(3);
        assert!(result.is_ok());

        let stats = writer.stats();
        assert_eq!(stats.writes_succeeded, 1);
    }

    #[test]
    fn test_record_write_result_zero_success() {
        let ring = create_test_ring();
        let writer = ReplicationWriter::new(ring, WriteConfig::default());

        let result = writer.record_write_result(0);
        assert!(result.is_err());

        match result.unwrap_err() {
            ObjectStoreError::InsufficientReplicas { required, achieved } => {
                assert_eq!(required, 2);
                assert_eq!(achieved, 0);
            }
            _ => panic!("Expected InsufficientReplicas error"),
        }
    }

    #[test]
    fn test_batch_writer_add_returns_batch_at_exact_max() {
        let ring = create_test_ring();
        let writer = Arc::new(ReplicationWriter::new(ring, WriteConfig::default()));
        let mut batch_writer = BatchWriter::new(writer, 2);

        assert!(batch_writer.add(create_test_blob(b"1")).is_none());
        let batch = batch_writer.add(create_test_blob(b"2"));
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 2);
        assert!(batch_writer.is_empty());
    }

    #[test]
    fn test_quorum_writer_select_nodes_returns_up_to_replication_factor() {
        let nodes = vec![
            NodeClient::new("node-1", "127.0.0.1:9001"),
            NodeClient::new("node-2", "127.0.0.1:9002"),
            NodeClient::new("node-3", "127.0.0.1:9003"),
            NodeClient::new("node-4", "127.0.0.1:9004"),
            NodeClient::new("node-5", "127.0.0.1:9005"),
        ];
        let ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .add_node(NodeInfo::new("node-1", "127.0.0.1", 9001))
                .add_node(NodeInfo::new("node-2", "127.0.0.1", 9002))
                .add_node(NodeInfo::new("node-3", "127.0.0.1", 9003))
                .add_node(NodeInfo::new("node-4", "127.0.0.1", 9004))
                .add_node(NodeInfo::new("node-5", "127.0.0.1", 9005))
                .build()
                .unwrap(),
        );

        let writer = QuorumWriter::new(nodes, 3).with_ring(ring);
        let oid = Oid::hash(b"test");
        let selected = writer.select_nodes(&oid);
        assert!(selected.len() <= 3);
    }
}
