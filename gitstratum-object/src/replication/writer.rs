use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use gitstratum_core::{Blob, Oid};
use gitstratum_hashring::{ConsistentHashRing, NodeInfo};

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
}
