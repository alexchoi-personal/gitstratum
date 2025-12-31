use async_trait::async_trait;
use gitstratum_core::{Blob, Oid};
use std::time::Duration;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct ObjectClientConfig {
    pub timeout: Duration,
    pub pool_size: u32,
    pub batch_size: usize,
}

impl ObjectClientConfig {
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            pool_size: 100,
            batch_size: 100,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_pool_size(mut self, size: u32) -> Self {
        self.pool_size = size;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

impl Default for ObjectClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait ObjectClusterClient: Send + Sync {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>>;
    async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>>;
    async fn put_blob(&self, blob: &Blob) -> Result<()>;
    async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()>;
    async fn exists(&self, oid: &Oid) -> Result<bool>;
    async fn exists_batch(&self, oids: Vec<Oid>) -> Result<Vec<bool>>;
}

#[derive(Debug, Clone)]
pub struct ObjectLocation {
    pub oid: Oid,
    pub node_ids: Vec<String>,
    pub primary_node: String,
}

impl ObjectLocation {
    pub fn new(oid: Oid, node_ids: Vec<String>) -> Self {
        let primary = node_ids.first().cloned().unwrap_or_default();
        Self {
            oid,
            node_ids,
            primary_node: primary,
        }
    }

    pub fn replica_count(&self) -> usize {
        self.node_ids.len()
    }

    pub fn is_replicated(&self) -> bool {
        self.node_ids.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_client_config_new() {
        let config = ObjectClientConfig::new();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.pool_size, 100);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_object_client_config_default() {
        let config = ObjectClientConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_object_client_config_builders() {
        let config = ObjectClientConfig::new()
            .with_timeout(Duration::from_secs(60))
            .with_pool_size(200)
            .with_batch_size(50);

        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.pool_size, 200);
        assert_eq!(config.batch_size, 50);
    }

    #[test]
    fn test_object_location_new() {
        let oid = Oid::hash(b"test");
        let nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let location = ObjectLocation::new(oid, nodes);

        assert_eq!(location.oid, oid);
        assert_eq!(location.primary_node, "node-1");
        assert_eq!(location.replica_count(), 2);
        assert!(location.is_replicated());
    }

    #[test]
    fn test_object_location_single_node() {
        let oid = Oid::hash(b"test");
        let nodes = vec!["node-1".to_string()];
        let location = ObjectLocation::new(oid, nodes);

        assert_eq!(location.replica_count(), 1);
        assert!(!location.is_replicated());
    }

    #[test]
    fn test_object_location_empty_nodes() {
        let oid = Oid::hash(b"test");
        let nodes = vec![];
        let location = ObjectLocation::new(oid, nodes);

        assert!(location.primary_node.is_empty());
        assert_eq!(location.replica_count(), 0);
    }
}
