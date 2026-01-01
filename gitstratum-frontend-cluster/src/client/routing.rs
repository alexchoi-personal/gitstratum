use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use gitstratum_core::{Blob, Oid};
use gitstratum_hashring::{ConsistentHashRing, NodeId};
use std::collections::HashMap;
use std::sync::Arc;

use super::object::ObjectClusterClient;
use crate::error::{FrontendError, Result};

pub struct RoutingObjectClient<C> {
    ring: Arc<ConsistentHashRing>,
    node_clients: HashMap<NodeId, Arc<C>>,
    max_concurrent_nodes: usize,
}

impl<C> RoutingObjectClient<C>
where
    C: ObjectClusterClient + 'static,
{
    pub fn new(ring: Arc<ConsistentHashRing>, node_clients: HashMap<NodeId, Arc<C>>) -> Self {
        Self {
            ring,
            node_clients,
            max_concurrent_nodes: 16,
        }
    }

    pub fn with_max_concurrent_nodes(mut self, max: usize) -> Self {
        self.max_concurrent_nodes = max;
        self
    }

    fn get_client(&self, node_id: &NodeId) -> Result<Arc<C>> {
        self.node_clients
            .get(node_id)
            .cloned()
            .ok_or_else(|| FrontendError::ObjectService(format!("no client for node {}", node_id)))
    }

    fn partition_by_node(&self, oids: Vec<Oid>) -> Result<HashMap<NodeId, Vec<Oid>>> {
        let mut by_node: HashMap<NodeId, Vec<Oid>> = HashMap::new();

        for oid in oids {
            let node = self.ring.primary_node_for_oid(&oid)?;
            by_node.entry(node.id).or_default().push(oid);
        }

        Ok(by_node)
    }

    fn partition_by_node_balanced(&self, oids: Vec<Oid>) -> Result<HashMap<NodeId, Vec<Oid>>> {
        let mut by_node: HashMap<NodeId, Vec<Oid>> = HashMap::new();
        let mut node_load: HashMap<NodeId, usize> = HashMap::new();

        for oid in oids {
            let replicas = self.ring.nodes_for_oid(&oid)?;

            let best_node = replicas
                .iter()
                .min_by_key(|n| node_load.get(&n.id).unwrap_or(&0))
                .ok_or_else(|| FrontendError::HashRing("no replicas found".to_string()))?;

            *node_load.entry(best_node.id.clone()).or_insert(0) += 1;
            by_node.entry(best_node.id.clone()).or_default().push(oid);
        }

        Ok(by_node)
    }

    fn partition_blobs_by_node(&self, blobs: Vec<Blob>) -> Result<HashMap<NodeId, Vec<Blob>>> {
        let mut by_node: HashMap<NodeId, Vec<Blob>> = HashMap::new();

        for blob in blobs {
            let replicas = self.ring.nodes_for_oid(&blob.oid)?;

            for node in replicas {
                by_node.entry(node.id).or_default().push(blob.clone());
            }
        }

        Ok(by_node)
    }
}

#[async_trait]
impl<C> ObjectClusterClient for RoutingObjectClient<C>
where
    C: ObjectClusterClient + 'static,
{
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>> {
        let node = self.ring.primary_node_for_oid(oid)?;
        let client = self.get_client(&node.id)?;
        client.get_blob(oid).await
    }

    async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>> {
        if oids.is_empty() {
            return Ok(Vec::new());
        }

        let by_node = self.partition_by_node_balanced(oids)?;

        let results: Vec<Result<Vec<Blob>>> = stream::iter(by_node)
            .map(|(node_id, node_oids)| {
                let client = self.get_client(&node_id);
                async move {
                    let client = client?;
                    client.get_blobs(node_oids).await
                }
            })
            .buffer_unordered(self.max_concurrent_nodes)
            .collect()
            .await;

        let mut blobs = Vec::new();
        for result in results {
            blobs.extend(result?);
        }

        Ok(blobs)
    }

    async fn put_blob(&self, blob: &Blob) -> Result<()> {
        let replicas = self.ring.nodes_for_oid(&blob.oid)?;
        let quorum = (replicas.len() / 2) + 1;

        let results: Vec<Result<()>> = stream::iter(replicas)
            .map(|node| {
                let client = self.get_client(&node.id);
                let blob = blob.clone();
                async move {
                    let client = client?;
                    client.put_blob(&blob).await
                }
            })
            .buffer_unordered(self.max_concurrent_nodes)
            .collect()
            .await;

        let success_count = results.iter().filter(|r| r.is_ok()).count();
        if success_count >= quorum {
            Ok(())
        } else {
            Err(FrontendError::ObjectService(format!(
                "quorum not reached: {} of {} succeeded, need {}",
                success_count,
                results.len(),
                quorum
            )))
        }
    }

    async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }

        let by_node = self.partition_blobs_by_node(blobs)?;

        let results: Vec<Result<()>> = stream::iter(by_node)
            .map(|(node_id, node_blobs)| {
                let client = self.get_client(&node_id);
                async move {
                    let client = client?;
                    client.put_blobs(node_blobs).await
                }
            })
            .buffer_unordered(self.max_concurrent_nodes)
            .collect()
            .await;

        for result in results {
            result?;
        }

        Ok(())
    }

    async fn exists(&self, oid: &Oid) -> Result<bool> {
        let node = self.ring.primary_node_for_oid(oid)?;
        let client = self.get_client(&node.id)?;
        client.exists(oid).await
    }

    async fn exists_batch(&self, oids: Vec<Oid>) -> Result<Vec<bool>> {
        if oids.is_empty() {
            return Ok(Vec::new());
        }

        let oid_positions: Vec<(usize, Oid)> = oids.iter().cloned().enumerate().collect();

        let mut by_node: HashMap<NodeId, Vec<(usize, Oid)>> = HashMap::new();
        for (idx, oid) in oid_positions {
            let node = self.ring.primary_node_for_oid(&oid)?;
            by_node.entry(node.id).or_default().push((idx, oid));
        }

        let mut results_map: HashMap<usize, bool> = HashMap::with_capacity(oids.len());

        let node_results: Vec<Result<Vec<(usize, bool)>>> = stream::iter(by_node)
            .map(|(node_id, indexed_oids)| {
                let client = self.get_client(&node_id);
                async move {
                    let client = client?;
                    let oids_only: Vec<Oid> = indexed_oids.iter().map(|(_, o)| *o).collect();
                    let exists_results = client.exists_batch(oids_only).await?;
                    let indexed_results: Vec<(usize, bool)> = indexed_oids
                        .iter()
                        .zip(exists_results)
                        .map(|((idx, _), exists)| (*idx, exists))
                        .collect();
                    Ok(indexed_results)
                }
            })
            .buffer_unordered(self.max_concurrent_nodes)
            .collect()
            .await;

        for result in node_results {
            for (idx, exists) in result? {
                results_map.insert(idx, exists);
            }
        }

        let results: Vec<bool> = (0..oids.len())
            .map(|i| results_map.get(&i).copied().unwrap_or(false))
            .collect();

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_hashring::{HashRingBuilder, NodeInfo};
    use std::collections::HashSet;
    use tokio::sync::RwLock;

    struct MockNodeClient {
        node_id: String,
        blobs: RwLock<HashMap<Oid, Blob>>,
    }

    impl MockNodeClient {
        fn new(node_id: &str) -> Self {
            Self {
                node_id: node_id.to_string(),
                blobs: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl ObjectClusterClient for MockNodeClient {
        async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>> {
            Ok(self.blobs.read().await.get(oid).cloned())
        }

        async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>> {
            let blobs = self.blobs.read().await;
            Ok(oids
                .iter()
                .filter_map(|oid| blobs.get(oid).cloned())
                .collect())
        }

        async fn put_blob(&self, blob: &Blob) -> Result<()> {
            self.blobs.write().await.insert(blob.oid, blob.clone());
            Ok(())
        }

        async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()> {
            let mut store = self.blobs.write().await;
            for blob in blobs {
                store.insert(blob.oid, blob);
            }
            Ok(())
        }

        async fn exists(&self, oid: &Oid) -> Result<bool> {
            Ok(self.blobs.read().await.contains_key(oid))
        }

        async fn exists_batch(&self, oids: Vec<Oid>) -> Result<Vec<bool>> {
            let blobs = self.blobs.read().await;
            Ok(oids.iter().map(|oid| blobs.contains_key(oid)).collect())
        }
    }

    fn create_test_ring() -> Arc<ConsistentHashRing> {
        Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(3)
                .add_node(NodeInfo::new("node-1", "10.0.0.1", 9000))
                .add_node(NodeInfo::new("node-2", "10.0.0.2", 9000))
                .add_node(NodeInfo::new("node-3", "10.0.0.3", 9000))
                .build()
                .unwrap(),
        )
    }

    fn create_test_clients() -> HashMap<NodeId, Arc<MockNodeClient>> {
        let mut clients = HashMap::new();
        clients.insert(
            NodeId::new("node-1"),
            Arc::new(MockNodeClient::new("node-1")),
        );
        clients.insert(
            NodeId::new("node-2"),
            Arc::new(MockNodeClient::new("node-2")),
        );
        clients.insert(
            NodeId::new("node-3"),
            Arc::new(MockNodeClient::new("node-3")),
        );
        clients
    }

    #[tokio::test]
    async fn test_get_blob() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let blob = Blob::new(b"test data".to_vec());
        let node = ring.primary_node_for_oid(&blob.oid).unwrap();
        let client = clients.get(&node.id).unwrap();
        client.put_blob(&blob).await.unwrap();

        let result = routing.get_blob(&blob.oid).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data.as_ref(), b"test data");
    }

    #[tokio::test]
    async fn test_get_blobs_routes_correctly() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let mut blobs = Vec::new();
        for i in 0..10 {
            let blob = Blob::new(format!("data-{}", i).into_bytes());
            let replicas = ring.nodes_for_oid(&blob.oid).unwrap();
            for node in replicas {
                let client = clients.get(&node.id).unwrap();
                client.put_blob(&blob).await.unwrap();
            }
            blobs.push(blob);
        }

        let oids: Vec<Oid> = blobs.iter().map(|b| b.oid).collect();
        let result = routing.get_blobs(oids).await.unwrap();

        assert_eq!(result.len(), 10);
    }

    #[tokio::test]
    async fn test_put_blob_writes_to_replicas() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let blob = Blob::new(b"replicated data".to_vec());
        routing.put_blob(&blob).await.unwrap();

        let replicas = ring.nodes_for_oid(&blob.oid).unwrap();
        for node in replicas {
            let client = clients.get(&node.id).unwrap();
            let result = client.get_blob(&blob.oid).await.unwrap();
            assert!(result.is_some());
        }
    }

    #[tokio::test]
    async fn test_exists() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let blob = Blob::new(b"exists test".to_vec());
        let node = ring.primary_node_for_oid(&blob.oid).unwrap();
        clients
            .get(&node.id)
            .unwrap()
            .put_blob(&blob)
            .await
            .unwrap();

        assert!(routing.exists(&blob.oid).await.unwrap());

        let missing_oid = Oid::hash(b"missing");
        assert!(!routing.exists(&missing_oid).await.unwrap());
    }

    #[tokio::test]
    async fn test_exists_batch() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let blob1 = Blob::new(b"blob1".to_vec());
        let blob2 = Blob::new(b"blob2".to_vec());
        let missing_oid = Oid::hash(b"missing");

        for blob in [&blob1, &blob2] {
            let node = ring.primary_node_for_oid(&blob.oid).unwrap();
            clients.get(&node.id).unwrap().put_blob(blob).await.unwrap();
        }

        let oids = vec![blob1.oid, missing_oid, blob2.oid];
        let results = routing.exists_batch(oids).await.unwrap();

        assert_eq!(results, vec![true, false, true]);
    }

    #[tokio::test]
    async fn test_get_blobs_empty() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring, clients);

        let result = routing.get_blobs(vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_put_blobs() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let blobs: Vec<Blob> = (0..5)
            .map(|i| Blob::new(format!("blob-{}", i).into_bytes()))
            .collect();

        routing.put_blobs(blobs.clone()).await.unwrap();

        for blob in &blobs {
            let replicas = ring.nodes_for_oid(&blob.oid).unwrap();
            for node in replicas {
                let client = clients.get(&node.id).unwrap();
                assert!(client.exists(&blob.oid).await.unwrap());
            }
        }
    }

    #[tokio::test]
    async fn test_load_balancing() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients.clone());

        let blobs: Vec<Blob> = (0..100)
            .map(|i| Blob::new(format!("data-{}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let replicas = ring.nodes_for_oid(&blob.oid).unwrap();
            for node in replicas {
                clients.get(&node.id).unwrap().put_blob(blob).await.unwrap();
            }
        }

        let oids: Vec<Oid> = blobs.iter().map(|b| b.oid).collect();

        let by_node = routing.partition_by_node_balanced(oids.clone()).unwrap();

        let counts: Vec<usize> = by_node.values().map(|v| v.len()).collect();
        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();

        assert!(max - min <= 50, "Load should be roughly balanced");
    }

    #[tokio::test]
    async fn test_with_max_concurrent_nodes() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring, clients).with_max_concurrent_nodes(2);

        assert_eq!(routing.max_concurrent_nodes, 2);
    }

    #[tokio::test]
    async fn test_partition_by_node() {
        let ring = create_test_ring();
        let clients = create_test_clients();
        let routing = RoutingObjectClient::new(ring.clone(), clients);

        let oids: Vec<Oid> = (0..10)
            .map(|i| Oid::hash(format!("oid-{}", i).as_bytes()))
            .collect();

        let by_node = routing.partition_by_node(oids.clone()).unwrap();

        let total: usize = by_node.values().map(|v| v.len()).sum();
        assert_eq!(total, 10);

        let unique_oids: HashSet<Oid> = by_node.values().flatten().copied().collect();
        assert_eq!(unique_oids.len(), 10);
    }
}
