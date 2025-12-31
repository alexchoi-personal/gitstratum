use gitstratum_core::{Blob as CoreBlob, Oid};
use gitstratum_hashring::{ConsistentHashRing, NodeId, NodeInfo};
use gitstratum_proto::object_service_client::ObjectServiceClient;
use gitstratum_proto::{
    Blob, GetBlobRequest, GetStatsRequest, HasBlobRequest, PutBlobRequest,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, instrument, warn};

use crate::error::{ObjectStoreError, Result};
use crate::store::StorageStats;

type ClientPool = HashMap<String, ObjectServiceClient<Channel>>;

pub struct ObjectClusterClient {
    ring: Arc<ConsistentHashRing>,
    clients: RwLock<ClientPool>,
}

impl ObjectClusterClient {
    pub fn new(ring: Arc<ConsistentHashRing>) -> Self {
        Self {
            ring,
            clients: RwLock::new(HashMap::new()),
        }
    }

    async fn get_client(&self, node: &NodeInfo) -> Result<ObjectServiceClient<Channel>> {
        let endpoint = node.endpoint();

        {
            let clients = self.clients.read();
            if let Some(client) = clients.get(&endpoint) {
                return Ok(client.clone());
            }
        }

        let addr = format!("http://{}", endpoint);
        let channel = Channel::from_shared(addr)
            .map_err(|e| ObjectStoreError::InvalidUri(e.to_string()))?
            .connect()
            .await?;
        let client = ObjectServiceClient::new(channel);

        {
            let mut clients = self.clients.write();
            clients.insert(endpoint, client.clone());
        }

        Ok(client)
    }

    fn proto_oid_to_core(oid: &gitstratum_proto::Oid) -> Result<Oid> {
        Oid::from_slice(&oid.bytes).map_err(|e| ObjectStoreError::InvalidOid(e.to_string()))
    }

    fn core_oid_to_proto(oid: &Oid) -> gitstratum_proto::Oid {
        gitstratum_proto::Oid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    fn proto_blob_to_core(blob: &Blob) -> Result<CoreBlob> {
        let oid = blob
            .oid
            .as_ref()
            .ok_or_else(|| ObjectStoreError::InvalidOid("missing oid".to_string()))?;
        let oid = Self::proto_oid_to_core(oid)?;
        Ok(CoreBlob::with_oid(oid, blob.data.clone()))
    }

    fn core_blob_to_proto(blob: &CoreBlob) -> Blob {
        Blob {
            oid: Some(Self::core_oid_to_proto(&blob.oid)),
            data: blob.data.to_vec(),
            compressed: false,
        }
    }

    #[instrument(skip(self), fields(oid = %oid))]
    pub async fn get(&self, oid: &Oid) -> Result<Option<CoreBlob>> {
        let nodes = self.ring.nodes_for_oid(oid)?;

        if nodes.is_empty() {
            return Err(ObjectStoreError::NoAvailableNodes);
        }

        for node in &nodes {
            debug!(node_id = %node.id, "trying node for get");

            match self.get_from_node(node, oid).await {
                Ok(Some(blob)) => return Ok(Some(blob)),
                Ok(None) => continue,
                Err(e) => {
                    warn!(node_id = %node.id, error = %e, "failed to get from node");
                    continue;
                }
            }
        }

        Ok(None)
    }

    async fn get_from_node(&self, node: &NodeInfo, oid: &Oid) -> Result<Option<CoreBlob>> {
        let mut client = self.get_client(node).await?;

        let request = GetBlobRequest {
            oid: Some(Self::core_oid_to_proto(oid)),
        };

        let response = client.get_blob(request).await?.into_inner();

        if response.found {
            let blob = response
                .blob
                .ok_or_else(|| ObjectStoreError::BlobNotFound(oid.to_string()))?;
            Ok(Some(Self::proto_blob_to_core(&blob)?))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self, blob), fields(oid = %blob.oid))]
    pub async fn put(&self, blob: &CoreBlob) -> Result<()> {
        let nodes = self.ring.nodes_for_oid(&blob.oid)?;

        if nodes.is_empty() {
            return Err(ObjectStoreError::NoAvailableNodes);
        }

        let mut success_count = 0;
        let mut last_error = None;

        for node in &nodes {
            debug!(node_id = %node.id, "putting to node");

            match self.put_to_node(node, blob).await {
                Ok(()) => success_count += 1,
                Err(e) => {
                    warn!(node_id = %node.id, error = %e, "failed to put to node");
                    last_error = Some(e);
                }
            }
        }

        if success_count == 0 {
            return Err(last_error.unwrap_or(ObjectStoreError::AllReplicasFailed));
        }

        Ok(())
    }

    async fn put_to_node(&self, node: &NodeInfo, blob: &CoreBlob) -> Result<()> {
        let mut client = self.get_client(node).await?;

        let request = PutBlobRequest {
            blob: Some(Self::core_blob_to_proto(blob)),
        };

        let response = client.put_blob(request).await?.into_inner();

        if response.success {
            Ok(())
        } else {
            Err(ObjectStoreError::Grpc(tonic::Status::internal(
                response.error,
            )))
        }
    }

    #[instrument(skip(self), fields(oid = %oid))]
    pub async fn has(&self, oid: &Oid) -> Result<bool> {
        let nodes = self.ring.nodes_for_oid(oid)?;

        if nodes.is_empty() {
            return Err(ObjectStoreError::NoAvailableNodes);
        }

        for node in &nodes {
            match self.has_on_node(node, oid).await {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(e) => {
                    warn!(node_id = %node.id, error = %e, "failed to check on node");
                    continue;
                }
            }
        }

        Ok(false)
    }

    async fn has_on_node(&self, node: &NodeInfo, oid: &Oid) -> Result<bool> {
        let mut client = self.get_client(node).await?;

        let request = HasBlobRequest {
            oid: Some(Self::core_oid_to_proto(oid)),
        };

        let response = client.has_blob(request).await?.into_inner();
        Ok(response.exists)
    }

    #[instrument(skip(self))]
    pub async fn stats(&self) -> Result<StorageStats> {
        let nodes = self.ring.get_nodes();

        if nodes.is_empty() {
            return Ok(StorageStats::default());
        }

        let mut total_blobs = 0u64;
        let mut total_bytes = 0u64;
        let mut used_bytes = 0u64;
        let mut available_bytes = 0u64;
        let mut io_total = 0.0f32;
        let mut node_count = 0u32;

        for node in &nodes {
            match self.stats_from_node(node).await {
                Ok(stats) => {
                    total_blobs += stats.total_blobs;
                    total_bytes += stats.total_bytes;
                    used_bytes += stats.used_bytes;
                    available_bytes += stats.available_bytes;
                    io_total += stats.io_utilization;
                    node_count += 1;
                }
                Err(e) => {
                    warn!(node_id = %node.id, error = %e, "failed to get stats from node");
                }
            }
        }

        let io_utilization = if node_count > 0 {
            io_total / node_count as f32
        } else {
            0.0
        };

        Ok(StorageStats {
            total_blobs,
            total_bytes,
            used_bytes,
            available_bytes,
            io_utilization,
        })
    }

    async fn stats_from_node(&self, node: &NodeInfo) -> Result<StorageStats> {
        let mut client = self.get_client(node).await?;

        let response = client.get_stats(GetStatsRequest {}).await?.into_inner();

        Ok(StorageStats {
            total_blobs: response.total_blobs,
            total_bytes: response.total_bytes,
            used_bytes: response.used_bytes,
            available_bytes: response.available_bytes,
            io_utilization: response.io_utilization,
        })
    }

    pub fn add_node(&self, node: NodeInfo) -> Result<()> {
        self.ring.add_node(node)?;
        Ok(())
    }

    pub fn remove_node(&self, node_id: &NodeId) -> Result<NodeInfo> {
        let node = self.ring.remove_node(node_id)?;
        let mut clients = self.clients.write();
        clients.remove(&node.endpoint());
        Ok(node)
    }

    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.ring.get_nodes()
    }

    pub fn node_count(&self) -> usize {
        self.ring.node_count()
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
                .replication_factor(2)
                .add_node(NodeInfo::new("node-1", "127.0.0.1", 9001))
                .add_node(NodeInfo::new("node-2", "127.0.0.1", 9002))
                .add_node(NodeInfo::new("node-3", "127.0.0.1", 9003))
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_client_creation() {
        let ring = create_test_ring();
        let client = ObjectClusterClient::new(ring);
        assert_eq!(client.node_count(), 3);
    }

    #[test]
    fn test_add_remove_node() {
        let ring = create_test_ring();
        let client = ObjectClusterClient::new(ring);

        client
            .add_node(NodeInfo::new("node-4", "127.0.0.1", 9004))
            .unwrap();
        assert_eq!(client.node_count(), 4);

        client.remove_node(&NodeId::new("node-4")).unwrap();
        assert_eq!(client.node_count(), 3);
    }

    #[test]
    fn test_nodes() {
        let ring = create_test_ring();
        let client = ObjectClusterClient::new(ring);

        let nodes = client.nodes();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn test_proto_conversion() {
        let blob = CoreBlob::new(b"test data".to_vec());
        let proto = ObjectClusterClient::core_blob_to_proto(&blob);
        let back = ObjectClusterClient::proto_blob_to_core(&proto).unwrap();

        assert_eq!(blob.oid, back.oid);
        assert_eq!(blob.data, back.data);
    }
}
