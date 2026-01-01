use gitstratum_core::{Blob as CoreBlob, Oid};
use gitstratum_hashring::{ConsistentHashRing, NodeId, NodeInfo};
use gitstratum_proto::object_service_client::ObjectServiceClient;
use gitstratum_proto::{Blob, GetBlobRequest, GetStatsRequest, HasBlobRequest, PutBlobRequest};
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

    #[allow(clippy::result_large_err)]
    fn proto_oid_to_core(oid: &gitstratum_proto::Oid) -> Result<Oid> {
        Oid::from_slice(&oid.bytes).map_err(|e| ObjectStoreError::InvalidOid(e.to_string()))
    }

    fn core_oid_to_proto(oid: &Oid) -> gitstratum_proto::Oid {
        gitstratum_proto::Oid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    #[allow(clippy::result_large_err)]
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
    use std::sync::Arc;
    use std::thread;

    fn create_ring(node_count: usize, replication_factor: usize) -> Arc<ConsistentHashRing> {
        let mut builder = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(replication_factor);

        for i in 1..=node_count {
            builder = builder.add_node(NodeInfo::new(
                format!("node-{}", i),
                "127.0.0.1",
                9000 + i as u16,
            ));
        }

        Arc::new(builder.build().unwrap())
    }

    #[test]
    fn test_client_lifecycle_and_node_management() {
        let configs = [(3, 2), (1, 1), (0, 2), (5, 5)];
        for (node_count, replication_factor) in configs {
            let ring = create_ring(node_count, replication_factor);
            let client = ObjectClusterClient::new(ring.clone());

            assert_eq!(client.node_count(), node_count);
            assert_eq!(client.nodes().len(), node_count);
            assert!(client.clients.read().is_empty());
            assert_eq!(client.node_count(), ring.node_count());
        }

        let ring = create_ring(3, 2);
        let client = ObjectClusterClient::new(ring.clone());

        for i in 4..10 {
            client
                .add_node(NodeInfo::new(format!("node-{}", i), "127.0.0.1", 9000 + i))
                .unwrap();
        }
        assert_eq!(client.node_count(), 9);
        assert_eq!(client.node_count(), client.nodes().len());

        for (id, port) in [("node-1", 9001u16), ("node-2", 9002), ("node-3", 9003)] {
            let removed = client.remove_node(&NodeId::new(id)).unwrap();
            assert_eq!(removed.id.as_str(), id);
            assert_eq!(removed.port, port);
            assert!(!client.nodes().iter().any(|n| n.id.as_str() == id));
        }
        assert_eq!(client.node_count(), 6);
        assert!(client.remove_node(&NodeId::new("nonexistent")).is_err());
        assert!(client.remove_node(&NodeId::new("node-1")).is_err());

        let ring = create_ring(0, 2);
        let client = ObjectClusterClient::new(ring);

        let addresses = [
            ("127.0.0.1", 1),
            ("127.0.0.1", 80),
            ("192.168.1.1", 443),
            ("localhost", 8080),
            ("::1", 65535),
            ("10.20.30.40", 5678),
        ];
        for (i, (addr, port)) in addresses.iter().enumerate() {
            client
                .add_node(NodeInfo::new(format!("node-{}", i), *addr, *port))
                .unwrap();
        }
        assert_eq!(client.node_count(), addresses.len());

        for node in client.nodes() {
            let endpoint = node.endpoint();
            assert!(endpoint.contains(':'));
            let last_colon = endpoint.rfind(':').unwrap();
            let port_str = &endpoint[last_colon + 1..];
            assert!(port_str.parse::<u16>().is_ok());
        }

        let ring = create_ring(3, 2);
        let client = ObjectClusterClient::new(ring.clone());
        ring.add_node(NodeInfo::new("external-node", "127.0.0.1", 8888))
            .unwrap();
        assert!(client
            .nodes()
            .iter()
            .any(|n| n.id.as_str() == "external-node"));
    }

    #[test]
    fn test_proto_conversions_comprehensive() {
        for i in 0..100 {
            let data = format!("test data {}", i);
            let oid = Oid::hash(data.as_bytes());
            let proto = ObjectClusterClient::core_oid_to_proto(&oid);
            assert_eq!(proto.bytes.len(), 32);
            assert_eq!(proto.bytes, oid.as_bytes().to_vec());
            let back = ObjectClusterClient::proto_oid_to_core(&proto).unwrap();
            assert_eq!(oid, back);
        }

        let special_oids = [
            Oid::ZERO,
            Oid::hash(b"test"),
            Oid::from_slice(&[0xAB; 32]).unwrap(),
            Oid::from_slice(&[
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
                0xEE, 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C,
                0x0D, 0x0E, 0x0F, 0x10,
            ])
            .unwrap(),
        ];
        for oid in &special_oids {
            let proto = ObjectClusterClient::core_oid_to_proto(oid);
            let back = ObjectClusterClient::proto_oid_to_core(&proto).unwrap();
            assert_eq!(back, *oid);
        }

        for len in [0, 1, 15, 16, 31, 33, 64, 128] {
            let proto = gitstratum_proto::Oid {
                bytes: vec![0xAB; len],
            };
            let result = ObjectClusterClient::proto_oid_to_core(&proto);
            assert!(result.is_err(), "Expected error for OID length {}", len);
        }
        let valid = gitstratum_proto::Oid {
            bytes: vec![0xAB; 32],
        };
        assert!(ObjectClusterClient::proto_oid_to_core(&valid).is_ok());

        let boundary_oids = [vec![0x00; 32], vec![0xFF; 32]];
        for bytes in &boundary_oids {
            let proto = gitstratum_proto::Oid {
                bytes: bytes.clone(),
            };
            assert!(ObjectClusterClient::proto_oid_to_core(&proto).is_ok());
        }

        let data_patterns: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x00; 100],
            vec![0xFF; 100],
            (0..=255).collect(),
            vec![0x00, 0xFF, 0x00, 0xFF, 0x00],
            vec![0xDE, 0xAD, 0xBE, 0xEF],
            b"hello world with special chars: \x00\xFF\n\t".to_vec(),
            "Hello, \u{4e16}\u{754c}! \u{1F600}".as_bytes().to_vec(),
        ];
        for size in [0, 1, 100, 1024, 10240, 102400, 1024 * 1024] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let blob = CoreBlob::new(data.clone());
            let proto = ObjectClusterClient::core_blob_to_proto(&blob);
            assert_eq!(proto.data, data);
            assert!(!proto.compressed);
            assert!(proto.oid.is_some());
            assert_eq!(
                proto.oid.as_ref().unwrap().bytes,
                blob.oid.as_bytes().to_vec()
            );

            let back = ObjectClusterClient::proto_blob_to_core(&proto).unwrap();
            assert_eq!(back.oid, blob.oid);
            assert_eq!(back.data.as_ref(), data.as_slice());
        }
        for data in data_patterns {
            let blob = CoreBlob::new(data.clone());
            let proto = ObjectClusterClient::core_blob_to_proto(&blob);
            let back = ObjectClusterClient::proto_blob_to_core(&proto).unwrap();
            assert_eq!(back.data.as_ref(), data.as_slice());
        }

        let blob1 = CoreBlob::new(b"data1".to_vec());
        let blob2 = CoreBlob::new(b"data2".to_vec());
        let proto1 = ObjectClusterClient::core_blob_to_proto(&blob1);
        let proto2 = ObjectClusterClient::core_blob_to_proto(&blob2);
        assert_ne!(
            proto1.oid.as_ref().unwrap().bytes,
            proto2.oid.as_ref().unwrap().bytes
        );
        assert_ne!(proto1.data, proto2.data);

        let identical1 = CoreBlob::new(b"identical data".to_vec());
        let identical2 = CoreBlob::new(b"identical data".to_vec());
        assert_eq!(identical1.oid, identical2.oid);
        let proto_i1 = ObjectClusterClient::core_blob_to_proto(&identical1);
        let proto_i2 = ObjectClusterClient::core_blob_to_proto(&identical2);
        assert_eq!(proto_i1.oid.unwrap().bytes, proto_i2.oid.unwrap().bytes);

        let missing_oid = Blob {
            oid: None,
            data: b"test".to_vec(),
            compressed: false,
        };
        match ObjectClusterClient::proto_blob_to_core(&missing_oid) {
            Err(ObjectStoreError::InvalidOid(msg)) => assert!(msg.contains("missing")),
            _ => panic!("Expected InvalidOid error"),
        }

        let invalid_oid = Blob {
            oid: Some(gitstratum_proto::Oid { bytes: vec![0; 16] }),
            data: b"test".to_vec(),
            compressed: false,
        };
        assert!(ObjectClusterClient::proto_blob_to_core(&invalid_oid).is_err());

        let empty_oid = Blob {
            oid: Some(gitstratum_proto::Oid { bytes: vec![] }),
            data: b"test".to_vec(),
            compressed: false,
        };
        assert!(ObjectClusterClient::proto_blob_to_core(&empty_oid).is_err());

        let valid_blob = Blob {
            oid: Some(gitstratum_proto::Oid {
                bytes: Oid::hash(b"test").as_bytes().to_vec(),
            }),
            data: b"test data".to_vec(),
            compressed: true,
        };
        let result = ObjectClusterClient::proto_blob_to_core(&valid_blob).unwrap();
        assert_eq!(result.data.as_ref(), b"test data");

        let oid = Oid::hash(b"test");
        let get_req = GetBlobRequest {
            oid: Some(ObjectClusterClient::core_oid_to_proto(&oid)),
        };
        assert!(get_req.oid.is_some());
        assert_eq!(get_req.oid.as_ref().unwrap().bytes.len(), 32);

        let has_req = HasBlobRequest {
            oid: Some(ObjectClusterClient::core_oid_to_proto(&oid)),
        };
        assert!(has_req.oid.is_some());
        assert_eq!(has_req.oid.as_ref().unwrap().bytes.len(), 32);

        let blob = CoreBlob::new(b"test data".to_vec());
        let put_req = PutBlobRequest {
            blob: Some(ObjectClusterClient::core_blob_to_proto(&blob)),
        };
        assert!(put_req.blob.is_some());
        let pb = put_req.blob.as_ref().unwrap();
        assert!(pb.oid.is_some());
        assert_eq!(pb.data, b"test data".to_vec());
        assert!(!pb.compressed);

        let stats_req = GetStatsRequest {};
        assert_eq!(std::mem::size_of_val(&stats_req), 0);
    }

    #[tokio::test]
    async fn test_async_operations_with_empty_and_unreachable_nodes() {
        let empty_ring = create_ring(0, 2);
        let empty_client = ObjectClusterClient::new(empty_ring);

        let oid = Oid::hash(b"test");
        match empty_client.get(&oid).await {
            Err(ObjectStoreError::HashRing(_)) => {}
            _ => panic!("Expected HashRing error for get on empty ring"),
        }

        let blob = CoreBlob::new(b"test".to_vec());
        match empty_client.put(&blob).await {
            Err(ObjectStoreError::HashRing(_)) => {}
            _ => panic!("Expected HashRing error for put on empty ring"),
        }

        match empty_client.has(&oid).await {
            Err(ObjectStoreError::HashRing(_)) => {}
            _ => panic!("Expected HashRing error for has on empty ring"),
        }

        let stats = empty_client.stats().await.unwrap();
        assert_eq!(stats.total_blobs, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.available_bytes, 0);
        assert_eq!(stats.io_utilization, 0.0);

        let ring = create_ring(3, 2);
        let client = ObjectClusterClient::new(ring);

        for i in 0..10 {
            let oid = Oid::hash(format!("test-{}", i).as_bytes());
            let result = client.get(&oid).await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());

            let result = client.has(&oid).await;
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }

        let zero_oid = Oid::ZERO;
        assert!(client.get(&zero_oid).await.unwrap().is_none());
        assert!(!client.has(&zero_oid).await.unwrap());

        for i in 0..5 {
            let blob = CoreBlob::new(format!("blob-{}", i).into_bytes());
            assert!(client.put(&blob).await.is_err());
        }

        assert!(client.put(&CoreBlob::new(vec![])).await.is_err());

        let large_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        assert!(client.put(&CoreBlob::new(large_data)).await.is_err());

        let stats = client.stats().await.unwrap();
        assert_eq!(stats.total_blobs, 0);
        assert_eq!(stats.io_utilization, 0.0);

        let oid = Oid::hash(b"sequential test");
        assert!(client.has(&oid).await.is_ok());
        assert!(client.get(&oid).await.is_ok());

        let routing_oid = Oid::hash(b"routing test");
        let nodes = client.ring.nodes_for_oid(&routing_oid);
        assert!(nodes.is_ok());
        assert!(!nodes.unwrap().is_empty());
    }

    #[test]
    fn test_error_types_and_storage_stats() {
        let error_checks = [
            (ObjectStoreError::NoAvailableNodes, "no available nodes"),
            (ObjectStoreError::AllReplicasFailed, "all replicas failed"),
            (
                ObjectStoreError::InvalidOid("test error".to_string()),
                "test error",
            ),
            (
                ObjectStoreError::InvalidUri("bad uri".to_string()),
                "bad uri",
            ),
            (
                ObjectStoreError::BlobNotFound("abc123".to_string()),
                "abc123",
            ),
            (
                ObjectStoreError::Compression("failed to compress".to_string()),
                "compression",
            ),
            (
                ObjectStoreError::Decompression("failed to decompress".to_string()),
                "decompression",
            ),
        ];
        for (err, expected_substr) in &error_checks {
            let display = format!("{}", err);
            assert!(
                display.contains(expected_substr),
                "Error '{}' should contain '{}'",
                display,
                expected_substr
            );
        }

        let insufficient = ObjectStoreError::InsufficientReplicas {
            required: 3,
            achieved: 1,
        };
        let display = format!("{}", insufficient);
        assert!(display.contains("3") && display.contains("1"));

        let stats = StorageStats::default();
        assert_eq!(stats.total_blobs, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.available_bytes, 0);
        assert_eq!(stats.io_utilization, 0.0);

        let stats = StorageStats {
            total_blobs: 100,
            total_bytes: 1024 * 1024,
            used_bytes: 512 * 1024,
            available_bytes: 512 * 1024,
            io_utilization: 0.5,
        };
        assert_eq!(stats.total_blobs, 100);
        assert_eq!(stats.total_bytes, 1024 * 1024);
        assert_eq!(stats.used_bytes, 512 * 1024);
        assert_eq!(stats.available_bytes, 512 * 1024);
        assert!((stats.io_utilization - 0.5).abs() < f32::EPSILON);

        let cloned = stats.clone();
        assert_eq!(cloned.total_blobs, stats.total_blobs);
        assert_eq!(cloned.total_bytes, stats.total_bytes);

        let debug_str = format!("{:?}", StorageStats::default());
        assert!(debug_str.contains("StorageStats"));
    }

    #[test]
    fn test_thread_safety_and_concurrent_access() {
        let ring = create_ring(3, 2);
        let client = Arc::new(ObjectClusterClient::new(ring));

        let mut handles = vec![];
        for i in 0..10 {
            let client = Arc::clone(&client);
            let handle = thread::spawn(move || {
                client
                    .add_node(NodeInfo::new(
                        format!("thread-node-{}", i),
                        "127.0.0.1",
                        10000 + i as u16,
                    ))
                    .unwrap();
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(client.node_count(), 13);

        let mut handles = vec![];
        for _ in 0..10 {
            let client = Arc::clone(&client);
            let handle = thread::spawn(move || {
                let _ = client.nodes();
                let _ = client.node_count();
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
