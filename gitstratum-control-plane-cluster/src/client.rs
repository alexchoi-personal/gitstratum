use gitstratum_hashring::NodeState as HashRingNodeState;
use gitstratum_proto::{
    control_plane_service_client::ControlPlaneServiceClient, AcquireRefLockRequest, AddNodeRequest,
    GetClusterStateRequest, GetHashRingRequest, GetRebalanceStatusRequest,
    NodeInfo as ProtoNodeInfo, NodeState as ProtoNodeState, NodeType as ProtoNodeType,
    ReleaseRefLockRequest, RemoveNodeRequest, SetNodeStateRequest, TriggerRebalanceRequest,
};
use std::time::Duration;
use tonic::transport::Channel;

use crate::error::{ControlPlaneError, Result};
use crate::membership::{ExtendedNodeInfo, NodeType};

fn hashring_node_state_to_proto(state: HashRingNodeState) -> ProtoNodeState {
    match state {
        HashRingNodeState::Active => ProtoNodeState::Active,
        HashRingNodeState::Joining => ProtoNodeState::Joining,
        HashRingNodeState::Draining => ProtoNodeState::Draining,
        HashRingNodeState::Down => ProtoNodeState::Down,
    }
}

fn proto_node_state_to_hashring(state: ProtoNodeState) -> HashRingNodeState {
    match state {
        ProtoNodeState::Active => HashRingNodeState::Active,
        ProtoNodeState::Joining => HashRingNodeState::Joining,
        ProtoNodeState::Draining => HashRingNodeState::Draining,
        ProtoNodeState::Down => HashRingNodeState::Down,
        ProtoNodeState::Unknown => HashRingNodeState::Down,
    }
}

fn internal_node_type_to_proto(node_type: NodeType) -> ProtoNodeType {
    match node_type {
        NodeType::ControlPlane => ProtoNodeType::ControlPlane,
        NodeType::Metadata => ProtoNodeType::Metadata,
        NodeType::Object => ProtoNodeType::Object,
        NodeType::Frontend => ProtoNodeType::Frontend,
    }
}

fn proto_node_type_to_internal(node_type: ProtoNodeType) -> NodeType {
    match node_type {
        ProtoNodeType::ControlPlane => NodeType::ControlPlane,
        ProtoNodeType::Metadata => NodeType::Metadata,
        ProtoNodeType::Object => NodeType::Object,
        ProtoNodeType::Frontend => NodeType::Frontend,
        ProtoNodeType::Unknown => NodeType::Frontend,
    }
}

fn extended_node_to_proto(node: &ExtendedNodeInfo) -> ProtoNodeInfo {
    ProtoNodeInfo {
        id: node.id.clone(),
        address: node.address.clone(),
        port: node.port as u32,
        state: hashring_node_state_to_proto(node.state).into(),
        r#type: internal_node_type_to_proto(node.node_type).into(),
    }
}

fn proto_node_to_extended(node: &ProtoNodeInfo) -> ExtendedNodeInfo {
    ExtendedNodeInfo {
        id: node.id.clone(),
        address: node.address.clone(),
        port: node.port as u16,
        state: proto_node_state_to_hashring(
            ProtoNodeState::try_from(node.state).unwrap_or(ProtoNodeState::Unknown),
        ),
        node_type: proto_node_type_to_internal(
            ProtoNodeType::try_from(node.r#type).unwrap_or(ProtoNodeType::Unknown),
        ),
    }
}

#[derive(Debug, Clone)]
pub struct ClusterStateResponse {
    pub control_plane_nodes: Vec<ExtendedNodeInfo>,
    pub metadata_nodes: Vec<ExtendedNodeInfo>,
    pub object_nodes: Vec<ExtendedNodeInfo>,
    pub frontend_nodes: Vec<ExtendedNodeInfo>,
    pub leader_id: String,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct AcquireLockResult {
    pub acquired: bool,
    pub lock_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RebalanceStatus {
    pub in_progress: bool,
    pub progress_percent: f32,
    pub bytes_moved: u64,
    pub bytes_remaining: u64,
}

#[derive(Clone)]
pub struct ControlPlaneClient {
    inner: ControlPlaneServiceClient<Channel>,
}

impl ControlPlaneClient {
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let addr = addr.into();
        let channel = Channel::from_shared(addr)
            .map_err(|e| ControlPlaneError::Internal(e.to_string()))?
            .connect()
            .await
            .map_err(|e| ControlPlaneError::Internal(e.to_string()))?;

        Ok(Self {
            inner: ControlPlaneServiceClient::new(channel),
        })
    }

    pub async fn get_cluster_state(&mut self) -> Result<ClusterStateResponse> {
        let response = self
            .inner
            .get_cluster_state(GetClusterStateRequest {})
            .await?
            .into_inner();

        Ok(ClusterStateResponse {
            control_plane_nodes: response
                .control_plane_nodes
                .iter()
                .map(proto_node_to_extended)
                .collect(),
            metadata_nodes: response
                .metadata_nodes
                .iter()
                .map(proto_node_to_extended)
                .collect(),
            object_nodes: response
                .object_nodes
                .iter()
                .map(proto_node_to_extended)
                .collect(),
            frontend_nodes: response
                .frontend_nodes
                .iter()
                .map(proto_node_to_extended)
                .collect(),
            leader_id: response.leader_id,
            version: response.version,
        })
    }

    pub async fn add_node(&mut self, node: ExtendedNodeInfo) -> Result<()> {
        let proto_node = extended_node_to_proto(&node);

        let response = self
            .inner
            .add_node(AddNodeRequest {
                node: Some(proto_node),
            })
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(ControlPlaneError::Internal(response.error))
        }
    }

    pub async fn remove_node(&mut self, node_id: impl Into<String>) -> Result<()> {
        let response = self
            .inner
            .remove_node(RemoveNodeRequest {
                node_id: node_id.into(),
            })
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(ControlPlaneError::Internal(response.error))
        }
    }

    pub async fn set_node_state(
        &mut self,
        node_id: impl Into<String>,
        state: HashRingNodeState,
    ) -> Result<()> {
        let proto_state = hashring_node_state_to_proto(state);

        let response = self
            .inner
            .set_node_state(SetNodeStateRequest {
                node_id: node_id.into(),
                state: proto_state.into(),
            })
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(ControlPlaneError::Internal(response.error))
        }
    }

    pub async fn acquire_ref_lock(
        &mut self,
        repo_id: impl Into<String>,
        ref_name: impl Into<String>,
        holder_id: impl Into<String>,
        timeout: Duration,
    ) -> Result<AcquireLockResult> {
        let response = self
            .inner
            .acquire_ref_lock(AcquireRefLockRequest {
                repo_id: repo_id.into(),
                ref_name: ref_name.into(),
                holder_id: holder_id.into(),
                timeout_ms: timeout.as_millis() as u64,
            })
            .await?
            .into_inner();

        Ok(AcquireLockResult {
            acquired: response.acquired,
            lock_id: if response.lock_id.is_empty() {
                None
            } else {
                Some(response.lock_id)
            },
            error: if response.error.is_empty() {
                None
            } else {
                Some(response.error)
            },
        })
    }

    pub async fn release_ref_lock(&mut self, lock_id: impl Into<String>) -> Result<bool> {
        let response = self
            .inner
            .release_ref_lock(ReleaseRefLockRequest {
                lock_id: lock_id.into(),
            })
            .await?
            .into_inner();

        Ok(response.success)
    }

    pub async fn get_hash_ring(&mut self) -> Result<(Vec<(u64, String)>, u32, u64)> {
        let response = self
            .inner
            .get_hash_ring(GetHashRingRequest {})
            .await?
            .into_inner();

        let entries: Vec<(u64, String)> = response
            .entries
            .into_iter()
            .map(|e| (e.position, e.node_id))
            .collect();

        Ok((entries, response.replication_factor, response.version))
    }

    pub async fn trigger_rebalance(&mut self, reason: impl Into<String>) -> Result<Option<String>> {
        let response = self
            .inner
            .trigger_rebalance(TriggerRebalanceRequest {
                reason: reason.into(),
            })
            .await?
            .into_inner();

        if response.started {
            Ok(Some(response.rebalance_id))
        } else {
            Err(ControlPlaneError::Internal(response.error))
        }
    }

    pub async fn get_rebalance_status(
        &mut self,
        rebalance_id: impl Into<String>,
    ) -> Result<RebalanceStatus> {
        let response = self
            .inner
            .get_rebalance_status(GetRebalanceStatusRequest {
                rebalance_id: rebalance_id.into(),
            })
            .await?
            .into_inner();

        if !response.error.is_empty() {
            return Err(ControlPlaneError::Internal(response.error));
        }

        Ok(RebalanceStatus {
            in_progress: response.in_progress,
            progress_percent: response.progress_percent,
            bytes_moved: response.bytes_moved,
            bytes_remaining: response.bytes_remaining,
        })
    }
}

pub struct ControlPlaneClientPool {
    endpoints: Vec<String>,
    current_leader: parking_lot::RwLock<Option<String>>,
}

impl ControlPlaneClientPool {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            current_leader: parking_lot::RwLock::new(None),
        }
    }

    pub async fn get_client(&self) -> Result<ControlPlaneClient> {
        if let Some(leader) = self.current_leader.read().clone() {
            if let Ok(client) = ControlPlaneClient::connect(&leader).await {
                return Ok(client);
            }
        }

        for endpoint in &self.endpoints {
            if let Ok(client) = ControlPlaneClient::connect(endpoint).await {
                return Ok(client);
            }
        }

        Err(ControlPlaneError::Internal(
            "failed to connect to any control plane node".to_string(),
        ))
    }

    pub fn set_leader(&self, leader: String) {
        *self.current_leader.write() = Some(leader);
    }

    pub fn clear_leader(&self) {
        *self.current_leader.write() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_hashring::NodeState as HashRingNodeState;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_proto_conversion_roundtrips_and_edge_cases() {
        let hashring_states = [
            HashRingNodeState::Active,
            HashRingNodeState::Joining,
            HashRingNodeState::Draining,
            HashRingNodeState::Down,
        ];
        for state in hashring_states {
            let proto = hashring_node_state_to_proto(state);
            let back = proto_node_state_to_hashring(proto);
            assert_eq!(back, state);
        }

        let proto_states = [
            (ProtoNodeState::Unknown, HashRingNodeState::Down),
            (ProtoNodeState::Active, HashRingNodeState::Active),
            (ProtoNodeState::Joining, HashRingNodeState::Joining),
            (ProtoNodeState::Draining, HashRingNodeState::Draining),
            (ProtoNodeState::Down, HashRingNodeState::Down),
        ];
        for (proto, expected) in proto_states {
            assert_eq!(proto_node_state_to_hashring(proto), expected);
        }

        let internal_types = [
            NodeType::ControlPlane,
            NodeType::Metadata,
            NodeType::Object,
            NodeType::Frontend,
        ];
        for node_type in internal_types {
            let proto = internal_node_type_to_proto(node_type);
            let back = proto_node_type_to_internal(proto);
            assert_eq!(back, node_type);
        }

        let proto_types = [
            (ProtoNodeType::Unknown, NodeType::Frontend),
            (ProtoNodeType::ControlPlane, NodeType::ControlPlane),
            (ProtoNodeType::Metadata, NodeType::Metadata),
            (ProtoNodeType::Object, NodeType::Object),
            (ProtoNodeType::Frontend, NodeType::Frontend),
        ];
        for (proto, expected) in proto_types {
            assert_eq!(proto_node_type_to_internal(proto), expected);
        }
    }

    #[test]
    fn test_extended_node_roundtrip_all_state_type_combinations() {
        let states = [
            HashRingNodeState::Active,
            HashRingNodeState::Joining,
            HashRingNodeState::Draining,
            HashRingNodeState::Down,
        ];
        let types = [
            NodeType::ControlPlane,
            NodeType::Metadata,
            NodeType::Object,
            NodeType::Frontend,
        ];

        for state in states {
            for node_type in types {
                let original = ExtendedNodeInfo {
                    id: format!("node-{:?}-{:?}", state, node_type),
                    address: "192.168.1.1".to_string(),
                    port: 8080,
                    state,
                    node_type,
                };

                let proto = extended_node_to_proto(&original);
                let back = proto_node_to_extended(&proto);

                assert_eq!(original.id, back.id);
                assert_eq!(original.address, back.address);
                assert_eq!(original.port, back.port);
                assert_eq!(original.state, back.state);
                assert_eq!(original.node_type, back.node_type);
            }
        }

        let edge_case_protos = [
            ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: 999,
                r#type: ProtoNodeType::Object.into(),
            },
            ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: ProtoNodeState::Active.into(),
                r#type: 999,
            },
            ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: i32::MAX,
                r#type: ProtoNodeType::Object.into(),
            },
            ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: -1,
                r#type: ProtoNodeType::Object.into(),
            },
            ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: ProtoNodeState::Active.into(),
                r#type: i32::MAX,
            },
            ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: ProtoNodeState::Active.into(),
                r#type: -1,
            },
        ];

        for proto in edge_case_protos {
            let node = proto_node_to_extended(&proto);
            assert!(!node.id.is_empty() || proto.id.is_empty());
        }

        let port_tests = [0u16, 1, 8080, 65535];
        for port in port_tests {
            let node = ExtendedNodeInfo::new("node-1", "10.0.0.1", port, NodeType::Object);
            let proto = extended_node_to_proto(&node);
            assert_eq!(proto.port, port as u32);
            let back = proto_node_to_extended(&proto);
            assert_eq!(back.port, port);
        }

        let proto_large_port = ProtoNodeInfo {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: 70000,
            state: ProtoNodeState::Active.into(),
            r#type: ProtoNodeType::Object.into(),
        };
        let node = proto_node_to_extended(&proto_large_port);
        assert_eq!(node.port, 70000u32 as u16);

        let string_tests = [
            ("", ""),
            ("node-with-special-chars-@#$%", "10.0.0.1"),
            ("node-1", "::1"),
            ("node-1", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
            ("\u{1F600}-node", "10.0.0.1"),
            ("node-1", "hostname-\u{00E9}"),
        ];
        for (id, address) in string_tests {
            let node = ExtendedNodeInfo {
                id: id.to_string(),
                address: address.to_string(),
                port: 9001,
                state: HashRingNodeState::Active,
                node_type: NodeType::Object,
            };
            let proto = extended_node_to_proto(&node);
            let back = proto_node_to_extended(&proto);
            assert_eq!(back.id, id);
            assert_eq!(back.address, address);
        }
    }

    #[test]
    fn test_response_structs_lifecycle() {
        let empty_response = ClusterStateResponse {
            control_plane_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            frontend_nodes: vec![],
            leader_id: String::new(),
            version: 0,
        };
        assert!(empty_response.control_plane_nodes.is_empty());
        assert!(empty_response.metadata_nodes.is_empty());
        assert!(empty_response.object_nodes.is_empty());
        assert!(empty_response.frontend_nodes.is_empty());
        assert!(empty_response.leader_id.is_empty());
        assert_eq!(empty_response.version, 0);

        let cloned = empty_response.clone();
        assert_eq!(cloned.version, empty_response.version);

        let debug_str = format!("{:?}", empty_response);
        assert!(debug_str.contains("ClusterStateResponse"));

        let mut cp_nodes = vec![];
        let mut md_nodes = vec![];
        let mut obj_nodes = vec![];
        let mut fe_nodes = vec![];

        for i in 0..10 {
            cp_nodes.push(ExtendedNodeInfo::new(
                format!("cp-{}", i),
                format!("10.0.0.{}", i),
                9001 + i,
                NodeType::ControlPlane,
            ));
            md_nodes.push(ExtendedNodeInfo::new(
                format!("md-{}", i),
                format!("10.0.1.{}", i),
                9001 + i,
                NodeType::Metadata,
            ));
            obj_nodes.push(ExtendedNodeInfo::new(
                format!("obj-{}", i),
                format!("10.0.2.{}", i),
                9001 + i,
                NodeType::Object,
            ));
            fe_nodes.push(ExtendedNodeInfo::new(
                format!("fe-{}", i),
                format!("10.0.3.{}", i),
                9001 + i,
                NodeType::Frontend,
            ));
        }

        let full_response = ClusterStateResponse {
            control_plane_nodes: cp_nodes,
            metadata_nodes: md_nodes,
            object_nodes: obj_nodes,
            frontend_nodes: fe_nodes,
            leader_id: "cp-0".to_string(),
            version: u64::MAX,
        };
        assert_eq!(full_response.control_plane_nodes.len(), 10);
        assert_eq!(full_response.metadata_nodes.len(), 10);
        assert_eq!(full_response.object_nodes.len(), 10);
        assert_eq!(full_response.frontend_nodes.len(), 10);
        assert_eq!(full_response.version, u64::MAX);

        let cloned_full = full_response.clone();
        assert_eq!(cloned_full.control_plane_nodes.len(), 10);

        let debug_full = format!("{:?}", full_response);
        assert!(debug_full.contains("cp-0"));

        let mut nodes_with_states = vec![];
        for (i, state) in [
            HashRingNodeState::Active,
            HashRingNodeState::Joining,
            HashRingNodeState::Draining,
            HashRingNodeState::Down,
        ]
        .iter()
        .enumerate()
        {
            let mut node = ExtendedNodeInfo::new(
                format!("node-{}", i),
                format!("10.0.0.{}", i),
                9001 + i as u16,
                NodeType::Object,
            );
            node.state = *state;
            nodes_with_states.push(node);
        }
        let mixed_response = ClusterStateResponse {
            control_plane_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: nodes_with_states,
            frontend_nodes: vec![],
            leader_id: "leader".to_string(),
            version: 1,
        };
        assert_eq!(mixed_response.object_nodes.len(), 4);
        assert_eq!(
            mixed_response.object_nodes[0].state,
            HashRingNodeState::Active
        );
        assert_eq!(
            mixed_response.object_nodes[1].state,
            HashRingNodeState::Joining
        );
        assert_eq!(
            mixed_response.object_nodes[2].state,
            HashRingNodeState::Draining
        );
        assert_eq!(
            mixed_response.object_nodes[3].state,
            HashRingNodeState::Down
        );

        let lock_combinations = [
            (true, Some("lock-1".to_string()), None),
            (false, None, Some("error".to_string())),
            (false, None, None),
            (true, None, None),
            (
                false,
                Some("lock-2".to_string()),
                Some("unexpected".to_string()),
            ),
            (true, Some("a".repeat(1000)), None),
            (
                false,
                None,
                Some("Error: \u{1F4A5} something went wrong".to_string()),
            ),
            (false, Some(String::new()), Some(String::new())),
        ];

        for (acquired, lock_id, error) in lock_combinations {
            let result = AcquireLockResult {
                acquired,
                lock_id: lock_id.clone(),
                error: error.clone(),
            };
            assert_eq!(result.acquired, acquired);
            assert_eq!(result.lock_id, lock_id);
            assert_eq!(result.error, error);

            let cloned = result.clone();
            assert_eq!(cloned.acquired, result.acquired);
            assert_eq!(cloned.lock_id, result.lock_id);
            assert_eq!(cloned.error, result.error);

            let debug = format!("{:?}", result);
            assert!(debug.contains("AcquireLockResult"));
        }

        let rebalance_statuses = [
            (false, 0.0, 0u64, 0u64),
            (true, 45.5, 1024 * 1024 * 100, 1024 * 1024 * 200),
            (false, 100.0, 1024 * 1024 * 500, 0),
            (true, 99.99, u64::MAX - 1, u64::MAX),
            (true, 33.333, 1000, 2000),
            (false, -1.0, 0, 0),
            (false, 150.0, 1500, 0),
        ];

        for (in_progress, progress_percent, bytes_moved, bytes_remaining) in rebalance_statuses {
            let status = RebalanceStatus {
                in_progress,
                progress_percent,
                bytes_moved,
                bytes_remaining,
            };
            assert_eq!(status.in_progress, in_progress);
            assert_eq!(status.progress_percent, progress_percent);
            assert_eq!(status.bytes_moved, bytes_moved);
            assert_eq!(status.bytes_remaining, bytes_remaining);

            let cloned = status.clone();
            assert_eq!(cloned.in_progress, status.in_progress);
            assert_eq!(cloned.progress_percent, status.progress_percent);

            let debug = format!("{:?}", status);
            assert!(debug.contains("RebalanceStatus"));
        }

        let nan_status = RebalanceStatus {
            in_progress: true,
            progress_percent: f32::NAN,
            bytes_moved: 0,
            bytes_remaining: 0,
        };
        assert!(nan_status.progress_percent.is_nan());

        let inf_status = RebalanceStatus {
            in_progress: true,
            progress_percent: f32::INFINITY,
            bytes_moved: 0,
            bytes_remaining: 0,
        };
        assert!(inf_status.progress_percent.is_infinite());
    }

    #[test]
    fn test_client_pool_leader_management() {
        let endpoints = vec![
            "http://10.0.0.1:9001".to_string(),
            "http://10.0.0.2:9001".to_string(),
            "http://10.0.0.3:9001".to_string(),
        ];
        let pool = ControlPlaneClientPool::new(endpoints.clone());

        assert!(pool.current_leader.read().is_none());
        assert_eq!(pool.endpoints.len(), 3);
        assert_eq!(pool.endpoints[0], "http://10.0.0.1:9001");
        assert_eq!(pool.endpoints[1], "http://10.0.0.2:9001");
        assert_eq!(pool.endpoints[2], "http://10.0.0.3:9001");

        pool.set_leader("http://10.0.0.1:9001".to_string());
        assert_eq!(
            *pool.current_leader.read(),
            Some("http://10.0.0.1:9001".to_string())
        );

        pool.set_leader("http://10.0.0.2:9001".to_string());
        assert_eq!(
            *pool.current_leader.read(),
            Some("http://10.0.0.2:9001".to_string())
        );

        pool.clear_leader();
        assert!(pool.current_leader.read().is_none());

        pool.set_leader("http://10.0.0.3:9001".to_string());
        assert!(pool.current_leader.read().is_some());
        pool.clear_leader();
        assert!(pool.current_leader.read().is_none());

        for i in 0..100 {
            pool.set_leader(format!("http://10.0.0.{}:9001", i));
            assert_eq!(
                *pool.current_leader.read(),
                Some(format!("http://10.0.0.{}:9001", i))
            );
        }

        let empty_pool = ControlPlaneClientPool::new(vec![]);
        assert!(empty_pool.current_leader.read().is_none());

        let single_pool = ControlPlaneClientPool::new(vec!["http://localhost:9001".to_string()]);
        assert_eq!(single_pool.endpoints.len(), 1);
        assert!(single_pool.current_leader.read().is_none());

        let edge_pool = ControlPlaneClientPool::new(vec![]);
        edge_pool.set_leader(String::new());
        assert_eq!(edge_pool.current_leader.read().clone(), Some(String::new()));

        edge_pool.set_leader("http://host with spaces:9001".to_string());
        assert_eq!(
            edge_pool.current_leader.read().clone(),
            Some("http://host with spaces:9001".to_string())
        );

        let concurrent_pool = Arc::new(ControlPlaneClientPool::new(vec![]));
        let mut handles = vec![];

        for i in 0..10 {
            let pool_clone = Arc::clone(&concurrent_pool);
            let handle = thread::spawn(move || {
                pool_clone.set_leader(format!("http://10.0.0.{}:9001", i));
                let leader = pool_clone.current_leader.read().clone();
                assert!(leader.is_some());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[tokio::test]
    async fn test_client_connection_error_scenarios() {
        let invalid_uris = ["not-a-valid-uri", "", "://invalid"];
        for uri in invalid_uris {
            let result = ControlPlaneClient::connect(uri).await;
            assert!(result.is_err());
        }

        let unreachable_uris = [
            "http://127.0.0.1:59999",
            "http://127.0.0.1:59996",
            "https://127.0.0.1:59997",
        ];
        for uri in unreachable_uris {
            let result = ControlPlaneClient::connect(uri).await;
            assert!(result.is_err());
        }

        let empty_pool = ControlPlaneClientPool::new(vec![]);
        let result = empty_pool.get_client().await;
        assert!(result.is_err());
        match result {
            Err(ControlPlaneError::Internal(msg)) => {
                assert!(msg.contains("failed to connect"));
            }
            _ => panic!("Expected Internal error"),
        }

        let unreachable_pool = ControlPlaneClientPool::new(vec![
            "http://127.0.0.1:59991".to_string(),
            "http://127.0.0.1:59992".to_string(),
        ]);
        let result = unreachable_pool.get_client().await;
        assert!(result.is_err());

        let pool_with_bad_leader =
            ControlPlaneClientPool::new(vec!["http://127.0.0.1:59993".to_string()]);
        pool_with_bad_leader.set_leader("http://127.0.0.1:59994".to_string());
        let result = pool_with_bad_leader.get_client().await;
        assert!(result.is_err());

        let pool_invalid_leader =
            ControlPlaneClientPool::new(vec!["http://127.0.0.1:59995".to_string()]);
        pool_invalid_leader.set_leader("invalid-uri".to_string());
        let result = pool_invalid_leader.get_client().await;
        assert!(result.is_err());

        let pool_leader_fail = ControlPlaneClientPool::new(vec![
            "http://127.0.0.1:60001".to_string(),
            "http://127.0.0.1:60002".to_string(),
        ]);
        pool_leader_fail.set_leader("http://127.0.0.1:60000".to_string());
        let result = pool_leader_fail.get_client().await;
        assert!(result.is_err());
    }
}
