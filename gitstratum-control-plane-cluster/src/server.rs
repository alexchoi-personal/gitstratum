use gitstratum_hashring::NodeState as HashRingNodeState;
use gitstratum_proto::{
    control_plane_service_server::ControlPlaneService, AcquireRefLockRequest,
    AcquireRefLockResponse, AddNodeRequest, AddNodeResponse, GetClusterStateRequest,
    GetClusterStateResponse, GetHashRingRequest, GetHashRingResponse, GetRebalanceStatusRequest,
    GetRebalanceStatusResponse, HashRingEntry, NodeInfo as ProtoNodeInfo,
    NodeState as ProtoNodeState, NodeType as ProtoNodeType, ReleaseRefLockRequest,
    ReleaseRefLockResponse, RemoveNodeRequest, RemoveNodeResponse, SetNodeStateRequest,
    SetNodeStateResponse, TriggerRebalanceRequest, TriggerRebalanceResponse,
};
use std::time::Duration;
use tonic::{Request, Response, Status};

use crate::error::ControlPlaneError;
use crate::membership::{ClusterStateSnapshot, ExtendedNodeInfo, LockInfo, NodeType, RefLockKey};
use crate::raft::{ControlPlaneRaft, NodeId, Request as RaftRequest, StateMachineStore};

fn proto_node_state_to_hashring(state: ProtoNodeState) -> HashRingNodeState {
    match state {
        ProtoNodeState::Active => HashRingNodeState::Active,
        ProtoNodeState::Joining => HashRingNodeState::Joining,
        ProtoNodeState::Draining => HashRingNodeState::Draining,
        ProtoNodeState::Down => HashRingNodeState::Down,
        ProtoNodeState::Unknown => HashRingNodeState::Down,
    }
}

fn hashring_node_state_to_proto(state: HashRingNodeState) -> ProtoNodeState {
    match state {
        HashRingNodeState::Active => ProtoNodeState::Active,
        HashRingNodeState::Joining => ProtoNodeState::Joining,
        HashRingNodeState::Draining => ProtoNodeState::Draining,
        HashRingNodeState::Down => ProtoNodeState::Down,
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

fn internal_node_type_to_proto(node_type: NodeType) -> ProtoNodeType {
    match node_type {
        NodeType::ControlPlane => ProtoNodeType::ControlPlane,
        NodeType::Metadata => ProtoNodeType::Metadata,
        NodeType::Object => ProtoNodeType::Object,
        NodeType::Frontend => ProtoNodeType::Frontend,
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

pub struct ControlPlaneServer {
    raft: ControlPlaneRaft,
    state_machine: StateMachineStore,
    node_id: NodeId,
}

impl ControlPlaneServer {
    pub fn new(raft: ControlPlaneRaft, state_machine: StateMachineStore, node_id: NodeId) -> Self {
        Self {
            raft,
            state_machine,
            node_id,
        }
    }

    async fn get_state(&self) -> ClusterStateSnapshot {
        let storage = self.state_machine.storage().await;
        storage.get_state()
    }

    async fn ensure_leader(&self) -> Result<(), ControlPlaneError> {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(self.node_id) {
            let leader_id = metrics.current_leader.map(|id| id.to_string());
            return Err(ControlPlaneError::NotLeader(leader_id));
        }
        Ok(())
    }

    async fn forward_to_leader<T>(
        &self,
        request: RaftRequest,
    ) -> Result<crate::raft::Response, ControlPlaneError> {
        let result = self.raft.client_write(request).await;
        match result {
            Ok(resp) => Ok(resp.data),
            Err(e) => match e {
                openraft::error::RaftError::APIError(api_err) => match api_err {
                    openraft::error::ClientWriteError::ForwardToLeader(forward) => {
                        let leader_id = forward.leader_id.map(|id| id.to_string());
                        Err(ControlPlaneError::NotLeader(leader_id))
                    }
                    openraft::error::ClientWriteError::ChangeMembershipError(_) => Err(
                        ControlPlaneError::Raft("membership change error".to_string()),
                    ),
                },
                openraft::error::RaftError::Fatal(fatal) => {
                    Err(ControlPlaneError::Raft(fatal.to_string()))
                }
            },
        }
    }
}

#[tonic::async_trait]
impl ControlPlaneService for ControlPlaneServer {
    async fn get_cluster_state(
        &self,
        _request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        let state = self.get_state().await;

        let control_plane_nodes: Vec<ProtoNodeInfo> = state
            .control_plane_nodes
            .values()
            .map(extended_node_to_proto)
            .collect();

        let metadata_nodes: Vec<ProtoNodeInfo> = state
            .metadata_nodes
            .values()
            .map(extended_node_to_proto)
            .collect();

        let object_nodes: Vec<ProtoNodeInfo> = state
            .object_nodes
            .values()
            .map(extended_node_to_proto)
            .collect();

        let frontend_nodes: Vec<ProtoNodeInfo> = state
            .frontend_nodes
            .values()
            .map(extended_node_to_proto)
            .collect();

        let metrics = self.raft.metrics().borrow().clone();
        let leader_id = metrics
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_default();

        Ok(Response::new(GetClusterStateResponse {
            control_plane_nodes,
            metadata_nodes,
            object_nodes,
            frontend_nodes,
            leader_id,
            version: state.version,
        }))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let node = request
            .into_inner()
            .node
            .ok_or_else(|| Status::invalid_argument("node is required"))?;

        let extended_node = proto_node_to_extended(&node);

        let raft_request = RaftRequest::AddNode {
            node: extended_node,
        };

        match self.forward_to_leader::<()>(raft_request).await {
            Ok(_) => Ok(Response::new(AddNodeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(AddNodeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id;

        let state = self.get_state().await;
        let node_type = state
            .find_node_type(&node_id)
            .ok_or_else(|| Status::not_found("node not found"))?;

        let raft_request = RaftRequest::RemoveNode { node_id, node_type };

        match self.forward_to_leader::<()>(raft_request).await {
            Ok(_) => Ok(Response::new(RemoveNodeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(RemoveNodeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn set_node_state(
        &self,
        request: Request<SetNodeStateRequest>,
    ) -> Result<Response<SetNodeStateResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id;
        let proto_state = ProtoNodeState::try_from(req.state).unwrap_or(ProtoNodeState::Unknown);
        let state = proto_node_state_to_hashring(proto_state);

        let current_state = self.get_state().await;
        let node_type = current_state
            .find_node_type(&node_id)
            .ok_or_else(|| Status::not_found("node not found"))?;

        let raft_request = RaftRequest::SetNodeState {
            node_id,
            node_type,
            state,
        };

        match self.forward_to_leader::<()>(raft_request).await {
            Ok(_) => Ok(Response::new(SetNodeStateResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SetNodeStateResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn acquire_ref_lock(
        &self,
        request: Request<AcquireRefLockRequest>,
    ) -> Result<Response<AcquireRefLockResponse>, Status> {
        let req = request.into_inner();

        let lock_id = uuid::Uuid::new_v4().to_string();
        let key = RefLockKey::new(&req.repo_id, &req.ref_name);
        let lock = LockInfo::new(
            &lock_id,
            &req.repo_id,
            &req.ref_name,
            &req.holder_id,
            Duration::from_millis(req.timeout_ms),
        );

        let raft_request = RaftRequest::AcquireLock { key, lock };

        match self.forward_to_leader::<()>(raft_request).await {
            Ok(response) => match response {
                crate::raft::Response::LockAcquired { lock_id } => {
                    Ok(Response::new(AcquireRefLockResponse {
                        acquired: true,
                        lock_id,
                        error: String::new(),
                    }))
                }
                crate::raft::Response::LockNotAcquired { reason } => {
                    Ok(Response::new(AcquireRefLockResponse {
                        acquired: false,
                        lock_id: String::new(),
                        error: reason,
                    }))
                }
                _ => Ok(Response::new(AcquireRefLockResponse {
                    acquired: false,
                    lock_id: String::new(),
                    error: "unexpected response".to_string(),
                })),
            },
            Err(e) => Ok(Response::new(AcquireRefLockResponse {
                acquired: false,
                lock_id: String::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn release_ref_lock(
        &self,
        request: Request<ReleaseRefLockRequest>,
    ) -> Result<Response<ReleaseRefLockResponse>, Status> {
        let req = request.into_inner();

        let raft_request = RaftRequest::ReleaseLock {
            lock_id: req.lock_id,
        };

        match self.forward_to_leader::<()>(raft_request).await {
            Ok(response) => match response {
                crate::raft::Response::LockReleased { found } => {
                    if found {
                        Ok(Response::new(ReleaseRefLockResponse {
                            success: true,
                            error: String::new(),
                        }))
                    } else {
                        Ok(Response::new(ReleaseRefLockResponse {
                            success: false,
                            error: "lock not found".to_string(),
                        }))
                    }
                }
                _ => Ok(Response::new(ReleaseRefLockResponse {
                    success: false,
                    error: "unexpected response".to_string(),
                })),
            },
            Err(e) => Ok(Response::new(ReleaseRefLockResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_hash_ring(
        &self,
        _request: Request<GetHashRingRequest>,
    ) -> Result<Response<GetHashRingResponse>, Status> {
        let state = self.get_state().await;

        let mut entries = Vec::new();

        for node in state.object_nodes.values() {
            entries.push(HashRingEntry {
                position: 0,
                node_id: node.id.clone(),
            });
        }

        Ok(Response::new(GetHashRingResponse {
            entries,
            replication_factor: 3,
            version: state.version,
        }))
    }

    async fn trigger_rebalance(
        &self,
        _request: Request<TriggerRebalanceRequest>,
    ) -> Result<Response<TriggerRebalanceResponse>, Status> {
        if let Err(e) = self.ensure_leader().await {
            return Ok(Response::new(TriggerRebalanceResponse {
                started: false,
                rebalance_id: String::new(),
                error: e.to_string(),
            }));
        }

        let rebalance_id = uuid::Uuid::new_v4().to_string();

        Ok(Response::new(TriggerRebalanceResponse {
            started: true,
            rebalance_id,
            error: String::new(),
        }))
    }

    async fn get_rebalance_status(
        &self,
        request: Request<GetRebalanceStatusRequest>,
    ) -> Result<Response<GetRebalanceStatusResponse>, Status> {
        let _req = request.into_inner();

        Ok(Response::new(GetRebalanceStatusResponse {
            in_progress: false,
            progress_percent: 100.0,
            bytes_moved: 0,
            bytes_remaining: 0,
            error: String::new(),
        }))
    }
}

pub async fn start_server(
    addr: std::net::SocketAddr,
    raft: ControlPlaneRaft,
    state_machine: StateMachineStore,
    node_id: NodeId,
) -> Result<(), ControlPlaneError> {
    let server = ControlPlaneServer::new(raft, state_machine, node_id);

    tonic::transport::Server::builder()
        .add_service(
            gitstratum_proto::control_plane_service_server::ControlPlaneServiceServer::new(server),
        )
        .serve(addr)
        .await
        .map_err(|e| ControlPlaneError::Internal(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_hashring::NodeState as HashRingNodeState;
    use gitstratum_proto::{
        NodeInfo as ProtoNodeInfo, NodeState as ProtoNodeState, NodeType as ProtoNodeType,
    };
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_state_and_type_conversions() {
        let proto_to_hashring_states = [
            (ProtoNodeState::Active, HashRingNodeState::Active),
            (ProtoNodeState::Joining, HashRingNodeState::Joining),
            (ProtoNodeState::Draining, HashRingNodeState::Draining),
            (ProtoNodeState::Down, HashRingNodeState::Down),
            (ProtoNodeState::Unknown, HashRingNodeState::Down),
        ];
        for (proto, expected) in proto_to_hashring_states {
            assert_eq!(proto_node_state_to_hashring(proto), expected);
        }

        let hashring_to_proto_states = [
            (HashRingNodeState::Active, ProtoNodeState::Active),
            (HashRingNodeState::Joining, ProtoNodeState::Joining),
            (HashRingNodeState::Draining, ProtoNodeState::Draining),
            (HashRingNodeState::Down, ProtoNodeState::Down),
        ];
        for (hashring, expected) in hashring_to_proto_states {
            assert_eq!(hashring_node_state_to_proto(hashring), expected);
        }

        for state in [
            HashRingNodeState::Active,
            HashRingNodeState::Joining,
            HashRingNodeState::Draining,
            HashRingNodeState::Down,
        ] {
            let proto = hashring_node_state_to_proto(state);
            assert_eq!(proto_node_state_to_hashring(proto), state);
        }

        let proto_to_internal_types = [
            (ProtoNodeType::ControlPlane, NodeType::ControlPlane),
            (ProtoNodeType::Metadata, NodeType::Metadata),
            (ProtoNodeType::Object, NodeType::Object),
            (ProtoNodeType::Frontend, NodeType::Frontend),
            (ProtoNodeType::Unknown, NodeType::Frontend),
        ];
        for (proto, expected) in proto_to_internal_types {
            assert_eq!(proto_node_type_to_internal(proto), expected);
        }

        let internal_to_proto_types = [
            (NodeType::ControlPlane, ProtoNodeType::ControlPlane),
            (NodeType::Metadata, ProtoNodeType::Metadata),
            (NodeType::Object, ProtoNodeType::Object),
            (NodeType::Frontend, ProtoNodeType::Frontend),
        ];
        for (internal, expected) in internal_to_proto_types {
            assert_eq!(internal_node_type_to_proto(internal), expected);
        }

        for node_type in [
            NodeType::ControlPlane,
            NodeType::Metadata,
            NodeType::Object,
            NodeType::Frontend,
        ] {
            let proto = internal_node_type_to_proto(node_type);
            assert_eq!(proto_node_type_to_internal(proto), node_type);
        }

        assert_eq!(i32::from(ProtoNodeState::Unknown), 0);
        assert_eq!(i32::from(ProtoNodeState::Active), 1);
        assert_eq!(i32::from(ProtoNodeState::Joining), 2);
        assert_eq!(i32::from(ProtoNodeState::Draining), 3);
        assert_eq!(i32::from(ProtoNodeState::Down), 4);

        assert_eq!(i32::from(ProtoNodeType::Unknown), 0);
        assert_eq!(i32::from(ProtoNodeType::ControlPlane), 1);
        assert_eq!(i32::from(ProtoNodeType::Metadata), 2);
        assert_eq!(i32::from(ProtoNodeType::Object), 3);
        assert_eq!(i32::from(ProtoNodeType::Frontend), 4);

        assert_eq!(NodeType::try_from(1), Ok(NodeType::ControlPlane));
        assert_eq!(NodeType::try_from(2), Ok(NodeType::Metadata));
        assert_eq!(NodeType::try_from(3), Ok(NodeType::Object));
        assert_eq!(NodeType::try_from(4), Ok(NodeType::Frontend));
        assert!(NodeType::try_from(0).is_err());
        assert!(NodeType::try_from(5).is_err());
        assert!(NodeType::try_from(999).is_err());

        assert_eq!(i32::from(NodeType::ControlPlane), 1);
        assert_eq!(i32::from(NodeType::Metadata), 2);
        assert_eq!(i32::from(NodeType::Object), 3);
        assert_eq!(i32::from(NodeType::Frontend), 4);

        let t1 = NodeType::ControlPlane;
        let t2 = t1;
        let t3 = t1.clone();
        assert_eq!(t1, t2);
        assert_eq!(t1, t3);
        let debug_str = format!("{:?}", t1);
        assert_eq!(debug_str, "ControlPlane");
    }

    #[test]
    fn test_node_info_conversions_and_roundtrips() {
        let states_and_types = [
            (HashRingNodeState::Active, NodeType::ControlPlane),
            (HashRingNodeState::Joining, NodeType::Metadata),
            (HashRingNodeState::Draining, NodeType::Object),
            (HashRingNodeState::Down, NodeType::Frontend),
        ];

        for (state, node_type) in states_and_types {
            let node = ExtendedNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state,
                node_type,
            };
            let proto = extended_node_to_proto(&node);
            assert_eq!(proto.id, node.id);
            assert_eq!(proto.address, node.address);
            assert_eq!(proto.port, node.port as u32);

            let back = proto_node_to_extended(&proto);
            assert_eq!(back.id, node.id);
            assert_eq!(back.address, node.address);
            assert_eq!(back.port, node.port);
            assert_eq!(back.state, node.state);
            assert_eq!(back.node_type, node.node_type);
        }

        let proto_states = [
            (ProtoNodeState::Active, HashRingNodeState::Active),
            (ProtoNodeState::Joining, HashRingNodeState::Joining),
            (ProtoNodeState::Draining, HashRingNodeState::Draining),
            (ProtoNodeState::Down, HashRingNodeState::Down),
            (ProtoNodeState::Unknown, HashRingNodeState::Down),
        ];
        for (proto_state, expected) in proto_states {
            let proto = ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: i32::from(proto_state),
                r#type: i32::from(ProtoNodeType::Object),
            };
            assert_eq!(proto_node_to_extended(&proto).state, expected);
        }

        let proto_types = [
            (ProtoNodeType::ControlPlane, NodeType::ControlPlane),
            (ProtoNodeType::Metadata, NodeType::Metadata),
            (ProtoNodeType::Object, NodeType::Object),
            (ProtoNodeType::Frontend, NodeType::Frontend),
            (ProtoNodeType::Unknown, NodeType::Frontend),
        ];
        for (proto_type, expected) in proto_types {
            let proto = ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: i32::from(ProtoNodeState::Active),
                r#type: i32::from(proto_type),
            };
            assert_eq!(proto_node_to_extended(&proto).node_type, expected);
        }

        for invalid in [999, -1, i32::MAX] {
            let proto = ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: invalid,
                r#type: i32::from(ProtoNodeType::Object),
            };
            assert_eq!(proto_node_to_extended(&proto).state, HashRingNodeState::Down);
        }

        for invalid in [999, -1, i32::MAX] {
            let proto = ProtoNodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9001,
                state: i32::from(ProtoNodeState::Active),
                r#type: invalid,
            };
            assert_eq!(proto_node_to_extended(&proto).node_type, NodeType::Frontend);
        }

        let node_empty = ExtendedNodeInfo {
            id: "".to_string(),
            address: "".to_string(),
            port: 0,
            state: HashRingNodeState::Active,
            node_type: NodeType::Object,
        };
        let proto_empty = extended_node_to_proto(&node_empty);
        assert_eq!(proto_empty.id, "");
        assert_eq!(proto_empty.address, "");
        assert_eq!(proto_empty.port, 0);

        let node_max = ExtendedNodeInfo {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: 65535,
            state: HashRingNodeState::Active,
            node_type: NodeType::Object,
        };
        assert_eq!(extended_node_to_proto(&node_max).port, 65535);

        let proto_max_port = ProtoNodeInfo {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: u32::MAX,
            state: i32::from(ProtoNodeState::Active),
            r#type: i32::from(ProtoNodeType::Object),
        };
        assert_eq!(proto_node_to_extended(&proto_max_port).port, u16::MAX);

        let addresses = [
            "localhost", "127.0.0.1", "0.0.0.0", "255.255.255.255",
            "::1", "fe80::1", "example.com", "node-1.cluster.local",
        ];
        for addr in addresses {
            let proto = ProtoNodeInfo {
                id: "node-1".to_string(),
                address: addr.to_string(),
                port: 9001,
                state: i32::from(ProtoNodeState::Active),
                r#type: i32::from(ProtoNodeType::Object),
            };
            assert_eq!(proto_node_to_extended(&proto).address, addr);
        }
    }

    #[test]
    fn test_extended_node_info_methods_and_traits() {
        let node = ExtendedNodeInfo::new("node-1", "10.0.0.1", 9001, NodeType::Object);
        assert_eq!(node.id, "node-1");
        assert_eq!(node.address, "10.0.0.1");
        assert_eq!(node.port, 9001);
        assert_eq!(node.state, HashRingNodeState::Active);
        assert_eq!(node.node_type, NodeType::Object);
        assert_eq!(node.endpoint(), "10.0.0.1:9001");

        let node_info = node.to_node_info();
        assert_eq!(node_info.id.as_str(), "node-1");

        let node_id = node.node_id();
        assert_eq!(node_id.as_str(), "node-1");

        let cloned = node.clone();
        assert_eq!(node, cloned);

        let node2 = ExtendedNodeInfo::new("node-2", "10.0.0.2", 9002, NodeType::Object);
        assert_ne!(node, node2);

        let node_diff_state = ExtendedNodeInfo {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: 9001,
            state: HashRingNodeState::Draining,
            node_type: NodeType::Object,
        };
        assert_ne!(node, node_diff_state);

        let debug_str = format!("{:?}", node);
        assert!(debug_str.contains("node-1"));
        assert!(debug_str.contains("10.0.0.1"));

        let long_id = "a".repeat(1000);
        let node_long = ExtendedNodeInfo::new(&long_id, "10.0.0.1", 9001, NodeType::Object);
        assert_eq!(node_long.id.len(), 1000);

        let node_ipv6 = ExtendedNodeInfo::new("node-1", "::1", 9001, NodeType::Object);
        assert_eq!(node_ipv6.endpoint(), "::1:9001");

        let node_special = ExtendedNodeInfo::new(
            "node-with-special-chars_123",
            "my-host.example.com",
            9001,
            NodeType::Object,
        );
        let proto = extended_node_to_proto(&node_special);
        let roundtrip = proto_node_to_extended(&proto);
        assert_eq!(roundtrip.id, "node-with-special-chars_123");
        assert_eq!(roundtrip.address, "my-host.example.com");

        let serialized = serde_json::to_string(&node).unwrap();
        let deserialized: ExtendedNodeInfo = serde_json::from_str(&serialized).unwrap();
        assert_eq!(node.id, deserialized.id);
        assert_eq!(node.address, deserialized.address);
        assert_eq!(node.port, deserialized.port);
        assert_eq!(node.state, deserialized.state);
        assert_eq!(node.node_type, deserialized.node_type);
    }

    #[test]
    fn test_cluster_state_snapshot_full_lifecycle() {
        let mut snapshot = ClusterStateSnapshot::new();
        assert!(snapshot.control_plane_nodes.is_empty());
        assert!(snapshot.metadata_nodes.is_empty());
        assert!(snapshot.object_nodes.is_empty());
        assert!(snapshot.frontend_nodes.is_empty());
        assert!(snapshot.ref_locks.is_empty());
        assert_eq!(snapshot.version, 0);
        assert!(snapshot.leader_id.is_none());

        let default_snapshot = ClusterStateSnapshot::default();
        assert!(default_snapshot.control_plane_nodes.is_empty());
        assert_eq!(default_snapshot.version, 0);

        let nodes = [
            ("cp-1", "10.0.0.1", 9001, NodeType::ControlPlane),
            ("md-1", "10.0.0.2", 9002, NodeType::Metadata),
            ("obj-1", "10.0.0.3", 9003, NodeType::Object),
            ("fe-1", "10.0.0.4", 9004, NodeType::Frontend),
        ];
        for (id, addr, port, ntype) in nodes {
            snapshot.add_node(ExtendedNodeInfo::new(id, addr, port, ntype));
        }
        assert_eq!(snapshot.control_plane_nodes.len(), 1);
        assert_eq!(snapshot.metadata_nodes.len(), 1);
        assert_eq!(snapshot.object_nodes.len(), 1);
        assert_eq!(snapshot.frontend_nodes.len(), 1);
        assert_eq!(snapshot.version, 4);

        for (id, _, _, ntype) in nodes {
            assert!(snapshot.get_node(id).is_some());
            assert_eq!(snapshot.find_node_type(id), Some(ntype));
        }
        assert!(snapshot.get_node("nonexistent").is_none());
        assert!(snapshot.find_node_type("nonexistent").is_none());

        let all_nodes = snapshot.all_nodes();
        assert_eq!(all_nodes.len(), 4);

        let state_transitions = [
            ("cp-1", NodeType::ControlPlane, HashRingNodeState::Draining),
            ("md-1", NodeType::Metadata, HashRingNodeState::Down),
            ("obj-1", NodeType::Object, HashRingNodeState::Active),
            ("fe-1", NodeType::Frontend, HashRingNodeState::Joining),
        ];
        for (id, ntype, state) in state_transitions {
            assert!(snapshot.set_node_state(id, ntype, state));
        }
        assert!(!snapshot.set_node_state("nonexistent", NodeType::Object, HashRingNodeState::Down));

        assert_eq!(snapshot.control_plane_nodes.get("cp-1").unwrap().state, HashRingNodeState::Draining);
        assert_eq!(snapshot.metadata_nodes.get("md-1").unwrap().state, HashRingNodeState::Down);
        assert_eq!(snapshot.object_nodes.get("obj-1").unwrap().state, HashRingNodeState::Active);
        assert_eq!(snapshot.frontend_nodes.get("fe-1").unwrap().state, HashRingNodeState::Joining);

        for (id, ntype, _) in state_transitions {
            assert!(snapshot.remove_node(id, ntype).is_some());
        }
        assert!(snapshot.remove_node("nonexistent", NodeType::Object).is_none());
        assert!(snapshot.control_plane_nodes.is_empty());
        assert!(snapshot.metadata_nodes.is_empty());
        assert!(snapshot.object_nodes.is_empty());
        assert!(snapshot.frontend_nodes.is_empty());

        snapshot.add_node(ExtendedNodeInfo::new("node-1", "10.0.0.1", 9001, NodeType::Object));
        snapshot.remove_node("node-1", NodeType::Object);
        snapshot.add_node(ExtendedNodeInfo::new("node-1", "10.0.0.2", 9002, NodeType::Object));
        assert_eq!(snapshot.object_nodes.get("node-1").unwrap().address, "10.0.0.2");

        let mut snapshot2 = ClusterStateSnapshot::new();
        for i in 0..4 {
            snapshot2.add_node(ExtendedNodeInfo::new(
                &format!("node-{}", i),
                "10.0.0.1",
                9000 + i,
                [NodeType::ControlPlane, NodeType::Metadata, NodeType::Object, NodeType::Frontend][i as usize],
            ));
        }
        assert_eq!(snapshot2.all_nodes().len(), 4);

        let mut snapshot3 = ClusterStateSnapshot::new();
        snapshot3.add_node(ExtendedNodeInfo::new("node-1", "10.0.0.1", 9001, NodeType::Object));
        snapshot3.leader_id = Some("leader".to_string());
        let cloned = snapshot3.clone();
        assert_eq!(cloned.object_nodes.len(), 1);
        assert_eq!(cloned.leader_id, Some("leader".to_string()));

        let serialized = serde_json::to_string(&snapshot3).unwrap();
        let deserialized: ClusterStateSnapshot = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.object_nodes.len(), 1);
        assert_eq!(deserialized.version, snapshot3.version);

        let debug_str = format!("{:?}", snapshot3);
        assert!(debug_str.contains("ClusterStateSnapshot"));

        let mut snapshot4 = ClusterStateSnapshot::new();
        assert_eq!(snapshot4.version, 0);
        snapshot4.add_node(ExtendedNodeInfo::new("node-1", "10.0.0.1", 9001, NodeType::Object));
        assert_eq!(snapshot4.version, 1);
        snapshot4.add_node(ExtendedNodeInfo::new("node-2", "10.0.0.2", 9002, NodeType::Object));
        assert_eq!(snapshot4.version, 2);
    }

    #[test]
    fn test_lock_management_lifecycle() {
        let key = RefLockKey::new("repo-1", "refs/heads/main");
        assert_eq!(key.repo_id, "repo-1");
        assert_eq!(key.ref_name, "refs/heads/main");

        let key2 = RefLockKey::new("repo-1", "refs/heads/main");
        let key3 = RefLockKey::new("repo-2", "refs/heads/main");
        assert_eq!(key, key2);
        assert_ne!(key, key3);

        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let mut hasher2 = DefaultHasher::new();
        key2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());

        let cloned_key = key.clone();
        assert_eq!(key.repo_id, cloned_key.repo_id);

        let debug_key = format!("{:?}", key);
        assert!(debug_key.contains("repo-1"));
        assert!(debug_key.contains("refs/heads/main"));

        let key_serialized = serde_json::to_string(&key).unwrap();
        let key_deserialized: RefLockKey = serde_json::from_str(&key_serialized).unwrap();
        assert_eq!(key.repo_id, key_deserialized.repo_id);

        let lock = LockInfo::new(
            "lock-1",
            "repo-1",
            "refs/heads/main",
            "holder-1",
            Duration::from_secs(30),
        );
        assert_eq!(lock.lock_id, "lock-1");
        assert_eq!(lock.repo_id, "repo-1");
        assert_eq!(lock.ref_name, "refs/heads/main");
        assert_eq!(lock.holder_id, "holder-1");
        assert_eq!(lock.timeout_ms, 30000);
        assert!(!lock.is_expired());
        let remaining = lock.remaining_ms();
        assert!(remaining > 0 && remaining <= 30000);

        let cloned_lock = lock.clone();
        assert_eq!(lock.lock_id, cloned_lock.lock_id);

        let debug_lock = format!("{:?}", lock);
        assert!(debug_lock.contains("lock-1"));
        assert!(debug_lock.contains("repo-1"));

        let lock2 = LockInfo {
            lock_id: "lock-1".to_string(),
            repo_id: "repo-1".to_string(),
            ref_name: "refs/heads/main".to_string(),
            holder_id: "holder-1".to_string(),
            timeout_ms: 30000,
            acquired_at_epoch_ms: lock.acquired_at_epoch_ms,
        };
        assert_eq!(lock, lock2);

        let lock_serialized = serde_json::to_string(&lock).unwrap();
        let lock_deserialized: LockInfo = serde_json::from_str(&lock_serialized).unwrap();
        assert_eq!(lock.lock_id, lock_deserialized.lock_id);

        let mut expired_lock = LockInfo::new(
            "lock-expired",
            "repo-1",
            "refs/heads/main",
            "holder-1",
            Duration::from_millis(1),
        );
        expired_lock.acquired_at_epoch_ms = 0;
        assert!(expired_lock.is_expired());
        assert_eq!(expired_lock.remaining_ms(), 0);

        let short_lock = LockInfo::new(
            "lock-short",
            "repo-1",
            "refs/heads/main",
            "holder-1",
            Duration::from_millis(1),
        );
        assert_eq!(short_lock.timeout_ms, 1);
        std::thread::sleep(Duration::from_millis(5));
        assert!(short_lock.is_expired());
        assert_eq!(short_lock.remaining_ms(), 0);

        let long_lock = LockInfo::new(
            "lock-long",
            "repo-1",
            "refs/heads/main",
            "holder-1",
            Duration::from_secs(86400 * 365),
        );
        assert!(!long_lock.is_expired());
        assert!(long_lock.remaining_ms() > 0);

        let long_repo = "r".repeat(1000);
        let long_ref = "refs/".to_string() + &"h".repeat(1000);
        let key_long = RefLockKey::new(&long_repo, &long_ref);
        assert_eq!(key_long.repo_id.len(), 1000);
        assert!(key_long.ref_name.len() > 1000);

        let mut snapshot = ClusterStateSnapshot::new();
        let key1 = RefLockKey::new("repo-1", "refs/heads/main");
        let lock1 = LockInfo::new("lock-1", "repo-1", "refs/heads/main", "holder-1", Duration::from_secs(30));
        snapshot.ref_locks.insert(key1.clone(), lock1);
        let key2 = RefLockKey::new("repo-2", "refs/heads/dev");
        let lock2 = LockInfo::new("lock-2", "repo-2", "refs/heads/dev", "holder-2", Duration::from_secs(60));
        snapshot.ref_locks.insert(key2.clone(), lock2);
        assert_eq!(snapshot.ref_locks.len(), 2);
        assert!(snapshot.ref_locks.contains_key(&key1));
        assert!(snapshot.ref_locks.contains_key(&key2));

        snapshot.leader_id = Some("leader-node".to_string());
        assert_eq!(snapshot.leader_id, Some("leader-node".to_string()));
        snapshot.leader_id = None;
        assert!(snapshot.leader_id.is_none());
    }

    #[test]
    fn test_raft_request_response_types_and_apply() {
        let req_cleanup = RaftRequest::CleanupExpiredLocks;
        assert_eq!(req_cleanup.clone(), RaftRequest::CleanupExpiredLocks);
        let debug_req = format!("{:?}", req_cleanup);
        assert!(debug_req.contains("CleanupExpiredLocks"));

        let resp_success = crate::raft::Response::Success;
        assert_eq!(resp_success.clone(), crate::raft::Response::Success);
        let debug_resp = format!("{:?}", resp_success);
        assert!(debug_resp.contains("Success"));

        let resp_node_added = crate::raft::Response::NodeAdded;
        assert_eq!(resp_node_added, crate::raft::Response::NodeAdded);
        assert_ne!(resp_success, resp_node_added);

        let resp_removed_true = crate::raft::Response::NodeRemoved { found: true };
        let resp_removed_false = crate::raft::Response::NodeRemoved { found: false };
        match resp_removed_true { crate::raft::Response::NodeRemoved { found } => assert!(found), _ => panic!() }
        match resp_removed_false { crate::raft::Response::NodeRemoved { found } => assert!(!found), _ => panic!() }

        let resp_state_set = crate::raft::Response::NodeStateSet { found: true };
        match resp_state_set { crate::raft::Response::NodeStateSet { found } => assert!(found), _ => panic!() }

        let resp_lock_acquired = crate::raft::Response::LockAcquired { lock_id: "lock-123".to_string() };
        match resp_lock_acquired.clone() {
            crate::raft::Response::LockAcquired { lock_id } => assert_eq!(lock_id, "lock-123"),
            _ => panic!()
        }

        let resp_lock_not_acquired = crate::raft::Response::LockNotAcquired { reason: "held by another".to_string() };
        match resp_lock_not_acquired {
            crate::raft::Response::LockNotAcquired { reason } => assert_eq!(reason, "held by another"),
            _ => panic!()
        }

        let resp_lock_released = crate::raft::Response::LockReleased { found: true };
        match resp_lock_released { crate::raft::Response::LockReleased { found } => assert!(found), _ => panic!() }

        let resp_cleanup = crate::raft::Response::LocksCleanedUp { count: 5 };
        match resp_cleanup { crate::raft::Response::LocksCleanedUp { count } => assert_eq!(count, 5), _ => panic!() }

        let node = ExtendedNodeInfo::new("node-1", "10.0.0.1", 9001, NodeType::Object);
        let req_add = RaftRequest::AddNode { node: node.clone() };
        match req_add.clone() { RaftRequest::AddNode { node: n } => assert_eq!(n.id, "node-1"), _ => panic!() }

        let req_remove = RaftRequest::RemoveNode { node_id: "node-1".to_string(), node_type: NodeType::Object };
        match req_remove {
            RaftRequest::RemoveNode { node_id, node_type } => {
                assert_eq!(node_id, "node-1");
                assert_eq!(node_type, NodeType::Object);
            }
            _ => panic!()
        }

        let req_set_state = RaftRequest::SetNodeState {
            node_id: "node-1".to_string(),
            node_type: NodeType::Object,
            state: HashRingNodeState::Draining,
        };
        match req_set_state {
            RaftRequest::SetNodeState { node_id, node_type, state } => {
                assert_eq!(node_id, "node-1");
                assert_eq!(node_type, NodeType::Object);
                assert_eq!(state, HashRingNodeState::Draining);
            }
            _ => panic!()
        }

        let key = RefLockKey::new("repo-1", "refs/heads/main");
        let lock = LockInfo::new("lock-1", "repo-1", "refs/heads/main", "holder-1", Duration::from_secs(30));
        let req_acquire = RaftRequest::AcquireLock { key: key.clone(), lock: lock.clone() };
        match req_acquire {
            RaftRequest::AcquireLock { key: k, lock: l } => {
                assert_eq!(k.repo_id, "repo-1");
                assert_eq!(l.lock_id, "lock-1");
            }
            _ => panic!()
        }

        let req_release = RaftRequest::ReleaseLock { lock_id: "lock-1".to_string() };
        match req_release { RaftRequest::ReleaseLock { lock_id } => assert_eq!(lock_id, "lock-1"), _ => panic!() }

        let key2 = RefLockKey::new("repo-1", "refs/heads/main");
        let lock2 = LockInfo::new("lock-1", "repo-1", "refs/heads/main", "holder-1", Duration::from_secs(30));
        let req_ser = RaftRequest::AcquireLock { key: key2, lock: lock2 };
        let serialized = serde_json::to_string(&req_ser).unwrap();
        let deserialized: RaftRequest = serde_json::from_str(&serialized).unwrap();
        match deserialized {
            RaftRequest::AcquireLock { key: k, lock: l } => {
                assert_eq!(k.repo_id, "repo-1");
                assert_eq!(l.lock_id, "lock-1");
            }
            _ => panic!()
        }

        let resp_ser = crate::raft::Response::LockNotAcquired { reason: "already held".to_string() };
        let ser_resp = serde_json::to_string(&resp_ser).unwrap();
        let de_resp: crate::raft::Response = serde_json::from_str(&ser_resp).unwrap();
        match de_resp {
            crate::raft::Response::LockNotAcquired { reason } => assert_eq!(reason, "already held"),
            _ => panic!()
        }
    }

    #[test]
    fn test_apply_request_operations() {
        let mut state = ClusterStateSnapshot::new();

        for (id, addr, port, ntype) in [
            ("cp-1", "10.0.0.1", 9001, NodeType::ControlPlane),
            ("md-1", "10.0.0.2", 9002, NodeType::Metadata),
            ("obj-1", "10.0.0.3", 9003, NodeType::Object),
            ("fe-1", "10.0.0.4", 9004, NodeType::Frontend),
        ] {
            let req = RaftRequest::AddNode { node: ExtendedNodeInfo::new(id, addr, port, ntype) };
            let resp = crate::raft::apply_request(&mut state, &req);
            assert_eq!(resp, crate::raft::Response::NodeAdded);
        }
        assert_eq!(state.control_plane_nodes.len(), 1);
        assert_eq!(state.metadata_nodes.len(), 1);
        assert_eq!(state.object_nodes.len(), 1);
        assert_eq!(state.frontend_nodes.len(), 1);

        for (id, ntype, new_state) in [
            ("cp-1", NodeType::ControlPlane, HashRingNodeState::Draining),
            ("md-1", NodeType::Metadata, HashRingNodeState::Down),
            ("obj-1", NodeType::Object, HashRingNodeState::Active),
            ("fe-1", NodeType::Frontend, HashRingNodeState::Joining),
        ] {
            let req = RaftRequest::SetNodeState { node_id: id.to_string(), node_type: ntype, state: new_state };
            let resp = crate::raft::apply_request(&mut state, &req);
            assert!(matches!(resp, crate::raft::Response::NodeStateSet { found: true }));
        }
        let req_not_found = RaftRequest::SetNodeState {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
            state: HashRingNodeState::Down,
        };
        let resp = crate::raft::apply_request(&mut state, &req_not_found);
        assert!(matches!(resp, crate::raft::Response::NodeStateSet { found: false }));

        for (id, ntype) in [
            ("cp-1", NodeType::ControlPlane),
            ("md-1", NodeType::Metadata),
            ("obj-1", NodeType::Object),
            ("fe-1", NodeType::Frontend),
        ] {
            let req = RaftRequest::RemoveNode { node_id: id.to_string(), node_type: ntype };
            let resp = crate::raft::apply_request(&mut state, &req);
            assert!(matches!(resp, crate::raft::Response::NodeRemoved { found: true }));
        }
        let req_remove_not_found = RaftRequest::RemoveNode {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
        };
        let resp = crate::raft::apply_request(&mut state, &req_remove_not_found);
        assert!(matches!(resp, crate::raft::Response::NodeRemoved { found: false }));

        let key = RefLockKey::new("repo-1", "refs/heads/main");
        let lock = LockInfo::new("lock-1", "repo-1", "refs/heads/main", "holder-1", Duration::from_secs(30));
        let req_acquire = RaftRequest::AcquireLock { key: key.clone(), lock };
        let resp = crate::raft::apply_request(&mut state, &req_acquire);
        match resp {
            crate::raft::Response::LockAcquired { lock_id } => assert_eq!(lock_id, "lock-1"),
            _ => panic!("Expected LockAcquired")
        }

        let lock2 = LockInfo::new("lock-2", "repo-1", "refs/heads/main", "holder-2", Duration::from_secs(30));
        let req_acquire2 = RaftRequest::AcquireLock { key: key.clone(), lock: lock2 };
        let resp = crate::raft::apply_request(&mut state, &req_acquire2);
        match resp {
            crate::raft::Response::LockNotAcquired { reason } => assert!(reason.contains("holder-1")),
            _ => panic!("Expected LockNotAcquired")
        }

        let req_release = RaftRequest::ReleaseLock { lock_id: "lock-1".to_string() };
        let resp = crate::raft::apply_request(&mut state, &req_release);
        assert!(matches!(resp, crate::raft::Response::LockReleased { found: true }));

        let req_release_not_found = RaftRequest::ReleaseLock { lock_id: "nonexistent".to_string() };
        let resp = crate::raft::apply_request(&mut state, &req_release_not_found);
        assert!(matches!(resp, crate::raft::Response::LockReleased { found: false }));

        let mut expired_lock = LockInfo::new("lock-exp", "repo-1", "refs/heads/main", "holder-1", Duration::from_millis(1));
        expired_lock.acquired_at_epoch_ms = 0;
        state.ref_locks.insert(key.clone(), expired_lock);
        let new_lock = LockInfo::new("lock-new", "repo-1", "refs/heads/main", "holder-2", Duration::from_secs(30));
        let req_acquire_over_expired = RaftRequest::AcquireLock { key: key.clone(), lock: new_lock };
        let resp = crate::raft::apply_request(&mut state, &req_acquire_over_expired);
        match resp {
            crate::raft::Response::LockAcquired { lock_id } => assert_eq!(lock_id, "lock-new"),
            _ => panic!("Expected LockAcquired")
        }
        assert_eq!(state.ref_locks.get(&key).unwrap().holder_id, "holder-2");

        state.ref_locks.clear();
        let req_cleanup = RaftRequest::CleanupExpiredLocks;
        let resp = crate::raft::apply_request(&mut state, &req_cleanup);
        match resp {
            crate::raft::Response::LocksCleanedUp { count } => assert_eq!(count, 0),
            _ => panic!("Expected LocksCleanedUp")
        }

        for i in 0..10 {
            let k = RefLockKey::new(&format!("repo-{}", i), "refs/heads/main");
            let mut exp_lock = LockInfo::new(
                &format!("lock-{}", i),
                &format!("repo-{}", i),
                "refs/heads/main",
                &format!("holder-{}", i),
                Duration::from_millis(1),
            );
            exp_lock.acquired_at_epoch_ms = 0;
            state.ref_locks.insert(k, exp_lock);
        }
        let valid_key = RefLockKey::new("repo-valid", "refs/heads/main");
        let valid_lock = LockInfo::new("lock-valid", "repo-valid", "refs/heads/main", "holder-valid", Duration::from_secs(3600));
        state.ref_locks.insert(valid_key.clone(), valid_lock);

        let resp = crate::raft::apply_request(&mut state, &req_cleanup);
        match resp {
            crate::raft::Response::LocksCleanedUp { count } => assert_eq!(count, 10),
            _ => panic!("Expected LocksCleanedUp")
        }
        assert_eq!(state.ref_locks.len(), 1);
        assert!(state.ref_locks.get(&valid_key).is_some());
    }

    #[test]
    fn test_version_tracking() {
        let mut state = ClusterStateSnapshot::new();
        assert_eq!(state.version, 0);

        let req = RaftRequest::AddNode { node: ExtendedNodeInfo::new("node-1", "10.0.0.1", 9001, NodeType::Object) };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 1);

        let req = RaftRequest::SetNodeState {
            node_id: "node-1".to_string(),
            node_type: NodeType::Object,
            state: HashRingNodeState::Draining,
        };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 2);

        state.version = 10;
        let req = RaftRequest::SetNodeState {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
            state: HashRingNodeState::Down,
        };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 10);

        let req = RaftRequest::RemoveNode { node_id: "node-1".to_string(), node_type: NodeType::Object };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 11);

        let req = RaftRequest::RemoveNode { node_id: "nonexistent".to_string(), node_type: NodeType::Object };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 11);

        let key = RefLockKey::new("repo-1", "refs/heads/main");
        let lock = LockInfo::new("lock-1", "repo-1", "refs/heads/main", "holder-1", Duration::from_secs(30));
        let req = RaftRequest::AcquireLock { key: key.clone(), lock };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 12);

        let lock2 = LockInfo::new("lock-2", "repo-1", "refs/heads/main", "holder-2", Duration::from_secs(30));
        let req = RaftRequest::AcquireLock { key: key.clone(), lock: lock2 };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 12);

        let req = RaftRequest::ReleaseLock { lock_id: "lock-1".to_string() };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 13);

        let req = RaftRequest::ReleaseLock { lock_id: "nonexistent".to_string() };
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 13);

        let req = RaftRequest::CleanupExpiredLocks;
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 13);

        let key2 = RefLockKey::new("repo-2", "refs/heads/main");
        let mut expired = LockInfo::new("lock-exp", "repo-2", "refs/heads/main", "holder", Duration::from_millis(1));
        expired.acquired_at_epoch_ms = 0;
        state.ref_locks.insert(key2, expired);
        crate::raft::apply_request(&mut state, &req);
        assert_eq!(state.version, 14);
    }

    #[test]
    fn test_control_plane_error_variants() {
        let errors_and_checks: Vec<(ControlPlaneError, &str, tonic::Code)> = vec![
            (ControlPlaneError::NotLeader(Some("node-2".to_string())), "node-2", tonic::Code::FailedPrecondition),
            (ControlPlaneError::NotLeader(None), "none", tonic::Code::FailedPrecondition),
            (ControlPlaneError::Raft("raft failure".to_string()), "raft", tonic::Code::Internal),
            (ControlPlaneError::Storage("storage error".to_string()), "storage", tonic::Code::Internal),
            (ControlPlaneError::NodeNotFound("node-123".to_string()), "node-123", tonic::Code::NotFound),
            (ControlPlaneError::LockNotFound("lock-456".to_string()), "lock-456", tonic::Code::NotFound),
            (ControlPlaneError::LockHeld("holder-789".to_string()), "holder-789", tonic::Code::AlreadyExists),
            (ControlPlaneError::LockExpired, "expired", tonic::Code::FailedPrecondition),
            (ControlPlaneError::InvalidRequest("bad request".to_string()), "bad request", tonic::Code::InvalidArgument),
            (ControlPlaneError::Serialization("ser failed".to_string()), "ser", tonic::Code::Internal),
            (ControlPlaneError::Internal("internal".to_string()), "internal", tonic::Code::Internal),
        ];

        for (err, expected_msg, expected_code) in errors_and_checks {
            let display = format!("{}", err);
            assert!(display.to_lowercase().contains(expected_msg), "Expected '{}' in '{}'", expected_msg, display);
            let status: tonic::Status = err.into();
            assert_eq!(status.code(), expected_code);
        }

        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = ControlPlaneError::Io(io_err);
        let display = format!("{}", err);
        assert!(display.contains("io error"));
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);

        let io_err2 = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let err2: ControlPlaneError = io_err2.into();
        match err2 {
            ControlPlaneError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied),
            _ => panic!("Expected Io variant")
        }

        let hashring_err = gitstratum_hashring::HashRingError::EmptyRing;
        let err = ControlPlaneError::HashRing(hashring_err);
        let display = format!("{}", err);
        assert!(display.contains("hash ring error"));

        let status = tonic::Status::not_found("resource not found");
        let err = ControlPlaneError::Grpc(status);
        let display = format!("{}", err);
        assert!(display.contains("grpc error"));

        use bincode::Options;
        let config = bincode::DefaultOptions::new().with_fixint_encoding();
        let bincode_err = config.serialize::<u8>(&0u8).and_then(|_| config.deserialize::<u64>(&[0u8; 1])).unwrap_err();
        let err: ControlPlaneError = bincode_err.into();
        match err { ControlPlaneError::Serialization(msg) => assert!(!msg.is_empty()), _ => panic!() }

        let json_err = serde_json::from_str::<String>("invalid json").unwrap_err();
        let err: ControlPlaneError = json_err.into();
        match err { ControlPlaneError::Serialization(msg) => assert!(!msg.is_empty()), _ => panic!() }

        let debug_err = ControlPlaneError::NotLeader(Some("leader-1".to_string()));
        let debug_str = format!("{:?}", debug_err);
        assert!(debug_str.contains("NotLeader"));
        assert!(debug_str.contains("leader-1"));
    }

    #[test]
    fn test_raft_data_structures() {
        let snapshot = crate::raft::StoredSnapshot::default();
        assert!(snapshot.data.is_empty());

        let mut snapshot2 = crate::raft::StoredSnapshot::default();
        snapshot2.data = vec![1, 2, 3, 4];
        let cloned = snapshot2.clone();
        assert_eq!(cloned.data, vec![1, 2, 3, 4]);

        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("StoredSnapshot"));

        let snapshot3 = crate::raft::StoredSnapshot::default();
        let snapshot4 = crate::raft::StoredSnapshot::default();
        assert_eq!(snapshot3, snapshot4);

        let serialized = serde_json::to_string(&snapshot).unwrap();
        let deserialized: crate::raft::StoredSnapshot = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.data.is_empty());

        let data = crate::raft::StateMachineData::default();
        assert!(data.last_applied.is_none());
        assert!(data.state.control_plane_nodes.is_empty());
        assert!(data.state.metadata_nodes.is_empty());
        assert!(data.state.object_nodes.is_empty());
        assert!(data.state.frontend_nodes.is_empty());
        assert!(data.state.ref_locks.is_empty());
        assert_eq!(data.state.version, 0);
    }

    #[test]
    fn test_node_type_serialization() {
        for node_type in [NodeType::ControlPlane, NodeType::Metadata, NodeType::Object, NodeType::Frontend] {
            let serialized = serde_json::to_string(&node_type).unwrap();
            let deserialized: NodeType = serde_json::from_str(&serialized).unwrap();
            assert_eq!(node_type, deserialized);
        }
    }
}
