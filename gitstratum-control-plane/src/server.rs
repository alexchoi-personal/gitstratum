
use gitstratum_hashring::NodeState as HashRingNodeState;
use gitstratum_proto::{
    control_plane_service_server::ControlPlaneService, AddNodeRequest, AddNodeResponse,
    AcquireRefLockRequest, AcquireRefLockResponse, GetClusterStateRequest, GetClusterStateResponse,
    GetHashRingRequest, GetHashRingResponse, GetRebalanceStatusRequest, GetRebalanceStatusResponse,
    HashRingEntry, NodeInfo as ProtoNodeInfo, NodeState as ProtoNodeState,
    NodeType as ProtoNodeType, ReleaseRefLockRequest, ReleaseRefLockResponse, RemoveNodeRequest,
    RemoveNodeResponse, SetNodeStateRequest, SetNodeStateResponse, TriggerRebalanceRequest,
    TriggerRebalanceResponse,
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
        state: proto_node_state_to_hashring(ProtoNodeState::try_from(node.state).unwrap_or(ProtoNodeState::Unknown)),
        node_type: proto_node_type_to_internal(ProtoNodeType::try_from(node.r#type).unwrap_or(ProtoNodeType::Unknown)),
    }
}

pub struct ControlPlaneServer {
    raft: ControlPlaneRaft,
    state_machine: StateMachineStore,
    node_id: NodeId,
}

impl ControlPlaneServer {
    pub fn new(
        raft: ControlPlaneRaft,
        state_machine: StateMachineStore,
        node_id: NodeId,
    ) -> Self {
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
                openraft::error::RaftError::APIError(api_err) => {
                    match api_err {
                        openraft::error::ClientWriteError::ForwardToLeader(forward) => {
                            let leader_id = forward.leader_id.map(|id| id.to_string());
                            Err(ControlPlaneError::NotLeader(leader_id))
                        }
                        openraft::error::ClientWriteError::ChangeMembershipError(_) => {
                            Err(ControlPlaneError::Raft("membership change error".to_string()))
                        }
                    }
                }
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
        let leader_id = metrics.current_leader.map(|id| id.to_string()).unwrap_or_default();

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
        .add_service(gitstratum_proto::control_plane_service_server::ControlPlaneServiceServer::new(server))
        .serve(addr)
        .await
        .map_err(|e| ControlPlaneError::Internal(e.to_string()))?;

    Ok(())
}
