use gitstratum_hashring::NodeState as HashRingNodeState;
use gitstratum_proto::{
    control_plane_service_client::ControlPlaneServiceClient, AddNodeRequest, AcquireRefLockRequest,
    GetClusterStateRequest, GetHashRingRequest, GetRebalanceStatusRequest, NodeInfo as ProtoNodeInfo,
    NodeState as ProtoNodeState, NodeType as ProtoNodeType, ReleaseRefLockRequest,
    RemoveNodeRequest, SetNodeStateRequest, TriggerRebalanceRequest,
};
use std::time::Duration;
use tonic::transport::Channel;

use crate::error::{ControlPlaneError, Result};
use crate::state::{ExtendedNodeInfo, NodeType};

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

    pub async fn trigger_rebalance(
        &mut self,
        reason: impl Into<String>,
    ) -> Result<Option<String>> {
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
