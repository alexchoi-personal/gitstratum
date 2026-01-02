use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use gitstratum_proto::coordinator_service_server::CoordinatorService;
use gitstratum_proto::{
    AddNodeRequest, AddNodeResponse, DeregisterNodeRequest, DeregisterNodeResponse,
    GetClusterStateRequest, GetClusterStateResponse, GetHashRingRequest, GetHashRingResponse,
    GetTopologyRequest, GetTopologyResponse, HashRingEntry, HealthCheckRequest,
    HealthCheckResponse, HeartbeatRequest, HeartbeatResponse, NodeType, RaftState,
    RegisterNodeRequest, RegisterNodeResponse, RemoveNodeRequest, RemoveNodeResponse,
    SetNodeStateRequest, SetNodeStateResponse, TopologyUpdate, WatchTopologyRequest,
};
use k8s_operator::raft::{KeyValueStateMachine, RaftNodeManager, RaftRequest};

use crate::commands::ClusterCommand;
use crate::config::CoordinatorConfig;
use crate::error::CoordinatorError;
use crate::heartbeat_batcher::HeartbeatBatcher;
use crate::state_machine::{apply_command, deserialize_topology, serialize_topology, topology_key};
use crate::topology::{ClusterTopology, NodeEntry};

pub struct CoordinatorServer {
    raft: Arc<RaftNodeManager<KeyValueStateMachine>>,
    topology_cache: RwLock<ClusterTopology>,
    topology_updates: broadcast::Sender<TopologyUpdate>,
    #[allow(dead_code)]
    config: CoordinatorConfig,
    #[allow(dead_code)]
    heartbeat_batcher: Arc<HeartbeatBatcher>,
}

impl CoordinatorServer {
    pub fn new(
        raft: Arc<RaftNodeManager<KeyValueStateMachine>>,
        config: CoordinatorConfig,
        heartbeat_batcher: Arc<HeartbeatBatcher>,
    ) -> Self {
        let (tx, _) = broadcast::channel(config.watch_buffer_size);
        Self {
            raft,
            topology_cache: RwLock::new(ClusterTopology::default()),
            topology_updates: tx,
            config,
            heartbeat_batcher,
        }
    }

    fn is_leader(&self) -> bool {
        self.raft.leader_election().is_leader()
    }

    fn ensure_leader(&self) -> Result<(), CoordinatorError> {
        if self.is_leader() {
            Ok(())
        } else {
            Err(CoordinatorError::NotLeader)
        }
    }

    async fn write_topology(&self, topology: &ClusterTopology) -> Result<(), CoordinatorError> {
        let raft = self.raft.raft();
        let request = RaftRequest {
            key: topology_key().to_string(),
            value: serialize_topology(topology)
                .map_err(|e| CoordinatorError::Serialization(e.to_string()))?,
        };
        raft.client_write(request)
            .await
            .map_err(|e| CoordinatorError::Raft(format!("{:?}", e)))?;
        Ok(())
    }

    async fn read_topology(&self) -> ClusterTopology {
        if let Some(store) = self.raft.mem_store() {
            let data = store.data().await;
            if let Some(value) = data.get(topology_key()) {
                return deserialize_topology(value).unwrap_or_default();
            }
        }
        ClusterTopology::default()
    }

    async fn apply_and_write(
        &self,
        cmd: ClusterCommand,
    ) -> Result<crate::commands::ClusterResponse, CoordinatorError> {
        let mut topology = self.read_topology().await;
        let response = apply_command(&mut topology, &cmd);

        if response.is_success() {
            self.write_topology(&topology).await?;
            *self.topology_cache.write() = topology;
        }

        Ok(response)
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServer {
    async fn get_cluster_state(
        &self,
        _request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        let topo = self.read_topology().await;

        let metadata_nodes = topo
            .metadata_nodes
            .values()
            .map(|n| n.to_proto(NodeType::Metadata))
            .collect();

        let object_nodes = topo
            .object_nodes
            .values()
            .map(|n| n.to_proto(NodeType::Object))
            .collect();

        let leader = self.raft.leader_election();
        let leader_id = if leader.is_leader() {
            leader.node_id().to_string()
        } else {
            String::new()
        };

        Ok(Response::new(GetClusterStateResponse {
            metadata_nodes,
            object_nodes,
            leader_id,
            version: topo.version,
            hash_ring_config: Some(topo.hash_ring_config.to_proto()),
        }))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        self.ensure_leader()?;

        let req = request.into_inner();
        let node = req
            .node
            .ok_or_else(|| Status::invalid_argument("Missing node"))?;

        let entry = NodeEntry::from_proto(&node);
        let node_type = NodeType::try_from(node.r#type).unwrap_or(NodeType::Unknown);

        let cmd = match node_type {
            NodeType::Object => ClusterCommand::AddObjectNode(entry),
            NodeType::Metadata => ClusterCommand::AddMetadataNode(entry),
            _ => return Err(CoordinatorError::InvalidNodeType.into()),
        };

        let resp = self.apply_and_write(cmd).await?;

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version: 0,
            update: Some(gitstratum_proto::topology_update::Update::NodeAdded(node)),
        });

        let (success, error) = resp.to_result();
        Ok(Response::new(AddNodeResponse { success, error }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        self.ensure_leader()?;

        let req = request.into_inner();
        let cmd = ClusterCommand::RemoveNode {
            node_id: req.node_id.clone(),
        };

        let resp = self.apply_and_write(cmd).await?;

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version: 0,
            update: Some(gitstratum_proto::topology_update::Update::NodeRemoved(
                req.node_id,
            )),
        });

        let (success, error) = resp.to_result();
        Ok(Response::new(RemoveNodeResponse { success, error }))
    }

    async fn set_node_state(
        &self,
        request: Request<SetNodeStateRequest>,
    ) -> Result<Response<SetNodeStateResponse>, Status> {
        self.ensure_leader()?;

        let req = request.into_inner();
        let cmd = ClusterCommand::SetNodeState {
            node_id: req.node_id,
            state: req.state,
        };

        let resp = self.apply_and_write(cmd).await?;

        let (success, error) = resp.to_result();
        Ok(Response::new(SetNodeStateResponse { success, error }))
    }

    async fn get_hash_ring(
        &self,
        _request: Request<GetHashRingRequest>,
    ) -> Result<Response<GetHashRingResponse>, Status> {
        let topo = self.read_topology().await;

        let ring = topo
            .to_hash_ring()
            .map_err(|e| Status::internal(format!("Failed to build hash ring: {}", e)))?;

        let entries: Vec<HashRingEntry> = ring
            .get_ring_entries()
            .into_iter()
            .map(|(position, node_id)| HashRingEntry {
                position,
                node_id: node_id.to_string(),
            })
            .collect();

        Ok(Response::new(GetHashRingResponse {
            entries,
            replication_factor: topo.hash_ring_config.replication_factor,
            version: topo.version,
        }))
    }

    type WatchTopologyStream = Pin<Box<dyn Stream<Item = Result<TopologyUpdate, Status>> + Send>>;

    async fn watch_topology(
        &self,
        _request: Request<WatchTopologyRequest>,
    ) -> Result<Response<Self::WatchTopologyStream>, Status> {
        let rx = self.topology_updates.subscribe();
        let stream = BroadcastStream::new(rx)
            .map(|result| result.map_err(|e| Status::internal(format!("Stream error: {}", e))));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_topology(
        &self,
        _request: Request<GetTopologyRequest>,
    ) -> Result<Response<GetTopologyResponse>, Status> {
        let topo = self.read_topology().await;

        let metadata_nodes = topo
            .metadata_nodes
            .values()
            .map(|n| n.to_proto(NodeType::Metadata))
            .collect();

        let object_nodes = topo
            .object_nodes
            .values()
            .map(|n| n.to_proto(NodeType::Object))
            .collect();

        let leader = self.raft.leader_election();
        let leader_id = if leader.is_leader() {
            leader.node_id().to_string()
        } else {
            String::new()
        };

        Ok(Response::new(GetTopologyResponse {
            version: topo.version,
            frontend_nodes: vec![],
            metadata_nodes,
            object_nodes,
            hash_ring_config: Some(topo.hash_ring_config.to_proto()),
            leader_id,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let topo = self.read_topology().await;
        let leader = self.raft.leader_election();
        let is_leader = leader.is_leader();

        let raft_state = if is_leader {
            RaftState::Leader
        } else {
            RaftState::Follower
        };

        let leader_id = if is_leader {
            leader.node_id().to_string()
        } else {
            String::new()
        };

        let active_nodes = (topo.metadata_nodes.len() + topo.object_nodes.len()) as u32;

        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            raft_state: raft_state.into(),
            raft_term: 0,
            committed_index: 0,
            applied_index: 0,
            leader_id,
            leader_address: String::new(),
            topology_version: topo.version,
            active_nodes,
            suspect_nodes: 0,
            down_nodes: 0,
            watch_subscribers: self.topology_updates.receiver_count() as u32,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        self.ensure_leader()?;

        let req = request.into_inner();
        let node = req
            .node
            .ok_or_else(|| Status::invalid_argument("Missing node"))?;

        let entry = NodeEntry::from_proto(&node);
        let node_type = NodeType::try_from(node.r#type).unwrap_or(NodeType::Unknown);

        let cmd = match node_type {
            NodeType::Object => ClusterCommand::AddObjectNode(entry),
            NodeType::Metadata => ClusterCommand::AddMetadataNode(entry),
            _ => return Err(CoordinatorError::InvalidNodeType.into()),
        };

        let resp = self.apply_and_write(cmd).await?;

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version: 0,
            update: Some(gitstratum_proto::topology_update::Update::NodeAdded(node)),
        });

        Ok(Response::new(RegisterNodeResponse {
            topology_version: resp.version().unwrap_or(0),
            already_registered: resp.is_already_registered(),
        }))
    }

    async fn deregister_node(
        &self,
        request: Request<DeregisterNodeRequest>,
    ) -> Result<Response<DeregisterNodeResponse>, Status> {
        self.ensure_leader()?;

        let req = request.into_inner();
        let cmd = ClusterCommand::RemoveNode {
            node_id: req.node_id.clone(),
        };

        let resp = self.apply_and_write(cmd).await?;

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version: 0,
            update: Some(gitstratum_proto::topology_update::Update::NodeRemoved(
                req.node_id,
            )),
        });

        Ok(Response::new(DeregisterNodeResponse {
            topology_version: resp.version().unwrap_or(0),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let topo = self.read_topology().await;

        let leader = self.raft.leader_election();
        let is_leader = leader.is_leader();

        let leader_id = if is_leader {
            leader.node_id().to_string()
        } else {
            String::new()
        };

        let refresh_required = req.known_version < topo.version;

        Ok(Response::new(HeartbeatResponse {
            current_version: topo.version,
            refresh_required,
            leader_id,
            leader_address: String::new(),
            raft_term: 0,
        }))
    }
}
