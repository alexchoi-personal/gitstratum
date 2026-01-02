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
    AddNodeRequest, AddNodeResponse, GetClusterStateRequest, GetClusterStateResponse,
    GetHashRingRequest, GetHashRingResponse, HashRingEntry, NodeType, RemoveNodeRequest,
    RemoveNodeResponse, SetNodeStateRequest, SetNodeStateResponse, TopologyUpdate,
    WatchTopologyRequest,
};
use k8s_operator::raft::{KeyValueStateMachine, RaftNodeManager, RaftRequest};

use crate::commands::ClusterCommand;
use crate::error::CoordinatorError;
use crate::state_machine::{apply_command, deserialize_topology, serialize_topology, topology_key};
use crate::topology::{ClusterTopology, NodeEntry};

pub struct CoordinatorServer {
    raft: Arc<RaftNodeManager<KeyValueStateMachine>>,
    topology_cache: RwLock<ClusterTopology>,
    topology_updates: broadcast::Sender<TopologyUpdate>,
}

impl CoordinatorServer {
    pub fn new(raft: Arc<RaftNodeManager<KeyValueStateMachine>>) -> Self {
        let (tx, _) = broadcast::channel(100);
        Self {
            raft,
            topology_cache: RwLock::new(ClusterTopology::default()),
            topology_updates: tx,
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
            value: serialize_topology(topology),
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
                return deserialize_topology(value);
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

        if response.success {
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
            version: resp.version,
            update: Some(gitstratum_proto::topology_update::Update::NodeAdded(node)),
        });

        Ok(Response::new(AddNodeResponse {
            success: resp.success,
            error: resp.error.unwrap_or_default(),
        }))
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
            version: resp.version,
            update: Some(gitstratum_proto::topology_update::Update::NodeRemoved(
                req.node_id,
            )),
        });

        Ok(Response::new(RemoveNodeResponse {
            success: resp.success,
            error: resp.error.unwrap_or_default(),
        }))
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

        Ok(Response::new(SetNodeStateResponse {
            success: resp.success,
            error: resp.error.unwrap_or_default(),
        }))
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
}
