use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_stream::stream;
use futures::Stream;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::info;

use gitstratum_proto::coordinator_service_server::CoordinatorService;
use gitstratum_proto::{
    AddNodeRequest, AddNodeResponse, DeregisterNodeRequest, DeregisterNodeResponse,
    GetClusterStateRequest, GetClusterStateResponse, GetHashRingRequest, GetHashRingResponse,
    GetTopologyRequest, GetTopologyResponse, HashRingEntry, HealthCheckRequest,
    HealthCheckResponse, HeartbeatRequest, HeartbeatResponse, Keepalive, Lagged, NodeState,
    NodeType, RaftState, RegisterNodeRequest, RegisterNodeResponse, RemoveNodeRequest,
    RemoveNodeResponse, SetNodeStateRequest, SetNodeStateResponse, TopologyUpdate,
    WatchTopologyRequest,
};
use k8s_operator::raft::{KeyValueStateMachine, RaftNodeManager, RaftRequest};

use crate::commands::ClusterCommand;
use crate::config::CoordinatorConfig;
use crate::error::CoordinatorError;
use crate::heartbeat_batcher::HeartbeatBatcher;
use crate::rate_limit::{GlobalRateLimiter, RateLimitError};
use crate::state_machine::{apply_command, deserialize_topology, serialize_topology, topology_key};
use crate::topology::{ClusterTopology, NodeEntry};

impl From<RateLimitError> for Status {
    fn from(err: RateLimitError) -> Self {
        Status::resource_exhausted(err.to_string())
    }
}

pub struct NodeFlap {
    pub suspect_count: u32,
    pub last_suspect_at: Instant,
    pub last_stable_at: Instant,
}

pub struct CoordinatorServer {
    raft: Arc<RaftNodeManager<KeyValueStateMachine>>,
    topology_cache: RwLock<ClusterTopology>,
    topology_updates: broadcast::Sender<TopologyUpdate>,
    config: CoordinatorConfig,
    #[allow(dead_code)]
    heartbeat_batcher: Arc<HeartbeatBatcher>,
    leader_since: RwLock<Option<Instant>>,
    last_heartbeat: RwLock<HashMap<String, Instant>>,
    node_flap_info: RwLock<HashMap<String, NodeFlap>>,
    global_limiter: Arc<GlobalRateLimiter>,
}

impl CoordinatorServer {
    pub fn new(
        raft: Arc<RaftNodeManager<KeyValueStateMachine>>,
        config: CoordinatorConfig,
        heartbeat_batcher: Arc<HeartbeatBatcher>,
        global_limiter: Arc<GlobalRateLimiter>,
    ) -> Self {
        let (tx, _) = broadcast::channel(config.watch_buffer_size);
        Self {
            raft,
            topology_cache: RwLock::new(ClusterTopology::default()),
            topology_updates: tx,
            config,
            heartbeat_batcher,
            leader_since: RwLock::new(None),
            last_heartbeat: RwLock::new(HashMap::new()),
            node_flap_info: RwLock::new(HashMap::new()),
            global_limiter,
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

    pub fn get_timeout_for_node(&self, node_id: &str) -> (Duration, Duration) {
        let flap_info = self.node_flap_info.read();
        let base_suspect = self.config.suspect_timeout;
        let base_down = self.config.down_timeout;

        if let Some(flap) = flap_info.get(node_id) {
            let now = Instant::now();
            if flap.suspect_count >= self.config.flap_threshold
                && now.duration_since(flap.last_suspect_at) < self.config.flap_window
            {
                let multiplier = self.config.flap_multiplier;
                return (
                    base_suspect.mul_f32(multiplier),
                    base_down.mul_f32(multiplier),
                );
            }
        }

        (base_suspect, base_down)
    }

    pub async fn run_failure_detector(&self) {
        loop {
            tokio::time::sleep(self.config.detector_interval).await;

            if !self.is_leader() {
                *self.leader_since.write() = None;
                continue;
            }

            let now = Instant::now();

            let leader_start = {
                let mut leader_since = self.leader_since.write();
                if leader_since.is_none() {
                    *leader_since = Some(now);
                }
                leader_since.unwrap()
            };

            let in_grace_period =
                now.duration_since(leader_start) < self.config.leader_grace_period;

            let topology = self.read_topology().await;
            let last_heartbeat = self.last_heartbeat.read().clone();

            let all_nodes: Vec<(String, NodeState)> = topology
                .object_nodes
                .iter()
                .chain(topology.metadata_nodes.iter())
                .map(|(id, entry)| (id.clone(), entry.state()))
                .collect();

            for (node_id, current_state) in all_nodes {
                let last_hb = last_heartbeat.get(&node_id).copied();
                let (suspect_timeout, down_timeout) = self.get_timeout_for_node(&node_id);

                match current_state {
                    NodeState::Active => {
                        if in_grace_period {
                            continue;
                        }
                        if let Some(last) = last_hb {
                            if now.duration_since(last) > suspect_timeout {
                                info!(node_id = %node_id, "Marking node SUSPECT");
                                self.mark_node_suspect(&node_id).await;
                            }
                        } else if now.duration_since(leader_start) > suspect_timeout {
                            info!(node_id = %node_id, "Marking node SUSPECT (no heartbeat received)");
                            self.mark_node_suspect(&node_id).await;
                        }
                    }
                    NodeState::Suspect => {
                        if let Some(last) = last_hb {
                            if now.duration_since(last) > suspect_timeout + down_timeout {
                                info!(node_id = %node_id, "Marking node DOWN");
                                self.mark_node_down(&node_id).await;
                            }
                        } else if now.duration_since(leader_start) > suspect_timeout + down_timeout
                        {
                            info!(node_id = %node_id, "Marking node DOWN (no heartbeat received)");
                            self.mark_node_down(&node_id).await;
                        }
                    }
                    NodeState::Joining => {
                        if let Some(last) = last_hb {
                            if now.duration_since(last) > self.config.joining_timeout {
                                info!(node_id = %node_id, "Marking JOINING node DOWN (timeout)");
                                self.mark_node_down(&node_id).await;
                            }
                        } else if now.duration_since(leader_start) > self.config.joining_timeout {
                            info!(node_id = %node_id, "Marking JOINING node DOWN (no heartbeat)");
                            self.mark_node_down(&node_id).await;
                        }
                    }
                    NodeState::Draining => {
                        if let Some(last) = last_hb {
                            if now.duration_since(last) > self.config.draining_timeout {
                                info!(node_id = %node_id, "Marking DRAINING node DOWN (timeout)");
                                self.mark_node_down(&node_id).await;
                            }
                        } else if now.duration_since(leader_start) > self.config.draining_timeout {
                            info!(node_id = %node_id, "Marking DRAINING node DOWN (no heartbeat)");
                            self.mark_node_down(&node_id).await;
                        }
                    }
                    _ => {}
                }

                self.maybe_reset_flap_count(&node_id, now);
            }
        }
    }

    async fn mark_node_suspect(&self, node_id: &str) {
        let now = Instant::now();
        {
            let mut flap_info = self.node_flap_info.write();
            let entry = flap_info
                .entry(node_id.to_string())
                .or_insert_with(|| NodeFlap {
                    suspect_count: 0,
                    last_suspect_at: now,
                    last_stable_at: now,
                });
            entry.suspect_count += 1;
            entry.last_suspect_at = now;
        }

        let cmd = ClusterCommand::SetNodeState {
            node_id: node_id.to_string(),
            state: NodeState::Suspect as i32,
        };
        let _ = self.apply_and_write(cmd).await;
    }

    async fn mark_node_down(&self, node_id: &str) {
        let cmd = ClusterCommand::SetNodeState {
            node_id: node_id.to_string(),
            state: NodeState::Down as i32,
        };
        let _ = self.apply_and_write(cmd).await;
    }

    fn maybe_reset_flap_count(&self, node_id: &str, now: Instant) {
        let mut flap_info = self.node_flap_info.write();
        if let Some(entry) = flap_info.get_mut(node_id) {
            if now.duration_since(entry.last_stable_at) >= self.config.stability_window
                && now.duration_since(entry.last_suspect_at) >= self.config.stability_window
            {
                entry.suspect_count = 0;
            }
        }
    }

    fn reset_flap_on_recovery(&self, node_id: &str) {
        let mut flap_info = self.node_flap_info.write();
        if let Some(entry) = flap_info.get_mut(node_id) {
            entry.last_stable_at = Instant::now();
        }
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

        crate::validation::validate_node_info(&node)?;

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
        request: Request<WatchTopologyRequest>,
    ) -> Result<Response<Self::WatchTopologyStream>, Status> {
        self.global_limiter.try_increment_watch()?;
        let limiter = Arc::clone(&self.global_limiter);
        let req = request.into_inner();
        let since_version = req.since_version;
        let current_version = self.read_topology().await.version;
        let keepalive_interval = self.config.keepalive_interval;
        let full_sync_threshold = self.config.full_sync_threshold;

        let initial_full_sync = if current_version > since_version
            && current_version.saturating_sub(since_version) > full_sync_threshold
        {
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
            Some(GetTopologyResponse {
                version: topo.version,
                frontend_nodes: vec![],
                metadata_nodes,
                object_nodes,
                hash_ring_config: Some(topo.hash_ring_config.to_proto()),
                leader_id,
            })
        } else {
            None
        };

        let rx = self.topology_updates.subscribe();

        let output_stream = stream! {
            struct WatchDropGuard(Arc<GlobalRateLimiter>);
            impl Drop for WatchDropGuard {
                fn drop(&mut self) {
                    self.0.decrement_watch();
                }
            }
            let _watch_guard = WatchDropGuard(limiter);

            if let Some(topology_response) = initial_full_sync {
                yield Ok(TopologyUpdate {
                    version: topology_response.version,
                    previous_version: since_version,
                    update: Some(gitstratum_proto::topology_update::Update::FullSync(
                        topology_response,
                    )),
                });
            }

            let mut broadcast_stream = BroadcastStream::new(rx);
            let mut keepalive_timer = tokio::time::interval(keepalive_interval);
            keepalive_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    result = broadcast_stream.next() => {
                        match result {
                            Some(Ok(update)) => {
                                yield Ok(update);
                            }
                            Some(Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(missed))) => {
                                let server_time = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as u64;
                                yield Ok(TopologyUpdate {
                                    version: 0,
                                    previous_version: 0,
                                    update: Some(gitstratum_proto::topology_update::Update::Lagged(
                                        Lagged {
                                            current_version: server_time,
                                            missed_updates: missed,
                                            message: format!("Client lagged behind by {} updates", missed),
                                        },
                                    )),
                                });
                                break;
                            }
                            None => {
                                break;
                            }
                        }
                    }
                    _ = keepalive_timer.tick() => {
                        let server_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        yield Ok(TopologyUpdate {
                            version: 0,
                            previous_version: 0,
                            update: Some(gitstratum_proto::topology_update::Update::Keepalive(
                                Keepalive { server_time },
                            )),
                        });
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn get_topology(
        &self,
        _request: Request<GetTopologyRequest>,
    ) -> Result<Response<GetTopologyResponse>, Status> {
        self.global_limiter.try_topology_read()?;
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
        self.global_limiter.try_register()?;
        self.ensure_leader()?;

        let req = request.into_inner();
        let node = req
            .node
            .ok_or_else(|| Status::invalid_argument("Missing node"))?;

        crate::validation::validate_node_info(&node)?;

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
        self.global_limiter.try_heartbeat()?;
        let req = request.into_inner();

        if req.node_id.is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }
        if !req.generation_id.is_empty() && !crate::validation::is_valid_uuid(&req.generation_id) {
            return Err(Status::invalid_argument("invalid generation_id format"));
        }

        let node_id = req.node_id.clone();
        self.last_heartbeat
            .write()
            .insert(node_id.clone(), Instant::now());

        let topo = self.read_topology().await;

        let registered_node = topo
            .metadata_nodes
            .get(&req.node_id)
            .or_else(|| topo.object_nodes.get(&req.node_id));

        if let Some(node) = registered_node {
            if !req.generation_id.is_empty() && node.generation_id != req.generation_id {
                return Err(Status::failed_precondition("Generation ID mismatch"));
            }
            if node.state() == NodeState::Suspect {
                self.reset_flap_on_recovery(&node_id);
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::RateLimitError;
    use std::time::Duration;
    use tonic::Code;

    #[test]
    fn test_rate_limit_error_to_status_global() {
        let err = RateLimitError::GlobalLimitExceeded {
            operation: "RegisterNode".to_string(),
            limit: "100/sec".to_string(),
            retry_after: Duration::from_secs(1),
        };
        let status: Status = err.into();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert!(status.message().contains("RegisterNode"));
    }

    #[test]
    fn test_rate_limit_error_to_status_per_client() {
        let err = RateLimitError::PerClientLimitExceeded {
            operation: "Heartbeat".to_string(),
            limit: "100/min".to_string(),
            retry_after: Duration::from_millis(500),
        };
        let status: Status = err.into();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert!(status.message().contains("Heartbeat"));
    }

    #[test]
    fn test_rate_limit_error_to_status_too_many_watchers() {
        let err = RateLimitError::TooManyWatchers {
            current: 1000,
            max: 1000,
        };
        let status: Status = err.into();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert!(status.message().contains("1000"));
    }

    #[test]
    fn test_node_flap_struct() {
        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: 3,
            last_suspect_at: now,
            last_stable_at: now,
        };
        assert_eq!(flap.suspect_count, 3);
    }

    #[test]
    fn test_node_flap_increment() {
        let now = Instant::now();
        let mut flap = NodeFlap {
            suspect_count: 0,
            last_suspect_at: now,
            last_stable_at: now,
        };
        flap.suspect_count += 1;
        assert_eq!(flap.suspect_count, 1);
        flap.last_suspect_at = Instant::now();
        assert!(flap.last_suspect_at >= now);
    }
}
