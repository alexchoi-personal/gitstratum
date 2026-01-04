use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_stream::stream;
use futures::Stream;
use parking_lot::RwLock;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

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
use crate::heartbeat_batcher::{HeartbeatBatcher, HeartbeatInfo};
use crate::rate_limit::{ClientRateLimiter, GlobalRateLimiter, RateLimitError};
use crate::state_machine::{apply_command, deserialize_topology, serialize_topology, topology_key};
use crate::topology::{ClusterTopology, NodeEntry};

struct ClientLimiterEntry {
    limiter: ClientRateLimiter,
}

impl ClientLimiterEntry {
    fn new() -> Self {
        Self {
            limiter: ClientRateLimiter::new(),
        }
    }
}

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
    topology_cache: Arc<RwLock<ClusterTopology>>,
    topology_updates: broadcast::Sender<TopologyUpdate>,
    config: CoordinatorConfig,
    heartbeat_batcher: Arc<HeartbeatBatcher>,
    leader_since: RwLock<Option<Instant>>,
    last_heartbeat: RwLock<HashMap<String, Instant>>,
    node_flap_info: RwLock<HashMap<String, NodeFlap>>,
    global_limiter: Arc<GlobalRateLimiter>,
    client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>>,
    write_lock: Mutex<()>,
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
            topology_cache: Arc::new(RwLock::new(ClusterTopology::default())),
            topology_updates: tx,
            config,
            heartbeat_batcher,
            leader_since: RwLock::new(None),
            last_heartbeat: RwLock::new(HashMap::new()),
            node_flap_info: RwLock::new(HashMap::new()),
            global_limiter,
            client_limiters: Arc::new(RwLock::new(HashMap::new())),
            write_lock: Mutex::new(()),
        }
    }

    fn extract_client_id<T>(request: &Request<T>) -> String {
        if let Some(addr) = request.remote_addr() {
            addr.ip().to_string()
        } else {
            "unknown".to_string()
        }
    }

    fn get_client_limiter(
        &self,
        client_id: &str,
    ) -> Arc<RwLock<HashMap<String, ClientLimiterEntry>>> {
        {
            let limiters = self.client_limiters.read();
            if limiters.contains_key(client_id) {
                return Arc::clone(&self.client_limiters);
            }
        }
        {
            let mut limiters = self.client_limiters.write();
            limiters
                .entry(client_id.to_string())
                .or_insert_with(ClientLimiterEntry::new);
        }
        Arc::clone(&self.client_limiters)
    }

    fn try_client_heartbeat(&self, client_id: &str) -> Result<(), RateLimitError> {
        let _ = self.get_client_limiter(client_id);
        let limiters = self.client_limiters.read();
        if let Some(entry) = limiters.get(client_id) {
            entry.limiter.try_heartbeat()
        } else {
            Ok(())
        }
    }

    fn try_client_register(&self, client_id: &str) -> Result<(), RateLimitError> {
        let _ = self.get_client_limiter(client_id);
        let limiters = self.client_limiters.read();
        if let Some(entry) = limiters.get(client_id) {
            entry.limiter.try_register()
        } else {
            Ok(())
        }
    }

    fn try_client_topology_read(&self, client_id: &str) -> Result<(), RateLimitError> {
        let _ = self.get_client_limiter(client_id);
        let limiters = self.client_limiters.read();
        if let Some(entry) = limiters.get(client_id) {
            entry.limiter.try_topology_read()
        } else {
            Ok(())
        }
    }

    fn try_client_watch(&self, client_id: &str) -> Result<(), RateLimitError> {
        let _ = self.get_client_limiter(client_id);
        let limiters = self.client_limiters.read();
        if let Some(entry) = limiters.get(client_id) {
            entry.limiter.try_increment_watch()
        } else {
            Ok(())
        }
    }

    fn decrement_client_watch(&self, client_id: &str) {
        let limiters = self.client_limiters.read();
        if let Some(entry) = limiters.get(client_id) {
            entry.limiter.decrement_watch();
        }
    }

    pub async fn run_client_limiter_cleanup(&self) {
        loop {
            tokio::time::sleep(self.config.client_limiter_cleanup_interval).await;

            let now = Instant::now();
            let max_idle = self.config.client_limiter_max_idle;

            let to_remove: Vec<String> = {
                let limiters = self.client_limiters.read();
                limiters
                    .iter()
                    .filter(|(_, entry)| {
                        !entry.limiter.has_active_watches()
                            && now.duration_since(entry.limiter.last_access_time()) > max_idle
                    })
                    .map(|(k, _)| k.clone())
                    .collect()
            };

            if !to_remove.is_empty() {
                let mut limiters = self.client_limiters.write();
                for client_id in to_remove {
                    limiters.remove(&client_id);
                }
            }
        }
    }

    pub fn client_limiter_count(&self) -> usize {
        self.client_limiters.read().len()
    }

    fn is_leader(&self) -> bool {
        self.raft.leader_election().is_leader()
    }

    fn get_leader_address(&self) -> Option<String> {
        let metrics = self.raft.raft().metrics().borrow().clone();
        let leader_id = metrics.current_leader?;
        let node = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)?;
        Some(node.addr.clone())
    }

    fn ensure_leader(&self) -> Result<(), CoordinatorError> {
        if self.is_leader() {
            Ok(())
        } else {
            Err(CoordinatorError::NotLeader(self.get_leader_address()))
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
                match deserialize_topology(value) {
                    Ok(topology) => return topology,
                    Err(e) => {
                        warn!(
                            error = %e,
                            "failed to deserialize topology from raft store, using default"
                        );
                    }
                }
            }
        }
        ClusterTopology::default()
    }

    async fn apply_and_write(
        &self,
        cmd: ClusterCommand,
    ) -> Result<crate::commands::ClusterResponse, CoordinatorError> {
        let _guard = self.write_lock.lock().await;
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

            let all_nodes: Vec<(String, NodeState)> = topology
                .object_nodes
                .iter()
                .chain(topology.metadata_nodes.iter())
                .map(|(id, entry)| (id.clone(), entry.state()))
                .collect();

            for (node_id, current_state) in all_nodes {
                let last_hb = self.last_heartbeat.read().get(&node_id).copied();
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
            entry.suspect_count = entry.suspect_count.saturating_add(1);
            entry.last_suspect_at = now;
        }

        if let Err(e) = self
            .set_node_state_and_broadcast(node_id, NodeState::Suspect as i32)
            .await
        {
            tracing::warn!(node_id = %node_id, error = %e, "Failed to mark node as SUSPECT via Raft");
        }
    }

    async fn mark_node_down(&self, node_id: &str) {
        if let Err(e) = self
            .set_node_state_and_broadcast(node_id, NodeState::Down as i32)
            .await
        {
            tracing::warn!(node_id = %node_id, error = %e, "Failed to mark node as DOWN via Raft");
        }
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

    fn cleanup_node_state(&self, node_id: &str) {
        self.last_heartbeat.write().remove(node_id);
        self.node_flap_info.write().remove(node_id);
    }

    async fn set_node_state_and_broadcast(
        &self,
        node_id: &str,
        state: i32,
    ) -> Result<crate::commands::ClusterResponse, CoordinatorError> {
        let previous_version = self.topology_cache.read().version;
        let cmd = ClusterCommand::SetNodeState {
            node_id: node_id.to_string(),
            state,
        };
        let resp = self.apply_and_write(cmd).await?;

        if resp.is_success() {
            let cache = self.topology_cache.read();
            let node_info = cache
                .object_nodes
                .get(node_id)
                .map(|n| n.to_proto(NodeType::Object))
                .or_else(|| {
                    cache
                        .metadata_nodes
                        .get(node_id)
                        .map(|n| n.to_proto(NodeType::Metadata))
                });

            if let Some(info) = node_info {
                let _ = self.topology_updates.send(TopologyUpdate {
                    version: resp.version().unwrap_or(0),
                    previous_version,
                    update: Some(gitstratum_proto::topology_update::Update::NodeUpdated(info)),
                });
            }
        }

        Ok(resp)
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

        let previous_version = self.topology_cache.read().version;
        let resp = self.apply_and_write(cmd).await?;

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version,
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
        if !crate::validation::is_valid_node_id(&req.node_id) {
            return Err(Status::invalid_argument(
                "Node ID must be 1-63 alphanumeric characters or hyphens, without leading/trailing hyphens",
            ));
        }
        let cmd = ClusterCommand::RemoveNode {
            node_id: req.node_id.clone(),
        };

        let previous_version = self.topology_cache.read().version;
        let resp = self.apply_and_write(cmd).await?;

        self.cleanup_node_state(&req.node_id);

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version,
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
        if !crate::validation::is_valid_node_id(&req.node_id) {
            return Err(Status::invalid_argument(
                "Node ID must be 1-63 alphanumeric characters or hyphens, without leading/trailing hyphens",
            ));
        }
        if !(0..=5).contains(&req.state) {
            return Err(Status::invalid_argument(
                "Invalid state value: must be 0-5 (Unknown, Active, Joining, Draining, Down, Suspect)",
            ));
        }

        let resp = self
            .set_node_state_and_broadcast(&req.node_id, req.state)
            .await?;

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
        let client_id = Self::extract_client_id(&request);
        self.global_limiter.try_increment_watch()?;
        self.try_client_watch(&client_id)?;
        let limiter = Arc::clone(&self.global_limiter);
        let client_limiters = Arc::clone(&self.client_limiters);
        let client_id_for_stream = client_id.clone();
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
        let topology_cache = Arc::clone(&self.topology_cache);

        let output_stream = stream! {
            struct WatchDropGuard {
                global_limiter: Arc<GlobalRateLimiter>,
                client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>>,
                client_id: String,
            }
            impl Drop for WatchDropGuard {
                fn drop(&mut self) {
                    self.global_limiter.decrement_watch();
                    let limiters = self.client_limiters.read();
                    if let Some(entry) = limiters.get(&self.client_id) {
                        entry.limiter.decrement_watch();
                    }
                }
            }
            let _watch_guard = WatchDropGuard {
                global_limiter: limiter,
                client_limiters,
                client_id: client_id_for_stream,
            };

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
                                let current_topo_version = topology_cache.read().version;
                                yield Ok(TopologyUpdate {
                                    version: current_topo_version,
                                    previous_version: 0,
                                    update: Some(gitstratum_proto::topology_update::Update::Lagged(
                                        Lagged {
                                            current_version: current_topo_version,
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
        request: Request<GetTopologyRequest>,
    ) -> Result<Response<GetTopologyResponse>, Status> {
        let client_id = Self::extract_client_id(&request);
        self.global_limiter.try_topology_read()?;
        self.try_client_topology_read(&client_id)?;
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
        let metrics = self.raft.raft().metrics().borrow().clone();
        let is_leader = self.raft.leader_election().is_leader();

        let raft_state = if is_leader {
            RaftState::Leader
        } else {
            RaftState::Follower
        };

        let (leader_id, leader_address) = if let Some(lid) = &metrics.current_leader {
            let addr = metrics
                .membership_config
                .membership()
                .get_node(lid)
                .map(|n| n.addr.clone())
                .unwrap_or_default();
            (lid.to_string(), addr)
        } else {
            (String::new(), String::new())
        };

        let (active_count, suspect_count, down_count) = topo
            .metadata_nodes
            .values()
            .chain(topo.object_nodes.values())
            .fold((0u32, 0u32, 0u32), |(a, s, d), node| match node.state() {
                NodeState::Active | NodeState::Joining => (a + 1, s, d),
                NodeState::Suspect => (a, s + 1, d),
                NodeState::Down | NodeState::Draining => (a, s, d + 1),
                _ => (a, s, d),
            });

        let healthy = metrics.current_leader.is_some();
        let committed_index = metrics.last_log_index.unwrap_or(0);
        let applied_index = metrics.last_applied.map(|l| l.index).unwrap_or(0);

        Ok(Response::new(HealthCheckResponse {
            healthy,
            raft_state: raft_state.into(),
            raft_term: metrics.current_term,
            committed_index,
            applied_index,
            leader_id,
            leader_address,
            topology_version: topo.version,
            active_nodes: active_count,
            suspect_nodes: suspect_count,
            down_nodes: down_count,
            watch_subscribers: self.topology_updates.receiver_count() as u32,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let client_id = Self::extract_client_id(&request);
        self.global_limiter.try_register()?;
        self.try_client_register(&client_id)?;
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

        let previous_version = self.topology_cache.read().version;
        let resp = self.apply_and_write(cmd).await?;

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version,
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
        let client_id = Self::extract_client_id(&request);
        self.global_limiter.try_register()?;
        self.try_client_register(&client_id)?;
        self.ensure_leader()?;

        let req = request.into_inner();
        if !crate::validation::is_valid_node_id(&req.node_id) {
            return Err(Status::invalid_argument(
                "Node ID must be 1-63 alphanumeric characters or hyphens, without leading/trailing hyphens",
            ));
        }
        let cmd = ClusterCommand::RemoveNode {
            node_id: req.node_id.clone(),
        };

        let previous_version = self.topology_cache.read().version;
        let resp = self.apply_and_write(cmd).await?;

        self.cleanup_node_state(&req.node_id);

        let _ = self.topology_updates.send(TopologyUpdate {
            version: resp.version().unwrap_or(0),
            previous_version,
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
        let client_id = Self::extract_client_id(&request);
        self.global_limiter.try_heartbeat()?;
        self.try_client_heartbeat(&client_id)?;
        let req = request.into_inner();

        if req.node_id.is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }
        if !req.generation_id.is_empty() && !crate::validation::is_valid_uuid(&req.generation_id) {
            return Err(Status::invalid_argument("invalid generation_id format"));
        }

        let node_id = req.node_id.clone();
        let now = Instant::now();

        let topo = self.read_topology().await;

        let registered_node = topo
            .metadata_nodes
            .get(&req.node_id)
            .or_else(|| topo.object_nodes.get(&req.node_id));

        if let Some(node) = registered_node {
            if req.generation_id.is_empty() {
                return Err(Status::invalid_argument(
                    "generation_id is required for registered nodes",
                ));
            }
            if node.generation_id != req.generation_id {
                return Err(Status::failed_precondition("Generation ID mismatch"));
            }

            if self.is_leader() {
                self.heartbeat_batcher.record_heartbeat(
                    node_id.clone(),
                    HeartbeatInfo::new(
                        req.known_version,
                        req.reported_state,
                        req.generation_id.clone(),
                    ),
                );
            }

            self.last_heartbeat.write().insert(node_id.clone(), now);

            if self.is_leader() {
                let current_state = node.state();
                if current_state == NodeState::Suspect || current_state == NodeState::Joining {
                    if current_state == NodeState::Suspect {
                        self.reset_flap_on_recovery(&node_id);
                    }
                    if let Err(e) = self
                        .set_node_state_and_broadcast(&node_id, NodeState::Active as i32)
                        .await
                    {
                        tracing::warn!(node_id = %node_id, error = %e, "Failed to transition node to ACTIVE via Raft");
                    }
                }
            }
        }

        let metrics = self.raft.raft().metrics().borrow().clone();

        let (leader_id, leader_address) = if let Some(lid) = &metrics.current_leader {
            let addr = metrics
                .membership_config
                .membership()
                .get_node(lid)
                .map(|n| n.addr.clone())
                .unwrap_or_default();
            (lid.to_string(), addr)
        } else {
            (String::new(), String::new())
        };

        let refresh_required = req.known_version < topo.version;

        Ok(Response::new(HeartbeatResponse {
            current_version: topo.version,
            refresh_required,
            leader_id,
            leader_address,
            raft_term: metrics.current_term,
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

    fn test_heartbeat_info_creation() {
        let now = Instant::now();
        let info = HeartbeatInfo {
            known_version: 42,
            reported_state: 1,
            generation_id: "gen-123".to_string(),
            received_at: now,
        };
        assert_eq!(info.known_version, 42);
        assert_eq!(info.reported_state, 1);
        assert_eq!(info.generation_id, "gen-123");
        assert!(info.received_at <= Instant::now());
    }

    #[test]
    fn test_heartbeat_batcher_records_heartbeat() {
        let batcher = HeartbeatBatcher::new(Duration::from_secs(1));
        let now = Instant::now();

        let info = HeartbeatInfo {
            known_version: 10,
            reported_state: 1,
            generation_id: "gen-456".to_string(),
            received_at: now,
        };
        batcher.record_heartbeat("node-1".to_string(), info);

        assert_eq!(batcher.pending_count(), 1);

        let batch = batcher.take_batch();
        assert!(batch.contains_key("node-1"));
        let recorded = batch.get("node-1").unwrap();
        assert_eq!(recorded.known_version, 10);
        assert_eq!(recorded.generation_id, "gen-456");
    }

    #[test]
    fn test_lagged_notification_uses_topology_version_not_timestamp() {
        let topology_version: u64 = 42;
        let missed: u64 = 5;

        let lagged = Lagged {
            current_version: topology_version,
            missed_updates: missed,
            message: format!("Client lagged behind by {} updates", missed),
        };

        assert_eq!(lagged.current_version, topology_version);
        assert!(
            lagged.current_version < 1_000_000,
            "current_version should be a topology version (small number), not a timestamp"
        );

        let update = TopologyUpdate {
            version: topology_version,
            previous_version: 0,
            update: Some(gitstratum_proto::topology_update::Update::Lagged(lagged)),
        };

        assert_eq!(update.version, topology_version);
        if let Some(gitstratum_proto::topology_update::Update::Lagged(lagged_inner)) = update.update
        {
            assert_eq!(lagged_inner.current_version, topology_version);
            assert_eq!(lagged_inner.missed_updates, 5);
        } else {
            panic!("Expected Lagged update variant");
        }
    }

    #[test]
    fn test_client_limiter_entry_creation() {
        let entry = ClientLimiterEntry::new();
        assert_eq!(entry.limiter.watch_count(), 0);
    }

    #[test]
    fn test_client_limiter_heartbeat_limit() {
        let client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));

        {
            let mut limiters = client_limiters.write();
            limiters.insert("client-1".to_string(), ClientLimiterEntry::new());
        }

        let limiters = client_limiters.read();
        let entry = limiters.get("client-1").unwrap();

        for _ in 0..100 {
            assert!(entry.limiter.try_heartbeat().is_ok());
        }
        assert!(entry.limiter.try_heartbeat().is_err());
    }

    #[test]
    fn test_client_limiter_register_limit() {
        let client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));

        {
            let mut limiters = client_limiters.write();
            limiters.insert("client-1".to_string(), ClientLimiterEntry::new());
        }

        let limiters = client_limiters.read();
        let entry = limiters.get("client-1").unwrap();

        for _ in 0..10 {
            assert!(entry.limiter.try_register().is_ok());
        }
        assert!(entry.limiter.try_register().is_err());
    }

    #[test]
    fn test_client_limiter_topology_read_limit() {
        let client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));

        {
            let mut limiters = client_limiters.write();
            limiters.insert("client-1".to_string(), ClientLimiterEntry::new());
        }

        let limiters = client_limiters.read();
        let entry = limiters.get("client-1").unwrap();

        for _ in 0..1_000 {
            assert!(entry.limiter.try_topology_read().is_ok());
        }
        assert!(entry.limiter.try_topology_read().is_err());
    }

    #[test]
    fn test_client_limiter_watch_limit() {
        let client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));

        {
            let mut limiters = client_limiters.write();
            limiters.insert("client-1".to_string(), ClientLimiterEntry::new());
        }

        let limiters = client_limiters.read();
        let entry = limiters.get("client-1").unwrap();

        for _ in 0..10 {
            assert!(entry.limiter.try_increment_watch().is_ok());
        }
        assert!(entry.limiter.try_increment_watch().is_err());

        entry.limiter.decrement_watch();
        assert!(entry.limiter.try_increment_watch().is_ok());
    }

    #[test]
    fn test_client_limiter_isolation() {
        let client_limiters: Arc<RwLock<HashMap<String, ClientLimiterEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));

        {
            let mut limiters = client_limiters.write();
            limiters.insert("client-1".to_string(), ClientLimiterEntry::new());
            limiters.insert("client-2".to_string(), ClientLimiterEntry::new());
        }

        let limiters = client_limiters.read();
        let entry1 = limiters.get("client-1").unwrap();
        let entry2 = limiters.get("client-2").unwrap();

        for _ in 0..10 {
            assert!(entry1.limiter.try_register().is_ok());
        }
        assert!(entry1.limiter.try_register().is_err());

        for _ in 0..10 {
            assert!(entry2.limiter.try_register().is_ok());
        }
        assert!(entry2.limiter.try_register().is_err());
    }

    #[test]
    fn test_client_limiter_last_access_updated() {
        let entry = ClientLimiterEntry::new();
        let before = entry.limiter.last_access_time();

        std::thread::sleep(Duration::from_millis(10));
        entry.limiter.try_heartbeat().unwrap();

        let after = entry.limiter.last_access_time();
        assert!(after > before);
    }

    #[test]
    fn test_client_limiter_has_active_watches() {
        let entry = ClientLimiterEntry::new();
        assert!(!entry.limiter.has_active_watches());

        entry.limiter.try_increment_watch().unwrap();
        assert!(entry.limiter.has_active_watches());

        entry.limiter.decrement_watch();
        assert!(!entry.limiter.has_active_watches());
        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: 0,
            last_suspect_at: now,
            last_stable_at: now,
        };
        assert_eq!(flap.suspect_count, 0);
        assert!(flap.last_suspect_at <= Instant::now());
        assert!(flap.last_stable_at <= Instant::now());
    }

    #[test]
    fn test_node_flap_timestamps_ordering() {
        let earlier = Instant::now();
        std::thread::sleep(Duration::from_millis(1));
        let later = Instant::now();

        let flap = NodeFlap {
            suspect_count: 1,
            last_suspect_at: later,
            last_stable_at: earlier,
        };

        assert!(flap.last_suspect_at > flap.last_stable_at);
    }

    #[test]
    fn test_node_flap_suspect_count_overflow_protection() {
        let now = Instant::now();
        let mut flap = NodeFlap {
            suspect_count: u32::MAX - 1,
            last_suspect_at: now,
            last_stable_at: now,
        };
        flap.suspect_count = flap.suspect_count.saturating_add(1);
        assert_eq!(flap.suspect_count, u32::MAX);
        flap.suspect_count = flap.suspect_count.saturating_add(1);
        assert_eq!(flap.suspect_count, u32::MAX);
    }

    #[test]
    fn test_node_flap_multiple_suspects() {
        let now = Instant::now();
        let mut flap = NodeFlap {
            suspect_count: 0,
            last_suspect_at: now,
            last_stable_at: now,
        };

        for i in 1..=10 {
            flap.suspect_count += 1;
            flap.last_suspect_at = Instant::now();
            assert_eq!(flap.suspect_count, i);
        }
    }

    #[test]
    fn test_node_flap_reset_count() {
        let now = Instant::now();
        let mut flap = NodeFlap {
            suspect_count: 5,
            last_suspect_at: now,
            last_stable_at: now,
        };

        flap.suspect_count = 0;
        assert_eq!(flap.suspect_count, 0);
    }

    fn calculate_timeout_with_flap(
        config: &CoordinatorConfig,
        flap_info: Option<&NodeFlap>,
    ) -> (Duration, Duration) {
        let base_suspect = config.suspect_timeout;
        let base_down = config.down_timeout;

        if let Some(flap) = flap_info {
            let now = Instant::now();
            if flap.suspect_count >= config.flap_threshold
                && now.duration_since(flap.last_suspect_at) < config.flap_window
            {
                let multiplier = config.flap_multiplier;
                return (
                    base_suspect.mul_f32(multiplier),
                    base_down.mul_f32(multiplier),
                );
            }
        }

        (base_suspect, base_down)
    }

    #[test]
    fn test_timeout_calculation_no_flap() {
        let config = CoordinatorConfig::default();
        let (suspect, down) = calculate_timeout_with_flap(&config, None);
        assert_eq!(suspect, config.suspect_timeout);
        assert_eq!(down, config.down_timeout);
    }

    #[test]
    fn test_timeout_calculation_below_threshold() {
        let config = CoordinatorConfig::default();
        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: config.flap_threshold - 1,
            last_suspect_at: now,
            last_stable_at: now,
        };
        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        assert_eq!(suspect, config.suspect_timeout);
        assert_eq!(down, config.down_timeout);
    }

    #[test]
    fn test_timeout_calculation_at_threshold() {
        let config = CoordinatorConfig::default();
        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: config.flap_threshold,
            last_suspect_at: now,
            last_stable_at: now,
        };
        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        let expected_suspect = config.suspect_timeout.mul_f32(config.flap_multiplier);
        let expected_down = config.down_timeout.mul_f32(config.flap_multiplier);
        assert_eq!(suspect, expected_suspect);
        assert_eq!(down, expected_down);
    }

    #[test]
    fn test_timeout_calculation_above_threshold() {
        let config = CoordinatorConfig::default();
        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: config.flap_threshold + 5,
            last_suspect_at: now,
            last_stable_at: now,
        };
        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        let expected_suspect = config.suspect_timeout.mul_f32(config.flap_multiplier);
        let expected_down = config.down_timeout.mul_f32(config.flap_multiplier);
        assert_eq!(suspect, expected_suspect);
        assert_eq!(down, expected_down);
    }

    #[test]
    fn test_timeout_calculation_outside_flap_window() {
        let config = CoordinatorConfig {
            flap_window: Duration::from_millis(10),
            ..CoordinatorConfig::default()
        };

        let old_time = Instant::now() - Duration::from_millis(100);
        let flap = NodeFlap {
            suspect_count: config.flap_threshold + 1,
            last_suspect_at: old_time,
            last_stable_at: old_time,
        };
        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        assert_eq!(suspect, config.suspect_timeout);
        assert_eq!(down, config.down_timeout);
    }

    #[test]
    fn test_timeout_calculation_custom_multiplier() {
        let config = CoordinatorConfig {
            flap_multiplier: 3.0,
            ..CoordinatorConfig::default()
        };

        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: config.flap_threshold,
            last_suspect_at: now,
            last_stable_at: now,
        };
        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        let expected_suspect = config.suspect_timeout.mul_f32(3.0);
        let expected_down = config.down_timeout.mul_f32(3.0);
        assert_eq!(suspect, expected_suspect);
        assert_eq!(down, expected_down);
    }

    fn should_reset_flap_count(flap: &NodeFlap, stability_window: Duration, now: Instant) -> bool {
        now.duration_since(flap.last_stable_at) >= stability_window
            && now.duration_since(flap.last_suspect_at) >= stability_window
    }

    #[test]
    fn test_reset_flap_count_both_conditions_met() {
        let old_time = Instant::now() - Duration::from_secs(400);
        let flap = NodeFlap {
            suspect_count: 5,
            last_suspect_at: old_time,
            last_stable_at: old_time,
        };
        let stability_window = Duration::from_secs(300);
        assert!(should_reset_flap_count(
            &flap,
            stability_window,
            Instant::now()
        ));
    }

    #[test]
    fn test_reset_flap_count_recent_suspect() {
        let old_time = Instant::now() - Duration::from_secs(400);
        let recent_time = Instant::now() - Duration::from_secs(100);
        let flap = NodeFlap {
            suspect_count: 5,
            last_suspect_at: recent_time,
            last_stable_at: old_time,
        };
        let stability_window = Duration::from_secs(300);
        assert!(!should_reset_flap_count(
            &flap,
            stability_window,
            Instant::now()
        ));
    }

    #[test]
    fn test_reset_flap_count_recent_stable() {
        let old_time = Instant::now() - Duration::from_secs(400);
        let recent_time = Instant::now() - Duration::from_secs(100);
        let flap = NodeFlap {
            suspect_count: 5,
            last_suspect_at: old_time,
            last_stable_at: recent_time,
        };
        let stability_window = Duration::from_secs(300);
        assert!(!should_reset_flap_count(
            &flap,
            stability_window,
            Instant::now()
        ));
    }

    #[test]
    fn test_reset_flap_count_both_recent() {
        let recent_time = Instant::now() - Duration::from_secs(100);
        let flap = NodeFlap {
            suspect_count: 5,
            last_suspect_at: recent_time,
            last_stable_at: recent_time,
        };
        let stability_window = Duration::from_secs(300);
        assert!(!should_reset_flap_count(
            &flap,
            stability_window,
            Instant::now()
        ));
    }

    #[test]
    fn test_reset_flap_count_exactly_at_boundary() {
        let boundary_time = Instant::now() - Duration::from_secs(300);
        let flap = NodeFlap {
            suspect_count: 5,
            last_suspect_at: boundary_time,
            last_stable_at: boundary_time,
        };
        let stability_window = Duration::from_secs(300);
        assert!(should_reset_flap_count(
            &flap,
            stability_window,
            Instant::now()
        ));
    }

    #[test]
    fn test_flap_tracking_workflow() {
        let config = CoordinatorConfig {
            flap_threshold: 3,
            flap_window: Duration::from_secs(600),
            flap_multiplier: 2.0,
            stability_window: Duration::from_secs(300),
            ..CoordinatorConfig::default()
        };

        let now = Instant::now();
        let mut flap = NodeFlap {
            suspect_count: 0,
            last_suspect_at: now,
            last_stable_at: now,
        };

        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        assert_eq!(suspect, config.suspect_timeout);
        assert_eq!(down, config.down_timeout);

        for _ in 0..3 {
            flap.suspect_count += 1;
            flap.last_suspect_at = Instant::now();
        }

        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        assert_eq!(suspect, config.suspect_timeout.mul_f32(2.0));
        assert_eq!(down, config.down_timeout.mul_f32(2.0));
    }

    #[test]
    fn test_flap_info_hashmap_operations() {
        let flap_info: RwLock<HashMap<String, NodeFlap>> = RwLock::new(HashMap::new());

        {
            let read_guard = flap_info.read();
            assert!(read_guard.get("node-1").is_none());
        }

        {
            let mut write_guard = flap_info.write();
            let now = Instant::now();
            write_guard.insert(
                "node-1".to_string(),
                NodeFlap {
                    suspect_count: 1,
                    last_suspect_at: now,
                    last_stable_at: now,
                },
            );
        }

        {
            let read_guard = flap_info.read();
            let entry = read_guard.get("node-1").unwrap();
            assert_eq!(entry.suspect_count, 1);
        }

        {
            let mut write_guard = flap_info.write();
            if let Some(entry) = write_guard.get_mut("node-1") {
                entry.suspect_count += 1;
                entry.last_suspect_at = Instant::now();
            }
        }

        {
            let read_guard = flap_info.read();
            let entry = read_guard.get("node-1").unwrap();
            assert_eq!(entry.suspect_count, 2);
        }
    }

    #[test]
    fn test_flap_info_multiple_nodes() {
        let flap_info: RwLock<HashMap<String, NodeFlap>> = RwLock::new(HashMap::new());
        let now = Instant::now();

        {
            let mut write_guard = flap_info.write();
            for i in 1..=5 {
                write_guard.insert(
                    format!("node-{}", i),
                    NodeFlap {
                        suspect_count: i as u32,
                        last_suspect_at: now,
                        last_stable_at: now,
                    },
                );
            }
        }

        {
            let read_guard = flap_info.read();
            assert_eq!(read_guard.len(), 5);
            for i in 1..=5 {
                let entry = read_guard.get(&format!("node-{}", i)).unwrap();
                assert_eq!(entry.suspect_count, i as u32);
            }
        }
    }

    #[test]
    fn test_last_heartbeat_hashmap_operations() {
        let last_heartbeat: RwLock<HashMap<String, Instant>> = RwLock::new(HashMap::new());
        let now = Instant::now();

        {
            let mut write_guard = last_heartbeat.write();
            write_guard.insert("node-1".to_string(), now);
        }

        {
            let read_guard = last_heartbeat.read();
            let hb_time = read_guard.get("node-1").unwrap();
            assert_eq!(*hb_time, now);
        }

        std::thread::sleep(Duration::from_millis(10));
        let later = Instant::now();

        {
            let mut write_guard = last_heartbeat.write();
            write_guard.insert("node-1".to_string(), later);
        }

        {
            let read_guard = last_heartbeat.read();
            let hb_time = read_guard.get("node-1").unwrap();
            assert!(*hb_time > now);
        }
    }

    #[test]
    fn test_last_heartbeat_elapsed_calculation() {
        let last_heartbeat: RwLock<HashMap<String, Instant>> = RwLock::new(HashMap::new());
        let past = Instant::now() - Duration::from_secs(60);

        {
            let mut write_guard = last_heartbeat.write();
            write_guard.insert("node-1".to_string(), past);
        }

        {
            let read_guard = last_heartbeat.read();
            let hb_time = read_guard.get("node-1").unwrap();
            let elapsed = Instant::now().duration_since(*hb_time);
            assert!(elapsed >= Duration::from_secs(60));
        }
    }

    #[test]
    fn test_leader_since_state_transitions() {
        let leader_since: RwLock<Option<Instant>> = RwLock::new(None);

        {
            let read_guard = leader_since.read();
            assert!(read_guard.is_none());
        }

        let now = Instant::now();
        {
            let mut write_guard = leader_since.write();
            *write_guard = Some(now);
        }

        {
            let read_guard = leader_since.read();
            assert!(read_guard.is_some());
            assert_eq!(read_guard.unwrap(), now);
        }

        {
            let mut write_guard = leader_since.write();
            *write_guard = None;
        }

        {
            let read_guard = leader_since.read();
            assert!(read_guard.is_none());
        }
    }

    #[test]
    fn test_grace_period_calculation() {
        let leader_grace_period = Duration::from_secs(90);
        let leader_start = Instant::now() - Duration::from_secs(30);
        let now = Instant::now();

        let in_grace_period = now.duration_since(leader_start) < leader_grace_period;
        assert!(in_grace_period);

        let old_leader_start = Instant::now() - Duration::from_secs(100);
        let not_in_grace = now.duration_since(old_leader_start) < leader_grace_period;
        assert!(!not_in_grace);
    }

    #[test]
    fn test_topology_cache_default() {
        let topology_cache: Arc<RwLock<ClusterTopology>> =
            Arc::new(RwLock::new(ClusterTopology::default()));

        {
            let read_guard = topology_cache.read();
            assert!(read_guard.object_nodes.is_empty());
            assert!(read_guard.metadata_nodes.is_empty());
            assert_eq!(read_guard.version, 0);
        }
    }

    #[test]
    fn test_topology_cache_update() {
        let topology_cache: Arc<RwLock<ClusterTopology>> =
            Arc::new(RwLock::new(ClusterTopology::default()));

        {
            let mut write_guard = topology_cache.write();
            write_guard.version = 42;
            write_guard.object_nodes.insert(
                "node-1".to_string(),
                crate::topology::NodeEntry {
                    id: "node-1".to_string(),
                    address: "192.168.1.1".to_string(),
                    port: 9000,
                    state: gitstratum_proto::NodeState::Active as i32,
                    last_heartbeat_at: 0,
                    suspect_count: 0,
                    generation_id: "gen-1".to_string(),
                    registered_at: 0,
                },
            );
        }

        {
            let read_guard = topology_cache.read();
            assert_eq!(read_guard.version, 42);
            assert_eq!(read_guard.object_nodes.len(), 1);
            assert!(read_guard.object_nodes.contains_key("node-1"));
        }
    }

    #[test]
    fn test_broadcast_channel_creation() {
        let buffer_size = 10_000;
        let (tx, _rx) = broadcast::channel::<TopologyUpdate>(buffer_size);

        let update = TopologyUpdate {
            version: 1,
            previous_version: 0,
            update: None,
        };

        assert!(tx.send(update).is_ok());
    }

    #[test]
    fn test_broadcast_receiver_count() {
        let (tx, _rx1) = broadcast::channel::<TopologyUpdate>(100);

        assert_eq!(tx.receiver_count(), 1);

        let _rx2 = tx.subscribe();
        assert_eq!(tx.receiver_count(), 2);

        let _rx3 = tx.subscribe();
        assert_eq!(tx.receiver_count(), 3);
    }

    #[test]
    fn test_suspect_timeout_ordering() {
        let config = CoordinatorConfig::default();

        assert!(config.suspect_timeout <= config.down_timeout + config.suspect_timeout);

        assert!(config.joining_timeout > Duration::ZERO);
        assert!(config.draining_timeout > Duration::ZERO);
    }

    #[test]
    fn test_config_timeout_relationships() {
        let config = CoordinatorConfig::default();

        assert!(config.suspect_timeout > config.detector_interval);
        assert!(config.down_timeout >= config.suspect_timeout);
        assert!(config.draining_timeout > config.joining_timeout);
    }

    #[test]
    fn test_flap_threshold_boundary_values() {
        let config = CoordinatorConfig {
            flap_threshold: 1,
            ..CoordinatorConfig::default()
        };

        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: 1,
            last_suspect_at: now,
            last_stable_at: now,
        };

        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        assert_eq!(
            suspect,
            config.suspect_timeout.mul_f32(config.flap_multiplier)
        );
        assert_eq!(down, config.down_timeout.mul_f32(config.flap_multiplier));
    }

    #[test]
    fn test_flap_multiplier_one() {
        let config = CoordinatorConfig {
            flap_multiplier: 1.0,
            ..CoordinatorConfig::default()
        };

        let now = Instant::now();
        let flap = NodeFlap {
            suspect_count: config.flap_threshold,
            last_suspect_at: now,
            last_stable_at: now,
        };

        let (suspect, down) = calculate_timeout_with_flap(&config, Some(&flap));
        assert_eq!(suspect, config.suspect_timeout);
        assert_eq!(down, config.down_timeout);
    }

    #[test]
    fn test_write_lock_mutex() {
        let write_lock: Mutex<()> = Mutex::new(());

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let _guard = write_lock.lock().await;
        });
    }

    #[test]
    fn test_cleanup_node_state_removes_from_both_maps() {
        let last_heartbeat: RwLock<HashMap<String, Instant>> = RwLock::new(HashMap::new());
        let node_flap_info: RwLock<HashMap<String, NodeFlap>> = RwLock::new(HashMap::new());
        let now = Instant::now();

        {
            let mut hb = last_heartbeat.write();
            hb.insert("node-1".to_string(), now);
            hb.insert("node-2".to_string(), now);
        }

        {
            let mut flap = node_flap_info.write();
            flap.insert(
                "node-1".to_string(),
                NodeFlap {
                    suspect_count: 3,
                    last_suspect_at: now,
                    last_stable_at: now,
                },
            );
            flap.insert(
                "node-2".to_string(),
                NodeFlap {
                    suspect_count: 1,
                    last_suspect_at: now,
                    last_stable_at: now,
                },
            );
        }

        last_heartbeat.write().remove("node-1");
        node_flap_info.write().remove("node-1");

        assert!(last_heartbeat.read().get("node-1").is_none());
        assert!(node_flap_info.read().get("node-1").is_none());
        assert!(last_heartbeat.read().get("node-2").is_some());
        assert!(node_flap_info.read().get("node-2").is_some());
    }

    #[test]
    fn test_cleanup_node_state_nonexistent_node_is_safe() {
        let last_heartbeat: RwLock<HashMap<String, Instant>> = RwLock::new(HashMap::new());
        let node_flap_info: RwLock<HashMap<String, NodeFlap>> = RwLock::new(HashMap::new());

        last_heartbeat.write().remove("nonexistent");
        node_flap_info.write().remove("nonexistent");

        assert!(last_heartbeat.read().is_empty());
        assert!(node_flap_info.read().is_empty());
    }
}
