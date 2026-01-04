use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use gitstratum_hashring::ConsistentHashRing;
use gitstratum_proto::coordinator_service_client::CoordinatorServiceClient;
use gitstratum_proto::topology_update::Update;
use gitstratum_proto::{
    AddNodeRequest, DeregisterNodeRequest, GetClusterStateRequest, GetClusterStateResponse,
    GetTopologyResponse, HeartbeatRequest, HeartbeatResponse, NodeInfo, NodeState,
    RegisterNodeRequest, RegisterNodeResponse, RemoveNodeRequest, SetNodeStateRequest,
    WatchTopologyRequest,
};
use parking_lot::RwLock;
use tonic::transport::Channel;
use tonic::Code;

use crate::error::CoordinatorError;
use crate::topology::ClusterTopology;

pub struct TopologyCache {
    topology: RwLock<Option<GetTopologyResponse>>,
    last_updated: RwLock<Instant>,
    max_staleness: Duration,
}

impl TopologyCache {
    pub fn new(max_staleness: Duration) -> Self {
        Self {
            topology: RwLock::new(None),
            last_updated: RwLock::new(Instant::now()),
            max_staleness,
        }
    }

    pub fn get(&self) -> Option<GetTopologyResponse> {
        self.topology.read().clone()
    }

    pub fn get_version(&self) -> u64 {
        self.topology
            .read()
            .as_ref()
            .map(|t| t.version)
            .unwrap_or(0)
    }

    pub fn update(&self, topology: GetTopologyResponse) {
        *self.topology.write() = Some(topology);
        *self.last_updated.write() = Instant::now();
    }

    pub fn is_stale(&self) -> bool {
        self.last_updated.read().elapsed() > self.max_staleness
    }

    pub fn staleness(&self) -> Duration {
        self.last_updated.read().elapsed()
    }

    pub fn invalidate(&self) {
        *self.topology.write() = None;
        *self.last_updated.write() = Instant::now() - self.max_staleness - Duration::from_secs(1);
    }
}

#[derive(Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub timeout_per_attempt: Duration,
    pub jitter_fraction: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            timeout_per_attempt: Duration::from_secs(5),
            jitter_fraction: 0.25,
        }
    }
}

impl RetryConfig {
    pub fn compute_backoff_with_jitter(&self, attempt: u32) -> Duration {
        let base = self
            .initial_backoff
            .saturating_mul(2u32.saturating_pow(attempt));
        let capped = base.min(self.max_backoff);

        let jitter_ms = (capped.as_millis() as f64 * self.jitter_fraction * rand_fraction()) as u64;
        capped + Duration::from_millis(jitter_ms)
    }
}

fn rand_fraction() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );
    (hasher.finish() % 1000) as f64 / 1000.0
}

pub struct CoordinatorClient {
    client: CoordinatorServiceClient<Channel>,
    cache: Option<Arc<TopologyCache>>,
    endpoint: String,
}

impl CoordinatorClient {
    pub async fn connect(addr: &str) -> Result<Self, CoordinatorError> {
        let channel = Channel::from_shared(addr.to_string())
            .map_err(|e| CoordinatorError::Internal(format!("Invalid address: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .connect()
            .await
            .map_err(|e| CoordinatorError::Internal(format!("Connection failed: {}", e)))?;

        Ok(Self {
            client: CoordinatorServiceClient::new(channel),
            cache: None,
            endpoint: addr.to_string(),
        })
    }

    pub fn with_cache(mut self, cache: Arc<TopologyCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn get_cluster_state(&mut self) -> Result<GetClusterStateResponse, CoordinatorError> {
        let response = self
            .client
            .get_cluster_state(GetClusterStateRequest {})
            .await?;
        Ok(response.into_inner())
    }

    pub async fn add_node(&mut self, node: NodeInfo) -> Result<(), CoordinatorError> {
        let response = self
            .client
            .add_node(AddNodeRequest { node: Some(node) })
            .await?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(CoordinatorError::Internal(resp.error))
        }
    }

    pub async fn remove_node(&mut self, node_id: &str) -> Result<(), CoordinatorError> {
        let response = self
            .client
            .remove_node(RemoveNodeRequest {
                node_id: node_id.to_string(),
            })
            .await?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(CoordinatorError::Internal(resp.error))
        }
    }

    pub async fn set_node_state(
        &mut self,
        node_id: &str,
        state: NodeState,
    ) -> Result<(), CoordinatorError> {
        let response = self
            .client
            .set_node_state(SetNodeStateRequest {
                node_id: node_id.to_string(),
                state: state as i32,
            })
            .await?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(CoordinatorError::Internal(resp.error))
        }
    }

    pub async fn get_topology(&mut self) -> Result<ClusterTopology, CoordinatorError> {
        let state = self.get_cluster_state().await?;

        let mut topology = ClusterTopology::default();
        topology.version = state.version;

        for node in state.object_nodes {
            let entry = crate::topology::NodeEntry::from_proto(&node);
            topology.object_nodes.insert(node.id.clone(), entry);
        }

        for node in state.metadata_nodes {
            let entry = crate::topology::NodeEntry::from_proto(&node);
            topology.metadata_nodes.insert(node.id.clone(), entry);
        }

        if let Some(config) = state.hash_ring_config {
            topology.hash_ring_config = crate::topology::HashRingConfig::from_proto(&config);
        }

        Ok(topology)
    }

    pub async fn get_hash_ring(&mut self) -> Result<ConsistentHashRing, CoordinatorError> {
        let topology = self.get_topology().await?;
        topology
            .to_hash_ring()
            .map_err(|e| CoordinatorError::Internal(format!("Failed to build hash ring: {}", e)))
    }

    pub async fn register_node(
        &mut self,
        node: NodeInfo,
    ) -> Result<RegisterNodeResponse, CoordinatorError> {
        let response = self
            .client
            .register_node(RegisterNodeRequest { node: Some(node) })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn deregister_node(&mut self, node_id: &str) -> Result<u64, CoordinatorError> {
        let response = self
            .client
            .deregister_node(DeregisterNodeRequest {
                node_id: node_id.to_string(),
            })
            .await?;
        Ok(response.into_inner().topology_version)
    }

    pub async fn heartbeat(
        &mut self,
        node_id: &str,
        known_version: u64,
        reported_state: NodeState,
        generation_id: &str,
    ) -> Result<HeartbeatResponse, CoordinatorError> {
        let response = self
            .client
            .heartbeat(HeartbeatRequest {
                node_id: node_id.to_string(),
                known_version,
                reported_state: reported_state as i32,
                generation_id: generation_id.to_string(),
            })
            .await?;
        Ok(response.into_inner())
    }
}

pub struct SmartCoordinatorClient {
    endpoints: Vec<String>,
    current_leader: RwLock<Option<String>>,
    retry_config: RetryConfig,
    cache: Arc<TopologyCache>,
    channels: RwLock<HashMap<String, Channel>>,
}

impl SmartCoordinatorClient {
    pub fn new(endpoints: Vec<String>, cache: Arc<TopologyCache>) -> Self {
        Self {
            endpoints,
            current_leader: RwLock::new(None),
            retry_config: RetryConfig::default(),
            cache,
            channels: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    async fn get_channel(&self, endpoint: &str) -> Result<Channel, CoordinatorError> {
        if let Some(channel) = self.channels.read().get(endpoint) {
            return Ok(channel.clone());
        }

        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| CoordinatorError::Internal(format!("Invalid address: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| CoordinatorError::Internal(format!("Connection failed: {}", e)))?;

        let mut channels = self.channels.write();
        if let Some(existing) = channels.get(endpoint) {
            return Ok(existing.clone());
        }
        channels.insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    fn get_endpoint(&self, attempt: u32) -> String {
        if let Some(leader) = self.current_leader.read().as_ref() {
            return leader.clone();
        }
        self.endpoints[attempt as usize % self.endpoints.len()].clone()
    }

    fn update_leader_hint(&self, status: &tonic::Status) {
        if let Some(leader) = status.metadata().get("leader-address") {
            if let Ok(addr) = leader.to_str() {
                *self.current_leader.write() = Some(addr.to_string());
            }
        }
    }

    fn clear_leader_hint(&self) {
        *self.current_leader.write() = None;
    }

    pub async fn register_node(
        &self,
        node: NodeInfo,
    ) -> Result<RegisterNodeResponse, CoordinatorError> {
        let mut consecutive_leader_failures = 0u32;

        for attempt in 0..self.retry_config.max_attempts {
            if consecutive_leader_failures >= 2 {
                self.clear_leader_hint();
                consecutive_leader_failures = 0;
            }

            let endpoint = self.get_endpoint(attempt);

            let result = tokio::time::timeout(self.retry_config.timeout_per_attempt, async {
                let channel = self.get_channel(&endpoint).await?;
                let mut client = CoordinatorServiceClient::new(channel);
                let response = client
                    .register_node(RegisterNodeRequest {
                        node: Some(node.clone()),
                    })
                    .await?;
                Ok::<_, CoordinatorError>(response.into_inner())
            })
            .await;

            match result {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(CoordinatorError::Grpc(status)))
                    if status.code() == Code::FailedPrecondition =>
                {
                    self.update_leader_hint(&status);
                    consecutive_leader_failures = 0;
                }
                Ok(Err(e)) => {
                    tracing::warn!(attempt, error = %e, "Register attempt failed");
                    if self.current_leader.read().is_some() {
                        consecutive_leader_failures += 1;
                    }
                }
                Err(_) => {
                    tracing::warn!(attempt, "Register attempt timed out");
                    if self.current_leader.read().is_some() {
                        consecutive_leader_failures += 1;
                    }
                }
            }

            let backoff = self.retry_config.compute_backoff_with_jitter(attempt);
            tokio::time::sleep(backoff).await;
        }

        Err(CoordinatorError::Internal(format!(
            "Failed to register after {} attempts",
            self.retry_config.max_attempts
        )))
    }

    pub async fn heartbeat(
        &self,
        node_id: &str,
        reported_state: NodeState,
        generation_id: &str,
    ) -> Result<HeartbeatResponse, CoordinatorError> {
        let known_version = self.cache.get_version();
        let mut consecutive_leader_failures = 0u32;

        for attempt in 0..self.retry_config.max_attempts {
            if consecutive_leader_failures >= 2 {
                self.clear_leader_hint();
                consecutive_leader_failures = 0;
            }

            let endpoint = self.get_endpoint(attempt);

            let result = tokio::time::timeout(self.retry_config.timeout_per_attempt, async {
                let channel = self.get_channel(&endpoint).await?;
                let mut client = CoordinatorServiceClient::new(channel);
                let response = client
                    .heartbeat(HeartbeatRequest {
                        node_id: node_id.to_string(),
                        known_version,
                        reported_state: reported_state as i32,
                        generation_id: generation_id.to_string(),
                    })
                    .await?;
                Ok::<_, CoordinatorError>(response.into_inner())
            })
            .await;

            match result {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(CoordinatorError::Grpc(status)))
                    if status.code() == Code::FailedPrecondition =>
                {
                    self.update_leader_hint(&status);
                    consecutive_leader_failures = 0;
                }
                Ok(Err(e)) => {
                    tracing::warn!(attempt, error = %e, "Heartbeat attempt failed");
                    if self.current_leader.read().is_some() {
                        consecutive_leader_failures += 1;
                    }
                }
                Err(_) => {
                    tracing::warn!(attempt, "Heartbeat attempt timed out");
                    if self.current_leader.read().is_some() {
                        consecutive_leader_failures += 1;
                    }
                }
            }

            let backoff = self.retry_config.compute_backoff_with_jitter(attempt);
            tokio::time::sleep(backoff).await;
        }

        Err(CoordinatorError::Internal(format!(
            "Failed to heartbeat after {} attempts",
            self.retry_config.max_attempts
        )))
    }

    pub async fn refresh_topology(&self) -> Result<(), CoordinatorError> {
        for attempt in 0..self.retry_config.max_attempts {
            let endpoint = self.get_endpoint(attempt);

            let result = tokio::time::timeout(self.retry_config.timeout_per_attempt, async {
                let channel = self.get_channel(&endpoint).await?;
                let mut client = CoordinatorServiceClient::new(channel);
                let response = client.get_cluster_state(GetClusterStateRequest {}).await?;
                Ok::<_, CoordinatorError>(response.into_inner())
            })
            .await;

            match result {
                Ok(Ok(state)) => {
                    let topo = GetTopologyResponse {
                        version: state.version,
                        frontend_nodes: vec![],
                        metadata_nodes: state.metadata_nodes,
                        object_nodes: state.object_nodes,
                        hash_ring_config: state.hash_ring_config,
                        leader_id: state.leader_id,
                    };
                    self.cache.update(topo);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    tracing::warn!(attempt, error = %e, "Topology refresh failed");
                }
                Err(_) => {
                    tracing::warn!(attempt, "Topology refresh timed out");
                }
            }

            let backoff = self.retry_config.compute_backoff_with_jitter(attempt);
            tokio::time::sleep(backoff).await;
        }

        Err(CoordinatorError::Internal(
            "Failed to refresh topology".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEvent {
    NodeAdded,
    NodeUpdated,
    NodeRemoved,
    FullSync,
    Keepalive,
    Lagged,
    StreamEnded,
    Error,
}

pub struct WatchConfig {
    pub keepalive_timeout: Duration,
    pub reconnect_backoff: Duration,
    pub reconnect_jitter_fraction: f64,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            keepalive_timeout: Duration::from_secs(60),
            reconnect_backoff: Duration::from_secs(1),
            reconnect_jitter_fraction: 0.5,
        }
    }
}

pub async fn watch_topology_with_recovery<F>(
    endpoints: Vec<String>,
    cache: Arc<TopologyCache>,
    config: WatchConfig,
    mut on_event: F,
    shutdown: tokio::sync::watch::Receiver<bool>,
) where
    F: FnMut(WatchEvent, u64),
{
    let mut current_version = cache.get_version();
    let mut endpoint_index = 0usize;

    loop {
        if *shutdown.borrow() {
            break;
        }
        let endpoint = &endpoints[endpoint_index % endpoints.len()];
        endpoint_index += 1;

        let connect_result = CoordinatorClient::connect(endpoint).await;
        let mut client = match connect_result {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(endpoint, error = %e, "Failed to connect for watch");
                on_event(WatchEvent::Error, current_version);
                sleep_with_jitter(config.reconnect_backoff, config.reconnect_jitter_fraction).await;
                continue;
            }
        };

        let watch_result = client
            .client
            .watch_topology(WatchTopologyRequest {
                since_version: current_version,
            })
            .await;

        let mut stream = match watch_result {
            Ok(response) => response.into_inner(),
            Err(e) => {
                tracing::warn!(endpoint, error = %e, "Failed to start watch stream");
                on_event(WatchEvent::Error, current_version);
                sleep_with_jitter(config.reconnect_backoff, config.reconnect_jitter_fraction).await;
                continue;
            }
        };

        loop {
            let msg = tokio::time::timeout(config.keepalive_timeout, stream.next()).await;

            match msg {
                Ok(Some(Ok(update))) => match &update.update {
                    Some(Update::Keepalive(_)) => {
                        on_event(WatchEvent::Keepalive, current_version);
                    }
                    Some(Update::Lagged(lagged)) => {
                        tracing::warn!(
                            current_version = lagged.current_version,
                            missed_updates = lagged.missed_updates,
                            "Server sent Lagged notification"
                        );
                        on_event(WatchEvent::Lagged, lagged.current_version);
                        cache.invalidate();
                        current_version = 0;
                        break;
                    }
                    Some(Update::FullSync(topo)) => {
                        current_version = topo.version;
                        cache.update(topo.clone());
                        on_event(WatchEvent::FullSync, current_version);
                    }
                    Some(Update::NodeAdded(node)) => {
                        if update.previous_version != current_version
                            && update.previous_version != 0
                        {
                            tracing::warn!(
                                expected = current_version,
                                got = update.previous_version,
                                "Gap detected in watch stream"
                            );
                            cache.invalidate();
                            current_version = 0;
                            break;
                        }
                        current_version = update.version;
                        on_event(WatchEvent::NodeAdded, current_version);
                        tracing::debug!(node_id = %node.id, "Node added");
                    }
                    Some(Update::NodeUpdated(node)) => {
                        if update.previous_version != current_version
                            && update.previous_version != 0
                        {
                            tracing::warn!(
                                expected = current_version,
                                got = update.previous_version,
                                "Gap detected in watch stream"
                            );
                            cache.invalidate();
                            current_version = 0;
                            break;
                        }
                        current_version = update.version;
                        on_event(WatchEvent::NodeUpdated, current_version);
                        tracing::debug!(node_id = %node.id, "Node updated");
                    }
                    Some(Update::NodeRemoved(node_id)) => {
                        if update.previous_version != current_version
                            && update.previous_version != 0
                        {
                            tracing::warn!(
                                expected = current_version,
                                got = update.previous_version,
                                "Gap detected in watch stream"
                            );
                            cache.invalidate();
                            current_version = 0;
                            break;
                        }
                        current_version = update.version;
                        on_event(WatchEvent::NodeRemoved, current_version);
                        tracing::debug!(node_id = %node_id, "Node removed");
                    }
                    None => {}
                },
                Ok(Some(Err(e))) => {
                    tracing::warn!(error = %e, "Watch stream error");
                    on_event(WatchEvent::Error, current_version);
                    break;
                }
                Ok(None) => {
                    tracing::info!("Watch stream ended");
                    on_event(WatchEvent::StreamEnded, current_version);
                    break;
                }
                Err(_) => {
                    tracing::warn!("Watch keepalive timeout");
                    on_event(WatchEvent::Error, current_version);
                    break;
                }
            }
        }

        sleep_with_jitter(config.reconnect_backoff, config.reconnect_jitter_fraction).await;
    }
}

async fn sleep_with_jitter(base: Duration, jitter_fraction: f64) {
    let jitter_ms = (base.as_millis() as f64 * jitter_fraction * rand_fraction()) as u64;
    tokio::time::sleep(base + Duration::from_millis(jitter_ms)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_cache_new() {
        let cache = TopologyCache::new(Duration::from_secs(300));
        assert!(cache.get().is_none());
        assert_eq!(cache.get_version(), 0);
    }

    #[test]
    fn test_topology_cache_update() {
        let cache = TopologyCache::new(Duration::from_secs(300));
        let topo = GetTopologyResponse {
            version: 42,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: String::new(),
        };
        cache.update(topo);
        assert_eq!(cache.get_version(), 42);
        assert!(!cache.is_stale());
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_secs(5));
        assert!((config.jitter_fraction - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_backoff_with_jitter() {
        let config = RetryConfig::default();

        let backoff0 = config.compute_backoff_with_jitter(0);
        assert!(backoff0 >= Duration::from_millis(100));
        assert!(backoff0 <= Duration::from_millis(125));

        let backoff1 = config.compute_backoff_with_jitter(1);
        assert!(backoff1 >= Duration::from_millis(200));
        assert!(backoff1 <= Duration::from_millis(250));

        let backoff10 = config.compute_backoff_with_jitter(10);
        assert!(backoff10 <= Duration::from_millis(6250));
    }

    #[test]
    fn test_watch_config_default() {
        let config = WatchConfig::default();
        assert_eq!(config.keepalive_timeout, Duration::from_secs(60));
        assert_eq!(config.reconnect_backoff, Duration::from_secs(1));
        assert!((config.reconnect_jitter_fraction - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_watch_event_variants() {
        assert_eq!(WatchEvent::NodeAdded, WatchEvent::NodeAdded);
        assert_ne!(WatchEvent::NodeAdded, WatchEvent::NodeRemoved);

        let event = WatchEvent::FullSync;
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("FullSync"));
    }

    #[test]
    fn test_smart_client_get_endpoint_round_robin() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(
            vec![
                "http://coord-0:9000".to_string(),
                "http://coord-1:9000".to_string(),
                "http://coord-2:9000".to_string(),
            ],
            cache,
        );

        assert_eq!(client.get_endpoint(0), "http://coord-0:9000");
        assert_eq!(client.get_endpoint(1), "http://coord-1:9000");
        assert_eq!(client.get_endpoint(2), "http://coord-2:9000");
        assert_eq!(client.get_endpoint(3), "http://coord-0:9000");
    }

    #[test]
    fn test_smart_client_leader_hint() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(vec!["http://coord-0:9000".to_string()], cache);

        assert!(client.current_leader.read().is_none());

        *client.current_leader.write() = Some("http://coord-1:9000".to_string());
        assert_eq!(client.get_endpoint(0), "http://coord-1:9000");
        assert_eq!(client.get_endpoint(1), "http://coord-1:9000");

        client.clear_leader_hint();
        assert!(client.current_leader.read().is_none());
        assert_eq!(client.get_endpoint(0), "http://coord-0:9000");
    }

    #[test]
    fn test_topology_cache_invalidate_clears_cached_topology() {
        let cache = TopologyCache::new(Duration::from_secs(300));

        let topo = GetTopologyResponse {
            version: 42,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: String::new(),
        };
        cache.update(topo);

        assert!(cache.get().is_some());
        assert_eq!(cache.get_version(), 42);

        cache.invalidate();

        assert!(cache.get().is_none());
        assert_eq!(cache.get_version(), 0);
    }

    #[test]
    fn test_topology_cache_invalidate_allows_reupdate() {
        let cache = TopologyCache::new(Duration::from_secs(300));

        let topo1 = GetTopologyResponse {
            version: 10,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: "leader-1".to_string(),
        };
        cache.update(topo1);
        assert_eq!(cache.get_version(), 10);

        cache.invalidate();
        assert_eq!(cache.get_version(), 0);

        let topo2 = GetTopologyResponse {
            version: 20,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: "leader-2".to_string(),
        };
        cache.update(topo2);
        assert_eq!(cache.get_version(), 20);
        assert_eq!(cache.get().unwrap().leader_id, "leader-2");
    }

    #[test]
    fn test_topology_cache_invalidate_on_empty_is_noop() {
        let cache = TopologyCache::new(Duration::from_secs(300));

        assert!(cache.get().is_none());
        assert_eq!(cache.get_version(), 0);

        cache.invalidate();

        assert!(cache.get().is_none());
        assert_eq!(cache.get_version(), 0);
    }

    #[test]
    fn test_smart_client_channels_initially_empty() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(vec!["http://coord-0:9000".to_string()], cache);
        assert!(client.channels.read().is_empty());
    }

    #[tokio::test]
    async fn test_get_channel_double_checked_locking() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = Arc::new(SmartCoordinatorClient::new(
            vec!["http://localhost:50051".to_string()],
            cache,
        ));

        let mut handles = vec![];
        for _ in 0..10 {
            let c = Arc::clone(&client);
            let handle = tokio::spawn(async move {
                let _ = c.get_channel("http://localhost:50051").await;
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }

        let channels = client.channels.read();
        assert!(
            channels.len() <= 1,
            "Expected at most 1 channel due to double-checked locking, got {}",
            channels.len()
        );
    }

    #[test]
    fn test_topology_cache_staleness_returns_elapsed_time() {
        let cache = TopologyCache::new(Duration::from_secs(300));
        let staleness_before = cache.staleness();
        std::thread::sleep(Duration::from_millis(10));
        let staleness_after = cache.staleness();
        assert!(staleness_after > staleness_before);
        assert!(staleness_after >= Duration::from_millis(10));
    }

    #[test]
    fn test_topology_cache_is_stale_when_expired() {
        let cache = TopologyCache::new(Duration::from_millis(5));
        assert!(!cache.is_stale());
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.is_stale());
    }

    #[test]
    fn test_topology_cache_update_resets_staleness() {
        let cache = TopologyCache::new(Duration::from_millis(5));
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.is_stale());

        let topo = GetTopologyResponse {
            version: 1,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: String::new(),
        };
        cache.update(topo);

        assert!(!cache.is_stale());
    }

    #[test]
    fn test_retry_config_clone() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(10),
            timeout_per_attempt: Duration::from_secs(3),
            jitter_fraction: 0.5,
        };
        let cloned = config.clone();
        assert_eq!(cloned.max_attempts, 10);
        assert_eq!(cloned.initial_backoff, Duration::from_millis(50));
        assert_eq!(cloned.max_backoff, Duration::from_secs(10));
        assert_eq!(cloned.timeout_per_attempt, Duration::from_secs(3));
        assert!((cloned.jitter_fraction - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_zero_jitter() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            timeout_per_attempt: Duration::from_secs(5),
            jitter_fraction: 0.0,
        };
        let backoff = config.compute_backoff_with_jitter(0);
        assert_eq!(backoff, Duration::from_millis(100));
    }

    #[test]
    fn test_retry_config_backoff_capped_at_max() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(2),
            timeout_per_attempt: Duration::from_secs(5),
            jitter_fraction: 0.0,
        };
        let backoff = config.compute_backoff_with_jitter(10);
        assert_eq!(backoff, Duration::from_secs(2));
    }

    #[test]
    fn test_watch_config_custom_values() {
        let config = WatchConfig {
            keepalive_timeout: Duration::from_secs(120),
            reconnect_backoff: Duration::from_secs(5),
            reconnect_jitter_fraction: 0.75,
        };
        assert_eq!(config.keepalive_timeout, Duration::from_secs(120));
        assert_eq!(config.reconnect_backoff, Duration::from_secs(5));
        assert!((config.reconnect_jitter_fraction - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_watch_event_copy_clone() {
        let event = WatchEvent::NodeAdded;
        let copied = event;
        let cloned = event.clone();
        assert_eq!(copied, WatchEvent::NodeAdded);
        assert_eq!(cloned, WatchEvent::NodeAdded);
    }

    #[test]
    fn test_watch_event_all_variants_debug() {
        let variants = [
            WatchEvent::NodeAdded,
            WatchEvent::NodeUpdated,
            WatchEvent::NodeRemoved,
            WatchEvent::FullSync,
            WatchEvent::Keepalive,
            WatchEvent::Lagged,
            WatchEvent::StreamEnded,
            WatchEvent::Error,
        ];
        for event in variants {
            let debug_str = format!("{:?}", event);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_watch_event_equality() {
        assert_eq!(WatchEvent::NodeAdded, WatchEvent::NodeAdded);
        assert_eq!(WatchEvent::NodeUpdated, WatchEvent::NodeUpdated);
        assert_eq!(WatchEvent::NodeRemoved, WatchEvent::NodeRemoved);
        assert_eq!(WatchEvent::FullSync, WatchEvent::FullSync);
        assert_eq!(WatchEvent::Keepalive, WatchEvent::Keepalive);
        assert_eq!(WatchEvent::Lagged, WatchEvent::Lagged);
        assert_eq!(WatchEvent::StreamEnded, WatchEvent::StreamEnded);
        assert_eq!(WatchEvent::Error, WatchEvent::Error);

        assert_ne!(WatchEvent::NodeAdded, WatchEvent::NodeUpdated);
        assert_ne!(WatchEvent::FullSync, WatchEvent::Keepalive);
        assert_ne!(WatchEvent::Error, WatchEvent::StreamEnded);
    }

    #[test]
    fn test_smart_client_with_retry_config() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let custom_config = RetryConfig {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(10),
            timeout_per_attempt: Duration::from_secs(15),
            jitter_fraction: 0.1,
        };
        let client = SmartCoordinatorClient::new(vec!["http://coord-0:9000".to_string()], cache)
            .with_retry_config(custom_config);

        assert_eq!(client.retry_config.max_attempts, 10);
        assert_eq!(
            client.retry_config.initial_backoff,
            Duration::from_millis(50)
        );
        assert_eq!(client.retry_config.max_backoff, Duration::from_secs(10));
        assert_eq!(
            client.retry_config.timeout_per_attempt,
            Duration::from_secs(15)
        );
        assert!((client.retry_config.jitter_fraction - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_smart_client_update_leader_hint_with_valid_metadata() {
        use tonic::metadata::MetadataValue;

        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(vec!["http://coord-0:9000".to_string()], cache);

        assert!(client.current_leader.read().is_none());

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "leader-address",
            MetadataValue::try_from("http://coord-2:9000").unwrap(),
        );
        let status =
            tonic::Status::with_metadata(Code::FailedPrecondition, "not the leader", metadata);

        client.update_leader_hint(&status);

        assert_eq!(
            client.current_leader.read().as_ref().unwrap(),
            "http://coord-2:9000"
        );
    }

    #[test]
    fn test_smart_client_update_leader_hint_without_metadata() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(vec!["http://coord-0:9000".to_string()], cache);

        let status = tonic::Status::new(Code::FailedPrecondition, "not the leader");
        client.update_leader_hint(&status);

        assert!(client.current_leader.read().is_none());
    }

    #[test]
    fn test_smart_client_update_leader_hint_updates_get_endpoint() {
        use tonic::metadata::MetadataValue;

        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(
            vec![
                "http://coord-0:9000".to_string(),
                "http://coord-1:9000".to_string(),
            ],
            cache,
        );

        assert_eq!(client.get_endpoint(0), "http://coord-0:9000");
        assert_eq!(client.get_endpoint(1), "http://coord-1:9000");

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "leader-address",
            MetadataValue::try_from("http://coord-leader:9000").unwrap(),
        );
        let status =
            tonic::Status::with_metadata(Code::FailedPrecondition, "not the leader", metadata);
        client.update_leader_hint(&status);

        assert_eq!(client.get_endpoint(0), "http://coord-leader:9000");
        assert_eq!(client.get_endpoint(1), "http://coord-leader:9000");
        assert_eq!(client.get_endpoint(100), "http://coord-leader:9000");
    }

    #[test]
    fn test_smart_client_clear_leader_hint_falls_back_to_round_robin() {
        use tonic::metadata::MetadataValue;

        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client = SmartCoordinatorClient::new(
            vec![
                "http://coord-0:9000".to_string(),
                "http://coord-1:9000".to_string(),
                "http://coord-2:9000".to_string(),
            ],
            cache,
        );

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "leader-address",
            MetadataValue::try_from("http://leader:9000").unwrap(),
        );
        let status =
            tonic::Status::with_metadata(Code::FailedPrecondition, "not the leader", metadata);
        client.update_leader_hint(&status);
        assert_eq!(client.get_endpoint(0), "http://leader:9000");

        client.clear_leader_hint();
        assert_eq!(client.get_endpoint(0), "http://coord-0:9000");
        assert_eq!(client.get_endpoint(1), "http://coord-1:9000");
        assert_eq!(client.get_endpoint(2), "http://coord-2:9000");
        assert_eq!(client.get_endpoint(3), "http://coord-0:9000");
    }

    #[test]
    fn test_topology_cache_get_returns_full_topology() {
        let cache = TopologyCache::new(Duration::from_secs(300));
        let topo = GetTopologyResponse {
            version: 123,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: "test-leader".to_string(),
        };
        cache.update(topo);

        let retrieved = cache.get().unwrap();
        assert_eq!(retrieved.version, 123);
        assert_eq!(retrieved.leader_id, "test-leader");
    }

    #[test]
    fn test_smart_client_single_endpoint() {
        let cache = Arc::new(TopologyCache::new(Duration::from_secs(300)));
        let client =
            SmartCoordinatorClient::new(vec!["http://single-coord:9000".to_string()], cache);

        assert_eq!(client.get_endpoint(0), "http://single-coord:9000");
        assert_eq!(client.get_endpoint(1), "http://single-coord:9000");
        assert_eq!(client.get_endpoint(100), "http://single-coord:9000");
    }

    #[test]
    fn test_retry_config_exponential_backoff_sequence() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            timeout_per_attempt: Duration::from_secs(5),
            jitter_fraction: 0.0,
        };

        assert_eq!(
            config.compute_backoff_with_jitter(0),
            Duration::from_millis(100)
        );
        assert_eq!(
            config.compute_backoff_with_jitter(1),
            Duration::from_millis(200)
        );
        assert_eq!(
            config.compute_backoff_with_jitter(2),
            Duration::from_millis(400)
        );
        assert_eq!(
            config.compute_backoff_with_jitter(3),
            Duration::from_millis(800)
        );
        assert_eq!(
            config.compute_backoff_with_jitter(4),
            Duration::from_millis(1600)
        );
    }

    #[test]
    fn test_retry_config_saturating_pow_no_overflow() {
        let config = RetryConfig {
            max_attempts: 100,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            timeout_per_attempt: Duration::from_secs(5),
            jitter_fraction: 0.0,
        };

        let backoff = config.compute_backoff_with_jitter(50);
        assert_eq!(backoff, Duration::from_secs(60));
    }

    #[test]
    fn test_topology_cache_invalidate_marks_as_stale() {
        let cache = TopologyCache::new(Duration::from_secs(300));

        let topo = GetTopologyResponse {
            version: 42,
            frontend_nodes: vec![],
            metadata_nodes: vec![],
            object_nodes: vec![],
            hash_ring_config: None,
            leader_id: String::new(),
        };
        cache.update(topo);

        assert!(!cache.is_stale());

        cache.invalidate();

        assert!(cache.is_stale());
        assert!(cache.get().is_none());
    }
}
