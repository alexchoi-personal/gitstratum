use std::sync::Arc;
use std::time::{Duration, Instant};

use gitstratum_hashring::ConsistentHashRing;
use gitstratum_proto::coordinator_service_client::CoordinatorServiceClient;
use gitstratum_proto::{
    AddNodeRequest, GetClusterStateRequest, GetClusterStateResponse, GetTopologyResponse, NodeInfo,
    NodeState, RemoveNodeRequest, SetNodeStateRequest,
};
use parking_lot::RwLock;
use tonic::transport::Channel;

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
}

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

pub struct CoordinatorClient {
    client: CoordinatorServiceClient<Channel>,
    cache: Option<Arc<TopologyCache>>,
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
        })
    }

    pub fn with_cache(mut self, cache: Arc<TopologyCache>) -> Self {
        self.cache = Some(cache);
        self
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
}
