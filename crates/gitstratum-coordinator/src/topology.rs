use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use gitstratum_proto::{NodeInfo, NodeState, NodeType};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub object_nodes: HashMap<String, NodeEntry>,
    pub metadata_nodes: HashMap<String, NodeEntry>,
    pub hash_ring_config: HashRingConfig,
    pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: String,
    pub address: String,
    pub port: u32,
    pub state: i32,
}

impl NodeEntry {
    pub fn from_proto(info: &NodeInfo) -> Self {
        Self {
            id: info.id.clone(),
            address: info.address.clone(),
            port: info.port,
            state: info.state,
        }
    }

    pub fn to_proto(&self, node_type: NodeType) -> NodeInfo {
        NodeInfo {
            id: self.id.clone(),
            address: self.address.clone(),
            port: self.port,
            state: self.state,
            r#type: node_type as i32,
        }
    }

    pub fn state(&self) -> NodeState {
        NodeState::try_from(self.state).unwrap_or(NodeState::Unknown)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HashRingConfig {
    pub virtual_nodes_per_physical: u32,
    pub replication_factor: u32,
}

impl Default for HashRingConfig {
    fn default() -> Self {
        Self {
            virtual_nodes_per_physical: 16,
            replication_factor: 3,
        }
    }
}

impl HashRingConfig {
    pub fn to_proto(&self) -> gitstratum_proto::HashRingConfig {
        gitstratum_proto::HashRingConfig {
            virtual_nodes_per_physical: self.virtual_nodes_per_physical,
            replication_factor: self.replication_factor,
        }
    }

    pub fn from_proto(config: &gitstratum_proto::HashRingConfig) -> Self {
        Self {
            virtual_nodes_per_physical: config.virtual_nodes_per_physical,
            replication_factor: config.replication_factor,
        }
    }
}
