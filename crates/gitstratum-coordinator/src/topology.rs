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
    pub last_heartbeat_at: i64,
    pub suspect_count: u32,
    pub generation_id: String,
    pub registered_at: i64,
}

impl NodeEntry {
    pub fn from_proto(info: &NodeInfo) -> Self {
        Self {
            id: info.id.clone(),
            address: info.address.clone(),
            port: info.port,
            state: info.state,
            last_heartbeat_at: info.last_heartbeat_at,
            suspect_count: info.suspect_count,
            generation_id: info.generation_id.clone(),
            registered_at: info.registered_at,
        }
    }

    pub fn to_proto(&self, node_type: NodeType) -> NodeInfo {
        NodeInfo {
            id: self.id.clone(),
            address: self.address.clone(),
            port: self.port,
            state: self.state,
            r#type: node_type as i32,
            last_heartbeat_at: self.last_heartbeat_at,
            suspect_count: self.suspect_count,
            generation_id: self.generation_id.clone(),
            registered_at: self.registered_at,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_node_info() -> NodeInfo {
        NodeInfo {
            id: "node-1".to_string(),
            address: "192.168.1.10".to_string(),
            port: 9000,
            state: NodeState::Active as i32,
            r#type: NodeType::Object as i32,
            last_heartbeat_at: 1234567890,
            suspect_count: 0,
            generation_id: "uuid-1234".to_string(),
            registered_at: 1234567000,
        }
    }

    #[test]
    fn test_cluster_topology_default() {
        let topo = ClusterTopology::default();
        assert!(topo.object_nodes.is_empty());
        assert!(topo.metadata_nodes.is_empty());
        assert_eq!(topo.version, 0);
    }

    #[test]
    fn test_cluster_topology_clone() {
        let mut topo = ClusterTopology::default();
        topo.version = 42;
        let cloned = topo.clone();
        assert_eq!(cloned.version, 42);
    }

    #[test]
    fn test_cluster_topology_debug() {
        let topo = ClusterTopology::default();
        let debug_str = format!("{:?}", topo);
        assert!(debug_str.contains("ClusterTopology"));
    }

    #[test]
    fn test_cluster_topology_serialize() {
        let topo = ClusterTopology::default();
        let json = serde_json::to_string(&topo).unwrap();
        let deserialized: ClusterTopology = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.version, topo.version);
    }

    #[test]
    fn test_node_entry_from_proto() {
        let info = sample_node_info();
        let entry = NodeEntry::from_proto(&info);

        assert_eq!(entry.id, "node-1");
        assert_eq!(entry.address, "192.168.1.10");
        assert_eq!(entry.port, 9000);
        assert_eq!(entry.state, NodeState::Active as i32);
        assert_eq!(entry.last_heartbeat_at, 1234567890);
        assert_eq!(entry.suspect_count, 0);
        assert_eq!(entry.generation_id, "uuid-1234");
        assert_eq!(entry.registered_at, 1234567000);
    }

    #[test]
    fn test_node_entry_to_proto() {
        let info = sample_node_info();
        let entry = NodeEntry::from_proto(&info);
        let proto = entry.to_proto(NodeType::Object);

        assert_eq!(proto.id, "node-1");
        assert_eq!(proto.address, "192.168.1.10");
        assert_eq!(proto.port, 9000);
        assert_eq!(proto.r#type, NodeType::Object as i32);
    }

    #[test]
    fn test_node_entry_state() {
        let info = sample_node_info();
        let entry = NodeEntry::from_proto(&info);
        assert_eq!(entry.state(), NodeState::Active);
    }

    #[test]
    fn test_node_entry_unknown_state() {
        let mut info = sample_node_info();
        info.state = 999;
        let entry = NodeEntry::from_proto(&info);
        assert_eq!(entry.state(), NodeState::Unknown);
    }

    #[test]
    fn test_node_entry_clone() {
        let info = sample_node_info();
        let entry = NodeEntry::from_proto(&info);
        let cloned = entry.clone();
        assert_eq!(entry.id, cloned.id);
    }

    #[test]
    fn test_node_entry_debug() {
        let info = sample_node_info();
        let entry = NodeEntry::from_proto(&info);
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("NodeEntry"));
        assert!(debug_str.contains("node-1"));
    }

    #[test]
    fn test_node_entry_serialize() {
        let info = sample_node_info();
        let entry = NodeEntry::from_proto(&info);
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: NodeEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, entry.id);
    }

    #[test]
    fn test_hash_ring_config_default() {
        let config = HashRingConfig::default();
        assert_eq!(config.virtual_nodes_per_physical, 16);
        assert_eq!(config.replication_factor, 3);
    }

    #[test]
    fn test_hash_ring_config_to_proto() {
        let config = HashRingConfig {
            virtual_nodes_per_physical: 32,
            replication_factor: 5,
        };
        let proto = config.to_proto();
        assert_eq!(proto.virtual_nodes_per_physical, 32);
        assert_eq!(proto.replication_factor, 5);
    }

    #[test]
    fn test_hash_ring_config_from_proto() {
        let proto = gitstratum_proto::HashRingConfig {
            virtual_nodes_per_physical: 64,
            replication_factor: 2,
        };
        let config = HashRingConfig::from_proto(&proto);
        assert_eq!(config.virtual_nodes_per_physical, 64);
        assert_eq!(config.replication_factor, 2);
    }

    #[test]
    fn test_hash_ring_config_clone() {
        let config = HashRingConfig::default();
        let cloned = config.clone();
        assert_eq!(
            config.virtual_nodes_per_physical,
            cloned.virtual_nodes_per_physical
        );
    }

    #[test]
    fn test_hash_ring_config_serialize() {
        let config = HashRingConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HashRingConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.virtual_nodes_per_physical,
            config.virtual_nodes_per_physical
        );
    }
}
