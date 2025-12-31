use gitstratum_hashring::{ConsistentHashRing, NodeId, NodeInfo, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashRingTopology {
    pub nodes: Vec<String>,
    pub replication_factor: u32,
    pub version: u64,
}

impl HashRingTopology {
    pub fn new(replication_factor: u32) -> Self {
        Self {
            nodes: Vec::new(),
            replication_factor,
            version: 0,
        }
    }

    pub fn from_ring(ring: &ConsistentHashRing) -> Self {
        let nodes = ring.get_nodes().into_iter().map(|n| n.id.as_str().to_string()).collect();
        Self {
            nodes,
            replication_factor: ring.replication_factor() as u32,
            version: ring.version(),
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl Default for HashRingTopology {
    fn default() -> Self {
        Self::new(3)
    }
}

pub struct HashRingState {
    object_ring: ConsistentHashRing,
    metadata_ring: ConsistentHashRing,
    node_info: HashMap<String, NodeInfo>,
    version: u64,
}

impl HashRingState {
    pub fn new(virtual_nodes: u32, replication_factor: usize) -> Self {
        Self {
            object_ring: ConsistentHashRing::new(virtual_nodes, replication_factor),
            metadata_ring: ConsistentHashRing::new(virtual_nodes, replication_factor),
            node_info: HashMap::new(),
            version: 0,
        }
    }

    pub fn add_object_node(&mut self, node: NodeInfo) -> Result<(), gitstratum_hashring::HashRingError> {
        self.node_info.insert(node.id.as_str().to_string(), node.clone());
        self.object_ring.add_node(node)?;
        self.version += 1;
        Ok(())
    }

    pub fn add_metadata_node(&mut self, node: NodeInfo) -> Result<(), gitstratum_hashring::HashRingError> {
        self.node_info.insert(node.id.as_str().to_string(), node.clone());
        self.metadata_ring.add_node(node)?;
        self.version += 1;
        Ok(())
    }

    pub fn remove_object_node(&mut self, node_id: &NodeId) -> Result<(), gitstratum_hashring::HashRingError> {
        self.object_ring.remove_node(node_id)?;
        self.node_info.remove(node_id.as_str());
        self.version += 1;
        Ok(())
    }

    pub fn remove_metadata_node(&mut self, node_id: &NodeId) -> Result<(), gitstratum_hashring::HashRingError> {
        self.metadata_ring.remove_node(node_id)?;
        self.node_info.remove(node_id.as_str());
        self.version += 1;
        Ok(())
    }

    pub fn set_object_node_state(&mut self, node_id: &NodeId, state: NodeState) -> Result<(), gitstratum_hashring::HashRingError> {
        self.object_ring.set_node_state(node_id, state)?;
        self.version += 1;
        Ok(())
    }

    pub fn set_metadata_node_state(&mut self, node_id: &NodeId, state: NodeState) -> Result<(), gitstratum_hashring::HashRingError> {
        self.metadata_ring.set_node_state(node_id, state)?;
        self.version += 1;
        Ok(())
    }

    pub fn lookup_object_nodes(&self, key: &[u8]) -> Vec<NodeId> {
        self.object_ring
            .nodes_for_key(key)
            .map(|nodes| nodes.into_iter().map(|n| n.id).collect())
            .unwrap_or_default()
    }

    pub fn lookup_metadata_nodes(&self, key: &[u8]) -> Vec<NodeId> {
        self.metadata_ring
            .nodes_for_key(key)
            .map(|nodes| nodes.into_iter().map(|n| n.id).collect())
            .unwrap_or_default()
    }

    pub fn object_topology(&self) -> HashRingTopology {
        HashRingTopology::from_ring(&self.object_ring)
    }

    pub fn metadata_topology(&self) -> HashRingTopology {
        HashRingTopology::from_ring(&self.metadata_ring)
    }

    pub fn object_ring(&self) -> &ConsistentHashRing {
        &self.object_ring
    }

    pub fn metadata_ring(&self) -> &ConsistentHashRing {
        &self.metadata_ring
    }

    pub fn get_node_info(&self, node_id: &str) -> Option<&NodeInfo> {
        self.node_info.get(node_id)
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

impl Default for HashRingState {
    fn default() -> Self {
        Self::new(16, 3)
    }
}
