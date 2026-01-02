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
        let nodes = ring
            .get_nodes()
            .into_iter()
            .map(|n| n.id.as_str().to_string())
            .collect();
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

    pub fn add_object_node(
        &mut self,
        node: NodeInfo,
    ) -> Result<(), gitstratum_hashring::HashRingError> {
        self.node_info
            .insert(node.id.as_str().to_string(), node.clone());
        self.object_ring.add_node(node)?;
        self.version += 1;
        Ok(())
    }

    pub fn add_metadata_node(
        &mut self,
        node: NodeInfo,
    ) -> Result<(), gitstratum_hashring::HashRingError> {
        self.node_info
            .insert(node.id.as_str().to_string(), node.clone());
        self.metadata_ring.add_node(node)?;
        self.version += 1;
        Ok(())
    }

    pub fn remove_object_node(
        &mut self,
        node_id: &NodeId,
    ) -> Result<(), gitstratum_hashring::HashRingError> {
        self.object_ring.remove_node(node_id)?;
        self.node_info.remove(node_id.as_str());
        self.version += 1;
        Ok(())
    }

    pub fn remove_metadata_node(
        &mut self,
        node_id: &NodeId,
    ) -> Result<(), gitstratum_hashring::HashRingError> {
        self.metadata_ring.remove_node(node_id)?;
        self.node_info.remove(node_id.as_str());
        self.version += 1;
        Ok(())
    }

    pub fn set_object_node_state(
        &mut self,
        node_id: &NodeId,
        state: NodeState,
    ) -> Result<(), gitstratum_hashring::HashRingError> {
        self.object_ring.set_node_state(node_id, state)?;
        self.version += 1;
        Ok(())
    }

    pub fn set_metadata_node_state(
        &mut self,
        node_id: &NodeId,
        state: NodeState,
    ) -> Result<(), gitstratum_hashring::HashRingError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node(id: &str) -> NodeInfo {
        NodeInfo::new(id, format!("10.0.0.{}", id.len()), 9002)
    }

    fn create_test_node_with_state(id: &str, state: NodeState) -> NodeInfo {
        let mut node = NodeInfo::new(id, format!("10.0.0.{}", id.len()), 9002);
        node.state = state;
        node
    }

    #[test]
    fn test_hash_ring_topology_comprehensive() {
        let topology_default = HashRingTopology::default();
        assert!(topology_default.nodes.is_empty());
        assert_eq!(topology_default.replication_factor, 3);
        assert_eq!(topology_default.version, 0);

        let topology_custom = HashRingTopology::new(5);
        assert!(topology_custom.nodes.is_empty());
        assert_eq!(topology_custom.replication_factor, 5);
        assert_eq!(topology_custom.version, 0);

        let ring = ConsistentHashRing::new(16, 3);
        ring.add_node(create_test_node("node1")).unwrap();
        ring.add_node(create_test_node("node2")).unwrap();
        let ring_version = ring.version();
        let topology_from_ring = HashRingTopology::from_ring(&ring);
        assert_eq!(topology_from_ring.nodes.len(), 2);
        assert!(topology_from_ring.nodes.contains(&"node1".to_string()));
        assert!(topology_from_ring.nodes.contains(&"node2".to_string()));
        assert_eq!(topology_from_ring.replication_factor, 3);
        assert_eq!(topology_from_ring.version, ring_version);

        let empty_ring = ConsistentHashRing::new(16, 3);
        let topology_empty = HashRingTopology::from_ring(&empty_ring);
        assert!(topology_empty.nodes.is_empty());
        assert_eq!(topology_empty.replication_factor, 3);

        let mut topology_mutable = HashRingTopology::new(5);
        assert_eq!(topology_mutable.node_count(), 0);
        topology_mutable.nodes.push("node1".to_string());
        assert_eq!(topology_mutable.node_count(), 1);
        topology_mutable.nodes.push("node2".to_string());
        assert_eq!(topology_mutable.node_count(), 2);
        topology_mutable.version = 10;
        assert_eq!(topology_mutable.version, 10);

        let cloned = topology_mutable.clone();
        assert_eq!(cloned.nodes.len(), 2);
        assert_eq!(cloned.replication_factor, 5);
        assert_eq!(cloned.version, 10);

        let debug_str = format!("{:?}", topology_mutable);
        assert!(debug_str.contains("HashRingTopology"));

        let mut topology_serialize = HashRingTopology::new(3);
        topology_serialize.nodes.push("node1".to_string());
        topology_serialize.nodes.push("node2".to_string());
        topology_serialize.nodes.push("node3".to_string());
        topology_serialize.version = 42;

        let json = serde_json::to_string(&topology_serialize).unwrap();
        let deserialized: HashRingTopology = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.nodes.len(), 3);
        assert_eq!(deserialized.nodes[0], "node1");
        assert_eq!(deserialized.replication_factor, 3);
        assert_eq!(deserialized.version, 42);
    }

    #[test]
    fn test_hash_ring_state_node_lifecycle() {
        let state_default = HashRingState::default();
        assert_eq!(state_default.version(), 0);

        let state_custom1 = HashRingState::new(8, 2);
        let state_custom2 = HashRingState::new(32, 5);
        assert_eq!(state_custom1.version(), 0);
        assert_eq!(state_custom2.version(), 0);
        assert_eq!(state_custom1.object_ring().replication_factor(), 2);
        assert_eq!(state_custom2.object_ring().replication_factor(), 5);

        let mut state = HashRingState::new(16, 3);
        assert_eq!(state.version(), 0);
        assert!(state.get_node_info("nonexistent").is_none());

        let object_node1 = create_test_node("object_node1");
        assert!(state.add_object_node(object_node1).is_ok());
        assert_eq!(state.version(), 1);
        assert!(state.get_node_info("object_node1").is_some());
        assert_eq!(
            state.get_node_info("object_node1").unwrap().id.as_str(),
            "object_node1"
        );

        let object_node2 = create_test_node("object_node2");
        state.add_object_node(object_node2).unwrap();
        assert_eq!(state.version(), 2);
        assert!(state.get_node_info("object_node1").is_some());
        assert!(state.get_node_info("object_node2").is_some());

        let metadata_node1 = create_test_node("metadata_node1");
        assert!(state.add_metadata_node(metadata_node1).is_ok());
        assert_eq!(state.version(), 3);
        assert!(state.get_node_info("metadata_node1").is_some());

        let metadata_node2 = create_test_node("metadata_node2");
        state.add_metadata_node(metadata_node2).unwrap();
        assert_eq!(state.version(), 4);
        assert!(state.get_node_info("metadata_node1").is_some());
        assert!(state.get_node_info("metadata_node2").is_some());

        let object_node_dup = create_test_node("object_node1");
        let _ = state.add_object_node(object_node_dup);
        assert!(state.get_node_info("object_node1").is_some());

        let metadata_node_dup = create_test_node("metadata_node1");
        let _ = state.add_metadata_node(metadata_node_dup);
        assert!(state.get_node_info("metadata_node1").is_some());

        let mut state_transitions = HashRingState::new(16, 3);
        let joining_object = create_test_node_with_state("obj_trans", NodeState::Joining);
        let obj_id = NodeId::new("obj_trans");
        state_transitions.add_object_node(joining_object).unwrap();
        assert_eq!(state_transitions.version(), 1);

        assert!(state_transitions
            .set_object_node_state(&obj_id, NodeState::Active)
            .is_ok());
        assert_eq!(state_transitions.version(), 2);

        state_transitions
            .set_object_node_state(&obj_id, NodeState::Draining)
            .unwrap();
        assert_eq!(state_transitions.version(), 3);

        state_transitions
            .set_object_node_state(&obj_id, NodeState::Down)
            .unwrap();
        assert_eq!(state_transitions.version(), 4);

        let joining_metadata = create_test_node_with_state("meta_trans", NodeState::Joining);
        let meta_id = NodeId::new("meta_trans");
        state_transitions
            .add_metadata_node(joining_metadata)
            .unwrap();
        assert_eq!(state_transitions.version(), 5);

        assert!(state_transitions
            .set_metadata_node_state(&meta_id, NodeState::Active)
            .is_ok());
        assert_eq!(state_transitions.version(), 6);

        state_transitions
            .set_metadata_node_state(&meta_id, NodeState::Draining)
            .unwrap();
        assert_eq!(state_transitions.version(), 7);

        let mut state_remove = HashRingState::new(16, 3);
        let remove_obj = create_test_node("remove_obj");
        let remove_obj_id = NodeId::new("remove_obj");
        state_remove.add_object_node(remove_obj).unwrap();
        assert!(state_remove.get_node_info("remove_obj").is_some());

        assert!(state_remove.remove_object_node(&remove_obj_id).is_ok());
        assert_eq!(state_remove.version(), 2);
        assert!(state_remove.get_node_info("remove_obj").is_none());

        let remove_meta = create_test_node("remove_meta");
        let remove_meta_id = NodeId::new("remove_meta");
        state_remove.add_metadata_node(remove_meta).unwrap();
        assert!(state_remove.get_node_info("remove_meta").is_some());

        assert!(state_remove.remove_metadata_node(&remove_meta_id).is_ok());
        assert_eq!(state_remove.version(), 4);
        assert!(state_remove.get_node_info("remove_meta").is_none());
    }

    #[test]
    fn test_hash_ring_state_lookup_and_topology() {
        let state_empty = HashRingState::new(16, 3);
        assert!(state_empty.lookup_object_nodes(b"test_key").is_empty());
        assert!(state_empty.lookup_metadata_nodes(b"test_key").is_empty());

        let mut state = HashRingState::new(16, 3);
        for i in 0..5 {
            let obj_node =
                create_test_node_with_state(&format!("object_node{}", i), NodeState::Active);
            state.add_object_node(obj_node).unwrap();

            let meta_node =
                create_test_node_with_state(&format!("metadata_node{}", i), NodeState::Active);
            state.add_metadata_node(meta_node).unwrap();
        }

        let obj_nodes = state.lookup_object_nodes(b"test_key");
        assert!(!obj_nodes.is_empty());
        assert!(obj_nodes.len() <= 3);

        let meta_nodes = state.lookup_metadata_nodes(b"test_key");
        assert!(!meta_nodes.is_empty());
        assert!(meta_nodes.len() <= 3);

        let key = b"consistent_key";
        let lookup1 = state.lookup_object_nodes(key);
        let lookup2 = state.lookup_object_nodes(key);
        assert_eq!(lookup1, lookup2);

        let nodes_key1 = state.lookup_object_nodes(b"key1");
        let nodes_key2 = state.lookup_object_nodes(b"key2");
        let nodes_key3 = state.lookup_object_nodes(b"completely_different_key");
        assert!(!nodes_key1.is_empty());
        assert!(!nodes_key2.is_empty());
        assert!(!nodes_key3.is_empty());

        let object_topology = state.object_topology();
        assert_eq!(object_topology.node_count(), 5);
        assert_eq!(object_topology.replication_factor, 3);
        for i in 0..5 {
            assert!(object_topology.nodes.contains(&format!("object_node{}", i)));
        }
        for i in 0..5 {
            assert!(!object_topology
                .nodes
                .contains(&format!("metadata_node{}", i)));
        }

        let metadata_topology = state.metadata_topology();
        assert_eq!(metadata_topology.node_count(), 5);
        assert_eq!(metadata_topology.replication_factor, 3);
        for i in 0..5 {
            assert!(metadata_topology
                .nodes
                .contains(&format!("metadata_node{}", i)));
        }
        for i in 0..5 {
            assert!(!metadata_topology
                .nodes
                .contains(&format!("object_node{}", i)));
        }

        let object_ring = state.object_ring();
        assert_eq!(object_ring.get_nodes().len(), 5);
        assert_eq!(object_ring.replication_factor(), 3);

        let metadata_ring = state.metadata_ring();
        assert_eq!(metadata_ring.get_nodes().len(), 5);
        assert_eq!(metadata_ring.replication_factor(), 3);

        let mut state_single = HashRingState::new(16, 3);
        let single_node = create_test_node_with_state("single_node", NodeState::Active);
        state_single.add_object_node(single_node).unwrap();
        let single_lookup = state_single.lookup_object_nodes(b"any_key");
        assert!(single_lookup.len() <= 1);

        let mut state_joining = HashRingState::new(16, 3);
        let joining_node = create_test_node_with_state("joining_node", NodeState::Joining);
        state_joining.add_object_node(joining_node).unwrap();
        let _ = state_joining.lookup_object_nodes(b"any_key");
    }

    #[test]
    fn test_hash_ring_state_error_handling() {
        let mut state = HashRingState::new(16, 3);

        let nonexistent_id = NodeId::new("nonexistent");
        assert!(state.remove_object_node(&nonexistent_id).is_err());
        assert!(state.remove_metadata_node(&nonexistent_id).is_err());
        assert!(state
            .set_object_node_state(&nonexistent_id, NodeState::Active)
            .is_err());
        assert!(state
            .set_metadata_node_state(&nonexistent_id, NodeState::Active)
            .is_err());

        assert!(state.get_node_info("nonexistent").is_none());
    }

    #[test]
    fn test_hash_ring_state_version_tracking() {
        let mut state = HashRingState::new(16, 3);
        assert_eq!(state.version(), 0);

        let node1 = create_test_node("node1");
        state.add_object_node(node1).unwrap();
        assert_eq!(state.version(), 1);

        let node2 = create_test_node("node2");
        state.add_metadata_node(node2).unwrap();
        assert_eq!(state.version(), 2);

        let node_id1 = NodeId::new("node1");
        state
            .set_object_node_state(&node_id1, NodeState::Active)
            .unwrap();
        assert_eq!(state.version(), 3);

        let node_id2 = NodeId::new("node2");
        state
            .set_metadata_node_state(&node_id2, NodeState::Active)
            .unwrap();
        assert_eq!(state.version(), 4);

        state.remove_object_node(&node_id1).unwrap();
        assert_eq!(state.version(), 5);

        state.remove_metadata_node(&node_id2).unwrap();
        assert_eq!(state.version(), 6);

        let mut state_multi = HashRingState::new(16, 3);
        for i in 0..3 {
            let obj_node = create_test_node(&format!("obj{}", i));
            state_multi.add_object_node(obj_node).unwrap();
        }
        for i in 0..3 {
            let meta_node = create_test_node(&format!("meta{}", i));
            state_multi.add_metadata_node(meta_node).unwrap();
        }
        assert_eq!(state_multi.version(), 6);
        assert_eq!(state_multi.object_topology().node_count(), 3);
        assert_eq!(state_multi.metadata_topology().node_count(), 3);

        let remove_id = NodeId::new("obj0");
        state_multi.remove_object_node(&remove_id).unwrap();
        assert_eq!(state_multi.version(), 7);
        assert_eq!(state_multi.object_topology().node_count(), 2);
    }
}
