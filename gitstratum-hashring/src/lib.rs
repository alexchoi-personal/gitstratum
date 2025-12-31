use gitstratum_core::Oid;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, HashRingError>;

#[derive(Error, Debug)]
pub enum HashRingError {
    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("ring is empty")]
    EmptyRing,

    #[error("insufficient nodes for replication factor {0}, have {1}")]
    InsufficientNodes(usize, usize),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub port: u16,
    pub state: NodeState,
}

impl NodeInfo {
    pub fn new(id: impl Into<String>, address: impl Into<String>, port: u16) -> Self {
        Self {
            id: NodeId::new(id),
            address: address.into(),
            port,
            state: NodeState::Active,
        }
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Active,
    Joining,
    Draining,
    Down,
}

impl NodeState {
    pub fn is_active(&self) -> bool {
        matches!(self, NodeState::Active)
    }

    pub fn can_serve_reads(&self) -> bool {
        matches!(self, NodeState::Active | NodeState::Draining)
    }

    pub fn can_serve_writes(&self) -> bool {
        matches!(self, NodeState::Active)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct VirtualNode {
    position: u64,
    node_id: NodeId,
    virtual_index: u32,
}

#[derive(Debug)]
pub struct ConsistentHashRing {
    ring: RwLock<BTreeMap<u64, VirtualNode>>,
    nodes: RwLock<BTreeMap<NodeId, NodeInfo>>,
    virtual_nodes_per_physical: u32,
    replication_factor: usize,
    version: RwLock<u64>,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes_per_physical: u32, replication_factor: usize) -> Self {
        Self {
            ring: RwLock::new(BTreeMap::new()),
            nodes: RwLock::new(BTreeMap::new()),
            virtual_nodes_per_physical,
            replication_factor,
            version: RwLock::new(0),
        }
    }

    pub fn with_nodes(
        nodes: Vec<NodeInfo>,
        virtual_nodes_per_physical: u32,
        replication_factor: usize,
    ) -> Result<Self> {
        let ring = Self::new(virtual_nodes_per_physical, replication_factor);
        for node in nodes {
            ring.add_node(node)?;
        }
        Ok(ring)
    }

    fn hash_position(node_id: &NodeId, virtual_index: u32) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(node_id.as_str().as_bytes());
        hasher.update(virtual_index.to_le_bytes());
        let result = hasher.finalize();
        u64::from_le_bytes(result[..8].try_into().unwrap())
    }

    fn key_position(key: &[u8]) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(key);
        let result = hasher.finalize();
        u64::from_le_bytes(result[..8].try_into().unwrap())
    }

    pub fn add_node(&self, node: NodeInfo) -> Result<()> {
        let node_id = node.id.clone();

        {
            let mut nodes = self.nodes.write();
            nodes.insert(node_id.clone(), node);
        }

        {
            let mut ring = self.ring.write();
            for i in 0..self.virtual_nodes_per_physical {
                let position = Self::hash_position(&node_id, i);
                let vnode = VirtualNode {
                    position,
                    node_id: node_id.clone(),
                    virtual_index: i,
                };
                ring.insert(position, vnode);
            }
        }

        self.increment_version();
        Ok(())
    }

    pub fn remove_node(&self, node_id: &NodeId) -> Result<NodeInfo> {
        let node = {
            let mut nodes = self.nodes.write();
            nodes
                .remove(node_id)
                .ok_or_else(|| HashRingError::NodeNotFound(node_id.to_string()))?
        };

        {
            let mut ring = self.ring.write();
            let positions_to_remove: Vec<u64> = ring
                .iter()
                .filter(|(_, vnode)| &vnode.node_id == node_id)
                .map(|(pos, _)| *pos)
                .collect();

            for pos in positions_to_remove {
                ring.remove(&pos);
            }
        }

        self.increment_version();
        Ok(node)
    }

    pub fn set_node_state(&self, node_id: &NodeId, state: NodeState) -> Result<()> {
        let mut nodes = self.nodes.write();
        let node = nodes
            .get_mut(node_id)
            .ok_or_else(|| HashRingError::NodeNotFound(node_id.to_string()))?;
        node.state = state;
        drop(nodes);
        self.increment_version();
        Ok(())
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.nodes.read().get(node_id).cloned()
    }

    pub fn get_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().values().cloned().collect()
    }

    pub fn active_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.state.is_active())
            .cloned()
            .collect()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    fn increment_version(&self) {
        let mut version = self.version.write();
        *version += 1;
    }

    pub fn version(&self) -> u64 {
        *self.version.read()
    }

    pub fn primary_node(&self, key: &[u8]) -> Result<NodeInfo> {
        let position = Self::key_position(key);
        let ring = self.ring.read();

        if ring.is_empty() {
            return Err(HashRingError::EmptyRing);
        }

        let vnode = ring
            .range(position..)
            .next()
            .or_else(|| ring.iter().next())
            .map(|(_, v)| v)
            .ok_or(HashRingError::EmptyRing)?;

        self.nodes
            .read()
            .get(&vnode.node_id)
            .cloned()
            .ok_or_else(|| HashRingError::NodeNotFound(vnode.node_id.to_string()))
    }

    pub fn primary_node_for_oid(&self, oid: &Oid) -> Result<NodeInfo> {
        self.primary_node(oid.as_bytes())
    }

    pub fn nodes_for_key(&self, key: &[u8]) -> Result<Vec<NodeInfo>> {
        let nodes = self.nodes.read();
        let active_count = nodes.values().filter(|n| n.state.can_serve_reads()).count();

        if active_count < self.replication_factor {
            return Err(HashRingError::InsufficientNodes(
                self.replication_factor,
                active_count,
            ));
        }

        let position = Self::key_position(key);
        let ring = self.ring.read();

        if ring.is_empty() {
            return Err(HashRingError::EmptyRing);
        }

        let mut result = Vec::with_capacity(self.replication_factor);
        let mut seen_nodes = std::collections::HashSet::new();

        let iter = ring
            .range(position..)
            .chain(ring.iter())
            .map(|(_, v)| v);

        for vnode in iter {
            if seen_nodes.contains(&vnode.node_id) {
                continue;
            }

            let Some(node) = nodes.get(&vnode.node_id) else {
                continue;
            };

            if !node.state.can_serve_reads() {
                continue;
            }

            seen_nodes.insert(vnode.node_id.clone());
            result.push(node.clone());

            if result.len() >= self.replication_factor {
                break;
            }
        }

        if result.len() < self.replication_factor {
            return Err(HashRingError::InsufficientNodes(
                self.replication_factor,
                result.len(),
            ));
        }

        Ok(result)
    }

    pub fn nodes_for_oid(&self, oid: &Oid) -> Result<Vec<NodeInfo>> {
        self.nodes_for_key(oid.as_bytes())
    }

    pub fn nodes_for_prefix(&self, prefix: u8) -> Vec<NodeInfo> {
        let key = [prefix, 0, 0, 0, 0, 0, 0, 0];
        self.nodes_for_key(&key).unwrap_or_default()
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }
}

impl Clone for ConsistentHashRing {
    fn clone(&self) -> Self {
        Self {
            ring: RwLock::new(self.ring.read().clone()),
            nodes: RwLock::new(self.nodes.read().clone()),
            virtual_nodes_per_physical: self.virtual_nodes_per_physical,
            replication_factor: self.replication_factor,
            version: RwLock::new(*self.version.read()),
        }
    }
}

pub struct HashRingBuilder {
    virtual_nodes_per_physical: u32,
    replication_factor: usize,
    nodes: Vec<NodeInfo>,
}

impl HashRingBuilder {
    pub fn new() -> Self {
        Self {
            virtual_nodes_per_physical: 16,
            replication_factor: 2,
            nodes: Vec::new(),
        }
    }

    pub fn virtual_nodes(mut self, count: u32) -> Self {
        self.virtual_nodes_per_physical = count;
        self
    }

    pub fn replication_factor(mut self, rf: usize) -> Self {
        self.replication_factor = rf;
        self
    }

    pub fn add_node(mut self, node: NodeInfo) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn build(self) -> Result<ConsistentHashRing> {
        ConsistentHashRing::with_nodes(
            self.nodes,
            self.virtual_nodes_per_physical,
            self.replication_factor,
        )
    }
}

impl Default for HashRingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node(id: &str) -> NodeInfo {
        NodeInfo::new(id, format!("10.0.0.{}", id.chars().last().unwrap()), 9002)
    }

    #[test]
    fn test_add_remove_node() {
        let ring = ConsistentHashRing::new(16, 2);

        let node1 = create_test_node("node-1");
        ring.add_node(node1.clone()).unwrap();
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.version(), 1);

        let node2 = create_test_node("node-2");
        ring.add_node(node2.clone()).unwrap();
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.version(), 2);

        ring.remove_node(&node1.id).unwrap();
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.version(), 3);
    }

    #[test]
    fn test_primary_node() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let key = b"test-key";
        let primary = ring.primary_node(key).unwrap();
        assert!(["node-1", "node-2", "node-3"].contains(&primary.id.as_str()));

        let primary2 = ring.primary_node(key).unwrap();
        assert_eq!(primary.id, primary2.id);
    }

    #[test]
    fn test_nodes_for_key() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let key = b"test-key";
        let nodes = ring.nodes_for_key(key).unwrap();
        assert_eq!(nodes.len(), 2);

        assert_ne!(nodes[0].id, nodes[1].id);
    }

    #[test]
    fn test_nodes_for_oid() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let oid = Oid::hash(b"test content");
        let nodes = ring.nodes_for_oid(&oid).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_insufficient_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(3)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        let result = ring.nodes_for_key(b"test");
        assert!(matches!(result, Err(HashRingError::InsufficientNodes(3, 2))));
    }

    #[test]
    fn test_node_state() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Draining)
            .unwrap();

        let node = ring.get_node(&NodeId::new("node-1")).unwrap();
        assert_eq!(node.state, NodeState::Draining);
        assert!(node.state.can_serve_reads());
        assert!(!node.state.can_serve_writes());
    }

    #[test]
    fn test_distribution() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .build()
            .unwrap();

        let mut counts = std::collections::HashMap::new();

        for i in 0..1000 {
            let key = format!("key-{}", i);
            let primary = ring.primary_node(key.as_bytes()).unwrap();
            *counts.entry(primary.id.as_str().to_string()).or_insert(0) += 1;
        }

        for (node, count) in &counts {
            let percentage = (*count as f64 / 1000.0) * 100.0;
            assert!(
                percentage > 15.0 && percentage < 35.0,
                "Node {} has {}% of keys, expected ~25%",
                node,
                percentage
            );
        }
    }

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new(16, 2);
        let result = ring.primary_node(b"test");
        assert!(matches!(result, Err(HashRingError::EmptyRing)));
    }

    #[test]
    fn test_node_id() {
        let id = NodeId::new("node-1");
        assert_eq!(id.as_str(), "node-1");
        assert_eq!(format!("{}", id), "node-1");
    }

    #[test]
    fn test_node_info() {
        let node = NodeInfo::new("node-1", "10.0.0.1", 9002);
        assert_eq!(node.endpoint(), "10.0.0.1:9002");
        assert!(node.state.is_active());
        assert!(node.state.can_serve_reads());
        assert!(node.state.can_serve_writes());
    }

    #[test]
    fn test_node_state_draining() {
        let state = NodeState::Draining;
        assert!(!state.is_active());
        assert!(state.can_serve_reads());
        assert!(!state.can_serve_writes());
    }

    #[test]
    fn test_node_state_down() {
        let state = NodeState::Down;
        assert!(!state.is_active());
        assert!(!state.can_serve_reads());
        assert!(!state.can_serve_writes());
    }

    #[test]
    fn test_node_state_joining() {
        let state = NodeState::Joining;
        assert!(!state.is_active());
        assert!(!state.can_serve_reads());
        assert!(!state.can_serve_writes());
    }

    #[test]
    fn test_with_nodes() {
        let nodes = vec![
            create_test_node("node-1"),
            create_test_node("node-2"),
        ];
        let ring = ConsistentHashRing::with_nodes(nodes, 16, 2).unwrap();
        assert_eq!(ring.node_count(), 2);
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let ring = ConsistentHashRing::new(16, 2);
        let result = ring.remove_node(&NodeId::new("nonexistent"));
        assert!(matches!(result, Err(HashRingError::NodeNotFound(_))));
    }

    #[test]
    fn test_set_node_state_nonexistent() {
        let ring = ConsistentHashRing::new(16, 2);
        let result = ring.set_node_state(&NodeId::new("nonexistent"), NodeState::Draining);
        assert!(matches!(result, Err(HashRingError::NodeNotFound(_))));
    }

    #[test]
    fn test_get_nonexistent_node() {
        let ring = ConsistentHashRing::new(16, 2);
        assert!(ring.get_node(&NodeId::new("nonexistent")).is_none());
    }

    #[test]
    fn test_get_nodes() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        let nodes = ring.get_nodes();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_active_nodes() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down).unwrap();

        let active = ring.active_nodes();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn test_primary_node_for_oid() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        let oid = Oid::hash(b"test");
        let node = ring.primary_node_for_oid(&oid).unwrap();
        assert!(["node-1", "node-2"].contains(&node.id.as_str()));
    }

    #[test]
    fn test_nodes_for_prefix() {
        let ring = HashRingBuilder::new()
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let nodes = ring.nodes_for_prefix(0xAB);
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_nodes_for_prefix_empty_ring() {
        let ring = ConsistentHashRing::new(16, 2);
        let nodes = ring.nodes_for_prefix(0xAB);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_replication_factor() {
        let ring = ConsistentHashRing::new(16, 3);
        assert_eq!(ring.replication_factor(), 3);
    }

    #[test]
    fn test_clone_ring() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .build()
            .unwrap();

        let cloned = ring.clone();
        assert_eq!(cloned.node_count(), 1);
        assert_eq!(cloned.version(), ring.version());
    }

    #[test]
    fn test_hash_ring_builder_default() {
        let builder = HashRingBuilder::default();
        let ring = builder.build().unwrap();
        assert_eq!(ring.node_count(), 0);
    }

    #[test]
    fn test_nodes_for_key_empty() {
        let ring = ConsistentHashRing::new(16, 2);
        let result = ring.nodes_for_key(b"test");
        assert!(matches!(result, Err(HashRingError::InsufficientNodes(_, _))));
    }

    #[test]
    fn test_version_increments() {
        let ring = ConsistentHashRing::new(16, 2);
        assert_eq!(ring.version(), 0);

        ring.add_node(create_test_node("node-1")).unwrap();
        assert_eq!(ring.version(), 1);

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Draining).unwrap();
        assert_eq!(ring.version(), 2);

        ring.remove_node(&NodeId::new("node-1")).unwrap();
        assert_eq!(ring.version(), 3);
    }

    #[test]
    fn test_nodes_for_key_with_draining() {
        let ring = HashRingBuilder::new()
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Draining).unwrap();

        let nodes = ring.nodes_for_key(b"test").unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_hash_ring_error_display() {
        let err = HashRingError::NodeNotFound("test".to_string());
        assert!(format!("{}", err).contains("test"));

        let err = HashRingError::EmptyRing;
        assert!(format!("{}", err).contains("empty"));

        let err = HashRingError::InsufficientNodes(3, 2);
        assert!(format!("{}", err).contains("3"));
    }

    #[test]
    fn test_nodes_for_key_insufficient_active_after_state_change() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(3)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();
        ring.set_node_state(&NodeId::new("node-2"), NodeState::Joining)
            .unwrap();

        let result = ring.nodes_for_key(b"test-key");
        assert!(matches!(result, Err(HashRingError::InsufficientNodes(3, _))));
    }

    #[test]
    fn test_nodes_for_key_with_all_down_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();
        ring.set_node_state(&NodeId::new("node-2"), NodeState::Down)
            .unwrap();

        let result = ring.nodes_for_key(b"test-key");
        assert!(matches!(result, Err(HashRingError::InsufficientNodes(2, 0))));
    }

    #[test]
    fn test_distribution_variance() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .build()
            .unwrap();

        let mut counts = std::collections::HashMap::new();

        for i in 0..1000 {
            let key = format!("key-{}", i);
            let primary = ring.primary_node(key.as_bytes()).unwrap();
            *counts.entry(primary.id.as_str().to_string()).or_insert(0) += 1;
        }

        assert_eq!(counts.len(), 4);
        for count in counts.values() {
            assert!(*count > 100);
        }
    }

    #[test]
    fn test_nodes_for_key_with_zero_replication_factor_empty_ring() {
        let ring = ConsistentHashRing::new(16, 0);
        let result = ring.nodes_for_key(b"test");
        assert!(matches!(result, Err(HashRingError::EmptyRing)));
    }

    #[test]
    fn test_nodes_for_key_with_zero_replication_factor_with_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(0)
            .add_node(create_test_node("node-1"))
            .build()
            .unwrap();

        let result = ring.nodes_for_key(b"test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_nodes_for_key_iterates_multiple_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(3)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .add_node(create_test_node("node-5"))
            .build()
            .unwrap();

        for i in 0..100 {
            let key = format!("test-key-{}", i);
            let nodes = ring.nodes_for_key(key.as_bytes()).unwrap();
            assert_eq!(nodes.len(), 3);
            let unique: std::collections::HashSet<_> =
                nodes.iter().map(|n| n.id.as_str()).collect();
            assert_eq!(unique.len(), 3);
        }
    }

    #[test]
    fn test_nodes_for_key_skips_nodes_that_cannot_serve_reads() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();
        ring.set_node_state(&NodeId::new("node-3"), NodeState::Joining)
            .unwrap();

        for i in 0..100 {
            let key = format!("test-key-{}", i);
            let nodes = ring.nodes_for_key(key.as_bytes()).unwrap();
            assert_eq!(nodes.len(), 2);
            for node in &nodes {
                assert!(
                    node.id.as_str() == "node-2" || node.id.as_str() == "node-4",
                    "Expected only node-2 or node-4 but got {}",
                    node.id
                );
            }
        }
    }
}
