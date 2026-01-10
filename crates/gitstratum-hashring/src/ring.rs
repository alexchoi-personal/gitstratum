use std::collections::{BTreeMap, HashMap};

use gitstratum_core::Oid;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use crate::error::{HashRingError, Result};
use crate::node::{NodeId, NodeInfo, NodeState};

#[derive(Debug, Clone)]
pub(crate) struct VirtualNode {
    pub(crate) node_id: NodeId,
}

#[derive(Debug, Clone)]
struct RingState {
    ring: BTreeMap<u64, VirtualNode>,
    nodes: BTreeMap<NodeId, NodeInfo>,
    node_positions: HashMap<NodeId, Vec<u64>>,
    version: u64,
}

impl RingState {
    fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: BTreeMap::new(),
            node_positions: HashMap::new(),
            version: 0,
        }
    }
}

#[derive(Debug)]
pub struct ConsistentHashRing {
    state: RwLock<RingState>,
    virtual_nodes_per_physical: u32,
    replication_factor: usize,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes_per_physical: u32, replication_factor: usize) -> Result<Self> {
        if replication_factor == 0 {
            return Err(HashRingError::InvalidConfig(
                "replication_factor must be greater than 0".to_string(),
            ));
        }
        Ok(Self {
            state: RwLock::new(RingState::new()),
            virtual_nodes_per_physical,
            replication_factor,
        })
    }

    pub fn with_nodes(
        nodes: Vec<NodeInfo>,
        virtual_nodes_per_physical: u32,
        replication_factor: usize,
    ) -> Result<Self> {
        let ring = Self::new(virtual_nodes_per_physical, replication_factor)?;
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
        let bytes: [u8; 8] = result[..8].try_into().unwrap_or([0u8; 8]);
        u64::from_le_bytes(bytes)
    }

    fn key_position(key: &[u8]) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(key);
        let result = hasher.finalize();
        let bytes: [u8; 8] = result[..8].try_into().unwrap_or([0u8; 8]);
        u64::from_le_bytes(bytes)
    }

    fn oid_position(oid: &Oid) -> u64 {
        let bytes: [u8; 8] = oid.as_bytes()[..8].try_into().unwrap_or([0u8; 8]);
        u64::from_le_bytes(bytes)
    }

    pub fn add_node(&self, node: NodeInfo) -> Result<()> {
        let node_id = node.id.clone();
        let mut state = self.state.write();
        state.nodes.insert(node_id.clone(), node);

        let mut positions = Vec::with_capacity(self.virtual_nodes_per_physical as usize);
        for i in 0..self.virtual_nodes_per_physical {
            let position = Self::hash_position(&node_id, i);
            state.ring.insert(
                position,
                VirtualNode {
                    node_id: node_id.clone(),
                },
            );
            positions.push(position);
        }
        state.node_positions.insert(node_id, positions);
        state.version = state.version.wrapping_add(1);
        Ok(())
    }

    pub fn remove_node(&self, node_id: &NodeId) -> Result<NodeInfo> {
        let mut state = self.state.write();
        let node = state
            .nodes
            .remove(node_id)
            .ok_or_else(|| HashRingError::NodeNotFound(node_id.to_string()))?;

        if let Some(positions) = state.node_positions.remove(node_id) {
            for pos in positions {
                state.ring.remove(&pos);
            }
        }
        state.version = state.version.wrapping_add(1);
        Ok(node)
    }

    pub fn set_node_state(&self, node_id: &NodeId, new_state: NodeState) -> Result<()> {
        let mut state = self.state.write();
        let node = state
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| HashRingError::NodeNotFound(node_id.to_string()))?;
        node.state = new_state;
        state.version = state.version.wrapping_add(1);
        Ok(())
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.state.read().nodes.get(node_id).cloned()
    }

    pub fn get_nodes(&self) -> Vec<NodeInfo> {
        self.state.read().nodes.values().cloned().collect()
    }

    pub fn active_nodes(&self) -> Vec<NodeInfo> {
        self.state
            .read()
            .nodes
            .values()
            .filter(|n| n.state.is_active())
            .cloned()
            .collect()
    }

    pub fn node_count(&self) -> usize {
        self.state.read().nodes.len()
    }

    pub fn version(&self) -> u64 {
        self.state.read().version
    }

    pub fn get_ring_entries(&self) -> Vec<(u64, NodeId)> {
        self.state
            .read()
            .ring
            .iter()
            .map(|(pos, vnode)| (*pos, vnode.node_id.clone()))
            .collect()
    }

    fn primary_node_at_position(&self, position: u64) -> Result<NodeInfo> {
        let state = self.state.read();

        if state.ring.is_empty() {
            return Err(HashRingError::EmptyRing);
        }

        let vnode = state
            .ring
            .range(position..)
            .next()
            .or_else(|| state.ring.iter().next())
            .map(|(_, v)| v)
            .ok_or(HashRingError::EmptyRing)?;

        state
            .nodes
            .get(&vnode.node_id)
            .cloned()
            .ok_or_else(|| HashRingError::NodeNotFound(vnode.node_id.to_string()))
    }

    pub fn primary_node(&self, key: &[u8]) -> Result<NodeInfo> {
        let position = Self::key_position(key);
        self.primary_node_at_position(position)
    }

    pub fn primary_node_for_oid(&self, oid: &Oid) -> Result<NodeInfo> {
        let position = Self::oid_position(oid);
        self.primary_node_at_position(position)
    }

    fn nodes_at_position(&self, position: u64) -> Result<Vec<NodeInfo>> {
        let state = self.state.read();

        if state.ring.is_empty() {
            return Err(HashRingError::EmptyRing);
        }

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::with_capacity(self.replication_factor);

        for (_, vnode) in state.ring.range(position..).chain(state.ring.iter()) {
            if !seen.insert(&vnode.node_id) {
                continue;
            }

            let Some(node) = state.nodes.get(&vnode.node_id) else {
                continue;
            };

            if !node.state.can_serve_reads() {
                continue;
            }

            result.push(node.clone());

            if result.len() >= self.replication_factor {
                break;
            }
        }

        if result.len() < self.replication_factor {
            let active_count = state
                .nodes
                .values()
                .filter(|n| n.state.can_serve_reads())
                .count();
            return Err(HashRingError::InsufficientNodes(
                self.replication_factor,
                active_count,
            ));
        }

        Ok(result)
    }

    pub fn nodes_for_key(&self, key: &[u8]) -> Result<Vec<NodeInfo>> {
        let position = Self::key_position(key);
        self.nodes_at_position(position)
    }

    pub fn nodes_for_oid(&self, oid: &Oid) -> Result<Vec<NodeInfo>> {
        let position = Self::oid_position(oid);
        self.nodes_at_position(position)
    }

    pub fn nodes_for_prefix(&self, prefix: u8) -> Result<Vec<NodeInfo>> {
        let key = [prefix, 0, 0, 0, 0, 0, 0, 0];
        self.nodes_for_key(&key)
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }
}

impl Clone for ConsistentHashRing {
    fn clone(&self) -> Self {
        let state_guard = self.state.read();

        Self {
            state: RwLock::new(state_guard.clone()),
            virtual_nodes_per_physical: self.virtual_nodes_per_physical,
            replication_factor: self.replication_factor,
        }
    }
}
