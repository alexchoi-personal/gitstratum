use std::collections::BTreeMap;

use gitstratum_core::Oid;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use crate::error::{HashRingError, Result};
use crate::node::{NodeId, NodeInfo, NodeState};

#[derive(Debug, Clone)]
pub(crate) struct VirtualNode {
    pub(crate) node_id: NodeId,
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
        u64::from_le_bytes(
            result[..8]
                .try_into()
                .expect("SHA256 always produces 32 bytes"),
        )
    }

    fn key_position(key: &[u8]) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(key);
        let result = hasher.finalize();
        u64::from_le_bytes(
            result[..8]
                .try_into()
                .expect("SHA256 always produces 32 bytes"),
        )
    }

    fn oid_position(oid: &Oid) -> u64 {
        u64::from_le_bytes(
            oid.as_bytes()[..8]
                .try_into()
                .expect("OID is always 32 bytes"),
        )
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
                    node_id: node_id.clone(),
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
        *version = version.wrapping_add(1);
    }

    pub fn version(&self) -> u64 {
        *self.version.read()
    }

    pub fn get_ring_entries(&self) -> Vec<(u64, NodeId)> {
        self.ring
            .read()
            .iter()
            .map(|(pos, vnode)| (*pos, vnode.node_id.clone()))
            .collect()
    }

    fn primary_node_at_position(&self, position: u64) -> Result<NodeInfo> {
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

    pub fn primary_node(&self, key: &[u8]) -> Result<NodeInfo> {
        let position = Self::key_position(key);
        self.primary_node_at_position(position)
    }

    pub fn primary_node_for_oid(&self, oid: &Oid) -> Result<NodeInfo> {
        let position = Self::oid_position(oid);
        self.primary_node_at_position(position)
    }

    fn nodes_at_position(&self, position: u64) -> Result<Vec<NodeInfo>> {
        let candidate_node_ids: Vec<NodeId> = {
            let ring = self.ring.read();
            if ring.is_empty() {
                return Err(HashRingError::EmptyRing);
            }

            let mut seen = std::collections::HashSet::new();
            ring.range(position..)
                .chain(ring.iter())
                .filter_map(|(_, vnode)| {
                    if seen.insert(vnode.node_id.clone()) {
                        Some(vnode.node_id.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        let nodes = self.nodes.read();
        let mut result = Vec::with_capacity(self.replication_factor);

        for node_id in candidate_node_ids {
            let Some(node) = nodes.get(&node_id) else {
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
            let active_count = nodes.values().filter(|n| n.state.can_serve_reads()).count();
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
        Self {
            ring: RwLock::new(self.ring.read().clone()),
            nodes: RwLock::new(self.nodes.read().clone()),
            virtual_nodes_per_physical: self.virtual_nodes_per_physical,
            replication_factor: self.replication_factor,
            version: RwLock::new(*self.version.read()),
        }
    }
}
