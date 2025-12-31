use gitstratum_hashring::{ConsistentHashRing, NodeId, NodeInfo, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::locks::LockInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeType {
    ControlPlane,
    Metadata,
    Object,
    Frontend,
}

impl From<NodeType> for i32 {
    fn from(node_type: NodeType) -> Self {
        match node_type {
            NodeType::ControlPlane => 1,
            NodeType::Metadata => 2,
            NodeType::Object => 3,
            NodeType::Frontend => 4,
        }
    }
}

impl TryFrom<i32> for NodeType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(NodeType::ControlPlane),
            2 => Ok(NodeType::Metadata),
            3 => Ok(NodeType::Object),
            4 => Ok(NodeType::Frontend),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtendedNodeInfo {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub state: NodeState,
    pub node_type: NodeType,
}

impl ExtendedNodeInfo {
    pub fn new(
        id: impl Into<String>,
        address: impl Into<String>,
        port: u16,
        node_type: NodeType,
    ) -> Self {
        Self {
            id: id.into(),
            address: address.into(),
            port,
            state: NodeState::Active,
            node_type,
        }
    }

    pub fn to_node_info(&self) -> NodeInfo {
        let mut info = NodeInfo::new(self.id.clone(), self.address.clone(), self.port);
        info.state = self.state;
        info
    }

    pub fn node_id(&self) -> NodeId {
        NodeId::new(self.id.clone())
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefLockKey {
    pub repo_id: String,
    pub ref_name: String,
}

impl RefLockKey {
    pub fn new(repo_id: impl Into<String>, ref_name: impl Into<String>) -> Self {
        Self {
            repo_id: repo_id.into(),
            ref_name: ref_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub control_plane_nodes: HashMap<String, ExtendedNodeInfo>,
    pub metadata_nodes: HashMap<String, ExtendedNodeInfo>,
    pub object_nodes: HashMap<String, ExtendedNodeInfo>,
    pub frontend_nodes: HashMap<String, ExtendedNodeInfo>,
    pub ref_locks: HashMap<RefLockKey, LockInfo>,
    pub version: u64,
    pub leader_id: Option<String>,
}

impl ClusterStateSnapshot {
    pub fn new() -> Self {
        Self {
            control_plane_nodes: HashMap::new(),
            metadata_nodes: HashMap::new(),
            object_nodes: HashMap::new(),
            frontend_nodes: HashMap::new(),
            ref_locks: HashMap::new(),
            version: 0,
            leader_id: None,
        }
    }

    pub fn add_node(&mut self, node: ExtendedNodeInfo) {
        let nodes = match node.node_type {
            NodeType::ControlPlane => &mut self.control_plane_nodes,
            NodeType::Metadata => &mut self.metadata_nodes,
            NodeType::Object => &mut self.object_nodes,
            NodeType::Frontend => &mut self.frontend_nodes,
        };
        nodes.insert(node.id.clone(), node);
        self.version += 1;
    }

    pub fn remove_node(&mut self, node_id: &str, node_type: NodeType) -> Option<ExtendedNodeInfo> {
        let nodes = match node_type {
            NodeType::ControlPlane => &mut self.control_plane_nodes,
            NodeType::Metadata => &mut self.metadata_nodes,
            NodeType::Object => &mut self.object_nodes,
            NodeType::Frontend => &mut self.frontend_nodes,
        };
        let removed = nodes.remove(node_id);
        if removed.is_some() {
            self.version += 1;
        }
        removed
    }

    pub fn set_node_state(
        &mut self,
        node_id: &str,
        node_type: NodeType,
        state: NodeState,
    ) -> bool {
        let nodes = match node_type {
            NodeType::ControlPlane => &mut self.control_plane_nodes,
            NodeType::Metadata => &mut self.metadata_nodes,
            NodeType::Object => &mut self.object_nodes,
            NodeType::Frontend => &mut self.frontend_nodes,
        };
        if let Some(node) = nodes.get_mut(node_id) {
            node.state = state;
            self.version += 1;
            true
        } else {
            false
        }
    }

    pub fn get_node(&self, node_id: &str) -> Option<&ExtendedNodeInfo> {
        self.control_plane_nodes
            .get(node_id)
            .or_else(|| self.metadata_nodes.get(node_id))
            .or_else(|| self.object_nodes.get(node_id))
            .or_else(|| self.frontend_nodes.get(node_id))
    }

    pub fn find_node_type(&self, node_id: &str) -> Option<NodeType> {
        if self.control_plane_nodes.contains_key(node_id) {
            Some(NodeType::ControlPlane)
        } else if self.metadata_nodes.contains_key(node_id) {
            Some(NodeType::Metadata)
        } else if self.object_nodes.contains_key(node_id) {
            Some(NodeType::Object)
        } else if self.frontend_nodes.contains_key(node_id) {
            Some(NodeType::Frontend)
        } else {
            None
        }
    }

    pub fn all_nodes(&self) -> Vec<&ExtendedNodeInfo> {
        self.control_plane_nodes
            .values()
            .chain(self.metadata_nodes.values())
            .chain(self.object_nodes.values())
            .chain(self.frontend_nodes.values())
            .collect()
    }
}

impl Default for ClusterStateSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClusterState {
    snapshot: parking_lot::RwLock<ClusterStateSnapshot>,
    object_ring: parking_lot::RwLock<ConsistentHashRing>,
    metadata_ring: parking_lot::RwLock<ConsistentHashRing>,
    lock_timeouts: parking_lot::RwLock<HashMap<String, Instant>>,
}

impl ClusterState {
    pub fn new(virtual_nodes: u32, replication_factor: usize) -> Self {
        Self {
            snapshot: parking_lot::RwLock::new(ClusterStateSnapshot::new()),
            object_ring: parking_lot::RwLock::new(ConsistentHashRing::new(
                virtual_nodes,
                replication_factor,
            )),
            metadata_ring: parking_lot::RwLock::new(ConsistentHashRing::new(
                virtual_nodes,
                replication_factor,
            )),
            lock_timeouts: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn add_node(&self, node: ExtendedNodeInfo) -> crate::Result<()> {
        let node_type = node.node_type;
        let node_info = node.to_node_info();

        match node_type {
            NodeType::Object => {
                self.object_ring.write().add_node(node_info)?;
            }
            NodeType::Metadata => {
                self.metadata_ring.write().add_node(node_info)?;
            }
            _ => {}
        }

        self.snapshot.write().add_node(node);
        Ok(())
    }

    pub fn remove_node(&self, node_id: &str) -> crate::Result<Option<ExtendedNodeInfo>> {
        let snapshot = self.snapshot.read();
        let node_type = match snapshot.find_node_type(node_id) {
            Some(t) => t,
            None => return Ok(None),
        };
        drop(snapshot);

        let node_id_obj = NodeId::new(node_id);
        match node_type {
            NodeType::Object => {
                let _ = self.object_ring.write().remove_node(&node_id_obj);
            }
            NodeType::Metadata => {
                let _ = self.metadata_ring.write().remove_node(&node_id_obj);
            }
            _ => {}
        }

        Ok(self.snapshot.write().remove_node(node_id, node_type))
    }

    pub fn set_node_state(&self, node_id: &str, state: NodeState) -> crate::Result<bool> {
        let snapshot = self.snapshot.read();
        let node_type = match snapshot.find_node_type(node_id) {
            Some(t) => t,
            None => return Ok(false),
        };
        drop(snapshot);

        let node_id_obj = NodeId::new(node_id);
        match node_type {
            NodeType::Object => {
                let _ = self.object_ring.write().set_node_state(&node_id_obj, state);
            }
            NodeType::Metadata => {
                let _ = self.metadata_ring.write().set_node_state(&node_id_obj, state);
            }
            _ => {}
        }

        Ok(self.snapshot.write().set_node_state(node_id, node_type, state))
    }

    pub fn get_node(&self, node_id: &str) -> Option<ExtendedNodeInfo> {
        self.snapshot.read().get_node(node_id).cloned()
    }

    pub fn snapshot(&self) -> ClusterStateSnapshot {
        self.snapshot.read().clone()
    }

    pub fn apply_snapshot(&self, snapshot: ClusterStateSnapshot) -> crate::Result<()> {
        {
            let object_ring = self.object_ring.write();
            for node in snapshot.object_nodes.values() {
                let _ = object_ring.clone().add_node(node.to_node_info());
            }
            drop(object_ring);
        }

        {
            let metadata_ring = self.metadata_ring.write();
            for node in snapshot.metadata_nodes.values() {
                let _ = metadata_ring.clone().add_node(node.to_node_info());
            }
            drop(metadata_ring);
        }

        *self.snapshot.write() = snapshot;
        Ok(())
    }

    pub fn object_ring(&self) -> ConsistentHashRing {
        self.object_ring.read().clone()
    }

    pub fn metadata_ring(&self) -> ConsistentHashRing {
        self.metadata_ring.read().clone()
    }

    pub fn acquire_lock(&self, key: RefLockKey, lock: LockInfo, _timeout: Duration) -> bool {
        let mut snapshot = self.snapshot.write();

        if let Some(existing) = snapshot.ref_locks.get(&key) {
            let timeouts = self.lock_timeouts.read();
            if let Some(&acquired_at) = timeouts.get(&existing.lock_id) {
                if acquired_at.elapsed() < Duration::from_millis(existing.timeout_ms) {
                    return false;
                }
            }
        }

        let lock_id = lock.lock_id.clone();
        snapshot.ref_locks.insert(key, lock);
        snapshot.version += 1;

        let mut timeouts = self.lock_timeouts.write();
        timeouts.insert(lock_id, Instant::now());

        true
    }

    pub fn release_lock(&self, lock_id: &str) -> bool {
        let mut snapshot = self.snapshot.write();

        let key = snapshot
            .ref_locks
            .iter()
            .find(|(_, v)| v.lock_id == lock_id)
            .map(|(k, _)| k.clone());

        if let Some(key) = key {
            snapshot.ref_locks.remove(&key);
            snapshot.version += 1;

            let mut timeouts = self.lock_timeouts.write();
            timeouts.remove(lock_id);
            true
        } else {
            false
        }
    }

    pub fn get_lock(&self, key: &RefLockKey) -> Option<LockInfo> {
        self.snapshot.read().ref_locks.get(key).cloned()
    }

    pub fn cleanup_expired_locks(&self) {
        let mut snapshot = self.snapshot.write();
        let timeouts = self.lock_timeouts.read();

        let expired: Vec<RefLockKey> = snapshot
            .ref_locks
            .iter()
            .filter(|(_, lock)| {
                if let Some(&acquired_at) = timeouts.get(&lock.lock_id) {
                    acquired_at.elapsed() >= Duration::from_millis(lock.timeout_ms)
                } else {
                    true
                }
            })
            .map(|(k, _)| k.clone())
            .collect();

        drop(timeouts);

        let mut timeouts = self.lock_timeouts.write();
        for key in expired {
            if let Some(lock) = snapshot.ref_locks.remove(&key) {
                timeouts.remove(&lock.lock_id);
            }
        }

        if !snapshot.ref_locks.is_empty() {
            snapshot.version += 1;
        }
    }

    pub fn set_leader(&self, leader_id: Option<String>) {
        self.snapshot.write().leader_id = leader_id;
    }

    pub fn leader_id(&self) -> Option<String> {
        self.snapshot.read().leader_id.clone()
    }

    pub fn version(&self) -> u64 {
        self.snapshot.read().version
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new(16, 3)
    }
}
