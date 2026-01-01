use gitstratum_hashring::{ConsistentHashRing, NodeId, NodeInfo, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockInfo {
    pub lock_id: String,
    pub repo_id: String,
    pub ref_name: String,
    pub holder_id: String,
    pub timeout_ms: u64,
    pub acquired_at_epoch_ms: u64,
}

impl LockInfo {
    pub fn new(
        lock_id: impl Into<String>,
        repo_id: impl Into<String>,
        ref_name: impl Into<String>,
        holder_id: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            lock_id: lock_id.into(),
            repo_id: repo_id.into(),
            ref_name: ref_name.into(),
            holder_id: holder_id.into(),
            timeout_ms: timeout.as_millis() as u64,
            acquired_at_epoch_ms: now,
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        now > self.acquired_at_epoch_ms + self.timeout_ms
    }

    pub fn remaining_ms(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let expires_at = self.acquired_at_epoch_ms + self.timeout_ms;
        if now >= expires_at {
            0
        } else {
            expires_at - now
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireLockRequest {
    pub repo_id: String,
    pub ref_name: String,
    pub holder_id: String,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireLockResponse {
    pub acquired: bool,
    pub lock_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseLockRequest {
    pub lock_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseLockResponse {
    pub success: bool,
    pub error: Option<String>,
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

    pub fn set_node_state(&mut self, node_id: &str, node_type: NodeType, state: NodeState) -> bool {
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
                let _ = self
                    .metadata_ring
                    .write()
                    .set_node_state(&node_id_obj, state);
            }
            _ => {}
        }

        Ok(self
            .snapshot
            .write()
            .set_node_state(node_id, node_type, state))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_node_type_conversions_and_traits() {
        let node_types = [
            (NodeType::ControlPlane, 1),
            (NodeType::Metadata, 2),
            (NodeType::Object, 3),
            (NodeType::Frontend, 4),
        ];

        let mut hash_set = HashSet::new();
        for (node_type, expected_i32) in &node_types {
            assert_eq!(i32::from(*node_type), *expected_i32);
            assert_eq!(NodeType::try_from(*expected_i32), Ok(*node_type));

            let cloned = node_type.clone();
            let copied = *node_type;
            assert_eq!(*node_type, cloned);
            assert_eq!(*node_type, copied);

            hash_set.insert(*node_type);

            let debug_str = format!("{:?}", node_type);
            assert!(!debug_str.is_empty());
        }
        assert_eq!(hash_set.len(), 4);

        assert_ne!(NodeType::ControlPlane, NodeType::Metadata);
        assert_ne!(NodeType::Object, NodeType::Frontend);

        assert_eq!(NodeType::try_from(0), Err(()));
        assert_eq!(NodeType::try_from(5), Err(()));
        assert_eq!(NodeType::try_from(-1), Err(()));
    }

    #[test]
    fn test_node_info_types_and_lock_data_structures() {
        let info = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Object);
        assert_eq!(info.id, "node1");
        assert_eq!(info.address, "192.168.1.1");
        assert_eq!(info.port, 8080);
        assert_eq!(info.state, NodeState::Active);
        assert_eq!(info.node_type, NodeType::Object);
        assert_eq!(info.endpoint(), "192.168.1.1:8080");
        assert_eq!(info.node_id().as_str(), "node1");

        let node_info = info.to_node_info();
        assert_eq!(node_info.id.as_str(), "node1");
        assert_eq!(node_info.address, "192.168.1.1");
        assert_eq!(node_info.port, 8080);
        assert_eq!(node_info.state, NodeState::Active);

        let info_cloned = info.clone();
        assert_eq!(info, info_cloned);
        let info_different = ExtendedNodeInfo::new("node2", "192.168.1.1", 8080, NodeType::Object);
        assert_ne!(info, info_different);

        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("node1"));
        assert!(debug_str.contains("192.168.1.1"));

        let info_from_strings = ExtendedNodeInfo::new(
            String::from("node1"),
            String::from("192.168.1.1"),
            8080,
            NodeType::Object,
        );
        assert_eq!(info_from_strings.id, "node1");

        let key = RefLockKey::new("repo1", "refs/heads/main");
        assert_eq!(key.repo_id, "repo1");
        assert_eq!(key.ref_name, "refs/heads/main");
        let key_cloned = key.clone();
        assert_eq!(key, key_cloned);

        let key_debug = format!("{:?}", key);
        assert!(key_debug.contains("repo1"));
        assert!(key_debug.contains("refs/heads/main"));

        let key_from_strings =
            RefLockKey::new(String::from("repo1"), String::from("refs/heads/main"));
        assert_eq!(key_from_strings.repo_id, "repo1");

        let mut key_set = HashSet::new();
        key_set.insert(RefLockKey::new("repo1", "refs/heads/main"));
        key_set.insert(RefLockKey::new("repo1", "refs/heads/dev"));
        key_set.insert(RefLockKey::new("repo2", "refs/heads/main"));
        assert_eq!(key_set.len(), 3);
        key_set.insert(RefLockKey::new("repo1", "refs/heads/main"));
        assert_eq!(key_set.len(), 3);

        let lock = LockInfo::new(
            "lock1",
            "repo1",
            "refs/heads/main",
            "holder1",
            Duration::from_secs(30),
        );
        assert_eq!(lock.lock_id, "lock1");
        assert_eq!(lock.repo_id, "repo1");
        assert_eq!(lock.ref_name, "refs/heads/main");
        assert_eq!(lock.holder_id, "holder1");
        assert_eq!(lock.timeout_ms, 30000);
        assert!(lock.acquired_at_epoch_ms > 0);
        assert!(!lock.is_expired());

        let lock_cloned = lock.clone();
        assert_eq!(lock.lock_id, lock_cloned.lock_id);
        assert_eq!(lock.timeout_ms, lock_cloned.timeout_ms);

        let remaining = lock.remaining_ms();
        assert!(remaining > 0);
        assert!(remaining <= 30000);

        let lock1 = LockInfo {
            lock_id: "lock1".to_string(),
            repo_id: "repo1".to_string(),
            ref_name: "refs/heads/main".to_string(),
            holder_id: "holder1".to_string(),
            timeout_ms: 30000,
            acquired_at_epoch_ms: 1000,
        };
        let lock2 = lock1.clone();
        let lock3 = LockInfo {
            lock_id: "lock2".to_string(),
            ..lock1.clone()
        };
        assert_eq!(lock1, lock2);
        assert_ne!(lock1, lock3);

        let lock_from_strings = LockInfo::new(
            String::from("lock1"),
            String::from("repo1"),
            String::from("refs/heads/main"),
            String::from("holder1"),
            Duration::from_secs(30),
        );
        assert_eq!(lock_from_strings.lock_id, "lock1");

        let req = AcquireLockRequest {
            repo_id: "repo1".to_string(),
            ref_name: "refs/heads/main".to_string(),
            holder_id: "holder1".to_string(),
            timeout_ms: 30000,
        };
        assert_eq!(req.repo_id, "repo1");
        assert_eq!(req.timeout_ms, 30000);

        let resp_success = AcquireLockResponse {
            acquired: true,
            lock_id: Some("lock1".to_string()),
            error: None,
        };
        assert!(resp_success.acquired);
        assert!(resp_success.lock_id.is_some());
        assert!(resp_success.error.is_none());

        let resp_failed = AcquireLockResponse {
            acquired: false,
            lock_id: None,
            error: Some("Lock held".to_string()),
        };
        assert!(!resp_failed.acquired);
        assert!(resp_failed.error.is_some());

        let release_req = ReleaseLockRequest {
            lock_id: "lock1".to_string(),
        };
        assert_eq!(release_req.lock_id, "lock1");

        let release_resp = ReleaseLockResponse {
            success: true,
            error: None,
        };
        assert!(release_resp.success);

        let release_resp_failed = ReleaseLockResponse {
            success: false,
            error: Some("Not found".to_string()),
        };
        assert!(!release_resp_failed.success);
        assert!(release_resp_failed.error.is_some());
    }

    #[test]
    fn test_lock_expiration_and_remaining_time() {
        let short_lock = LockInfo::new(
            "lock1",
            "repo1",
            "refs/heads/main",
            "holder1",
            Duration::from_millis(50),
        );
        assert!(!short_lock.is_expired());
        assert!(short_lock.remaining_ms() > 0);

        std::thread::sleep(Duration::from_millis(100));

        assert!(short_lock.is_expired());
        assert_eq!(short_lock.remaining_ms(), 0);
    }

    #[test]
    fn test_cluster_state_snapshot_node_management_workflow() {
        let snapshot = ClusterStateSnapshot::new();
        assert!(snapshot.control_plane_nodes.is_empty());
        assert!(snapshot.metadata_nodes.is_empty());
        assert!(snapshot.object_nodes.is_empty());
        assert!(snapshot.frontend_nodes.is_empty());
        assert!(snapshot.ref_locks.is_empty());
        assert_eq!(snapshot.version, 0);
        assert!(snapshot.leader_id.is_none());

        let default_snapshot = ClusterStateSnapshot::default();
        assert_eq!(default_snapshot.version, 0);

        let mut snapshot = ClusterStateSnapshot::new();

        let cp_node = ExtendedNodeInfo::new("cp1", "192.168.1.1", 8080, NodeType::ControlPlane);
        snapshot.add_node(cp_node);
        assert_eq!(snapshot.control_plane_nodes.len(), 1);
        assert_eq!(snapshot.version, 1);

        let md_node = ExtendedNodeInfo::new("md1", "192.168.1.2", 8080, NodeType::Metadata);
        snapshot.add_node(md_node);
        assert_eq!(snapshot.metadata_nodes.len(), 1);
        assert_eq!(snapshot.version, 2);

        let obj_node = ExtendedNodeInfo::new("obj1", "192.168.1.3", 8080, NodeType::Object);
        snapshot.add_node(obj_node);
        assert_eq!(snapshot.object_nodes.len(), 1);
        assert_eq!(snapshot.version, 3);

        let fe_node = ExtendedNodeInfo::new("fe1", "192.168.1.4", 8080, NodeType::Frontend);
        snapshot.add_node(fe_node);
        assert_eq!(snapshot.frontend_nodes.len(), 1);
        assert_eq!(snapshot.version, 4);

        assert!(snapshot.get_node("cp1").is_some());
        assert!(snapshot.get_node("md1").is_some());
        assert!(snapshot.get_node("obj1").is_some());
        assert!(snapshot.get_node("fe1").is_some());
        assert!(snapshot.get_node("nonexistent").is_none());

        assert_eq!(snapshot.find_node_type("cp1"), Some(NodeType::ControlPlane));
        assert_eq!(snapshot.find_node_type("md1"), Some(NodeType::Metadata));
        assert_eq!(snapshot.find_node_type("obj1"), Some(NodeType::Object));
        assert_eq!(snapshot.find_node_type("fe1"), Some(NodeType::Frontend));
        assert!(snapshot.find_node_type("nonexistent").is_none());

        let all_nodes = snapshot.all_nodes();
        assert_eq!(all_nodes.len(), 4);

        let result = snapshot.set_node_state("obj1", NodeType::Object, NodeState::Draining);
        assert!(result);
        assert_eq!(
            snapshot.object_nodes.get("obj1").unwrap().state,
            NodeState::Draining
        );

        let result = snapshot.set_node_state("nonexistent", NodeType::Object, NodeState::Draining);
        assert!(!result);

        let removed = snapshot.remove_node("cp1", NodeType::ControlPlane);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, "cp1");
        assert!(snapshot.control_plane_nodes.is_empty());

        let not_found = snapshot.remove_node("nonexistent", NodeType::ControlPlane);
        assert!(not_found.is_none());
    }

    #[test]
    fn test_cluster_state_full_node_lifecycle_workflow() {
        let state = ClusterState::new(32, 5);
        assert_eq!(state.version(), 0);
        assert!(state.leader_id().is_none());

        let default_state = ClusterState::default();
        assert_eq!(default_state.version(), 0);

        let state = ClusterState::default();

        let cp_node = ExtendedNodeInfo::new("cp1", "192.168.1.1", 8080, NodeType::ControlPlane);
        state.add_node(cp_node).unwrap();
        let md_node = ExtendedNodeInfo::new("md1", "192.168.1.2", 8080, NodeType::Metadata);
        state.add_node(md_node).unwrap();
        let obj_node = ExtendedNodeInfo::new("obj1", "192.168.1.3", 8080, NodeType::Object);
        state.add_node(obj_node).unwrap();
        let fe_node = ExtendedNodeInfo::new("fe1", "192.168.1.4", 8080, NodeType::Frontend);
        state.add_node(fe_node).unwrap();

        let snapshot = state.snapshot();
        assert_eq!(snapshot.control_plane_nodes.len(), 1);
        assert_eq!(snapshot.metadata_nodes.len(), 1);
        assert_eq!(snapshot.object_nodes.len(), 1);
        assert_eq!(snapshot.frontend_nodes.len(), 1);

        let node = state.get_node("obj1");
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "obj1");
        assert!(state.get_node("nonexistent").is_none());

        let obj_ring = state.object_ring();
        assert!(obj_ring.node_count() >= 1);
        let md_ring = state.metadata_ring();
        assert!(md_ring.node_count() >= 1);

        let result = state.set_node_state("obj1", NodeState::Draining).unwrap();
        assert!(result);
        assert_eq!(state.get_node("obj1").unwrap().state, NodeState::Draining);

        let result = state.set_node_state("md1", NodeState::Draining).unwrap();
        assert!(result);

        let result = state.set_node_state("cp1", NodeState::Draining).unwrap();
        assert!(result);

        let result = state
            .set_node_state("nonexistent", NodeState::Draining)
            .unwrap();
        assert!(!result);

        let removed = state.remove_node("obj1").unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, "obj1");
        assert!(state.snapshot().object_nodes.is_empty());

        let removed = state.remove_node("md1").unwrap();
        assert!(removed.is_some());

        let removed = state.remove_node("cp1").unwrap();
        assert!(removed.is_some());

        let not_found = state.remove_node("nonexistent").unwrap();
        assert!(not_found.is_none());

        state.set_leader(Some("leader1".to_string()));
        assert_eq!(state.leader_id(), Some("leader1".to_string()));
        state.set_leader(None);
        assert!(state.leader_id().is_none());

        let mut new_snapshot = ClusterStateSnapshot::new();
        new_snapshot.add_node(ExtendedNodeInfo::new(
            "restored_obj",
            "192.168.2.1",
            8080,
            NodeType::Object,
        ));
        new_snapshot.add_node(ExtendedNodeInfo::new(
            "restored_md",
            "192.168.2.2",
            8080,
            NodeType::Metadata,
        ));
        new_snapshot.leader_id = Some("restored_leader".to_string());

        state.apply_snapshot(new_snapshot).unwrap();

        let current = state.snapshot();
        assert_eq!(current.object_nodes.len(), 1);
        assert!(current.object_nodes.contains_key("restored_obj"));
        assert_eq!(current.metadata_nodes.len(), 1);
        assert_eq!(current.leader_id, Some("restored_leader".to_string()));
    }

    #[test]
    fn test_cluster_state_locking_workflow() {
        let state = ClusterState::default();

        let key1 = RefLockKey::new("repo1", "refs/heads/main");
        assert!(state.get_lock(&key1).is_none());

        let lock1 = LockInfo::new(
            "lock1",
            "repo1",
            "refs/heads/main",
            "holder1",
            Duration::from_secs(30),
        );
        let acquired = state.acquire_lock(key1.clone(), lock1, Duration::from_secs(30));
        assert!(acquired);

        let retrieved = state.get_lock(&key1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().lock_id, "lock1");

        let lock2 = LockInfo::new(
            "lock2",
            "repo1",
            "refs/heads/main",
            "holder2",
            Duration::from_secs(30),
        );
        let acquired = state.acquire_lock(key1.clone(), lock2, Duration::from_secs(30));
        assert!(!acquired);

        let key2 = RefLockKey::new("repo2", "refs/heads/main");
        let lock3 = LockInfo::new(
            "lock3",
            "repo2",
            "refs/heads/main",
            "holder1",
            Duration::from_secs(30),
        );
        let acquired = state.acquire_lock(key2.clone(), lock3, Duration::from_secs(30));
        assert!(acquired);

        let released = state.release_lock("lock1");
        assert!(released);
        assert!(state.get_lock(&key1).is_none());

        let released_again = state.release_lock("lock1");
        assert!(!released_again);

        let not_found = state.release_lock("nonexistent");
        assert!(!not_found);

        state.release_lock("lock3");

        let short_key = RefLockKey::new("repo3", "refs/heads/feature");
        let short_lock = LockInfo::new(
            "short_lock",
            "repo3",
            "refs/heads/feature",
            "holder1",
            Duration::from_millis(50),
        );
        state.acquire_lock(short_key.clone(), short_lock, Duration::from_millis(50));
        assert!(state.get_lock(&short_key).is_some());

        std::thread::sleep(Duration::from_millis(100));

        let new_lock = LockInfo::new(
            "new_lock",
            "repo3",
            "refs/heads/feature",
            "holder2",
            Duration::from_secs(30),
        );
        let acquired = state.acquire_lock(short_key.clone(), new_lock, Duration::from_secs(30));
        assert!(acquired);

        let expired_key = RefLockKey::new("repo4", "refs/heads/cleanup");
        let expired_lock = LockInfo::new(
            "expired_lock",
            "repo4",
            "refs/heads/cleanup",
            "holder1",
            Duration::from_millis(50),
        );
        state.acquire_lock(expired_key.clone(), expired_lock, Duration::from_millis(50));

        let active_key = RefLockKey::new("repo5", "refs/heads/active");
        let active_lock = LockInfo::new(
            "active_lock",
            "repo5",
            "refs/heads/active",
            "holder1",
            Duration::from_secs(60),
        );
        state.acquire_lock(active_key.clone(), active_lock, Duration::from_secs(60));

        std::thread::sleep(Duration::from_millis(100));

        state.cleanup_expired_locks();

        assert!(state.get_lock(&expired_key).is_none());
        assert!(state.get_lock(&active_key).is_some());
    }

    #[test]
    fn test_cluster_state_concurrent_access() {
        let state = Arc::new(ClusterState::default());
        let mut handles = vec![];

        for i in 0..10 {
            let state_clone = Arc::clone(&state);
            let handle = thread::spawn(move || {
                let node = ExtendedNodeInfo::new(
                    format!("node{}", i),
                    format!("192.168.1.{}", i),
                    8080 + i as u16,
                    NodeType::Object,
                );
                state_clone.add_node(node).unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = state.snapshot();
        assert_eq!(snapshot.object_nodes.len(), 10);

        for i in 0..10 {
            assert!(snapshot.object_nodes.contains_key(&format!("node{}", i)));
        }
    }
}
