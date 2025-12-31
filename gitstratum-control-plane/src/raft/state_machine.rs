use openraft::storage::Adaptor;
use openraft::{
    BasicNode, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::membership::{ClusterStateSnapshot, ExtendedNodeInfo, NodeType, RefLockKey};
use crate::raft::node::{NodeId, TypeConfig};
use crate::raft::LockInfo;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
    AddNode {
        node: ExtendedNodeInfo,
    },
    RemoveNode {
        node_id: String,
        node_type: NodeType,
    },
    SetNodeState {
        node_id: String,
        node_type: NodeType,
        state: gitstratum_hashring::NodeState,
    },
    AcquireLock {
        key: RefLockKey,
        lock: LockInfo,
    },
    ReleaseLock {
        lock_id: String,
    },
    CleanupExpiredLocks,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    Success,
    NodeAdded,
    NodeRemoved { found: bool },
    NodeStateSet { found: bool },
    LockAcquired { lock_id: String },
    LockNotAcquired { reason: String },
    LockReleased { found: bool },
    LocksCleanedUp { count: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
}

pub struct StateMachineData {
    pub last_applied: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub state: ClusterStateSnapshot,
}

impl Default for StateMachineData {
    fn default() -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
            state: ClusterStateSnapshot::new(),
        }
    }
}

pub struct StateMachineSnapshotBuilder {
    pub(crate) data: StateMachineData,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data = serde_json::to_vec(&self.data.state)
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;

        let meta = SnapshotMeta {
            last_log_id: self.data.last_applied,
            last_membership: self.data.last_membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                self.data.last_applied.map(|l| l.index).unwrap_or(0),
                uuid::Uuid::new_v4()
            ),
        };

        Ok(openraft::Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

pub fn apply_request(state: &mut ClusterStateSnapshot, request: &Request) -> Response {
    match request {
        Request::AddNode { node } => {
            state.add_node(node.clone());
            Response::NodeAdded
        }
        Request::RemoveNode { node_id, node_type } => {
            let found = state.remove_node(node_id, *node_type).is_some();
            Response::NodeRemoved { found }
        }
        Request::SetNodeState {
            node_id,
            node_type,
            state: node_state,
        } => {
            let found = state.set_node_state(node_id, *node_type, *node_state);
            Response::NodeStateSet { found }
        }
        Request::AcquireLock { key, lock } => {
            if let Some(existing) = state.ref_locks.get(key) {
                if !existing.is_expired() {
                    return Response::LockNotAcquired {
                        reason: format!("lock held by {}", existing.holder_id),
                    };
                }
            }
            let lock_id = lock.lock_id.clone();
            state.ref_locks.insert(key.clone(), lock.clone());
            state.version += 1;
            Response::LockAcquired { lock_id }
        }
        Request::ReleaseLock { lock_id } => {
            let key = state
                .ref_locks
                .iter()
                .find(|(_, v)| v.lock_id == *lock_id)
                .map(|(k, _)| k.clone());

            if let Some(key) = key {
                state.ref_locks.remove(&key);
                state.version += 1;
                Response::LockReleased { found: true }
            } else {
                Response::LockReleased { found: false }
            }
        }
        Request::CleanupExpiredLocks => {
            let expired: Vec<RefLockKey> = state
                .ref_locks
                .iter()
                .filter(|(_, lock)| lock.is_expired())
                .map(|(k, _)| k.clone())
                .collect();

            let count = expired.len();
            for key in expired {
                state.ref_locks.remove(&key);
            }
            if count > 0 {
                state.version += 1;
            }
            Response::LocksCleanedUp { count }
        }
    }
}

pub type StateMachineStore = Adaptor<TypeConfig, super::node::ControlPlaneStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_hashring::NodeState;
    use openraft::CommittedLeaderId;
    use std::time::Duration;

    fn create_test_node(id: &str, node_type: NodeType) -> ExtendedNodeInfo {
        ExtendedNodeInfo::new(id, "192.168.1.1", 8080, node_type)
    }

    fn create_test_lock(lock_id: &str, holder_id: &str, timeout: Duration) -> LockInfo {
        LockInfo::new(lock_id, "repo1", "refs/heads/main", holder_id, timeout)
    }

    fn create_test_lock_key() -> RefLockKey {
        RefLockKey::new("repo1", "refs/heads/main")
    }

    fn create_expired_lock(lock_id: &str, holder_id: &str, repo_id: &str, ref_name: &str) -> LockInfo {
        LockInfo {
            lock_id: lock_id.to_string(),
            repo_id: repo_id.to_string(),
            ref_name: ref_name.to_string(),
            holder_id: holder_id.to_string(),
            timeout_ms: 1,
            acquired_at_epoch_ms: 0,
        }
    }

    #[test]
    fn test_request_response_types_derive_traits() {
        let key = create_test_lock_key();
        let lock = create_test_lock("lock1", "holder1", Duration::from_secs(30));
        let node = create_test_node("node1", NodeType::Object);

        let request_variants: Vec<Request> = vec![
            Request::AddNode { node: node.clone() },
            Request::RemoveNode { node_id: "node1".to_string(), node_type: NodeType::Object },
            Request::SetNodeState { node_id: "node1".to_string(), node_type: NodeType::Object, state: NodeState::Draining },
            Request::AcquireLock { key: key.clone(), lock: lock.clone() },
            Request::ReleaseLock { lock_id: "lock1".to_string() },
            Request::CleanupExpiredLocks,
        ];

        for request in &request_variants {
            let cloned = request.clone();
            assert_eq!(request, &cloned);

            let debug_str = format!("{:?}", request);
            assert!(!debug_str.is_empty());

            let serialized = serde_json::to_string(request).unwrap();
            let deserialized: Request = serde_json::from_str(&serialized).unwrap();
            assert_eq!(request, &deserialized);
        }

        let response_variants: Vec<Response> = vec![
            Response::Success,
            Response::NodeAdded,
            Response::NodeRemoved { found: true },
            Response::NodeRemoved { found: false },
            Response::NodeStateSet { found: true },
            Response::NodeStateSet { found: false },
            Response::LockAcquired { lock_id: "lock1".to_string() },
            Response::LockNotAcquired { reason: "lock held by holder1".to_string() },
            Response::LockReleased { found: true },
            Response::LockReleased { found: false },
            Response::LocksCleanedUp { count: 0 },
            Response::LocksCleanedUp { count: 100 },
        ];

        for response in &response_variants {
            let cloned = response.clone();
            assert_eq!(response, &cloned);

            let debug_str = format!("{:?}", response);
            assert!(!debug_str.is_empty());

            let serialized = serde_json::to_string(response).unwrap();
            let deserialized: Response = serde_json::from_str(&serialized).unwrap();
            assert_eq!(response, &deserialized);
        }

        let node1 = create_test_node("node1", NodeType::Object);
        let node2 = create_test_node("node1", NodeType::Object);
        let node3 = create_test_node("node2", NodeType::Object);
        assert_eq!(Request::AddNode { node: node1 }, Request::AddNode { node: node2 });
        assert_ne!(Request::AddNode { node: create_test_node("node1", NodeType::Object) }, Request::AddNode { node: node3 });
    }

    #[test]
    fn test_stored_snapshot_and_state_machine_data_derive_traits() {
        let snapshot_default = StoredSnapshot::default();
        assert!(snapshot_default.data.is_empty());
        assert!(snapshot_default.meta.last_log_id.is_none());
        assert!(snapshot_default.meta.snapshot_id.is_empty());

        let debug_str = format!("{:?}", snapshot_default);
        assert!(debug_str.contains("StoredSnapshot"));

        let mut snapshot1 = StoredSnapshot::default();
        snapshot1.data = vec![1, 2, 3];
        let cloned = snapshot1.clone();
        assert_eq!(snapshot1.data, cloned.data);

        let serialized = serde_json::to_string(&snapshot1).unwrap();
        let deserialized: StoredSnapshot = serde_json::from_str(&serialized).unwrap();
        assert_eq!(snapshot1, deserialized);

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 5)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-123".to_string(),
        };
        let snapshot_with_meta = StoredSnapshot {
            meta: meta.clone(),
            data: vec![1, 2, 3, 4, 5],
        };
        assert_eq!(snapshot_with_meta.meta.snapshot_id, "snap-123");
        assert_eq!(snapshot_with_meta.data.len(), 5);

        let snap1 = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 1)),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap1".to_string(),
            },
            data: vec![1, 2, 3],
        };
        let snap2 = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 1)),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap1".to_string(),
            },
            data: vec![1, 2, 3],
        };
        let snap3 = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 2)),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap2".to_string(),
            },
            data: vec![4, 5, 6],
        };
        assert_eq!(snap1, snap2);
        assert_ne!(snap1, snap3);

        let data = StateMachineData::default();
        assert!(data.last_applied.is_none());
        assert!(data.state.control_plane_nodes.is_empty());
        assert!(data.state.metadata_nodes.is_empty());
        assert!(data.state.object_nodes.is_empty());
        assert!(data.state.frontend_nodes.is_empty());
        assert!(data.last_membership.membership().get_joint_config().is_empty());

        let mut data_with_applied = StateMachineData::default();
        data_with_applied.last_applied = Some(LogId::new(CommittedLeaderId::new(1, 1), 100));
        data_with_applied.state.add_node(create_test_node("test", NodeType::Object));
        assert_eq!(data_with_applied.last_applied.unwrap().index, 100);
        assert_eq!(data_with_applied.state.object_nodes.len(), 1);
    }

    #[test]
    fn test_apply_request_node_operations_all_types() {
        let mut state = ClusterStateSnapshot::new();

        let node_configs = [
            ("cp1", NodeType::ControlPlane),
            ("md1", NodeType::Metadata),
            ("obj1", NodeType::Object),
            ("fe1", NodeType::Frontend),
        ];

        for (id, node_type) in &node_configs {
            let node = create_test_node(id, *node_type);
            let request = Request::AddNode { node };
            let response = apply_request(&mut state, &request);
            assert!(matches!(response, Response::NodeAdded));
        }

        assert_eq!(state.control_plane_nodes.len(), 1);
        assert!(state.control_plane_nodes.contains_key("cp1"));
        assert_eq!(state.metadata_nodes.len(), 1);
        assert!(state.metadata_nodes.contains_key("md1"));
        assert_eq!(state.object_nodes.len(), 1);
        assert!(state.object_nodes.contains_key("obj1"));
        assert_eq!(state.frontend_nodes.len(), 1);
        assert!(state.frontend_nodes.contains_key("fe1"));

        let duplicate_node = create_test_node("obj1", NodeType::Object);
        let response = apply_request(&mut state, &Request::AddNode { node: duplicate_node });
        assert!(matches!(response, Response::NodeAdded));
        assert_eq!(state.object_nodes.len(), 1);

        let states_to_test = [
            NodeState::Active,
            NodeState::Draining,
            NodeState::Down,
            NodeState::Joining,
        ];

        for node_state in &states_to_test {
            let request = Request::SetNodeState {
                node_id: "obj1".to_string(),
                node_type: NodeType::Object,
                state: *node_state,
            };
            let response = apply_request(&mut state, &request);
            assert!(matches!(response, Response::NodeStateSet { found: true }));
            assert_eq!(state.object_nodes.get("obj1").unwrap().state, *node_state);
        }

        for (id, node_type) in &node_configs {
            let request = Request::SetNodeState {
                node_id: id.to_string(),
                node_type: *node_type,
                state: NodeState::Draining,
            };
            let response = apply_request(&mut state, &request);
            assert!(matches!(response, Response::NodeStateSet { found: true }));
        }

        assert_eq!(state.control_plane_nodes.get("cp1").unwrap().state, NodeState::Draining);
        assert_eq!(state.metadata_nodes.get("md1").unwrap().state, NodeState::Draining);
        assert_eq!(state.object_nodes.get("obj1").unwrap().state, NodeState::Draining);
        assert_eq!(state.frontend_nodes.get("fe1").unwrap().state, NodeState::Draining);

        let not_found_response = apply_request(&mut state, &Request::SetNodeState {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
            state: NodeState::Draining,
        });
        assert!(matches!(not_found_response, Response::NodeStateSet { found: false }));

        for (id, node_type) in &node_configs {
            let request = Request::RemoveNode {
                node_id: id.to_string(),
                node_type: *node_type,
            };
            let response = apply_request(&mut state, &request);
            assert!(matches!(response, Response::NodeRemoved { found: true }));
        }

        assert!(state.control_plane_nodes.is_empty());
        assert!(state.metadata_nodes.is_empty());
        assert!(state.object_nodes.is_empty());
        assert!(state.frontend_nodes.is_empty());

        let not_found_response = apply_request(&mut state, &Request::RemoveNode {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
        });
        assert!(matches!(not_found_response, Response::NodeRemoved { found: false }));
    }

    #[test]
    fn test_apply_request_lock_lifecycle_and_contention() {
        let mut state = ClusterStateSnapshot::new();
        let key = create_test_lock_key();
        assert_eq!(state.version, 0);

        let lock1 = create_test_lock("lock1", "holder1", Duration::from_secs(30));
        let request = Request::AcquireLock { key: key.clone(), lock: lock1 };
        let response = apply_request(&mut state, &request);

        match response {
            Response::LockAcquired { lock_id } => assert_eq!(lock_id, "lock1"),
            _ => panic!("Expected LockAcquired response"),
        }
        assert!(state.ref_locks.contains_key(&key));
        assert_eq!(state.version, 1);

        let lock2 = create_test_lock("lock2", "holder2", Duration::from_secs(30));
        let request = Request::AcquireLock { key: key.clone(), lock: lock2 };
        let response = apply_request(&mut state, &request);

        match response {
            Response::LockNotAcquired { reason } => assert!(reason.contains("holder1")),
            _ => panic!("Expected LockNotAcquired response"),
        }
        assert_eq!(state.ref_locks.get(&key).unwrap().lock_id, "lock1");
        assert_eq!(state.version, 1);

        let request = Request::ReleaseLock { lock_id: "lock1".to_string() };
        let response = apply_request(&mut state, &request);

        match response {
            Response::LockReleased { found } => assert!(found),
            _ => panic!("Expected LockReleased response"),
        }
        assert!(!state.ref_locks.contains_key(&key));
        assert_eq!(state.version, 2);

        let not_found_response = apply_request(&mut state, &Request::ReleaseLock { lock_id: "nonexistent".to_string() });
        match not_found_response {
            Response::LockReleased { found } => assert!(!found),
            _ => panic!("Expected LockReleased response"),
        }
        assert_eq!(state.version, 2);

        for i in 0..3 {
            let lock = create_test_lock(&format!("lock{}", i), &format!("holder{}", i), Duration::from_secs(30));
            let acquire = Request::AcquireLock { key: key.clone(), lock };
            let resp = apply_request(&mut state, &acquire);
            assert!(matches!(resp, Response::LockAcquired { .. }));

            let release = Request::ReleaseLock { lock_id: format!("lock{}", i) };
            let resp = apply_request(&mut state, &release);
            assert!(matches!(resp, Response::LockReleased { found: true }));
        }

        let expired_lock = create_expired_lock("expired_lock", "old_holder", "repo1", "refs/heads/main");
        state.ref_locks.insert(key.clone(), expired_lock);
        let version_before = state.version;

        let new_lock = create_test_lock("new_lock", "new_holder", Duration::from_secs(30));
        let request = Request::AcquireLock { key: key.clone(), lock: new_lock };
        let response = apply_request(&mut state, &request);

        match response {
            Response::LockAcquired { lock_id } => assert_eq!(lock_id, "new_lock"),
            _ => panic!("Expected LockAcquired response"),
        }
        assert_eq!(state.ref_locks.get(&key).unwrap().lock_id, "new_lock");
        assert_eq!(state.version, version_before + 1);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        state.ref_locks.clear();
        let long_timeout_lock = LockInfo {
            lock_id: "long_lock".to_string(),
            repo_id: "repo1".to_string(),
            ref_name: "refs/heads/main".to_string(),
            holder_id: "long_holder".to_string(),
            timeout_ms: 1_000_000_000,
            acquired_at_epoch_ms: now,
        };
        let request = Request::AcquireLock { key: key.clone(), lock: long_timeout_lock };
        apply_request(&mut state, &request);

        let competing_lock = create_test_lock("competing", "competitor", Duration::from_secs(30));
        let response = apply_request(&mut state, &Request::AcquireLock { key: key.clone(), lock: competing_lock });
        match response {
            Response::LockNotAcquired { reason } => assert!(reason.contains("long_holder")),
            _ => panic!("Expected LockNotAcquired response"),
        }
    }

    #[test]
    fn test_apply_request_cleanup_expired_locks_scenarios() {
        let mut state = ClusterStateSnapshot::new();
        let initial_version = state.version;

        let response = apply_request(&mut state, &Request::CleanupExpiredLocks);
        match response {
            Response::LocksCleanedUp { count } => assert_eq!(count, 0),
            _ => panic!("Expected LocksCleanedUp response"),
        }
        assert_eq!(state.version, initial_version);

        let active_key = RefLockKey::new("active_repo", "refs/heads/main");
        let active_lock = create_test_lock("active_lock", "active_holder", Duration::from_secs(3600));
        let mut active_lock_keyed = active_lock;
        active_lock_keyed.repo_id = "active_repo".to_string();
        state.ref_locks.insert(active_key.clone(), active_lock_keyed);

        let response = apply_request(&mut state, &Request::CleanupExpiredLocks);
        match response {
            Response::LocksCleanedUp { count } => assert_eq!(count, 0),
            _ => panic!("Expected LocksCleanedUp response"),
        }
        assert_eq!(state.ref_locks.len(), 1);

        let expired_key = RefLockKey::new("expired_repo", "refs/heads/main");
        let expired_lock = create_expired_lock("expired_lock", "expired_holder", "expired_repo", "refs/heads/main");
        state.ref_locks.insert(expired_key.clone(), expired_lock);
        let version_before = state.version;

        let response = apply_request(&mut state, &Request::CleanupExpiredLocks);
        match response {
            Response::LocksCleanedUp { count } => assert_eq!(count, 1),
            _ => panic!("Expected LocksCleanedUp response"),
        }
        assert_eq!(state.ref_locks.len(), 1);
        assert!(state.ref_locks.contains_key(&active_key));
        assert!(!state.ref_locks.contains_key(&expired_key));
        assert_eq!(state.version, version_before + 1);

        state.ref_locks.clear();
        for i in 0..5 {
            let key = RefLockKey::new(&format!("repo{}", i), "refs/heads/main");
            let lock = create_expired_lock(&format!("lock{}", i), &format!("holder{}", i), &format!("repo{}", i), "refs/heads/main");
            state.ref_locks.insert(key, lock);
        }

        let response = apply_request(&mut state, &Request::CleanupExpiredLocks);
        match response {
            Response::LocksCleanedUp { count } => assert_eq!(count, 5),
            _ => panic!("Expected LocksCleanedUp response"),
        }
        assert!(state.ref_locks.is_empty());

        for i in 0..3 {
            let key = RefLockKey::new(&format!("repo{}", i), &format!("refs/heads/branch{}", i));
            let lock = create_test_lock(&format!("lock{}", i), &format!("holder{}", i), Duration::from_secs(30));
            let mut lock_with_key = lock;
            lock_with_key.repo_id = format!("repo{}", i);
            lock_with_key.ref_name = format!("refs/heads/branch{}", i);
            state.ref_locks.insert(key, lock_with_key);
        }
        assert_eq!(state.ref_locks.len(), 3);

        let release_req = Request::ReleaseLock { lock_id: "lock1".to_string() };
        let response = apply_request(&mut state, &release_req);
        assert!(matches!(response, Response::LockReleased { found: true }));
        assert_eq!(state.ref_locks.len(), 2);
    }

    #[test]
    fn test_version_tracking_across_operations() {
        let mut state = ClusterStateSnapshot::new();
        assert_eq!(state.version, 0);

        let node = create_test_node("node1", NodeType::Object);
        apply_request(&mut state, &Request::AddNode { node });
        assert_eq!(state.version, 1);

        let key = create_test_lock_key();
        let lock = create_test_lock("lock1", "holder1", Duration::from_secs(30));
        apply_request(&mut state, &Request::AcquireLock { key, lock });
        assert_eq!(state.version, 2);

        apply_request(&mut state, &Request::ReleaseLock { lock_id: "lock1".to_string() });
        assert_eq!(state.version, 3);

        apply_request(&mut state, &Request::ReleaseLock { lock_id: "nonexistent".to_string() });
        assert_eq!(state.version, 3);

        apply_request(&mut state, &Request::CleanupExpiredLocks);
        assert_eq!(state.version, 3);

        apply_request(&mut state, &Request::SetNodeState {
            node_id: "node1".to_string(),
            node_type: NodeType::Object,
            state: NodeState::Draining,
        });
        assert_eq!(state.version, 4);

        apply_request(&mut state, &Request::RemoveNode {
            node_id: "node1".to_string(),
            node_type: NodeType::Object,
        });
        assert_eq!(state.version, 5);
    }

    #[tokio::test]
    async fn test_snapshot_builder_creates_valid_snapshots() {
        let data_empty = StateMachineData::default();
        let mut builder_empty = StateMachineSnapshotBuilder { data: data_empty };
        let snapshot_empty = builder_empty.build_snapshot().await.unwrap();

        assert!(snapshot_empty.meta.snapshot_id.starts_with("0-"));
        assert!(snapshot_empty.meta.last_log_id.is_none());

        let mut data_with_state = StateMachineData::default();
        data_with_state.state.add_node(create_test_node("obj1", NodeType::Object));
        data_with_state.last_applied = Some(LogId::new(CommittedLeaderId::new(1, 1), 5));

        let mut builder_with_state = StateMachineSnapshotBuilder { data: data_with_state };
        let snapshot_with_state = builder_with_state.build_snapshot().await.unwrap();

        assert!(snapshot_with_state.meta.snapshot_id.starts_with("5-"));
        assert_eq!(snapshot_with_state.meta.last_log_id, Some(LogId::new(CommittedLeaderId::new(1, 1), 5)));

        let mut data_full = StateMachineData::default();
        data_full.state.add_node(create_test_node("obj1", NodeType::Object));
        data_full.state.add_node(create_test_node("md1", NodeType::Metadata));
        data_full.state.add_node(create_test_node("cp1", NodeType::ControlPlane));
        data_full.state.add_node(create_test_node("fe1", NodeType::Frontend));
        data_full.last_applied = Some(LogId::new(CommittedLeaderId::new(2, 3), 10));

        let mut builder_full = StateMachineSnapshotBuilder { data: data_full };
        let snapshot_full = builder_full.build_snapshot().await.unwrap();

        assert!(snapshot_full.meta.snapshot_id.starts_with("10-"));
        assert_eq!(snapshot_full.meta.last_log_id, Some(LogId::new(CommittedLeaderId::new(2, 3), 10)));

        let inner_data = snapshot_full.snapshot.into_inner();
        let deserialized: ClusterStateSnapshot = serde_json::from_slice(&inner_data).unwrap();
        assert_eq!(deserialized.object_nodes.len(), 1);
        assert_eq!(deserialized.metadata_nodes.len(), 1);
        assert_eq!(deserialized.control_plane_nodes.len(), 1);
        assert_eq!(deserialized.frontend_nodes.len(), 1);
    }
}
