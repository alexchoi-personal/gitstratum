use gitstratum_control_plane::{
    ClusterState, ClusterStateSnapshot, ExtendedNodeInfo, LockInfo, NodeType, RefLockKey,
};
use gitstratum_hashring::NodeState;
use std::time::Duration;

fn create_test_node(id: &str, node_type: NodeType) -> ExtendedNodeInfo {
    ExtendedNodeInfo::new(id, format!("10.0.0.{}", id.chars().last().unwrap()), 9000, node_type)
}

#[test]
fn test_cluster_state_snapshot_add_node() {
    let mut snapshot = ClusterStateSnapshot::new();
    assert_eq!(snapshot.version, 0);

    let node = create_test_node("node-1", NodeType::Object);
    snapshot.add_node(node.clone());

    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.object_nodes.len(), 1);
    assert!(snapshot.object_nodes.contains_key("node-1"));
}

#[test]
fn test_cluster_state_snapshot_remove_node() {
    let mut snapshot = ClusterStateSnapshot::new();

    let node = create_test_node("node-1", NodeType::Metadata);
    snapshot.add_node(node);

    let removed = snapshot.remove_node("node-1", NodeType::Metadata);
    assert!(removed.is_some());
    assert_eq!(snapshot.metadata_nodes.len(), 0);

    let not_found = snapshot.remove_node("node-1", NodeType::Metadata);
    assert!(not_found.is_none());
}

#[test]
fn test_cluster_state_snapshot_set_node_state() {
    let mut snapshot = ClusterStateSnapshot::new();

    let node = create_test_node("node-1", NodeType::Object);
    snapshot.add_node(node);

    let result = snapshot.set_node_state("node-1", NodeType::Object, NodeState::Draining);
    assert!(result);

    let node = snapshot.get_node("node-1").unwrap();
    assert_eq!(node.state, NodeState::Draining);
}

#[test]
fn test_cluster_state_snapshot_find_node_type() {
    let mut snapshot = ClusterStateSnapshot::new();

    snapshot.add_node(create_test_node("cp-1", NodeType::ControlPlane));
    snapshot.add_node(create_test_node("meta-1", NodeType::Metadata));
    snapshot.add_node(create_test_node("obj-1", NodeType::Object));
    snapshot.add_node(create_test_node("fe-1", NodeType::Frontend));

    assert_eq!(snapshot.find_node_type("cp-1"), Some(NodeType::ControlPlane));
    assert_eq!(snapshot.find_node_type("meta-1"), Some(NodeType::Metadata));
    assert_eq!(snapshot.find_node_type("obj-1"), Some(NodeType::Object));
    assert_eq!(snapshot.find_node_type("fe-1"), Some(NodeType::Frontend));
    assert_eq!(snapshot.find_node_type("unknown"), None);
}

#[test]
fn test_cluster_state_add_node() {
    let state = ClusterState::new(16, 3);

    let node = create_test_node("obj-1", NodeType::Object);
    state.add_node(node).unwrap();

    let snapshot = state.snapshot();
    assert_eq!(snapshot.object_nodes.len(), 1);
}

#[test]
fn test_cluster_state_remove_node() {
    let state = ClusterState::new(16, 3);

    let node = create_test_node("obj-1", NodeType::Object);
    state.add_node(node).unwrap();

    let removed = state.remove_node("obj-1").unwrap();
    assert!(removed.is_some());

    let snapshot = state.snapshot();
    assert_eq!(snapshot.object_nodes.len(), 0);
}

#[test]
fn test_cluster_state_set_node_state() {
    let state = ClusterState::new(16, 3);

    let node = create_test_node("obj-1", NodeType::Object);
    state.add_node(node).unwrap();

    let result = state.set_node_state("obj-1", NodeState::Draining).unwrap();
    assert!(result);

    let node = state.get_node("obj-1").unwrap();
    assert_eq!(node.state, NodeState::Draining);
}

#[test]
fn test_cluster_state_object_ring() {
    let state = ClusterState::new(16, 2);

    state.add_node(create_test_node("obj-1", NodeType::Object)).unwrap();
    state.add_node(create_test_node("obj-2", NodeType::Object)).unwrap();
    state.add_node(create_test_node("obj-3", NodeType::Object)).unwrap();

    let ring = state.object_ring();
    assert_eq!(ring.node_count(), 3);
}

#[test]
fn test_cluster_state_metadata_ring() {
    let state = ClusterState::new(16, 2);

    state.add_node(create_test_node("meta-1", NodeType::Metadata)).unwrap();
    state.add_node(create_test_node("meta-2", NodeType::Metadata)).unwrap();

    let ring = state.metadata_ring();
    assert_eq!(ring.node_count(), 2);
}

#[test]
fn test_cluster_state_acquire_release_lock() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let acquired = state.acquire_lock(key.clone(), lock, Duration::from_secs(30));
    assert!(acquired);

    let existing = state.get_lock(&key);
    assert!(existing.is_some());
    assert_eq!(existing.unwrap().holder_id, "holder-1");

    let released = state.release_lock("lock-1");
    assert!(released);

    let after_release = state.get_lock(&key);
    assert!(after_release.is_none());
}

#[test]
fn test_cluster_state_lock_conflict() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock1 = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let acquired1 = state.acquire_lock(key.clone(), lock1, Duration::from_secs(30));
    assert!(acquired1);

    let lock2 = LockInfo::new(
        "lock-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );

    let acquired2 = state.acquire_lock(key.clone(), lock2, Duration::from_secs(30));
    assert!(!acquired2);
}

#[test]
fn test_cluster_state_leader() {
    let state = ClusterState::new(16, 3);

    assert!(state.leader_id().is_none());

    state.set_leader(Some("leader-1".to_string()));
    assert_eq!(state.leader_id(), Some("leader-1".to_string()));

    state.set_leader(None);
    assert!(state.leader_id().is_none());
}

#[test]
fn test_cluster_state_version() {
    let state = ClusterState::new(16, 3);
    let initial_version = state.version();

    state.add_node(create_test_node("node-1", NodeType::Object)).unwrap();
    assert!(state.version() > initial_version);
}

#[test]
fn test_extended_node_info() {
    let node = ExtendedNodeInfo::new("node-1", "192.168.1.1", 9000, NodeType::Object);

    assert_eq!(node.id, "node-1");
    assert_eq!(node.address, "192.168.1.1");
    assert_eq!(node.port, 9000);
    assert_eq!(node.node_type, NodeType::Object);
    assert_eq!(node.state, NodeState::Active);
    assert_eq!(node.endpoint(), "192.168.1.1:9000");
}

#[test]
fn test_ref_lock_key() {
    let key1 = RefLockKey::new("repo-1", "refs/heads/main");
    let key2 = RefLockKey::new("repo-1", "refs/heads/main");
    let key3 = RefLockKey::new("repo-1", "refs/heads/develop");

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
}

#[test]
fn test_node_type_conversion() {
    assert_eq!(i32::from(NodeType::ControlPlane), 1);
    assert_eq!(i32::from(NodeType::Metadata), 2);
    assert_eq!(i32::from(NodeType::Object), 3);
    assert_eq!(i32::from(NodeType::Frontend), 4);

    assert_eq!(NodeType::try_from(1), Ok(NodeType::ControlPlane));
    assert_eq!(NodeType::try_from(2), Ok(NodeType::Metadata));
    assert_eq!(NodeType::try_from(3), Ok(NodeType::Object));
    assert_eq!(NodeType::try_from(4), Ok(NodeType::Frontend));
    assert!(NodeType::try_from(0).is_err());
}

#[test]
fn test_cluster_state_all_node_types() {
    let state = ClusterState::new(16, 3);

    state.add_node(create_test_node("cp-1", NodeType::ControlPlane)).unwrap();
    state.add_node(create_test_node("meta-1", NodeType::Metadata)).unwrap();
    state.add_node(create_test_node("obj-1", NodeType::Object)).unwrap();
    state.add_node(create_test_node("fe-1", NodeType::Frontend)).unwrap();

    let snapshot = state.snapshot();
    assert_eq!(snapshot.control_plane_nodes.len(), 1);
    assert_eq!(snapshot.metadata_nodes.len(), 1);
    assert_eq!(snapshot.object_nodes.len(), 1);
    assert_eq!(snapshot.frontend_nodes.len(), 1);
}

#[test]
fn test_cluster_state_apply_snapshot() {
    let state = ClusterState::new(16, 3);

    let mut snapshot = ClusterStateSnapshot::new();
    snapshot.add_node(create_test_node("obj-1", NodeType::Object));
    snapshot.add_node(create_test_node("obj-2", NodeType::Object));
    snapshot.add_node(create_test_node("meta-1", NodeType::Metadata));

    state.apply_snapshot(snapshot).unwrap();

    let result = state.snapshot();
    assert_eq!(result.object_nodes.len(), 2);
    assert_eq!(result.metadata_nodes.len(), 1);
}

#[test]
fn test_extended_node_info_node_id() {
    let node = ExtendedNodeInfo::new("test-node", "127.0.0.1", 8080, NodeType::Object);
    let node_id = node.node_id();
    assert_eq!(node_id.as_str(), "test-node");
}

#[test]
fn test_cluster_state_snapshot_remove_control_plane_node() {
    let mut snapshot = ClusterStateSnapshot::new();
    let node = create_test_node("cp-1", NodeType::ControlPlane);
    snapshot.add_node(node);

    let removed = snapshot.remove_node("cp-1", NodeType::ControlPlane);
    assert!(removed.is_some());
    assert_eq!(snapshot.control_plane_nodes.len(), 0);
}

#[test]
fn test_cluster_state_snapshot_remove_frontend_node() {
    let mut snapshot = ClusterStateSnapshot::new();
    let node = create_test_node("fe-1", NodeType::Frontend);
    snapshot.add_node(node);

    let removed = snapshot.remove_node("fe-1", NodeType::Frontend);
    assert!(removed.is_some());
    assert_eq!(snapshot.frontend_nodes.len(), 0);
}

#[test]
fn test_cluster_state_snapshot_set_control_plane_state() {
    let mut snapshot = ClusterStateSnapshot::new();
    let node = create_test_node("cp-1", NodeType::ControlPlane);
    snapshot.add_node(node);

    let result = snapshot.set_node_state("cp-1", NodeType::ControlPlane, NodeState::Draining);
    assert!(result);
    assert_eq!(
        snapshot.control_plane_nodes.get("cp-1").unwrap().state,
        NodeState::Draining
    );
}

#[test]
fn test_cluster_state_snapshot_set_frontend_state() {
    let mut snapshot = ClusterStateSnapshot::new();
    let node = create_test_node("fe-1", NodeType::Frontend);
    snapshot.add_node(node);

    let result = snapshot.set_node_state("fe-1", NodeType::Frontend, NodeState::Draining);
    assert!(result);
    assert_eq!(
        snapshot.frontend_nodes.get("fe-1").unwrap().state,
        NodeState::Draining
    );
}

#[test]
fn test_cluster_state_snapshot_set_node_state_not_found() {
    let mut snapshot = ClusterStateSnapshot::new();
    let result = snapshot.set_node_state("nonexistent", NodeType::Object, NodeState::Draining);
    assert!(!result);
}

#[test]
fn test_cluster_state_snapshot_all_nodes() {
    let mut snapshot = ClusterStateSnapshot::new();
    snapshot.add_node(create_test_node("cp-1", NodeType::ControlPlane));
    snapshot.add_node(create_test_node("meta-1", NodeType::Metadata));
    snapshot.add_node(create_test_node("obj-1", NodeType::Object));
    snapshot.add_node(create_test_node("fe-1", NodeType::Frontend));

    let all = snapshot.all_nodes();
    assert_eq!(all.len(), 4);
}

#[test]
fn test_cluster_state_snapshot_default() {
    let snapshot = ClusterStateSnapshot::default();
    assert_eq!(snapshot.version, 0);
    assert!(snapshot.control_plane_nodes.is_empty());
    assert!(snapshot.metadata_nodes.is_empty());
    assert!(snapshot.object_nodes.is_empty());
    assert!(snapshot.frontend_nodes.is_empty());
    assert!(snapshot.ref_locks.is_empty());
    assert!(snapshot.leader_id.is_none());
}

#[test]
fn test_cluster_state_default() {
    let state = ClusterState::default();
    let snapshot = state.snapshot();
    assert_eq!(snapshot.version, 0);
}

#[test]
fn test_cluster_state_remove_nonexistent_node() {
    let state = ClusterState::new(16, 3);
    let removed = state.remove_node("nonexistent").unwrap();
    assert!(removed.is_none());
}

#[test]
fn test_cluster_state_remove_control_plane_node() {
    let state = ClusterState::new(16, 3);
    state
        .add_node(create_test_node("cp-1", NodeType::ControlPlane))
        .unwrap();

    let removed = state.remove_node("cp-1").unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id, "cp-1");
}

#[test]
fn test_cluster_state_remove_frontend_node() {
    let state = ClusterState::new(16, 3);
    state
        .add_node(create_test_node("fe-1", NodeType::Frontend))
        .unwrap();

    let removed = state.remove_node("fe-1").unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id, "fe-1");
}

#[test]
fn test_cluster_state_remove_metadata_node() {
    let state = ClusterState::new(16, 3);
    state
        .add_node(create_test_node("meta-1", NodeType::Metadata))
        .unwrap();

    let removed = state.remove_node("meta-1").unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id, "meta-1");
}

#[test]
fn test_cluster_state_set_node_state_nonexistent() {
    let state = ClusterState::new(16, 3);
    let result = state.set_node_state("nonexistent", NodeState::Draining).unwrap();
    assert!(!result);
}

#[test]
fn test_cluster_state_set_control_plane_node_state() {
    let state = ClusterState::new(16, 3);
    state
        .add_node(create_test_node("cp-1", NodeType::ControlPlane))
        .unwrap();

    let result = state.set_node_state("cp-1", NodeState::Draining).unwrap();
    assert!(result);
    assert_eq!(state.get_node("cp-1").unwrap().state, NodeState::Draining);
}

#[test]
fn test_cluster_state_set_frontend_node_state() {
    let state = ClusterState::new(16, 3);
    state
        .add_node(create_test_node("fe-1", NodeType::Frontend))
        .unwrap();

    let result = state.set_node_state("fe-1", NodeState::Draining).unwrap();
    assert!(result);
    assert_eq!(state.get_node("fe-1").unwrap().state, NodeState::Draining);
}

#[test]
fn test_cluster_state_set_metadata_node_state() {
    let state = ClusterState::new(16, 3);
    state
        .add_node(create_test_node("meta-1", NodeType::Metadata))
        .unwrap();

    let result = state.set_node_state("meta-1", NodeState::Draining).unwrap();
    assert!(result);
    assert_eq!(state.get_node("meta-1").unwrap().state, NodeState::Draining);
}

#[test]
fn test_cluster_state_release_nonexistent_lock() {
    let state = ClusterState::new(16, 3);
    let released = state.release_lock("nonexistent-lock");
    assert!(!released);
}

#[test]
fn test_cluster_state_lock_expired_can_be_replaced() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock1 = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_millis(1),
    );

    let acquired1 = state.acquire_lock(key.clone(), lock1, Duration::from_millis(1));
    assert!(acquired1);

    std::thread::sleep(Duration::from_millis(10));

    let lock2 = LockInfo::new(
        "lock-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );

    let acquired2 = state.acquire_lock(key.clone(), lock2, Duration::from_secs(30));
    assert!(acquired2);

    let current = state.get_lock(&key).unwrap();
    assert_eq!(current.lock_id, "lock-2");
    assert_eq!(current.holder_id, "holder-2");
}

#[test]
fn test_cluster_state_cleanup_expired_locks() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_millis(1),
    );

    state.acquire_lock(key.clone(), lock, Duration::from_millis(1));

    std::thread::sleep(Duration::from_millis(10));

    state.cleanup_expired_locks();

    let lock_after = state.get_lock(&key);
    assert!(lock_after.is_none());
}

#[test]
fn test_cluster_state_cleanup_expired_locks_with_valid_lock() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(300),
    );

    state.acquire_lock(key.clone(), lock, Duration::from_secs(300));

    state.cleanup_expired_locks();

    let lock_after = state.get_lock(&key);
    assert!(lock_after.is_some());
}

#[test]
fn test_cluster_state_cleanup_expired_locks_empty() {
    let state = ClusterState::new(16, 3);
    state.cleanup_expired_locks();
    assert!(state.snapshot().ref_locks.is_empty());
}

#[test]
fn test_cluster_state_lock_without_timeout_tracking() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-orphan",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let mut snapshot = ClusterStateSnapshot::new();
    snapshot.ref_locks.insert(key.clone(), lock);
    state.apply_snapshot(snapshot).unwrap();

    let lock2 = LockInfo::new(
        "lock-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );

    let acquired = state.acquire_lock(key.clone(), lock2, Duration::from_secs(30));
    assert!(acquired);

    let current = state.get_lock(&key).unwrap();
    assert_eq!(current.lock_id, "lock-2");
}

#[test]
fn test_cluster_state_cleanup_locks_without_timeout_tracking() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-orphan",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let mut snapshot = ClusterStateSnapshot::new();
    snapshot.ref_locks.insert(key.clone(), lock);
    state.apply_snapshot(snapshot).unwrap();

    state.cleanup_expired_locks();

    let lock_after = state.get_lock(&key);
    assert!(lock_after.is_none());
}
