use gitstratum_control_plane_cluster::{
    ClusterState, ClusterStateSnapshot, ExtendedNodeInfo, LockInfo, NodeType, RefLockKey,
};
use gitstratum_hashring::NodeState;
use std::time::Duration;

fn create_test_node(id: &str, node_type: NodeType) -> ExtendedNodeInfo {
    ExtendedNodeInfo::new(
        id,
        format!("10.0.0.{}", id.chars().last().unwrap_or('0')),
        9000,
        node_type,
    )
}

#[test]
fn test_cluster_snapshot_node_lifecycle_all_types() {
    let mut snapshot = ClusterStateSnapshot::new();
    assert_eq!(snapshot.version, 0);

    snapshot.add_node(create_test_node("cp-1", NodeType::ControlPlane));
    snapshot.add_node(create_test_node("md-1", NodeType::Metadata));
    snapshot.add_node(create_test_node("obj-1", NodeType::Object));
    snapshot.add_node(create_test_node("fe-1", NodeType::Frontend));
    assert_eq!(snapshot.version, 4);
    assert_eq!(snapshot.control_plane_nodes.len(), 1);
    assert_eq!(snapshot.metadata_nodes.len(), 1);
    assert_eq!(snapshot.object_nodes.len(), 1);
    assert_eq!(snapshot.frontend_nodes.len(), 1);

    assert_eq!(
        snapshot.find_node_type("cp-1"),
        Some(NodeType::ControlPlane)
    );
    assert_eq!(snapshot.find_node_type("md-1"), Some(NodeType::Metadata));
    assert_eq!(snapshot.find_node_type("obj-1"), Some(NodeType::Object));
    assert_eq!(snapshot.find_node_type("fe-1"), Some(NodeType::Frontend));
    assert_eq!(snapshot.find_node_type("unknown"), None);

    let all = snapshot.all_nodes();
    assert_eq!(all.len(), 4);

    let _ = snapshot.get_node("obj-1").unwrap();

    assert!(snapshot.set_node_state("cp-1", NodeType::ControlPlane, NodeState::Draining));
    assert!(snapshot.set_node_state("md-1", NodeType::Metadata, NodeState::Draining));
    assert!(snapshot.set_node_state("obj-1", NodeType::Object, NodeState::Draining));
    assert!(snapshot.set_node_state("fe-1", NodeType::Frontend, NodeState::Draining));
    assert!(!snapshot.set_node_state("nonexistent", NodeType::Object, NodeState::Draining));

    assert_eq!(
        snapshot.control_plane_nodes.get("cp-1").unwrap().state,
        NodeState::Draining
    );
    assert_eq!(
        snapshot.metadata_nodes.get("md-1").unwrap().state,
        NodeState::Draining
    );
    assert_eq!(
        snapshot.object_nodes.get("obj-1").unwrap().state,
        NodeState::Draining
    );
    assert_eq!(
        snapshot.frontend_nodes.get("fe-1").unwrap().state,
        NodeState::Draining
    );

    assert!(snapshot
        .remove_node("cp-1", NodeType::ControlPlane)
        .is_some());
    assert!(snapshot.remove_node("md-1", NodeType::Metadata).is_some());
    assert!(snapshot.remove_node("obj-1", NodeType::Object).is_some());
    assert!(snapshot.remove_node("fe-1", NodeType::Frontend).is_some());
    assert!(snapshot
        .remove_node("nonexistent", NodeType::Object)
        .is_none());

    assert!(snapshot.control_plane_nodes.is_empty());
    assert!(snapshot.metadata_nodes.is_empty());
    assert!(snapshot.object_nodes.is_empty());
    assert!(snapshot.frontend_nodes.is_empty());
}

#[test]
fn test_cluster_state_membership_and_hashring_integration() {
    let state = ClusterState::new(16, 2);

    state
        .add_node(create_test_node("obj-1", NodeType::Object))
        .unwrap();
    state
        .add_node(create_test_node("obj-2", NodeType::Object))
        .unwrap();
    state
        .add_node(create_test_node("obj-3", NodeType::Object))
        .unwrap();
    state
        .add_node(create_test_node("md-1", NodeType::Metadata))
        .unwrap();
    state
        .add_node(create_test_node("md-2", NodeType::Metadata))
        .unwrap();
    state
        .add_node(create_test_node("cp-1", NodeType::ControlPlane))
        .unwrap();
    state
        .add_node(create_test_node("fe-1", NodeType::Frontend))
        .unwrap();

    let snapshot = state.snapshot();
    assert_eq!(snapshot.object_nodes.len(), 3);
    assert_eq!(snapshot.metadata_nodes.len(), 2);
    assert_eq!(snapshot.control_plane_nodes.len(), 1);
    assert_eq!(snapshot.frontend_nodes.len(), 1);

    let object_ring = state.object_ring();
    assert_eq!(object_ring.node_count(), 3);

    let metadata_ring = state.metadata_ring();
    assert_eq!(metadata_ring.node_count(), 2);

    assert!(state.get_node("obj-1").is_some());
    assert!(state.get_node("nonexistent").is_none());

    assert!(state.set_node_state("obj-1", NodeState::Draining).unwrap());
    assert!(state.set_node_state("md-1", NodeState::Draining).unwrap());
    assert!(state.set_node_state("cp-1", NodeState::Draining).unwrap());
    assert!(state.set_node_state("fe-1", NodeState::Draining).unwrap());
    assert!(!state
        .set_node_state("nonexistent", NodeState::Draining)
        .unwrap());

    assert_eq!(state.get_node("obj-1").unwrap().state, NodeState::Draining);
    assert_eq!(state.get_node("md-1").unwrap().state, NodeState::Draining);
    assert_eq!(state.get_node("cp-1").unwrap().state, NodeState::Draining);
    assert_eq!(state.get_node("fe-1").unwrap().state, NodeState::Draining);

    let initial_version = state.version();

    assert!(state.remove_node("obj-1").unwrap().is_some());
    assert!(state.remove_node("md-1").unwrap().is_some());
    assert!(state.remove_node("cp-1").unwrap().is_some());
    assert!(state.remove_node("fe-1").unwrap().is_some());
    assert!(state.remove_node("nonexistent").unwrap().is_none());

    assert!(state.version() > initial_version);

    let snapshot_after = state.snapshot();
    assert_eq!(snapshot_after.object_nodes.len(), 2);
    assert_eq!(snapshot_after.metadata_nodes.len(), 1);
    assert!(snapshot_after.control_plane_nodes.is_empty());
    assert!(snapshot_after.frontend_nodes.is_empty());
}

#[test]
fn test_distributed_lock_lifecycle_with_conflicts() {
    let state = ClusterState::new(16, 3);

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock1 = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    assert!(state.acquire_lock(key.clone(), lock1, Duration::from_secs(30)));

    let existing = state.get_lock(&key);
    assert!(existing.is_some());
    assert_eq!(existing.unwrap().holder_id, "holder-1");

    let lock2 = LockInfo::new(
        "lock-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );
    assert!(!state.acquire_lock(key.clone(), lock2, Duration::from_secs(30)));

    assert!(state.release_lock("lock-1"));
    assert!(!state.release_lock("lock-1"));
    assert!(!state.release_lock("nonexistent-lock"));

    assert!(state.get_lock(&key).is_none());

    let lock3 = LockInfo::new(
        "lock-3",
        "repo-1",
        "refs/heads/main",
        "holder-3",
        Duration::from_secs(30),
    );
    assert!(state.acquire_lock(key.clone(), lock3, Duration::from_secs(30)));
    assert_eq!(state.get_lock(&key).unwrap().lock_id, "lock-3");
}

#[test]
fn test_lock_expiration_and_cleanup() {
    let state = ClusterState::new(16, 3);

    let key1 = RefLockKey::new("repo-1", "refs/heads/main");
    let short_lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_millis(1),
    );
    assert!(state.acquire_lock(key1.clone(), short_lock, Duration::from_millis(1)));

    let key2 = RefLockKey::new("repo-2", "refs/heads/develop");
    let long_lock = LockInfo::new(
        "lock-2",
        "repo-2",
        "refs/heads/develop",
        "holder-2",
        Duration::from_secs(300),
    );
    assert!(state.acquire_lock(key2.clone(), long_lock, Duration::from_secs(300)));

    std::thread::sleep(Duration::from_millis(10));

    let new_lock = LockInfo::new(
        "lock-3",
        "repo-1",
        "refs/heads/main",
        "holder-3",
        Duration::from_secs(30),
    );
    assert!(state.acquire_lock(key1.clone(), new_lock, Duration::from_secs(30)));
    assert_eq!(state.get_lock(&key1).unwrap().holder_id, "holder-3");

    state.cleanup_expired_locks();

    assert!(state.get_lock(&key2).is_some());
}

#[test]
fn test_cleanup_expired_locks_various_scenarios() {
    let state = ClusterState::new(16, 3);
    state.cleanup_expired_locks();
    assert!(state.snapshot().ref_locks.is_empty());

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let orphan_lock = LockInfo::new(
        "lock-orphan",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let mut snapshot = ClusterStateSnapshot::new();
    snapshot.ref_locks.insert(key.clone(), orphan_lock);
    state.apply_snapshot(snapshot).unwrap();

    state.cleanup_expired_locks();
    assert!(state.get_lock(&key).is_none());

    let new_orphan = LockInfo::new(
        "lock-orphan-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );
    let mut snapshot2 = ClusterStateSnapshot::new();
    snapshot2.ref_locks.insert(key.clone(), new_orphan);
    state.apply_snapshot(snapshot2).unwrap();

    let replacing_lock = LockInfo::new(
        "lock-replace",
        "repo-1",
        "refs/heads/main",
        "holder-3",
        Duration::from_secs(30),
    );
    assert!(state.acquire_lock(key.clone(), replacing_lock, Duration::from_secs(30)));
    assert_eq!(state.get_lock(&key).unwrap().lock_id, "lock-replace");
}

#[test]
fn test_cluster_state_snapshot_apply_and_leader() {
    let state = ClusterState::new(16, 3);

    assert!(state.leader_id().is_none());
    state.set_leader(Some("leader-1".to_string()));
    assert_eq!(state.leader_id(), Some("leader-1".to_string()));
    state.set_leader(None);
    assert!(state.leader_id().is_none());

    let mut snapshot = ClusterStateSnapshot::new();
    snapshot.add_node(create_test_node("obj-1", NodeType::Object));
    snapshot.add_node(create_test_node("obj-2", NodeType::Object));
    snapshot.add_node(create_test_node("md-1", NodeType::Metadata));

    state.apply_snapshot(snapshot).unwrap();

    let result = state.snapshot();
    assert_eq!(result.object_nodes.len(), 2);
    assert_eq!(result.metadata_nodes.len(), 1);
}

#[test]
fn test_extended_node_info_and_node_type_conversion() {
    let node = ExtendedNodeInfo::new("node-1", "192.168.1.1", 9000, NodeType::Object);

    assert_eq!(node.id, "node-1");
    assert_eq!(node.address, "192.168.1.1");
    assert_eq!(node.port, 9000);
    assert_eq!(node.node_type, NodeType::Object);
    assert_eq!(node.state, NodeState::Active);
    assert_eq!(node.endpoint(), "192.168.1.1:9000");

    let node_id = node.node_id();
    assert_eq!(node_id.as_str(), "node-1");

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
fn test_ref_lock_key_equality() {
    let key1 = RefLockKey::new("repo-1", "refs/heads/main");
    let key2 = RefLockKey::new("repo-1", "refs/heads/main");
    let key3 = RefLockKey::new("repo-1", "refs/heads/develop");
    let key4 = RefLockKey::new("repo-2", "refs/heads/main");

    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
    assert_ne!(key1, key4);
}

#[test]
fn test_defaults_and_empty_state() {
    let snapshot_default = ClusterStateSnapshot::default();
    assert_eq!(snapshot_default.version, 0);
    assert!(snapshot_default.control_plane_nodes.is_empty());
    assert!(snapshot_default.metadata_nodes.is_empty());
    assert!(snapshot_default.object_nodes.is_empty());
    assert!(snapshot_default.frontend_nodes.is_empty());
    assert!(snapshot_default.ref_locks.is_empty());
    assert!(snapshot_default.leader_id.is_none());

    let state_default = ClusterState::default();
    let snapshot_from_default = state_default.snapshot();
    assert_eq!(snapshot_from_default.version, 0);
}
