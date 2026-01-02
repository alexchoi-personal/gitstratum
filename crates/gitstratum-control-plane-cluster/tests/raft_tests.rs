use gitstratum_control_plane_cluster::{
    ClusterStateSnapshot, ControlPlaneStore, ExtendedNodeInfo, LockInfo, NodeType, RaftRequest,
    RaftResponse, RefLockKey,
};
use gitstratum_hashring::NodeState;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftStorage};
use std::collections::BTreeSet;
use std::time::Duration;

fn create_test_node(id: &str, node_type: NodeType) -> ExtendedNodeInfo {
    ExtendedNodeInfo::new(
        id,
        format!("10.0.0.{}", id.chars().last().unwrap_or('0')),
        9000,
        node_type,
    )
}

fn create_log_entry(
    index: u64,
    request: RaftRequest,
) -> Entry<gitstratum_control_plane_cluster::TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(request),
    }
}

#[tokio::test]
async fn test_cluster_membership_lifecycle() {
    let mut store = ControlPlaneStore::new();
    let initial_state = store.get_state();
    assert_eq!(initial_state.version, 0);
    assert!(initial_state.control_plane_nodes.is_empty());

    let entries = vec![
        create_log_entry(
            1,
            RaftRequest::AddNode {
                node: create_test_node("cp-1", NodeType::ControlPlane),
            },
        ),
        create_log_entry(
            2,
            RaftRequest::AddNode {
                node: create_test_node("md-1", NodeType::Metadata),
            },
        ),
        create_log_entry(
            3,
            RaftRequest::AddNode {
                node: create_test_node("obj-1", NodeType::Object),
            },
        ),
        create_log_entry(
            4,
            RaftRequest::AddNode {
                node: create_test_node("fe-1", NodeType::Frontend),
            },
        ),
    ];
    let responses = store.apply_to_state_machine(&entries).await.unwrap();
    assert_eq!(responses.len(), 4);
    for response in &responses {
        assert!(matches!(response, RaftResponse::NodeAdded));
    }

    let state = store.get_state();
    assert_eq!(state.control_plane_nodes.len(), 1);
    assert_eq!(state.metadata_nodes.len(), 1);
    assert_eq!(state.object_nodes.len(), 1);
    assert_eq!(state.frontend_nodes.len(), 1);

    let state_changes = vec![
        create_log_entry(
            5,
            RaftRequest::SetNodeState {
                node_id: "obj-1".to_string(),
                node_type: NodeType::Object,
                state: NodeState::Draining,
            },
        ),
        create_log_entry(
            6,
            RaftRequest::SetNodeState {
                node_id: "nonexistent".to_string(),
                node_type: NodeType::Object,
                state: NodeState::Draining,
            },
        ),
    ];
    let responses = store.apply_to_state_machine(&state_changes).await.unwrap();
    assert!(matches!(
        responses[0],
        RaftResponse::NodeStateSet { found: true }
    ));
    assert!(matches!(
        responses[1],
        RaftResponse::NodeStateSet { found: false }
    ));

    let state = store.get_state();
    assert_eq!(
        state.object_nodes.get("obj-1").unwrap().state,
        NodeState::Draining
    );

    let removals = vec![
        create_log_entry(
            7,
            RaftRequest::RemoveNode {
                node_id: "obj-1".to_string(),
                node_type: NodeType::Object,
            },
        ),
        create_log_entry(
            8,
            RaftRequest::RemoveNode {
                node_id: "nonexistent".to_string(),
                node_type: NodeType::Object,
            },
        ),
    ];
    let responses = store.apply_to_state_machine(&removals).await.unwrap();
    assert!(matches!(
        responses[0],
        RaftResponse::NodeRemoved { found: true }
    ));
    assert!(matches!(
        responses[1],
        RaftResponse::NodeRemoved { found: false }
    ));

    let (last_applied, _membership) = store.last_applied_state().await.unwrap();
    assert!(last_applied.is_some());
    assert_eq!(last_applied.unwrap().index, 8);
}

#[tokio::test]
async fn test_distributed_lock_lifecycle() {
    let mut store = ControlPlaneStore::new();

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );
    let entry = create_log_entry(
        1,
        RaftRequest::AcquireLock {
            key: key.clone(),
            lock,
        },
    );
    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();
    match &responses[0] {
        RaftResponse::LockAcquired { lock_id } => assert_eq!(lock_id, "lock-1"),
        _ => panic!("expected LockAcquired response"),
    }

    let conflicting_lock = LockInfo::new(
        "lock-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );
    let entry2 = create_log_entry(
        2,
        RaftRequest::AcquireLock {
            key: key.clone(),
            lock: conflicting_lock,
        },
    );
    let responses = store.apply_to_state_machine(&[entry2]).await.unwrap();
    match &responses[0] {
        RaftResponse::LockNotAcquired { reason } => assert!(reason.contains("holder-1")),
        _ => panic!("expected LockNotAcquired response"),
    }

    let release_entry = create_log_entry(
        3,
        RaftRequest::ReleaseLock {
            lock_id: "lock-1".to_string(),
        },
    );
    let responses = store
        .apply_to_state_machine(&[release_entry])
        .await
        .unwrap();
    assert!(matches!(
        responses[0],
        RaftResponse::LockReleased { found: true }
    ));

    let release_again = create_log_entry(
        4,
        RaftRequest::ReleaseLock {
            lock_id: "lock-1".to_string(),
        },
    );
    let responses = store
        .apply_to_state_machine(&[release_again])
        .await
        .unwrap();
    assert!(matches!(
        responses[0],
        RaftResponse::LockReleased { found: false }
    ));

    let new_lock = LockInfo::new(
        "lock-3",
        "repo-1",
        "refs/heads/main",
        "holder-3",
        Duration::from_secs(30),
    );
    let entry3 = create_log_entry(
        5,
        RaftRequest::AcquireLock {
            key,
            lock: new_lock,
        },
    );
    let responses = store.apply_to_state_machine(&[entry3]).await.unwrap();
    assert!(matches!(responses[0], RaftResponse::LockAcquired { .. }));
}

#[tokio::test]
async fn test_expired_lock_handling() {
    let mut store = ControlPlaneStore::new();

    let key1 = RefLockKey::new("repo-1", "refs/heads/main");
    let expired_lock = LockInfo {
        lock_id: "lock-1".to_string(),
        repo_id: "repo-1".to_string(),
        ref_name: "refs/heads/main".to_string(),
        holder_id: "holder-1".to_string(),
        timeout_ms: 1,
        acquired_at_epoch_ms: 0,
    };

    let key2 = RefLockKey::new("repo-2", "refs/heads/develop");
    let expired_lock2 = LockInfo {
        lock_id: "lock-2".to_string(),
        repo_id: "repo-2".to_string(),
        ref_name: "refs/heads/develop".to_string(),
        holder_id: "holder-2".to_string(),
        timeout_ms: 1,
        acquired_at_epoch_ms: 0,
    };

    let entries = vec![
        create_log_entry(
            1,
            RaftRequest::AcquireLock {
                key: key1.clone(),
                lock: expired_lock,
            },
        ),
        create_log_entry(
            2,
            RaftRequest::AcquireLock {
                key: key2,
                lock: expired_lock2,
            },
        ),
    ];
    store.apply_to_state_machine(&entries).await.unwrap();
    std::thread::sleep(Duration::from_millis(10));

    let cleanup_entry = create_log_entry(3, RaftRequest::CleanupExpiredLocks);
    let responses = store
        .apply_to_state_machine(&[cleanup_entry])
        .await
        .unwrap();
    match &responses[0] {
        RaftResponse::LocksCleanedUp { count } => assert_eq!(*count, 2),
        _ => panic!("expected LocksCleanedUp response"),
    }

    let new_lock = LockInfo::new(
        "lock-3",
        "repo-1",
        "refs/heads/main",
        "holder-3",
        Duration::from_secs(30),
    );
    let entry = create_log_entry(
        4,
        RaftRequest::AcquireLock {
            key: key1,
            lock: new_lock,
        },
    );
    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();
    assert!(matches!(responses[0], RaftResponse::LockAcquired { .. }));
}

#[tokio::test]
async fn test_log_storage_and_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("raft_db");

    {
        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        assert!(store.read_vote().await.unwrap().is_none());
        let vote = openraft::Vote::new(1, 2);
        store.save_vote(&vote).await.unwrap();

        let log_id = LogId::new(CommittedLeaderId::new(1, 1), 10);
        store.save_committed(Some(log_id)).await.unwrap();

        let entries = vec![
            create_log_entry(
                1,
                RaftRequest::AddNode {
                    node: create_test_node("node-1", NodeType::Object),
                },
            ),
            create_log_entry(
                2,
                RaftRequest::AddNode {
                    node: create_test_node("node-2", NodeType::Metadata),
                },
            ),
            create_log_entry(
                3,
                RaftRequest::AddNode {
                    node: create_test_node("node-3", NodeType::Frontend),
                },
            ),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_log_id.unwrap().index, 3);
    }

    {
        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        let vote = store.read_vote().await.unwrap();
        assert!(vote.is_some());

        let committed = store.read_committed().await.unwrap();
        assert_eq!(committed.unwrap().index, 10);

        let log_state = store.get_log_state().await.unwrap();
        assert_eq!(log_state.last_log_id.unwrap().index, 3);
    }
}

#[tokio::test]
async fn test_log_operations_with_purging() {
    use openraft::RaftLogReader;

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("purge_db");

    let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

    let entries = vec![
        create_log_entry(
            1,
            RaftRequest::AddNode {
                node: create_test_node("node-1", NodeType::Object),
            },
        ),
        create_log_entry(
            2,
            RaftRequest::AddNode {
                node: create_test_node("node-2", NodeType::Object),
            },
        ),
        create_log_entry(
            3,
            RaftRequest::AddNode {
                node: create_test_node("node-3", NodeType::Object),
            },
        ),
    ];
    store.append_to_log(entries).await.unwrap();

    let retrieved = store.try_get_log_entries(1..=3).await.unwrap();
    assert_eq!(retrieved.len(), 3);

    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 2);
    store.purge_logs_upto(log_id).await.unwrap();

    let retrieved_after = store.try_get_log_entries(1..=3).await.unwrap();
    assert_eq!(retrieved_after.len(), 1);
    assert_eq!(retrieved_after[0].log_id.index, 3);

    let log_state = store.get_log_state().await.unwrap();
    assert_eq!(log_state.last_purged_log_id.unwrap().index, 2);
}

#[tokio::test]
async fn test_conflict_log_deletion() {
    use openraft::RaftLogReader;

    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("conflict_db");

    let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

    let entries = vec![
        create_log_entry(
            1,
            RaftRequest::AddNode {
                node: create_test_node("node-1", NodeType::Object),
            },
        ),
        create_log_entry(
            2,
            RaftRequest::AddNode {
                node: create_test_node("node-2", NodeType::Object),
            },
        ),
        create_log_entry(
            3,
            RaftRequest::AddNode {
                node: create_test_node("node-3", NodeType::Object),
            },
        ),
    ];
    store.append_to_log(entries).await.unwrap();

    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 2);
    store.delete_conflict_logs_since(log_id).await.unwrap();

    let retrieved = store.try_get_log_entries(1..=3).await.unwrap();
    assert_eq!(retrieved.len(), 1);
    assert_eq!(retrieved[0].log_id.index, 1);
}

#[tokio::test]
async fn test_snapshot_lifecycle() {
    let mut store = ControlPlaneStore::new();

    assert!(store.get_current_snapshot().await.unwrap().is_none());

    let entries = vec![
        create_log_entry(
            1,
            RaftRequest::AddNode {
                node: create_test_node("obj-1", NodeType::Object),
            },
        ),
        create_log_entry(
            2,
            RaftRequest::AddNode {
                node: create_test_node("md-1", NodeType::Metadata),
            },
        ),
    ];
    store.apply_to_state_machine(&entries).await.unwrap();

    let mut builder = store.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id.unwrap().index, 2);

    let state = store.get_state();
    assert_eq!(state.object_nodes.len(), 1);
    assert_eq!(state.metadata_nodes.len(), 1);

    let mut new_state = ClusterStateSnapshot::new();
    new_state.add_node(create_test_node("new-obj-1", NodeType::Object));
    let snapshot_data = serde_json::to_vec(&new_state).unwrap();

    let meta = openraft::SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 5)),
        last_membership: openraft::StoredMembership::default(),
        snapshot_id: "test-snapshot".to_string(),
    };

    let cursor = Box::new(std::io::Cursor::new(snapshot_data));
    store.install_snapshot(&meta, cursor).await.unwrap();

    let restored_state = store.get_state();
    assert_eq!(restored_state.object_nodes.len(), 1);
    assert!(restored_state.object_nodes.contains_key("new-obj-1"));
    assert!(restored_state.metadata_nodes.is_empty());

    let current = store.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta.snapshot_id, "test-snapshot");
}

#[tokio::test]
async fn test_snapshot_install_with_invalid_data() {
    let mut store = ControlPlaneStore::new();

    let meta = openraft::SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 5)),
        last_membership: openraft::StoredMembership::default(),
        snapshot_id: "invalid-snapshot".to_string(),
    };

    let cursor = Box::new(std::io::Cursor::new(b"invalid json".to_vec()));
    let result = store.install_snapshot(&meta, cursor).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_membership_config_entry() {
    let mut store = ControlPlaneStore::new();

    let mut members = BTreeSet::new();
    members.insert(1u64);
    members.insert(2u64);
    members.insert(3u64);
    let membership = openraft::Membership::new(vec![members], None);

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Membership(membership),
    };
    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();
    assert!(matches!(responses[0], RaftResponse::Success));

    let (_, stored_membership) = store.last_applied_state().await.unwrap();
    assert!(!stored_membership.membership().get_joint_config().is_empty());
}

#[tokio::test]
async fn test_blank_and_empty_entries() {
    let mut store = ControlPlaneStore::new();

    let blank_entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Blank,
    };
    let responses = store.apply_to_state_machine(&[blank_entry]).await.unwrap();
    assert!(matches!(responses[0], RaftResponse::Success));

    let empty_entries: Vec<Entry<gitstratum_control_plane_cluster::TypeConfig>> = vec![];
    let responses = store.apply_to_state_machine(&empty_entries).await.unwrap();
    assert!(responses.is_empty());

    store.append_to_log(vec![]).await.unwrap();
    let log_state = store.get_log_state().await.unwrap();
    assert!(log_state.last_log_id.is_none());
}

#[tokio::test]
async fn test_log_reader_and_stores() {
    use gitstratum_control_plane_cluster::create_stores;
    use openraft::RaftLogReader;

    let mut store = ControlPlaneStore::new();

    let entries = vec![
        create_log_entry(
            1,
            RaftRequest::AddNode {
                node: create_test_node("node-1", NodeType::Object),
            },
        ),
        create_log_entry(
            2,
            RaftRequest::AddNode {
                node: create_test_node("node-2", NodeType::Object),
            },
        ),
    ];
    store.append_to_log(entries).await.unwrap();

    let mut reader = store.get_log_reader().await;
    let retrieved = reader.try_get_log_entries(1..=2).await.unwrap();
    assert_eq!(retrieved.len(), 2);

    let base_store = ControlPlaneStore::new();
    let (_log_store, _sm_store) = create_stores(base_store);
}

#[tokio::test]
async fn test_receive_snapshot_cursor() {
    let mut store = ControlPlaneStore::new();

    let cursor = store.begin_receiving_snapshot().await.unwrap();
    let data = cursor.into_inner();
    assert!(data.is_empty());
}

#[tokio::test]
async fn test_empty_store_operations() {
    let mut store = ControlPlaneStore::new();

    let log_state_initial = store.get_log_state().await.unwrap();
    assert!(log_state_initial.last_log_id.is_none());

    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 1);
    store.delete_conflict_logs_since(log_id).await.unwrap();

    let log_state_after_delete = store.get_log_state().await.unwrap();
    assert!(log_state_after_delete.last_log_id.is_none());

    store.purge_logs_upto(log_id).await.unwrap();

    let log_state = store.get_log_state().await.unwrap();
    assert!(log_state.last_purged_log_id.is_some());
    assert_eq!(log_state.last_purged_log_id.unwrap().index, 1);
}

#[tokio::test]
async fn test_save_and_clear_committed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("committed_db");

    let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 5);
    store.save_committed(Some(log_id)).await.unwrap();
    assert_eq!(store.read_committed().await.unwrap().unwrap().index, 5);

    store.save_committed(None).await.unwrap();
    assert!(store.read_committed().await.unwrap().is_none());
}

#[tokio::test]
async fn test_serialization_via_workflow() {
    let node = create_test_node("node-1", NodeType::Object);
    let request = RaftRequest::AddNode { node: node.clone() };
    let serialized = serde_json::to_string(&request).unwrap();
    let deserialized: RaftRequest = serde_json::from_str(&serialized).unwrap();
    assert_eq!(request, deserialized);

    let response = RaftResponse::LockAcquired {
        lock_id: "lock-123".to_string(),
    };
    let serialized = serde_json::to_string(&response).unwrap();
    let deserialized: RaftResponse = serde_json::from_str(&serialized).unwrap();
    assert_eq!(response, deserialized);
}
