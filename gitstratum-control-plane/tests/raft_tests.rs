use gitstratum_control_plane::{
    ControlPlaneStore, ExtendedNodeInfo, LockInfo, NodeType, RaftRequest, RaftResponse, RefLockKey,
};
use gitstratum_hashring::NodeState;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftStorage};
use std::time::Duration;

fn create_test_node(id: &str, node_type: NodeType) -> ExtendedNodeInfo {
    ExtendedNodeInfo::new(id, format!("10.0.0.{}", id.chars().last().unwrap()), 9000, node_type)
}

fn create_log_entry(index: u64, request: RaftRequest) -> Entry<gitstratum_control_plane::TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(request),
    }
}

#[tokio::test]
async fn test_state_machine_store_new() {
    let store = ControlPlaneStore::new();
    let state = store.get_state();
    assert_eq!(state.version, 0);
    assert!(state.control_plane_nodes.is_empty());
    assert!(state.metadata_nodes.is_empty());
    assert!(state.object_nodes.is_empty());
    assert!(state.frontend_nodes.is_empty());
}

#[tokio::test]
async fn test_state_machine_apply_add_node() {
    let mut store = ControlPlaneStore::new();

    let node = create_test_node("node-1", NodeType::Object);
    let entry = create_log_entry(1, RaftRequest::AddNode { node: node.clone() });

    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::NodeAdded));

    let state = store.get_state();
    assert_eq!(state.object_nodes.len(), 1);
    assert!(state.object_nodes.contains_key("node-1"));
}

#[tokio::test]
async fn test_state_machine_apply_remove_node() {
    let mut store = ControlPlaneStore::new();

    let node = create_test_node("node-1", NodeType::Object);
    let add_entry = create_log_entry(1, RaftRequest::AddNode { node: node.clone() });
    store.apply_to_state_machine(&[add_entry]).await.unwrap();

    let remove_entry = create_log_entry(
        2,
        RaftRequest::RemoveNode {
            node_id: "node-1".to_string(),
            node_type: NodeType::Object,
        },
    );

    let responses = store.apply_to_state_machine(&[remove_entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::NodeRemoved { found: true }));

    let state = store.get_state();
    assert!(state.object_nodes.is_empty());
}

#[tokio::test]
async fn test_state_machine_apply_remove_nonexistent_node() {
    let mut store = ControlPlaneStore::new();

    let remove_entry = create_log_entry(
        1,
        RaftRequest::RemoveNode {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
        },
    );

    let responses = store.apply_to_state_machine(&[remove_entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::NodeRemoved { found: false }));
}

#[tokio::test]
async fn test_state_machine_apply_set_node_state() {
    let mut store = ControlPlaneStore::new();

    let node = create_test_node("node-1", NodeType::Object);
    let add_entry = create_log_entry(1, RaftRequest::AddNode { node: node.clone() });
    store.apply_to_state_machine(&[add_entry]).await.unwrap();

    let set_state_entry = create_log_entry(
        2,
        RaftRequest::SetNodeState {
            node_id: "node-1".to_string(),
            node_type: NodeType::Object,
            state: NodeState::Draining,
        },
    );

    let responses = store.apply_to_state_machine(&[set_state_entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::NodeStateSet { found: true }));

    let state = store.get_state();
    let node = state.object_nodes.get("node-1").unwrap();
    assert_eq!(node.state, NodeState::Draining);
}

#[tokio::test]
async fn test_state_machine_apply_acquire_lock() {
    let mut store = ControlPlaneStore::new();

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let entry = create_log_entry(1, RaftRequest::AcquireLock { key, lock });

    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    match &responses[0] {
        RaftResponse::LockAcquired { lock_id } => {
            assert_eq!(lock_id, "lock-1");
        }
        _ => panic!("expected LockAcquired response"),
    }
}

#[tokio::test]
async fn test_state_machine_apply_acquire_lock_conflict() {
    let mut store = ControlPlaneStore::new();

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock1 = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let entry1 = create_log_entry(1, RaftRequest::AcquireLock { key: key.clone(), lock: lock1 });
    store.apply_to_state_machine(&[entry1]).await.unwrap();

    let lock2 = LockInfo::new(
        "lock-2",
        "repo-1",
        "refs/heads/main",
        "holder-2",
        Duration::from_secs(30),
    );

    let entry2 = create_log_entry(2, RaftRequest::AcquireLock { key, lock: lock2 });
    let responses = store.apply_to_state_machine(&[entry2]).await.unwrap();

    assert_eq!(responses.len(), 1);
    match &responses[0] {
        RaftResponse::LockNotAcquired { reason } => {
            assert!(reason.contains("holder-1"));
        }
        _ => panic!("expected LockNotAcquired response"),
    }
}

#[tokio::test]
async fn test_state_machine_apply_release_lock() {
    let mut store = ControlPlaneStore::new();

    let key = RefLockKey::new("repo-1", "refs/heads/main");
    let lock = LockInfo::new(
        "lock-1",
        "repo-1",
        "refs/heads/main",
        "holder-1",
        Duration::from_secs(30),
    );

    let acquire_entry = create_log_entry(1, RaftRequest::AcquireLock { key, lock });
    store.apply_to_state_machine(&[acquire_entry]).await.unwrap();

    let release_entry = create_log_entry(
        2,
        RaftRequest::ReleaseLock {
            lock_id: "lock-1".to_string(),
        },
    );

    let responses = store.apply_to_state_machine(&[release_entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::LockReleased { found: true }));
}

#[tokio::test]
async fn test_state_machine_apply_release_nonexistent_lock() {
    let mut store = ControlPlaneStore::new();

    let release_entry = create_log_entry(
        1,
        RaftRequest::ReleaseLock {
            lock_id: "nonexistent".to_string(),
        },
    );

    let responses = store.apply_to_state_machine(&[release_entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::LockReleased { found: false }));
}

#[tokio::test]
async fn test_state_machine_apply_cleanup_expired_locks() {
    let mut store = ControlPlaneStore::new();

    let cleanup_entry = create_log_entry(1, RaftRequest::CleanupExpiredLocks);

    let responses = store.apply_to_state_machine(&[cleanup_entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    match &responses[0] {
        RaftResponse::LocksCleanedUp { count } => {
            assert_eq!(*count, 0);
        }
        _ => panic!("expected LocksCleanedUp response"),
    }
}

#[tokio::test]
async fn test_state_machine_last_applied_state() {
    let mut store = ControlPlaneStore::new();

    let (last_applied, _membership) = store.last_applied_state().await.unwrap();
    assert!(last_applied.is_none());

    let node = create_test_node("node-1", NodeType::Object);
    let entry = create_log_entry(1, RaftRequest::AddNode { node });
    store.apply_to_state_machine(&[entry]).await.unwrap();

    let (last_applied, _membership) = store.last_applied_state().await.unwrap();
    assert!(last_applied.is_some());
    assert_eq!(last_applied.unwrap().index, 1);
}

#[tokio::test]
async fn test_state_machine_multiple_entries() {
    let mut store = ControlPlaneStore::new();

    let entries = vec![
        create_log_entry(1, RaftRequest::AddNode { node: create_test_node("node-1", NodeType::Object) }),
        create_log_entry(2, RaftRequest::AddNode { node: create_test_node("node-2", NodeType::Object) }),
        create_log_entry(3, RaftRequest::AddNode { node: create_test_node("node-3", NodeType::Metadata) }),
    ];

    let responses = store.apply_to_state_machine(&entries).await.unwrap();

    assert_eq!(responses.len(), 3);
    assert!(matches!(responses[0], RaftResponse::NodeAdded));
    assert!(matches!(responses[1], RaftResponse::NodeAdded));
    assert!(matches!(responses[2], RaftResponse::NodeAdded));

    let state = store.get_state();
    assert_eq!(state.object_nodes.len(), 2);
    assert_eq!(state.metadata_nodes.len(), 1);
}

#[tokio::test]
async fn test_state_machine_blank_entry() {
    let mut store = ControlPlaneStore::new();

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Blank,
    };

    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], RaftResponse::Success));
}

#[tokio::test]
async fn test_request_serialization() {
    let request = RaftRequest::AddNode {
        node: create_test_node("node-1", NodeType::Object),
    };

    let serialized = serde_json::to_string(&request).unwrap();
    let deserialized: RaftRequest = serde_json::from_str(&serialized).unwrap();

    assert_eq!(request, deserialized);
}

#[tokio::test]
async fn test_response_serialization() {
    let response = RaftResponse::LockAcquired {
        lock_id: "lock-123".to_string(),
    };

    let serialized = serde_json::to_string(&response).unwrap();
    let deserialized: RaftResponse = serde_json::from_str(&serialized).unwrap();

    assert_eq!(response, deserialized);
}

#[tokio::test]
async fn test_log_storage_vote() {
    let mut store = ControlPlaneStore::new();

    let vote = store.read_vote().await.unwrap();
    assert!(vote.is_none());

    use openraft::Vote;
    let new_vote = Vote::new(1, 1);
    store.save_vote(&new_vote).await.unwrap();

    let read_vote = store.read_vote().await.unwrap();
    assert_eq!(read_vote, Some(new_vote));
}

#[tokio::test]
async fn test_log_storage_append_and_get() {
    let mut store = ControlPlaneStore::new();

    let node = create_test_node("node-1", NodeType::Object);
    let entry = create_log_entry(1, RaftRequest::AddNode { node });

    store.append_to_log(vec![entry.clone()]).await.unwrap();

    let log_state = store.get_log_state().await.unwrap();
    assert!(log_state.last_log_id.is_some());
    assert_eq!(log_state.last_log_id.unwrap().index, 1);
}

#[tokio::test]
async fn test_log_storage_purge() {
    let mut store = ControlPlaneStore::new();

    let entries = vec![
        create_log_entry(1, RaftRequest::AddNode { node: create_test_node("node-1", NodeType::Object) }),
        create_log_entry(2, RaftRequest::AddNode { node: create_test_node("node-2", NodeType::Object) }),
        create_log_entry(3, RaftRequest::AddNode { node: create_test_node("node-3", NodeType::Object) }),
    ];

    store.append_to_log(entries).await.unwrap();

    store.purge_logs_upto(LogId::new(CommittedLeaderId::new(1, 1), 2)).await.unwrap();

    let log_state = store.get_log_state().await.unwrap();
    assert!(log_state.last_purged_log_id.is_some());
    assert_eq!(log_state.last_purged_log_id.unwrap().index, 2);
}
