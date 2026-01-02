use async_trait::async_trait;
use gitstratum_core::{Blob, Oid};
use gitstratum_object_cluster::{
    error::Result,
    store::{ObjectStorage, StorageStats},
    NodeId, RebalanceDirection, RepairCoordinator, RepairCoordinatorConfig, RepairPriority,
    RepairSession, RepairSessionStatus, RepairType, SessionId,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Minimal mock for integration tests.
pub struct MockObjectStorage {
    objects: RwLock<HashMap<Oid, Blob>>,
}

impl MockObjectStorage {
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ObjectStorage for MockObjectStorage {
    async fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
        Ok(self.objects.read().unwrap().get(oid).cloned())
    }

    async fn put(&self, blob: &Blob) -> Result<()> {
        self.objects.write().unwrap().insert(blob.oid, blob.clone());
        Ok(())
    }

    async fn delete(&self, oid: &Oid) -> Result<bool> {
        Ok(self.objects.write().unwrap().remove(oid).is_some())
    }

    fn has(&self, oid: &Oid) -> bool {
        self.objects.read().unwrap().contains_key(oid)
    }

    fn stats(&self) -> StorageStats {
        StorageStats {
            total_blobs: self.objects.read().unwrap().len() as u64,
            total_bytes: 0,
            used_bytes: 0,
            available_bytes: 0,
            io_utilization: 0.0,
        }
    }
}

#[tokio::test]
async fn test_repair_session_lifecycle() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig::default();
    let coordinator = RepairCoordinator::new(store, NodeId::new("node-1"), config);

    let session = RepairSession::builder()
        .id("session-lifecycle-1")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .peer_node("peer-1")
        .build()
        .expect("Failed to build session");

    assert_eq!(session.status(), RepairSessionStatus::Pending);

    let session_id = coordinator
        .create_session(session)
        .expect("Failed to create session");
    assert_eq!(session_id.as_str(), "session-lifecycle-1");

    let retrieved = coordinator.get_session(&session_id).unwrap();
    assert_eq!(retrieved.status(), RepairSessionStatus::Pending);
    assert!(!retrieved.is_active());
    assert!(!retrieved.is_terminal());

    assert!(coordinator.start_session(&session_id));
    let in_progress = coordinator.get_session(&session_id).unwrap();
    assert_eq!(in_progress.status(), RepairSessionStatus::InProgress);
    assert!(in_progress.is_active());
    assert!(!in_progress.is_terminal());

    coordinator.update_progress(&session_id, 100, 10, 8, 2048);
    let with_progress = coordinator.get_session(&session_id).unwrap();
    assert_eq!(with_progress.progress().objects_compared, 100);
    assert_eq!(with_progress.progress().objects_missing, 10);
    assert_eq!(with_progress.progress().objects_transferred, 8);
    assert_eq!(with_progress.progress().bytes_transferred, 2048);

    coordinator.update_progress(&session_id, 50, 5, 4, 1024);
    let cumulative_progress = coordinator.get_session(&session_id).unwrap();
    assert_eq!(cumulative_progress.progress().objects_compared, 150);
    assert_eq!(cumulative_progress.progress().objects_missing, 15);
    assert_eq!(cumulative_progress.progress().objects_transferred, 12);
    assert_eq!(cumulative_progress.progress().bytes_transferred, 3072);

    assert!(coordinator.complete_session(&session_id));
    let completed = coordinator.get_session(&session_id).unwrap();
    assert_eq!(completed.status(), RepairSessionStatus::Completed);
    assert!(!completed.is_active());
    assert!(completed.is_terminal());

    let stats = coordinator.stats();
    assert_eq!(stats.sessions_created, 1);
    assert_eq!(stats.sessions_completed, 1);
    assert_eq!(stats.sessions_failed, 0);
    assert_eq!(stats.active_sessions, 0);
}

#[tokio::test]
async fn test_repair_session_with_crash_recovery_type() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig::default();
    let coordinator = RepairCoordinator::new(store, NodeId::new("node-crash-1"), config);

    let session = RepairSession::builder()
        .id("crash-recovery-session")
        .session_type(RepairType::crash_recovery(1000, 2000))
        .ring_version(1)
        .peer_node("peer-1")
        .peer_node("peer-2")
        .build()
        .expect("Failed to build crash recovery session");

    assert!(session.id().as_str().contains("crash-recovery"));
    assert_eq!(session.peer_nodes().len(), 2);

    match session.session_type() {
        RepairType::CrashRecovery {
            downtime_start,
            downtime_end,
        } => {
            assert_eq!(downtime_start, 1000);
            assert_eq!(downtime_end, 2000);
        }
        _ => panic!("Expected CrashRecovery session type"),
    }

    let session_id = coordinator.create_session(session).unwrap();
    coordinator.start_session(&session_id);
    coordinator.update_progress(&session_id, 100, 10, 10, 2048);
    coordinator.complete_session(&session_id);

    let completed = coordinator.get_session(&session_id).unwrap();
    assert_eq!(completed.status(), RepairSessionStatus::Completed);
}

#[tokio::test]
async fn test_repair_priority_ordering() {
    assert!(RepairPriority::FailedWrite < RepairPriority::CrashRecovery);
    assert!(RepairPriority::CrashRecovery < RepairPriority::Rebalance);
    assert!(RepairPriority::Rebalance < RepairPriority::AntiEntropy);

    let mut priorities = [
        RepairPriority::AntiEntropy,
        RepairPriority::FailedWrite,
        RepairPriority::Rebalance,
        RepairPriority::CrashRecovery,
    ];
    priorities.sort();

    assert_eq!(priorities[0], RepairPriority::FailedWrite);
    assert_eq!(priorities[1], RepairPriority::CrashRecovery);
    assert_eq!(priorities[2], RepairPriority::Rebalance);
    assert_eq!(priorities[3], RepairPriority::AntiEntropy);
}

#[tokio::test]
async fn test_session_checkpoint_and_resume() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig::default();
    let coordinator = RepairCoordinator::new(store, NodeId::new("node-checkpoint"), config);

    let session = RepairSession::builder()
        .id("checkpoint-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .peer_node("peer-1")
        .build()
        .unwrap();

    let session_id = coordinator.create_session(session).unwrap();
    coordinator.start_session(&session_id);

    coordinator.update_progress(&session_id, 100, 10, 8, 2048);

    coordinator.update_progress(&session_id, 0, 0, 0, 0);
    let with_progress = coordinator.get_session(&session_id).unwrap();
    assert_eq!(with_progress.progress().objects_compared, 100);

    let mut restored_session = RepairSession::builder()
        .id("checkpoint-session-restored")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .peer_node("peer-1")
        .build()
        .unwrap();

    let original = coordinator.get_session(&session_id).unwrap();
    *restored_session.progress_mut() = original.progress().clone();

    assert_eq!(restored_session.progress().objects_compared, 100);
    assert_eq!(restored_session.progress().objects_missing, 10);
    assert_eq!(restored_session.progress().objects_transferred, 8);
}

#[tokio::test]
async fn test_concurrent_session_limit() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig {
        max_concurrent_sessions: 2,
        ..Default::default()
    };
    let coordinator = RepairCoordinator::new(store, NodeId::new("limit-node"), config);

    let s1 = RepairSession::builder()
        .id("session-1")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();
    let s2 = RepairSession::builder()
        .id("session-2")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();
    let s3 = RepairSession::builder()
        .id("session-3")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    assert!(coordinator.create_session(s1).is_ok());
    assert!(coordinator.create_session(s2).is_ok());
    let result = coordinator.create_session(s3);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("max concurrent sessions"));

    let session_1_id = SessionId::new("session-1");
    coordinator.complete_session(&session_1_id);

    let s4 = RepairSession::builder()
        .id("session-4")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();
    assert!(coordinator.create_session(s4).is_ok());
}

#[tokio::test]
async fn test_session_failure_handling() {
    let store = Arc::new(MockObjectStorage::new());
    let coordinator = RepairCoordinator::new(
        store,
        NodeId::new("fail-node"),
        RepairCoordinatorConfig::default(),
    );

    let session = RepairSession::builder()
        .id("fail-session")
        .session_type(RepairType::crash_recovery(1000, 2000))
        .ring_version(1)
        .build()
        .unwrap();

    let session_id = coordinator.create_session(session).unwrap();
    coordinator.start_session(&session_id);

    coordinator.update_progress(&session_id, 50, 5, 3, 512);

    coordinator.fail_session(&session_id);

    let failed = coordinator.get_session(&session_id).unwrap();
    assert_eq!(failed.status(), RepairSessionStatus::Failed);
    assert!(failed.is_terminal());

    let stats = coordinator.stats();
    assert_eq!(stats.sessions_failed, 1);
    assert_eq!(stats.sessions_completed, 0);
    assert_eq!(stats.active_sessions, 0);

    assert_eq!(failed.progress().objects_compared, 50);
}

#[tokio::test]
async fn test_session_cancellation() {
    let store = Arc::new(MockObjectStorage::new());
    let coordinator = RepairCoordinator::new(
        store,
        NodeId::new("cancel-node"),
        RepairCoordinatorConfig::default(),
    );

    let session = RepairSession::builder()
        .id("cancel-session")
        .session_type(RepairType::rebalance_outgoing())
        .ring_version(5)
        .build()
        .unwrap();

    let session_id = coordinator.create_session(session).unwrap();
    coordinator.start_session(&session_id);

    assert!(coordinator.cancel_session(&session_id));

    let cancelled = coordinator.get_session(&session_id).unwrap();
    assert_eq!(cancelled.status(), RepairSessionStatus::Cancelled);
    assert!(cancelled.is_terminal());

    let stats = coordinator.stats();
    assert_eq!(stats.sessions_cancelled, 1);
}

#[tokio::test]
async fn test_repair_stats_tracking() {
    let store = Arc::new(MockObjectStorage::new());
    let coordinator = RepairCoordinator::new(
        store,
        NodeId::new("stats-node"),
        RepairCoordinatorConfig::default(),
    );

    assert_eq!(coordinator.total_sessions_created(), 0);
    assert_eq!(coordinator.total_objects_repaired(), 0);
    assert_eq!(coordinator.total_bytes_transferred(), 0);

    for i in 0..5 {
        let session = RepairSession::builder()
            .id(format!("stat-session-{}", i))
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session).unwrap();
    }

    assert_eq!(coordinator.total_sessions_created(), 5);

    coordinator.record_objects_repaired(100, 10_000);
    coordinator.record_objects_repaired(50, 5_000);

    assert_eq!(coordinator.total_objects_repaired(), 150);
    assert_eq!(coordinator.total_bytes_transferred(), 15_000);

    let stats = coordinator.stats();
    assert_eq!(stats.sessions_created, 5);
    assert_eq!(stats.objects_repaired, 150);
    assert_eq!(stats.bytes_transferred, 15_000);
}

#[tokio::test]
async fn test_multiple_repair_types_concurrently() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig {
        max_concurrent_sessions: 10,
        ..Default::default()
    };
    let coordinator = RepairCoordinator::new(store, NodeId::new("multi-node"), config);

    let sessions = vec![
        RepairSession::builder()
            .id("ae-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap(),
        RepairSession::builder()
            .id("ae-2")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap(),
        RepairSession::builder()
            .id("crash-1")
            .session_type(RepairType::crash_recovery(1000, 2000))
            .ring_version(1)
            .build()
            .unwrap(),
        RepairSession::builder()
            .id("rebal-in-1")
            .session_type(RepairType::rebalance_incoming())
            .ring_version(1)
            .build()
            .unwrap(),
        RepairSession::builder()
            .id("rebal-out-1")
            .session_type(RepairType::rebalance_outgoing())
            .ring_version(1)
            .build()
            .unwrap(),
    ];

    for session in sessions {
        coordinator.create_session(session).unwrap();
    }

    assert_eq!(coordinator.list_sessions().len(), 5);
    assert_eq!(coordinator.active_session_count(), 5);

    let ae_sessions = coordinator.list_sessions_by_type(RepairType::AntiEntropy);
    assert_eq!(ae_sessions.len(), 2);

    let crash_sessions = coordinator.list_sessions_by_type(RepairType::crash_recovery(0, 0));
    assert_eq!(crash_sessions.len(), 1);

    let rebal_sessions = coordinator.list_sessions_by_type(RepairType::rebalance_incoming());
    assert_eq!(rebal_sessions.len(), 2);

    let ae1_id = SessionId::new("ae-1");
    let ae2_id = SessionId::new("ae-2");
    let crash1_id = SessionId::new("crash-1");
    let rebal_in_id = SessionId::new("rebal-in-1");
    let rebal_out_id = SessionId::new("rebal-out-1");

    coordinator.complete_session(&ae1_id);
    coordinator.complete_session(&ae2_id);
    coordinator.fail_session(&crash1_id);
    coordinator.cancel_session(&rebal_in_id);
    coordinator.complete_session(&rebal_out_id);

    let stats = coordinator.stats();
    assert_eq!(stats.sessions_completed, 3);
    assert_eq!(stats.sessions_failed, 1);
    assert_eq!(stats.sessions_cancelled, 1);
    assert_eq!(stats.active_sessions, 0);
}

#[tokio::test]
async fn test_pause_on_high_write_load() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig {
        pause_on_high_write_load: true,
        pause_threshold_write_rate: 1000,
        ..Default::default()
    };
    let coordinator = RepairCoordinator::new(store, NodeId::new("pause-node"), config);

    assert!(!coordinator.should_pause(500));
    assert!(!coordinator.should_pause(1000));
    assert!(coordinator.should_pause(1001));
    assert!(coordinator.should_pause(5000));

    let disabled_config = RepairCoordinatorConfig {
        pause_on_high_write_load: false,
        pause_threshold_write_rate: 1000,
        ..Default::default()
    };
    let disabled_store = Arc::new(MockObjectStorage::new());
    let disabled_coordinator = RepairCoordinator::new(
        disabled_store,
        NodeId::new("no-pause-node"),
        disabled_config,
    );

    assert!(!disabled_coordinator.should_pause(10_000));
    assert!(!disabled_coordinator.should_pause(100_000));
}

#[tokio::test]
async fn test_session_list_by_status() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig {
        max_concurrent_sessions: 10,
        ..Default::default()
    };
    let coordinator = RepairCoordinator::new(store, NodeId::new("status-node"), config);

    for i in 0..6 {
        let session = RepairSession::builder()
            .id(format!("status-session-{}", i))
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session).unwrap();
    }

    let pending = coordinator.list_sessions_by_status(RepairSessionStatus::Pending);
    assert_eq!(pending.len(), 6);

    let s0_id = SessionId::new("status-session-0");
    let s1_id = SessionId::new("status-session-1");
    let s2_id = SessionId::new("status-session-2");
    let s3_id = SessionId::new("status-session-3");
    let s4_id = SessionId::new("status-session-4");

    coordinator.start_session(&s0_id);
    coordinator.start_session(&s1_id);

    let in_progress = coordinator.list_sessions_by_status(RepairSessionStatus::InProgress);
    assert_eq!(in_progress.len(), 2);

    coordinator.complete_session(&s0_id);
    coordinator.fail_session(&s1_id);
    coordinator.start_session(&s2_id);
    coordinator.cancel_session(&s2_id);

    let completed = coordinator.list_sessions_by_status(RepairSessionStatus::Completed);
    assert_eq!(completed.len(), 1);

    let failed = coordinator.list_sessions_by_status(RepairSessionStatus::Failed);
    assert_eq!(failed.len(), 1);

    let cancelled = coordinator.list_sessions_by_status(RepairSessionStatus::Cancelled);
    assert_eq!(cancelled.len(), 1);

    let pending_remaining = coordinator.list_sessions_by_status(RepairSessionStatus::Pending);
    assert_eq!(pending_remaining.len(), 3);
}

#[tokio::test]
async fn test_session_removal() {
    let store = Arc::new(MockObjectStorage::new());
    let coordinator = RepairCoordinator::new(
        store,
        NodeId::new("remove-node"),
        RepairCoordinatorConfig::default(),
    );

    let session = RepairSession::builder()
        .id("removable-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    let session_id = coordinator.create_session(session).unwrap();
    assert!(coordinator.get_session(&session_id).is_some());

    coordinator.complete_session(&session_id);

    let removed = coordinator.remove_session(&session_id);
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id().as_str(), "removable-session");

    assert!(coordinator.get_session(&session_id).is_none());

    let removed_again = coordinator.remove_session(&session_id);
    assert!(removed_again.is_none());
}

#[tokio::test]
async fn test_session_builder_validation() {
    let result = RepairSession::builder()
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("id is required"));

    let result = RepairSession::builder()
        .id("test-session")
        .ring_version(1)
        .build();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("session_type is required"));

    let result = RepairSession::builder()
        .id("test-session")
        .session_type(RepairType::AntiEntropy)
        .build();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("ring_version is required"));

    let result = RepairSession::builder()
        .id("test-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_repair_type_variants() {
    let anti_entropy = RepairType::AntiEntropy;
    assert_eq!(anti_entropy, RepairType::AntiEntropy);

    let crash_recovery = RepairType::crash_recovery(1000, 2000);
    match crash_recovery {
        RepairType::CrashRecovery {
            downtime_start,
            downtime_end,
        } => {
            assert_eq!(downtime_start, 1000);
            assert_eq!(downtime_end, 2000);
        }
        _ => panic!("Expected CrashRecovery"),
    }

    let rebal_incoming = RepairType::rebalance_incoming();
    match rebal_incoming {
        RepairType::Rebalance { direction } => {
            assert_eq!(direction, RebalanceDirection::Incoming);
        }
        _ => panic!("Expected Rebalance Incoming"),
    }

    let rebal_outgoing = RepairType::rebalance_outgoing();
    match rebal_outgoing {
        RepairType::Rebalance { direction } => {
            assert_eq!(direction, RebalanceDirection::Outgoing);
        }
        _ => panic!("Expected Rebalance Outgoing"),
    }
}

#[tokio::test]
async fn test_session_status_transitions() {
    let mut session = RepairSession::builder()
        .id("transition-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    assert_eq!(session.status(), RepairSessionStatus::Pending);
    assert!(!session.is_active());
    assert!(!session.is_terminal());

    session.start();
    assert_eq!(session.status(), RepairSessionStatus::InProgress);
    assert!(session.is_active());
    assert!(!session.is_terminal());

    session.complete();
    assert_eq!(session.status(), RepairSessionStatus::Completed);
    assert!(!session.is_active());
    assert!(session.is_terminal());
}

#[tokio::test]
async fn test_session_fail_transition() {
    let mut session = RepairSession::builder()
        .id("fail-transition-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    session.start();
    session.fail();
    assert_eq!(session.status(), RepairSessionStatus::Failed);
    assert!(session.is_terminal());
}

#[tokio::test]
async fn test_session_cancel_transition() {
    let mut session = RepairSession::builder()
        .id("cancel-transition-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    session.cancel();
    assert_eq!(session.status(), RepairSessionStatus::Cancelled);
    assert!(session.is_terminal());
}

#[tokio::test]
async fn test_progress_tracking_on_session() {
    let mut session = RepairSession::builder()
        .id("progress-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    session.add_compared(100);
    assert_eq!(session.progress().objects_compared, 100);

    session.add_missing(10);
    assert_eq!(session.progress().objects_missing, 10);

    session.add_transferred(8, 2048);
    assert_eq!(session.progress().objects_transferred, 8);
    assert_eq!(session.progress().bytes_transferred, 2048);

    let missing_rate = session.progress().missing_rate();
    assert!((missing_rate - 0.1).abs() < 0.001);

    let transfer_rate = session.progress().transfer_rate();
    assert!((transfer_rate - 0.8).abs() < 0.001);
}

#[tokio::test]
async fn test_coordinator_acquire_rate_limit() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig {
        max_bandwidth_bytes_per_sec: 1024 * 1024 * 100,
        ..Default::default()
    };
    let coordinator = RepairCoordinator::new(store, NodeId::new("rate-limit-node"), config);

    coordinator.acquire_rate_limit(1024).await;
    coordinator.acquire_rate_limit(2048).await;
}

#[tokio::test]
async fn test_node_id_and_session_id_types() {
    let node_id = NodeId::new("test-node");
    assert_eq!(node_id.as_str(), "test-node");
    assert_eq!(format!("{}", node_id), "test-node");

    let node_id_from_string: NodeId = "another-node".to_string().into();
    assert_eq!(node_id_from_string.as_str(), "another-node");

    let session_id = SessionId::new("test-session");
    assert_eq!(session_id.as_str(), "test-session");
    assert_eq!(format!("{}", session_id), "test-session");

    let session_id_from_string: SessionId = "another-session".to_string().into();
    assert_eq!(session_id_from_string.as_str(), "another-session");
}

#[tokio::test]
async fn test_session_with_multiple_peers() {
    let session = RepairSession::builder()
        .id("multi-peer-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .peer_node("peer-1")
        .peer_node("peer-2")
        .peer_node("peer-3")
        .build()
        .unwrap();

    assert_eq!(session.peer_nodes().len(), 3);
    assert_eq!(session.peer_nodes()[0].as_str(), "peer-1");
    assert_eq!(session.peer_nodes()[1].as_str(), "peer-2");
    assert_eq!(session.peer_nodes()[2].as_str(), "peer-3");
}

#[tokio::test]
async fn test_cleanup_timed_out_sessions() {
    let store = Arc::new(MockObjectStorage::new());
    let config = RepairCoordinatorConfig {
        session_timeout: Duration::from_millis(1),
        ..Default::default()
    };
    let coordinator = RepairCoordinator::new(store, NodeId::new("timeout-node"), config);

    let session = RepairSession::builder()
        .id("timeout-session")
        .session_type(RepairType::AntiEntropy)
        .ring_version(1)
        .build()
        .unwrap();

    let session_id = coordinator.create_session(session).unwrap();
    coordinator.start_session(&session_id);

    let removed = coordinator.cleanup_timed_out_sessions();
    assert_eq!(removed, 0);
}
