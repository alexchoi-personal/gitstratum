#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, info};

use crate::error::{ObjectStoreError, Result};
use crate::repair::constants::{
    DEFAULT_MAX_CONCURRENT_SESSIONS, DEFAULT_PAUSE_THRESHOLD_WRITE_RATE,
    DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC, DEFAULT_SESSION_TIMEOUT,
};
use crate::repair::types::{NodeId, SessionId};
use crate::repair::{RepairRateLimiter, RepairSession, RepairSessionStatus, RepairType};
use crate::store::ObjectStorage;

#[derive(Debug, Clone)]
pub struct RepairCoordinatorConfig {
    pub max_concurrent_sessions: usize,
    pub max_bandwidth_bytes_per_sec: u64,
    pub pause_on_high_write_load: bool,
    pub pause_threshold_write_rate: u64,
    pub session_timeout: Duration,
}

impl Default for RepairCoordinatorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_sessions: DEFAULT_MAX_CONCURRENT_SESSIONS,
            max_bandwidth_bytes_per_sec: DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC,
            pause_on_high_write_load: true,
            pause_threshold_write_rate: DEFAULT_PAUSE_THRESHOLD_WRITE_RATE,
            session_timeout: DEFAULT_SESSION_TIMEOUT,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RepairStats {
    pub sessions_created: u64,
    pub sessions_completed: u64,
    pub sessions_failed: u64,
    pub sessions_cancelled: u64,
    pub objects_repaired: u64,
    pub bytes_transferred: u64,
    pub active_sessions: u64,
}

pub struct RepairCoordinator<S: ObjectStorage> {
    store: Arc<S>,
    config: RepairCoordinatorConfig,
    local_node_id: NodeId,

    sessions: DashMap<SessionId, RepairSession>,

    rate_limiter: RwLock<RepairRateLimiter>,

    stats: RwLock<RepairStats>,
}

impl<S: ObjectStorage + Send + Sync + 'static> RepairCoordinator<S> {
    pub fn new(
        store: Arc<S>,
        local_node_id: impl Into<NodeId>,
        config: RepairCoordinatorConfig,
    ) -> Self {
        let rate_limiter = RwLock::new(RepairRateLimiter::new(config.max_bandwidth_bytes_per_sec));

        Self {
            store,
            config,
            local_node_id: local_node_id.into(),
            sessions: DashMap::new(),
            rate_limiter,
            stats: RwLock::new(RepairStats::default()),
        }
    }

    pub fn config(&self) -> &RepairCoordinatorConfig {
        &self.config
    }

    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub fn create_session(&self, session: RepairSession) -> Result<SessionId> {
        let active_count = self.active_session_count();
        if active_count >= self.config.max_concurrent_sessions {
            return Err(ObjectStoreError::Internal(
                "max concurrent sessions reached".to_string(),
            ));
        }

        let session_id = session.id().clone();
        info!(
            session_id = %session_id,
            session_type = ?session.session_type(),
            "Creating repair session"
        );

        self.sessions.insert(session_id.clone(), session);

        {
            let mut stats = self.stats.write();
            stats.sessions_created += 1;
            stats.active_sessions = self.active_session_count() as u64;
        }

        Ok(session_id)
    }

    pub fn get_session(&self, session_id: &SessionId) -> Option<RepairSession> {
        self.sessions.get(session_id).map(|r| r.clone())
    }

    pub fn update_session_status(
        &self,
        session_id: &SessionId,
        status: RepairSessionStatus,
    ) -> bool {
        let updated = {
            if let Some(mut session) = self.sessions.get_mut(session_id) {
                debug!(
                    session_id = %session_id,
                    old_status = ?session.status(),
                    new_status = ?status,
                    "Updating session status"
                );
                session.set_status(status);
                true
            } else {
                false
            }
        };

        if updated && status.is_terminal() {
            let mut stats = self.stats.write();
            match status {
                RepairSessionStatus::Completed => stats.sessions_completed += 1,
                RepairSessionStatus::Failed => stats.sessions_failed += 1,
                RepairSessionStatus::Cancelled => stats.sessions_cancelled += 1,
                _ => {}
            }
            stats.active_sessions = self.active_session_count() as u64;
        }

        updated
    }

    pub fn start_session(&self, session_id: &SessionId) -> bool {
        self.update_session_status(session_id, RepairSessionStatus::InProgress)
    }

    pub fn complete_session(&self, session_id: &SessionId) -> bool {
        self.update_session_status(session_id, RepairSessionStatus::Completed)
    }

    pub fn fail_session(&self, session_id: &SessionId) -> bool {
        self.update_session_status(session_id, RepairSessionStatus::Failed)
    }

    pub fn cancel_session(&self, session_id: &SessionId) -> bool {
        self.update_session_status(session_id, RepairSessionStatus::Cancelled)
    }

    pub fn remove_session(&self, session_id: &SessionId) -> Option<RepairSession> {
        self.sessions.remove(session_id).map(|(_, s)| s)
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions
            .iter()
            .filter(|s| s.status().is_active() || s.status() == RepairSessionStatus::Pending)
            .count()
    }

    pub fn list_sessions(&self) -> Vec<RepairSession> {
        self.sessions.iter().map(|r| r.clone()).collect()
    }

    pub fn list_sessions_by_status(&self, status: RepairSessionStatus) -> Vec<RepairSession> {
        self.sessions
            .iter()
            .filter(|s| s.status() == status)
            .map(|r| r.clone())
            .collect()
    }

    pub fn list_sessions_by_type(&self, repair_type: RepairType) -> Vec<RepairSession> {
        self.sessions
            .iter()
            .filter(|s| {
                std::mem::discriminant(&s.session_type()) == std::mem::discriminant(&repair_type)
            })
            .map(|r| r.clone())
            .collect()
    }

    pub fn record_objects_repaired(&self, count: u64, bytes: u64) {
        let mut stats = self.stats.write();
        stats.objects_repaired += count;
        stats.bytes_transferred += bytes;
    }

    pub fn update_progress(
        &self,
        session_id: &SessionId,
        compared: u64,
        missing: u64,
        transferred: u64,
        bytes: u64,
    ) {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.add_compared(compared);
            session.add_missing(missing);
            session.add_transferred(transferred, bytes);
        }
    }

    pub fn stats(&self) -> RepairStats {
        self.stats.read().clone()
    }

    pub fn should_pause(&self, current_write_rate: u64) -> bool {
        self.config.pause_on_high_write_load
            && current_write_rate > self.config.pause_threshold_write_rate
    }

    pub async fn acquire_rate_limit(&self, bytes: u64) {
        let sleep_duration = self.rate_limiter.write().acquire(bytes);
        if let Some(duration) = sleep_duration {
            tokio::time::sleep(duration).await;
        }
    }

    pub fn total_sessions_created(&self) -> u64 {
        self.stats.read().sessions_created
    }

    pub fn total_objects_repaired(&self) -> u64 {
        self.stats.read().objects_repaired
    }

    pub fn total_bytes_transferred(&self) -> u64 {
        self.stats.read().bytes_transferred
    }

    pub fn cleanup_timed_out_sessions(&self) -> usize {
        let now = crate::util::time::current_timestamp_millis();

        let timeout_ms = self.config.session_timeout.as_millis() as u64;
        let mut removed = 0;

        let to_remove: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|s| {
                if let Some(checkpoint) = s.progress().last_checkpoint.as_ref() {
                    now.saturating_sub(checkpoint.timestamp) > timeout_ms
                } else {
                    false
                }
            })
            .map(|s| s.id().clone())
            .collect();

        for session_id in to_remove {
            info!(session_id = %session_id, "Cleaning up timed out session");
            self.fail_session(&session_id);
            removed += 1;
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repair::{PositionRange, RepairCheckpoint};
    use crate::testutil::MockObjectStorage;
    use gitstratum_core::Oid;

    fn create_test_coordinator() -> RepairCoordinator<MockObjectStorage> {
        let store = Arc::new(MockObjectStorage::new());
        RepairCoordinator::new(
            store,
            "node-1".to_string(),
            RepairCoordinatorConfig::default(),
        )
    }

    fn create_test_session(id: &str) -> RepairSession {
        RepairSession::builder()
            .id(id)
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap()
    }

    fn sid(id: &str) -> SessionId {
        SessionId::new(id)
    }

    #[test]
    fn test_repair_coordinator_config_default() {
        let config = RepairCoordinatorConfig::default();
        assert_eq!(config.max_concurrent_sessions, 5);
        assert_eq!(config.max_bandwidth_bytes_per_sec, 100 * 1024 * 1024);
        assert!(config.pause_on_high_write_load);
        assert_eq!(config.pause_threshold_write_rate, 10_000);
        assert_eq!(config.session_timeout, Duration::from_secs(3600));
    }

    #[test]
    fn test_repair_coordinator_config_custom() {
        let config = RepairCoordinatorConfig {
            max_concurrent_sessions: 10,
            max_bandwidth_bytes_per_sec: 50 * 1024 * 1024,
            pause_on_high_write_load: false,
            pause_threshold_write_rate: 5000,
            session_timeout: Duration::from_secs(1800),
        };
        assert_eq!(config.max_concurrent_sessions, 10);
        assert_eq!(config.max_bandwidth_bytes_per_sec, 50 * 1024 * 1024);
        assert!(!config.pause_on_high_write_load);
        assert_eq!(config.pause_threshold_write_rate, 5000);
        assert_eq!(config.session_timeout, Duration::from_secs(1800));
    }

    #[test]
    fn test_repair_coordinator_config_clone() {
        let config = RepairCoordinatorConfig::default();
        let cloned = config.clone();
        assert_eq!(
            config.max_concurrent_sessions,
            cloned.max_concurrent_sessions
        );
        assert_eq!(
            config.max_bandwidth_bytes_per_sec,
            cloned.max_bandwidth_bytes_per_sec
        );
    }

    #[test]
    fn test_repair_coordinator_config_debug() {
        let config = RepairCoordinatorConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RepairCoordinatorConfig"));
        assert!(debug.contains("max_concurrent_sessions"));
    }

    #[test]
    fn test_repair_stats_default() {
        let stats = RepairStats::default();
        assert_eq!(stats.sessions_created, 0);
        assert_eq!(stats.sessions_completed, 0);
        assert_eq!(stats.sessions_failed, 0);
        assert_eq!(stats.sessions_cancelled, 0);
        assert_eq!(stats.objects_repaired, 0);
        assert_eq!(stats.bytes_transferred, 0);
        assert_eq!(stats.active_sessions, 0);
    }

    #[test]
    fn test_repair_stats_clone() {
        let stats = RepairStats {
            sessions_created: 10,
            sessions_completed: 5,
            ..RepairStats::default()
        };
        let cloned = stats.clone();
        assert_eq!(stats.sessions_created, cloned.sessions_created);
        assert_eq!(stats.sessions_completed, cloned.sessions_completed);
    }

    #[test]
    fn test_repair_stats_debug() {
        let stats = RepairStats::default();
        let debug = format!("{:?}", stats);
        assert!(debug.contains("RepairStats"));
        assert!(debug.contains("sessions_created"));
    }

    #[test]
    fn test_coordinator_new() {
        let coordinator = create_test_coordinator();
        assert_eq!(coordinator.local_node_id().as_str(), "node-1");
        assert_eq!(coordinator.config().max_concurrent_sessions, 5);
    }

    #[test]
    fn test_coordinator_config() {
        let store = Arc::new(MockObjectStorage::new());
        let config = RepairCoordinatorConfig {
            max_concurrent_sessions: 3,
            ..Default::default()
        };
        let coordinator = RepairCoordinator::new(store, "node-1".to_string(), config);
        assert_eq!(coordinator.config().max_concurrent_sessions, 3);
    }

    #[test]
    fn test_coordinator_local_node_id() {
        let store = Arc::new(MockObjectStorage::new());
        let coordinator = RepairCoordinator::new(
            store,
            "test-node-xyz".to_string(),
            RepairCoordinatorConfig::default(),
        );
        assert_eq!(coordinator.local_node_id().as_str(), "test-node-xyz");
    }

    #[test]
    fn test_coordinator_store() {
        let coordinator = create_test_coordinator();
        let stats = coordinator.store().stats();
        assert_eq!(stats.total_blobs, 0);
    }

    #[test]
    fn test_create_session_success() {
        let coordinator = create_test_coordinator();
        let session = create_test_session("session-1");
        let result = coordinator.create_session(session);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), "session-1");
    }

    #[test]
    fn test_create_session_updates_stats() {
        let coordinator = create_test_coordinator();
        let session = create_test_session("session-1");
        coordinator.create_session(session).unwrap();
        let stats = coordinator.stats();
        assert_eq!(stats.sessions_created, 1);
        assert_eq!(stats.active_sessions, 1);
    }

    #[test]
    fn test_create_session_increments_counter() {
        let coordinator = create_test_coordinator();
        let session = create_test_session("session-1");
        coordinator.create_session(session).unwrap();
        assert_eq!(coordinator.total_sessions_created(), 1);
    }

    #[test]
    fn test_create_session_max_concurrent_reached() {
        let store = Arc::new(MockObjectStorage::new());
        let config = RepairCoordinatorConfig {
            max_concurrent_sessions: 2,
            ..Default::default()
        };
        let coordinator = RepairCoordinator::new(store, "node-1".to_string(), config);

        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();

        let result = coordinator.create_session(create_test_session("session-3"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max concurrent sessions"));
    }

    #[test]
    fn test_get_session_exists() {
        let coordinator = create_test_coordinator();
        let session = create_test_session("session-1");
        coordinator.create_session(session).unwrap();

        let retrieved = coordinator.get_session(&sid("session-1"));
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id().as_str(), "session-1");
    }

    #[test]
    fn test_get_session_not_exists() {
        let coordinator = create_test_coordinator();
        let retrieved = coordinator.get_session(&sid("nonexistent"));
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_update_session_status_success() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let result =
            coordinator.update_session_status(&sid("session-1"), RepairSessionStatus::InProgress);
        assert!(result);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.status(), RepairSessionStatus::InProgress);
    }

    #[test]
    fn test_update_session_status_not_exists() {
        let coordinator = create_test_coordinator();
        let result =
            coordinator.update_session_status(&sid("nonexistent"), RepairSessionStatus::InProgress);
        assert!(!result);
    }

    #[test]
    fn test_start_session() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let result = coordinator.start_session(&sid("session-1"));
        assert!(result);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.status(), RepairSessionStatus::InProgress);
    }

    #[test]
    fn test_complete_session() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator.start_session(&sid("session-1"));

        let result = coordinator.complete_session(&sid("session-1"));
        assert!(result);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.status(), RepairSessionStatus::Completed);
    }

    #[test]
    fn test_complete_session_updates_stats() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator.start_session(&sid("session-1"));
        coordinator.complete_session(&sid("session-1"));

        let stats = coordinator.stats();
        assert_eq!(stats.sessions_completed, 1);
    }

    #[test]
    fn test_fail_session() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator.start_session(&sid("session-1"));

        let result = coordinator.fail_session(&sid("session-1"));
        assert!(result);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.status(), RepairSessionStatus::Failed);
    }

    #[test]
    fn test_fail_session_updates_stats() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator.start_session(&sid("session-1"));
        coordinator.fail_session(&sid("session-1"));

        let stats = coordinator.stats();
        assert_eq!(stats.sessions_failed, 1);
    }

    #[test]
    fn test_cancel_session() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let result = coordinator.cancel_session(&sid("session-1"));
        assert!(result);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.status(), RepairSessionStatus::Cancelled);
    }

    #[test]
    fn test_cancel_session_updates_stats() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator.cancel_session(&sid("session-1"));

        let stats = coordinator.stats();
        assert_eq!(stats.sessions_cancelled, 1);
    }

    #[test]
    fn test_remove_session_exists() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let removed = coordinator.remove_session(&sid("session-1"));
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id().as_str(), "session-1");

        let retrieved = coordinator.get_session(&sid("session-1"));
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_remove_session_not_exists() {
        let coordinator = create_test_coordinator();
        let removed = coordinator.remove_session(&sid("nonexistent"));
        assert!(removed.is_none());
    }

    #[test]
    fn test_active_session_count_pending() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();

        assert_eq!(coordinator.active_session_count(), 2);
    }

    #[test]
    fn test_active_session_count_in_progress() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator.start_session(&sid("session-1"));

        assert_eq!(coordinator.active_session_count(), 1);
    }

    #[test]
    fn test_active_session_count_excludes_completed() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator.complete_session(&sid("session-1"));

        assert_eq!(coordinator.active_session_count(), 1);
    }

    #[test]
    fn test_active_session_count_excludes_failed() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator.fail_session(&sid("session-1"));

        assert_eq!(coordinator.active_session_count(), 1);
    }

    #[test]
    fn test_active_session_count_excludes_cancelled() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator.cancel_session(&sid("session-1"));

        assert_eq!(coordinator.active_session_count(), 1);
    }

    #[test]
    fn test_list_sessions_empty() {
        let coordinator = create_test_coordinator();
        let sessions = coordinator.list_sessions();
        assert!(sessions.is_empty());
    }

    #[test]
    fn test_list_sessions_multiple() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-3"))
            .unwrap();

        let sessions = coordinator.list_sessions();
        assert_eq!(sessions.len(), 3);
    }

    #[test]
    fn test_list_sessions_by_status_pending() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator.start_session(&sid("session-2"));

        let pending = coordinator.list_sessions_by_status(RepairSessionStatus::Pending);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id().as_str(), "session-1");
    }

    #[test]
    fn test_list_sessions_by_status_in_progress() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator.start_session(&sid("session-1"));

        let in_progress = coordinator.list_sessions_by_status(RepairSessionStatus::InProgress);
        assert_eq!(in_progress.len(), 1);
        assert_eq!(in_progress[0].id().as_str(), "session-1");
    }

    #[test]
    fn test_list_sessions_by_status_completed() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator.complete_session(&sid("session-1"));
        coordinator.complete_session(&sid("session-2"));

        let completed = coordinator.list_sessions_by_status(RepairSessionStatus::Completed);
        assert_eq!(completed.len(), 2);
    }

    #[test]
    fn test_list_sessions_by_type_anti_entropy() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let session2 = RepairSession::builder()
            .id("session-2")
            .session_type(RepairType::crash_recovery(1000, 2000))
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session2).unwrap();

        let anti_entropy = coordinator.list_sessions_by_type(RepairType::AntiEntropy);
        assert_eq!(anti_entropy.len(), 1);
        assert_eq!(anti_entropy[0].id().as_str(), "session-1");
    }

    #[test]
    fn test_list_sessions_by_type_crash_recovery() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let session2 = RepairSession::builder()
            .id("session-2")
            .session_type(RepairType::crash_recovery(1000, 2000))
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session2).unwrap();

        let crash_recovery = coordinator.list_sessions_by_type(RepairType::crash_recovery(0, 0));
        assert_eq!(crash_recovery.len(), 1);
        assert_eq!(crash_recovery[0].id().as_str(), "session-2");
    }

    #[test]
    fn test_list_sessions_by_type_rebalance() {
        let coordinator = create_test_coordinator();

        let session1 = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::rebalance_incoming())
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session1).unwrap();

        let session2 = RepairSession::builder()
            .id("session-2")
            .session_type(RepairType::rebalance_outgoing())
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session2).unwrap();

        let rebalance = coordinator.list_sessions_by_type(RepairType::rebalance_incoming());
        assert_eq!(rebalance.len(), 2);
    }

    #[test]
    fn test_record_objects_repaired() {
        let coordinator = create_test_coordinator();
        coordinator.record_objects_repaired(10, 1024);

        assert_eq!(coordinator.total_objects_repaired(), 10);
        assert_eq!(coordinator.total_bytes_transferred(), 1024);
    }

    #[test]
    fn test_record_objects_repaired_multiple() {
        let coordinator = create_test_coordinator();
        coordinator.record_objects_repaired(10, 1024);
        coordinator.record_objects_repaired(5, 512);

        assert_eq!(coordinator.total_objects_repaired(), 15);
        assert_eq!(coordinator.total_bytes_transferred(), 1536);
    }

    #[test]
    fn test_record_objects_repaired_updates_stats() {
        let coordinator = create_test_coordinator();
        coordinator.record_objects_repaired(10, 1024);

        let stats = coordinator.stats();
        assert_eq!(stats.objects_repaired, 10);
        assert_eq!(stats.bytes_transferred, 1024);
    }

    #[test]
    fn test_update_progress() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        coordinator.update_progress(&sid("session-1"), 100, 10, 5, 512);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.progress().objects_compared, 100);
        assert_eq!(session.progress().objects_missing, 10);
        assert_eq!(session.progress().objects_transferred, 5);
        assert_eq!(session.progress().bytes_transferred, 512);
    }

    #[test]
    fn test_update_progress_nonexistent_session() {
        let coordinator = create_test_coordinator();
        coordinator.update_progress(&sid("nonexistent"), 100, 10, 5, 512);
    }

    #[test]
    fn test_update_progress_cumulative() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        coordinator.update_progress(&sid("session-1"), 100, 10, 5, 512);
        coordinator.update_progress(&sid("session-1"), 50, 5, 3, 256);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.progress().objects_compared, 150);
        assert_eq!(session.progress().objects_missing, 15);
        assert_eq!(session.progress().objects_transferred, 8);
        assert_eq!(session.progress().bytes_transferred, 768);
    }

    #[test]
    fn test_should_pause_below_threshold() {
        let coordinator = create_test_coordinator();
        assert!(!coordinator.should_pause(5000));
    }

    #[test]
    fn test_should_pause_above_threshold() {
        let coordinator = create_test_coordinator();
        assert!(coordinator.should_pause(15000));
    }

    #[test]
    fn test_should_pause_at_threshold() {
        let coordinator = create_test_coordinator();
        assert!(!coordinator.should_pause(10_000));
    }

    #[test]
    fn test_should_pause_disabled() {
        let store = Arc::new(MockObjectStorage::new());
        let config = RepairCoordinatorConfig {
            pause_on_high_write_load: false,
            ..Default::default()
        };
        let coordinator = RepairCoordinator::new(store, "node-1".to_string(), config);
        assert!(!coordinator.should_pause(100_000));
    }

    #[tokio::test]
    async fn test_acquire_rate_limit() {
        let coordinator = create_test_coordinator();
        coordinator.acquire_rate_limit(1024).await;
    }

    #[test]
    fn test_total_sessions_created_initial() {
        let coordinator = create_test_coordinator();
        assert_eq!(coordinator.total_sessions_created(), 0);
    }

    #[test]
    fn test_total_objects_repaired_initial() {
        let coordinator = create_test_coordinator();
        assert_eq!(coordinator.total_objects_repaired(), 0);
    }

    #[test]
    fn test_total_bytes_transferred_initial() {
        let coordinator = create_test_coordinator();
        assert_eq!(coordinator.total_bytes_transferred(), 0);
    }

    #[test]
    fn test_cleanup_timed_out_sessions_none() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        let removed = coordinator.cleanup_timed_out_sessions();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_cleanup_timed_out_sessions_with_checkpoint() {
        let store = Arc::new(MockObjectStorage::new());
        let config = RepairCoordinatorConfig {
            session_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let coordinator = RepairCoordinator::new(store, "node-1".to_string(), config);

        let mut session = create_test_session("session-1");
        let old_timestamp = 1;
        let oid = Oid::hash(b"test");
        session.set_checkpoint(RepairCheckpoint::new(0, oid, old_timestamp));

        coordinator.create_session(session).unwrap();
        coordinator.start_session(&sid("session-1"));

        std::thread::sleep(Duration::from_millis(10));

        let removed = coordinator.cleanup_timed_out_sessions();
        assert_eq!(removed, 1);

        let session = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(session.status(), RepairSessionStatus::Failed);
    }

    #[test]
    fn test_cleanup_timed_out_sessions_no_checkpoint() {
        let store = Arc::new(MockObjectStorage::new());
        let config = RepairCoordinatorConfig {
            session_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let coordinator = RepairCoordinator::new(store, "node-1".to_string(), config);

        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let removed = coordinator.cleanup_timed_out_sessions();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_session_lifecycle_complete() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        assert_eq!(
            coordinator.get_session(&sid("session-1")).unwrap().status(),
            RepairSessionStatus::Pending
        );

        coordinator.start_session(&sid("session-1"));
        assert_eq!(
            coordinator.get_session(&sid("session-1")).unwrap().status(),
            RepairSessionStatus::InProgress
        );

        coordinator.update_progress(&sid("session-1"), 100, 10, 10, 1024);

        coordinator.complete_session(&sid("session-1"));
        assert_eq!(
            coordinator.get_session(&sid("session-1")).unwrap().status(),
            RepairSessionStatus::Completed
        );

        let removed = coordinator.remove_session(&sid("session-1"));
        assert!(removed.is_some());
        assert!(coordinator.get_session(&sid("session-1")).is_none());
    }

    #[test]
    fn test_session_lifecycle_fail() {
        let coordinator = create_test_coordinator();
        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();

        coordinator.start_session(&sid("session-1"));
        coordinator.fail_session(&sid("session-1"));

        let stats = coordinator.stats();
        assert_eq!(stats.sessions_failed, 1);
    }

    #[test]
    fn test_concurrent_sessions_tracking() {
        let coordinator = create_test_coordinator();

        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-3"))
            .unwrap();

        assert_eq!(coordinator.active_session_count(), 3);

        coordinator.complete_session(&sid("session-1"));
        assert_eq!(coordinator.active_session_count(), 2);

        coordinator.fail_session(&sid("session-2"));
        assert_eq!(coordinator.active_session_count(), 1);

        coordinator.cancel_session(&sid("session-3"));
        assert_eq!(coordinator.active_session_count(), 0);
    }

    #[test]
    fn test_stats_consistency() {
        let coordinator = create_test_coordinator();

        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-3"))
            .unwrap();

        coordinator.complete_session(&sid("session-1"));
        coordinator.fail_session(&sid("session-2"));
        coordinator.cancel_session(&sid("session-3"));

        let stats = coordinator.stats();
        assert_eq!(stats.sessions_created, 3);
        assert_eq!(stats.sessions_completed, 1);
        assert_eq!(stats.sessions_failed, 1);
        assert_eq!(stats.sessions_cancelled, 1);
        assert_eq!(stats.active_sessions, 0);
    }

    #[test]
    fn test_max_concurrent_sessions_after_completion() {
        let store = Arc::new(MockObjectStorage::new());
        let config = RepairCoordinatorConfig {
            max_concurrent_sessions: 2,
            ..Default::default()
        };
        let coordinator = RepairCoordinator::new(store, "node-1".to_string(), config);

        coordinator
            .create_session(create_test_session("session-1"))
            .unwrap();
        coordinator
            .create_session(create_test_session("session-2"))
            .unwrap();

        let result = coordinator.create_session(create_test_session("session-3"));
        assert!(result.is_err());

        coordinator.complete_session(&sid("session-1"));

        let result = coordinator.create_session(create_test_session("session-3"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_session_types() {
        let coordinator = create_test_coordinator();

        let session1 = RepairSession::builder()
            .id("anti-entropy-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session1).unwrap();

        let session2 = RepairSession::builder()
            .id("crash-recovery-1")
            .session_type(RepairType::crash_recovery(1000, 2000))
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session2).unwrap();

        let session3 = RepairSession::builder()
            .id("rebalance-1")
            .session_type(RepairType::rebalance_incoming())
            .ring_version(1)
            .build()
            .unwrap();
        coordinator.create_session(session3).unwrap();

        assert_eq!(coordinator.list_sessions().len(), 3);
        assert_eq!(
            coordinator
                .list_sessions_by_type(RepairType::AntiEntropy)
                .len(),
            1
        );
        assert_eq!(
            coordinator
                .list_sessions_by_type(RepairType::crash_recovery(0, 0))
                .len(),
            1
        );
        assert_eq!(
            coordinator
                .list_sessions_by_type(RepairType::rebalance_incoming())
                .len(),
            1
        );
    }

    #[test]
    fn test_session_with_ranges() {
        let coordinator = create_test_coordinator();

        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .range(PositionRange::new(0, 1000))
            .range(PositionRange::new(1000, 2000))
            .build()
            .unwrap();
        coordinator.create_session(session).unwrap();

        let retrieved = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(retrieved.ranges().len(), 2);
    }

    #[test]
    fn test_session_with_peers() {
        let coordinator = create_test_coordinator();

        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .peer_node("peer-1")
            .peer_node("peer-2")
            .build()
            .unwrap();
        coordinator.create_session(session).unwrap();

        let retrieved = coordinator.get_session(&sid("session-1")).unwrap();
        assert_eq!(retrieved.peer_nodes().len(), 2);
    }
}
