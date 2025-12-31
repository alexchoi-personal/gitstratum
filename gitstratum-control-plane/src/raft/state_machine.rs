use openraft::storage::Adaptor;
use openraft::{BasicNode, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
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
