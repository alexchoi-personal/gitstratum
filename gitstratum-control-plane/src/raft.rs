use openraft::storage::Adaptor;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder,
    RaftStorage, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::RwLock as ParkingLotRwLock;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use crate::error::ControlPlaneError;
use crate::locks::LockInfo;
use crate::state::{ClusterStateSnapshot, ExtendedNodeInfo, NodeType, RefLockKey};

pub type NodeId = u64;

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

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

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
    data: StateMachineData,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
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

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

pub struct ControlPlaneStore {
    sm_data: ParkingLotRwLock<StateMachineData>,
    snapshot: ParkingLotRwLock<Option<StoredSnapshot>>,
    vote: ParkingLotRwLock<Option<Vote<NodeId>>>,
    committed: ParkingLotRwLock<Option<LogId<NodeId>>>,
    logs: ParkingLotRwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    last_purged: ParkingLotRwLock<Option<LogId<NodeId>>>,
    db: Option<Arc<DB>>,
}

impl ControlPlaneStore {
    pub fn new() -> Self {
        Self {
            sm_data: ParkingLotRwLock::new(StateMachineData::default()),
            snapshot: ParkingLotRwLock::new(None),
            vote: ParkingLotRwLock::new(None),
            committed: ParkingLotRwLock::new(None),
            logs: ParkingLotRwLock::new(BTreeMap::new()),
            last_purged: ParkingLotRwLock::new(None),
            db: None,
        }
    }

    pub fn with_db<P: AsRef<Path>>(path: P) -> Result<Self, ControlPlaneError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = DB::open(&opts, path)?;

        let vote = db
            .get(b"vote")?
            .map(|v| serde_json::from_slice(&v))
            .transpose()
            .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;

        let committed = db
            .get(b"committed")?
            .map(|v| serde_json::from_slice(&v))
            .transpose()
            .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;

        let mut logs = BTreeMap::new();
        let prefix = b"log:";
        let iter = db.prefix_iterator(prefix);
        for item in iter {
            let (key, value) = item?;
            if key.starts_with(prefix) {
                let entry: Entry<TypeConfig> = serde_json::from_slice(&value)
                    .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;
                logs.insert(entry.log_id.index, entry);
            }
        }

        Ok(Self {
            sm_data: ParkingLotRwLock::new(StateMachineData::default()),
            snapshot: ParkingLotRwLock::new(None),
            vote: ParkingLotRwLock::new(vote),
            committed: ParkingLotRwLock::new(committed),
            logs: ParkingLotRwLock::new(logs),
            last_purged: ParkingLotRwLock::new(None),
            db: Some(Arc::new(db)),
        })
    }

    pub fn get_state(&self) -> ClusterStateSnapshot {
        self.sm_data.read().state.clone()
    }

    fn log_key(index: u64) -> Vec<u8> {
        let mut key = b"log:".to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
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
}

impl Default for ControlPlaneStore {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LogReader {
    logs: BTreeMap<u64, Entry<TypeConfig>>,
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let entries: Vec<_> = self.logs.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogReader<TypeConfig> for ControlPlaneStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let logs = self.logs.read();
        let entries: Vec<_> = logs.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

impl RaftStorage<TypeConfig> for ControlPlaneStore {
    type LogReader = LogReader;
    type SnapshotBuilder = StateMachineSnapshotBuilder;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        if let Some(db) = &self.db {
            let data = serde_json::to_vec(vote).map_err(|e| StorageIOError::write_vote(&e))?;
            db.put(b"vote", data)
                .map_err(|e| StorageIOError::write_vote(&e))?;
        }
        *self.vote.write() = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(c) = committed {
            if let Some(db) = &self.db {
                let data = serde_json::to_vec(&c).map_err(|e| StorageIOError::write_logs(&e))?;
                db.put(b"committed", data)
                    .map_err(|e| StorageIOError::write_logs(&e))?;
            }
        }
        *self.committed.write() = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(*self.committed.read())
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let logs = self.logs.read();
        let last_purged = *self.last_purged.read();
        let last_log_id = logs.values().last().map(|e| e.log_id).or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            logs: self.logs.read().clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut logs = self.logs.write();

        if let Some(db) = &self.db {
            let mut batch = rocksdb::WriteBatch::default();
            for entry in entries {
                let key = Self::log_key(entry.log_id.index);
                let value =
                    serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?;
                batch.put(&key, &value);
                logs.insert(entry.log_id.index, entry);
            }
            db.write(batch).map_err(|e| StorageIOError::write_logs(&e))?;
        } else {
            for entry in entries {
                logs.insert(entry.log_id.index, entry);
            }
        }

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut logs = self.logs.write();
        let keys_to_remove: Vec<u64> = logs.range(log_id.index..).map(|(k, _)| *k).collect();

        if let Some(db) = &self.db {
            let mut batch = rocksdb::WriteBatch::default();
            for index in &keys_to_remove {
                batch.delete(Self::log_key(*index));
            }
            db.write(batch).map_err(|e| StorageIOError::write_logs(&e))?;
        }

        for index in keys_to_remove {
            logs.remove(&index);
        }

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut logs = self.logs.write();
        let keys_to_remove: Vec<u64> = logs.range(..=log_id.index).map(|(k, _)| *k).collect();

        if let Some(db) = &self.db {
            let mut batch = rocksdb::WriteBatch::default();
            for index in &keys_to_remove {
                batch.delete(Self::log_key(*index));
            }
            db.write(batch).map_err(|e| StorageIOError::write_logs(&e))?;
        }

        for index in keys_to_remove {
            logs.remove(&index);
        }

        *self.last_purged.write() = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>),
        StorageError<NodeId>,
    > {
        let data = self.sm_data.read();
        Ok((data.last_applied, data.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut data = self.sm_data.write();
        let mut responses = Vec::new();

        for entry in entries {
            data.last_applied = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(Response::Success);
                }
                EntryPayload::Normal(request) => {
                    let response = Self::apply_request(&mut data.state, request);
                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    data.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    responses.push(Response::Success);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let data = self.sm_data.read();
        StateMachineSnapshotBuilder {
            data: StateMachineData {
                last_applied: data.last_applied,
                last_membership: data.last_membership.clone(),
                state: data.state.clone(),
            },
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data_vec = snapshot.into_inner();
        let state: ClusterStateSnapshot = serde_json::from_slice(&data_vec)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let mut sm_data = self.sm_data.write();
        sm_data.last_applied = meta.last_log_id;
        sm_data.last_membership = meta.last_membership.clone();
        sm_data.state = state;

        let mut current_snapshot = self.snapshot.write();
        *current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data_vec,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.snapshot.read();
        match &*snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

pub type StateMachineStore = Adaptor<TypeConfig, ControlPlaneStore>;
pub type LogStore = Adaptor<TypeConfig, ControlPlaneStore>;

pub fn create_stores(store: ControlPlaneStore) -> (LogStore, StateMachineStore) {
    Adaptor::new(store)
}

pub type ControlPlaneRaft = openraft::Raft<TypeConfig>;
