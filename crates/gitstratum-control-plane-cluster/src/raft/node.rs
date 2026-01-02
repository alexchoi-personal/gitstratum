use openraft::storage::Adaptor;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftStorage, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::RwLock as ParkingLotRwLock;
use rocksdb::{Options, DB};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use crate::error::ControlPlaneError;
use crate::membership::ClusterStateSnapshot;
use crate::raft::log::LogReader;
use crate::raft::state_machine::{
    apply_request, Response, StateMachineData, StateMachineSnapshotBuilder, StoredSnapshot,
};

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = super::state_machine::Request,
        R = super::state_machine::Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

pub struct ControlPlaneStore {
    pub(crate) sm_data: ParkingLotRwLock<StateMachineData>,
    pub(crate) snapshot: ParkingLotRwLock<Option<StoredSnapshot>>,
    pub(crate) vote: ParkingLotRwLock<Option<Vote<NodeId>>>,
    pub(crate) committed: ParkingLotRwLock<Option<LogId<NodeId>>>,
    pub(crate) logs: ParkingLotRwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    pub(crate) last_purged: ParkingLotRwLock<Option<LogId<NodeId>>>,
    pub(crate) db: Option<Arc<DB>>,
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

    pub(crate) fn log_key(index: u64) -> Vec<u8> {
        let mut key = b"log:".to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }
}

impl Default for ControlPlaneStore {
    fn default() -> Self {
        Self::new()
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
            db.write(batch)
                .map_err(|e| StorageIOError::write_logs(&e))?;
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
            db.write(batch)
                .map_err(|e| StorageIOError::write_logs(&e))?;
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
            db.write(batch)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }

        for index in keys_to_remove {
            logs.remove(&index);
        }

        *self.last_purged.write() = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
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
                    let response = apply_request(&mut data.state, request);
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

pub type LogStore = Adaptor<TypeConfig, ControlPlaneStore>;

pub fn create_stores(
    store: ControlPlaneStore,
) -> (LogStore, super::state_machine::StateMachineStore) {
    Adaptor::new(store)
}

pub type ControlPlaneRaft = openraft::Raft<TypeConfig>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{ExtendedNodeInfo, NodeType, RefLockKey};
    use crate::raft::state_machine::Request;
    use crate::raft::LockInfo;
    use openraft::{LeaderId, LogId, Membership};
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tempfile::tempdir;

    fn create_test_entry(index: u64, payload: EntryPayload<TypeConfig>) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: LeaderId::new(1, 1),
                index,
            },
            payload,
        }
    }

    fn create_blank_entry(index: u64) -> Entry<TypeConfig> {
        create_test_entry(index, EntryPayload::Blank)
    }

    fn create_normal_entry(index: u64, request: Request) -> Entry<TypeConfig> {
        create_test_entry(index, EntryPayload::Normal(request))
    }

    fn create_membership_entry(index: u64) -> Entry<TypeConfig> {
        let mut members = BTreeSet::new();
        members.insert(1u64);
        let membership = Membership::new(vec![members], None);
        create_test_entry(index, EntryPayload::Membership(membership))
    }

    #[test]
    fn test_control_plane_store_new() {
        let store = ControlPlaneStore::new();
        assert!(store.vote.read().is_none());
        assert!(store.committed.read().is_none());
        assert!(store.logs.read().is_empty());
        assert!(store.last_purged.read().is_none());
        assert!(store.snapshot.read().is_none());
        assert!(store.db.is_none());
    }

    #[test]
    fn test_control_plane_store_default() {
        let store = ControlPlaneStore::default();
        assert!(store.vote.read().is_none());
        assert!(store.committed.read().is_none());
        assert!(store.logs.read().is_empty());
    }

    #[test]
    fn test_control_plane_store_with_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_raft_db");

        let store = ControlPlaneStore::with_db(&db_path).unwrap();
        assert!(store.db.is_some());
        assert!(store.vote.read().is_none());
        assert!(store.committed.read().is_none());
        assert!(store.logs.read().is_empty());
    }

    #[test]
    fn test_control_plane_store_get_state() {
        let store = ControlPlaneStore::new();
        let state = store.get_state();
        assert!(state.control_plane_nodes.is_empty());
        assert!(state.metadata_nodes.is_empty());
        assert!(state.object_nodes.is_empty());
        assert!(state.frontend_nodes.is_empty());
        assert_eq!(state.version, 0);
    }

    #[test]
    fn test_log_key_generation() {
        let key1 = ControlPlaneStore::log_key(0);
        assert!(key1.starts_with(b"log:"));
        assert_eq!(key1.len(), 4 + 8);

        let key2 = ControlPlaneStore::log_key(100);
        assert!(key2.starts_with(b"log:"));

        let key3 = ControlPlaneStore::log_key(u64::MAX);
        assert!(key3.starts_with(b"log:"));

        assert_ne!(key1, key2);
        assert_ne!(key2, key3);
    }

    #[tokio::test]
    async fn test_save_and_read_vote() {
        let mut store = ControlPlaneStore::new();

        let vote_result = store.read_vote().await.unwrap();
        assert!(vote_result.is_none());

        let vote = Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();

        let saved_vote = store.read_vote().await.unwrap();
        assert!(saved_vote.is_some());
        assert_eq!(saved_vote.unwrap(), vote);
    }

    #[tokio::test]
    async fn test_save_and_read_vote_with_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_vote_db");

        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        let vote = Vote::new(2, 3);
        store.save_vote(&vote).await.unwrap();

        let saved_vote = store.read_vote().await.unwrap();
        assert!(saved_vote.is_some());
        assert_eq!(saved_vote.unwrap(), vote);
    }

    #[tokio::test]
    async fn test_save_and_read_committed() {
        let mut store = ControlPlaneStore::new();

        let committed_result = store.read_committed().await.unwrap();
        assert!(committed_result.is_none());

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 5,
        };
        store.save_committed(Some(log_id)).await.unwrap();

        let saved_committed = store.read_committed().await.unwrap();
        assert!(saved_committed.is_some());
        assert_eq!(saved_committed.unwrap(), log_id);
    }

    #[tokio::test]
    async fn test_save_committed_none() {
        let mut store = ControlPlaneStore::new();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 5,
        };
        store.save_committed(Some(log_id)).await.unwrap();
        store.save_committed(None).await.unwrap();

        let saved_committed = store.read_committed().await.unwrap();
        assert!(saved_committed.is_none());
    }

    #[tokio::test]
    async fn test_save_committed_with_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_committed_db");

        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 10,
        };
        store.save_committed(Some(log_id)).await.unwrap();

        let saved_committed = store.read_committed().await.unwrap();
        assert!(saved_committed.is_some());
        assert_eq!(saved_committed.unwrap(), log_id);
    }

    #[tokio::test]
    async fn test_get_log_state_empty() {
        let mut store = ControlPlaneStore::new();

        let log_state = store.get_log_state().await.unwrap();
        assert!(log_state.last_purged_log_id.is_none());
        assert!(log_state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn test_get_log_state_with_entries() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_state = store.get_log_state().await.unwrap();
        assert!(log_state.last_purged_log_id.is_none());
        assert!(log_state.last_log_id.is_some());
        assert_eq!(log_state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_get_log_state_with_purged() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
        ];
        store.append_to_log(entries).await.unwrap();

        let purge_log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 2,
        };
        store.purge_logs_upto(purge_log_id).await.unwrap();

        let log_state = store.get_log_state().await.unwrap();
        assert!(log_state.last_purged_log_id.is_some());
        assert_eq!(log_state.last_purged_log_id.unwrap().index, 2);
        assert_eq!(log_state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_get_log_reader() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![create_blank_entry(1), create_blank_entry(2)];
        store.append_to_log(entries).await.unwrap();

        let reader = store.get_log_reader().await;
        assert_eq!(reader.logs.len(), 2);
    }

    #[tokio::test]
    async fn test_append_to_log() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
        ];
        store.append_to_log(entries).await.unwrap();

        let logs = store.logs.read();
        assert_eq!(logs.len(), 3);
        assert!(logs.contains_key(&1));
        assert!(logs.contains_key(&2));
        assert!(logs.contains_key(&3));
    }

    #[tokio::test]
    async fn test_append_to_log_with_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_append_db");

        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        let entries = vec![create_blank_entry(1), create_blank_entry(2)];
        store.append_to_log(entries).await.unwrap();

        let logs = store.logs.read();
        assert_eq!(logs.len(), 2);
    }

    #[tokio::test]
    async fn test_try_get_log_entries() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
            create_blank_entry(4),
            create_blank_entry(5),
        ];
        store.append_to_log(entries).await.unwrap();

        let retrieved = store.try_get_log_entries(2..4).await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].log_id.index, 2);
        assert_eq!(retrieved[1].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_try_get_log_entries_empty_range() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![create_blank_entry(1), create_blank_entry(2)];
        store.append_to_log(entries).await.unwrap();

        let retrieved = store.try_get_log_entries(10..20).await.unwrap();
        assert!(retrieved.is_empty());
    }

    #[tokio::test]
    async fn test_delete_conflict_logs_since() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
            create_blank_entry(4),
            create_blank_entry(5),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 3,
        };
        store.delete_conflict_logs_since(log_id).await.unwrap();

        let logs = store.logs.read();
        assert_eq!(logs.len(), 2);
        assert!(logs.contains_key(&1));
        assert!(logs.contains_key(&2));
        assert!(!logs.contains_key(&3));
        assert!(!logs.contains_key(&4));
        assert!(!logs.contains_key(&5));
    }

    #[tokio::test]
    async fn test_delete_conflict_logs_since_with_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_delete_conflict_db");

        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 2,
        };
        store.delete_conflict_logs_since(log_id).await.unwrap();

        let logs = store.logs.read();
        assert_eq!(logs.len(), 1);
        assert!(logs.contains_key(&1));
    }

    #[tokio::test]
    async fn test_purge_logs_upto() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
            create_blank_entry(4),
            create_blank_entry(5),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 3,
        };
        store.purge_logs_upto(log_id).await.unwrap();

        let logs = store.logs.read();
        assert_eq!(logs.len(), 2);
        assert!(!logs.contains_key(&1));
        assert!(!logs.contains_key(&2));
        assert!(!logs.contains_key(&3));
        assert!(logs.contains_key(&4));
        assert!(logs.contains_key(&5));

        let last_purged = store.last_purged.read();
        assert!(last_purged.is_some());
        assert_eq!(last_purged.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_purge_logs_upto_with_db() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_purge_db");

        let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 2,
        };
        store.purge_logs_upto(log_id).await.unwrap();

        let logs = store.logs.read();
        assert_eq!(logs.len(), 1);
        assert!(logs.contains_key(&3));
    }

    #[tokio::test]
    async fn test_last_applied_state_empty() {
        let mut store = ControlPlaneStore::new();

        let (last_applied, membership) = store.last_applied_state().await.unwrap();
        assert!(last_applied.is_none());
        assert!(membership.membership().get_joint_config().is_empty());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_blank_entry() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![create_blank_entry(1)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], Response::Success);

        let data = store.sm_data.read();
        assert!(data.last_applied.is_some());
        assert_eq!(data.last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_add_node() {
        let mut store = ControlPlaneStore::new();

        let node = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Object);
        let request = Request::AddNode { node };
        let entries = vec![create_normal_entry(1, request)];

        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], Response::NodeAdded);

        let state = store.get_state();
        assert_eq!(state.object_nodes.len(), 1);
        assert!(state.object_nodes.contains_key("node1"));
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_remove_node() {
        let mut store = ControlPlaneStore::new();

        let node = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Metadata);
        let add_request = Request::AddNode { node };
        let add_entries = vec![create_normal_entry(1, add_request)];
        store.apply_to_state_machine(&add_entries).await.unwrap();

        let remove_request = Request::RemoveNode {
            node_id: "node1".to_string(),
            node_type: NodeType::Metadata,
        };
        let remove_entries = vec![create_normal_entry(2, remove_request)];
        let responses = store.apply_to_state_machine(&remove_entries).await.unwrap();

        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Response::NodeRemoved { found } => assert!(found),
            _ => panic!("Expected NodeRemoved response"),
        }

        let state = store.get_state();
        assert!(state.metadata_nodes.is_empty());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_remove_nonexistent_node() {
        let mut store = ControlPlaneStore::new();

        let remove_request = Request::RemoveNode {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
        };
        let entries = vec![create_normal_entry(1, remove_request)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        match &responses[0] {
            Response::NodeRemoved { found } => assert!(!found),
            _ => panic!("Expected NodeRemoved response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_set_node_state() {
        let mut store = ControlPlaneStore::new();

        let node = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Object);
        let add_request = Request::AddNode { node };
        let add_entries = vec![create_normal_entry(1, add_request)];
        store.apply_to_state_machine(&add_entries).await.unwrap();

        let set_state_request = Request::SetNodeState {
            node_id: "node1".to_string(),
            node_type: NodeType::Object,
            state: gitstratum_hashring::NodeState::Draining,
        };
        let set_state_entries = vec![create_normal_entry(2, set_state_request)];
        let responses = store
            .apply_to_state_machine(&set_state_entries)
            .await
            .unwrap();

        match &responses[0] {
            Response::NodeStateSet { found } => assert!(found),
            _ => panic!("Expected NodeStateSet response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_set_state_nonexistent_node() {
        let mut store = ControlPlaneStore::new();

        let set_state_request = Request::SetNodeState {
            node_id: "nonexistent".to_string(),
            node_type: NodeType::Object,
            state: gitstratum_hashring::NodeState::Draining,
        };
        let entries = vec![create_normal_entry(1, set_state_request)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        match &responses[0] {
            Response::NodeStateSet { found } => assert!(!found),
            _ => panic!("Expected NodeStateSet response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_acquire_lock() {
        let mut store = ControlPlaneStore::new();

        let key = RefLockKey::new("repo1", "refs/heads/main");
        let lock = LockInfo::new(
            "lock1",
            "repo1",
            "refs/heads/main",
            "holder1",
            Duration::from_secs(30),
        );
        let request = Request::AcquireLock { key, lock };
        let entries = vec![create_normal_entry(1, request)];

        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        match &responses[0] {
            Response::LockAcquired { lock_id } => assert_eq!(lock_id, "lock1"),
            _ => panic!("Expected LockAcquired response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_acquire_lock_already_held() {
        let mut store = ControlPlaneStore::new();

        let key = RefLockKey::new("repo1", "refs/heads/main");
        let lock1 = LockInfo::new(
            "lock1",
            "repo1",
            "refs/heads/main",
            "holder1",
            Duration::from_secs(30),
        );
        let request1 = Request::AcquireLock {
            key: key.clone(),
            lock: lock1,
        };
        let entries1 = vec![create_normal_entry(1, request1)];
        store.apply_to_state_machine(&entries1).await.unwrap();

        let lock2 = LockInfo::new(
            "lock2",
            "repo1",
            "refs/heads/main",
            "holder2",
            Duration::from_secs(30),
        );
        let request2 = Request::AcquireLock { key, lock: lock2 };
        let entries2 = vec![create_normal_entry(2, request2)];
        let responses = store.apply_to_state_machine(&entries2).await.unwrap();

        match &responses[0] {
            Response::LockNotAcquired { reason } => {
                assert!(reason.contains("holder1"));
            }
            _ => panic!("Expected LockNotAcquired response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_release_lock() {
        let mut store = ControlPlaneStore::new();

        let key = RefLockKey::new("repo1", "refs/heads/main");
        let lock = LockInfo::new(
            "lock1",
            "repo1",
            "refs/heads/main",
            "holder1",
            Duration::from_secs(30),
        );
        let acquire_request = Request::AcquireLock { key, lock };
        let acquire_entries = vec![create_normal_entry(1, acquire_request)];
        store
            .apply_to_state_machine(&acquire_entries)
            .await
            .unwrap();

        let release_request = Request::ReleaseLock {
            lock_id: "lock1".to_string(),
        };
        let release_entries = vec![create_normal_entry(2, release_request)];
        let responses = store
            .apply_to_state_machine(&release_entries)
            .await
            .unwrap();

        match &responses[0] {
            Response::LockReleased { found } => assert!(found),
            _ => panic!("Expected LockReleased response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_release_nonexistent_lock() {
        let mut store = ControlPlaneStore::new();

        let release_request = Request::ReleaseLock {
            lock_id: "nonexistent".to_string(),
        };
        let entries = vec![create_normal_entry(1, release_request)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        match &responses[0] {
            Response::LockReleased { found } => assert!(!found),
            _ => panic!("Expected LockReleased response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_cleanup_expired_locks() {
        let mut store = ControlPlaneStore::new();

        let key = RefLockKey::new("repo1", "refs/heads/main");
        let lock = LockInfo {
            lock_id: "lock1".to_string(),
            repo_id: "repo1".to_string(),
            ref_name: "refs/heads/main".to_string(),
            holder_id: "holder1".to_string(),
            timeout_ms: 1,
            acquired_at_epoch_ms: 0,
        };
        let acquire_request = Request::AcquireLock { key, lock };
        let acquire_entries = vec![create_normal_entry(1, acquire_request)];
        store
            .apply_to_state_machine(&acquire_entries)
            .await
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let cleanup_request = Request::CleanupExpiredLocks;
        let cleanup_entries = vec![create_normal_entry(2, cleanup_request)];
        let responses = store
            .apply_to_state_machine(&cleanup_entries)
            .await
            .unwrap();

        match &responses[0] {
            Response::LocksCleanedUp { count } => assert_eq!(*count, 1),
            _ => panic!("Expected LocksCleanedUp response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_cleanup_no_expired_locks() {
        let mut store = ControlPlaneStore::new();

        let cleanup_request = Request::CleanupExpiredLocks;
        let entries = vec![create_normal_entry(1, cleanup_request)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        match &responses[0] {
            Response::LocksCleanedUp { count } => assert_eq!(*count, 0),
            _ => panic!("Expected LocksCleanedUp response"),
        }
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_membership_entry() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![create_membership_entry(1)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], Response::Success);

        let data = store.sm_data.read();
        assert!(!data
            .last_membership
            .membership()
            .get_joint_config()
            .is_empty());
    }

    #[tokio::test]
    async fn test_apply_to_state_machine_multiple_entries() {
        let mut store = ControlPlaneStore::new();

        let node1 = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Object);
        let node2 = ExtendedNodeInfo::new("node2", "192.168.1.2", 8080, NodeType::Metadata);

        let entries = vec![
            create_blank_entry(1),
            create_normal_entry(2, Request::AddNode { node: node1 }),
            create_normal_entry(3, Request::AddNode { node: node2 }),
        ];

        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0], Response::Success);
        assert_eq!(responses[1], Response::NodeAdded);
        assert_eq!(responses[2], Response::NodeAdded);

        let state = store.get_state();
        assert_eq!(state.object_nodes.len(), 1);
        assert_eq!(state.metadata_nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_get_snapshot_builder() {
        let mut store = ControlPlaneStore::new();

        let node = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Object);
        let request = Request::AddNode { node };
        let entries = vec![create_normal_entry(1, request)];
        store.apply_to_state_machine(&entries).await.unwrap();

        let builder = store.get_snapshot_builder().await;
        assert!(builder.data.last_applied.is_some());
        assert_eq!(builder.data.state.object_nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_begin_receiving_snapshot() {
        let mut store = ControlPlaneStore::new();

        let cursor = store.begin_receiving_snapshot().await.unwrap();
        let data = cursor.into_inner();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_install_snapshot() {
        let mut store = ControlPlaneStore::new();

        let mut state = ClusterStateSnapshot::new();
        state.add_node(ExtendedNodeInfo::new(
            "node1",
            "192.168.1.1",
            8080,
            NodeType::Object,
        ));
        let snapshot_data = serde_json::to_vec(&state).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(LogId {
                leader_id: LeaderId::new(1, 1),
                index: 5,
            }),
            last_membership: StoredMembership::default(),
            snapshot_id: "snapshot-1".to_string(),
        };

        let snapshot_cursor = Box::new(Cursor::new(snapshot_data));
        store
            .install_snapshot(&meta, snapshot_cursor)
            .await
            .unwrap();

        let installed_state = store.get_state();
        assert_eq!(installed_state.object_nodes.len(), 1);
        assert!(installed_state.object_nodes.contains_key("node1"));

        let sm_data = store.sm_data.read();
        assert!(sm_data.last_applied.is_some());
        assert_eq!(sm_data.last_applied.unwrap().index, 5);
    }

    #[tokio::test]
    async fn test_get_current_snapshot_empty() {
        let mut store = ControlPlaneStore::new();

        let snapshot = store.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_get_current_snapshot_after_install() {
        let mut store = ControlPlaneStore::new();

        let state = ClusterStateSnapshot::new();
        let snapshot_data = serde_json::to_vec(&state).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(LogId {
                leader_id: LeaderId::new(1, 1),
                index: 3,
            }),
            last_membership: StoredMembership::default(),
            snapshot_id: "snapshot-1".to_string(),
        };

        let snapshot_cursor = Box::new(Cursor::new(snapshot_data));
        store
            .install_snapshot(&meta, snapshot_cursor)
            .await
            .unwrap();

        let snapshot = store.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_some());

        let snap = snapshot.unwrap();
        assert_eq!(snap.meta.snapshot_id, "snapshot-1");
        assert!(snap.meta.last_log_id.is_some());
        assert_eq!(snap.meta.last_log_id.unwrap().index, 3);
    }

    #[test]
    fn test_create_stores() {
        let store = ControlPlaneStore::new();
        let (_log_store, _sm_store) = create_stores(store);
    }

    #[tokio::test]
    async fn test_with_db_persistence() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_persistence_db");

        {
            let mut store = ControlPlaneStore::with_db(&db_path).unwrap();

            let vote = Vote::new(1, 2);
            store.save_vote(&vote).await.unwrap();

            let log_id = LogId {
                leader_id: LeaderId::new(1, 1),
                index: 5,
            };
            store.save_committed(Some(log_id)).await.unwrap();

            let entries = vec![create_blank_entry(1), create_blank_entry(2)];
            store.append_to_log(entries).await.unwrap();
        }

        {
            let store = ControlPlaneStore::with_db(&db_path).unwrap();

            let vote = store.vote.read();
            assert!(vote.is_some());

            let committed = store.committed.read();
            assert!(committed.is_some());
            assert_eq!(committed.unwrap().index, 5);

            let logs = store.logs.read();
            assert_eq!(logs.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_apply_acquire_lock_on_expired() {
        let mut store = ControlPlaneStore::new();

        let key = RefLockKey::new("repo1", "refs/heads/main");
        let expired_lock = LockInfo {
            lock_id: "lock1".to_string(),
            repo_id: "repo1".to_string(),
            ref_name: "refs/heads/main".to_string(),
            holder_id: "holder1".to_string(),
            timeout_ms: 1,
            acquired_at_epoch_ms: 0,
        };

        {
            let mut sm_data = store.sm_data.write();
            sm_data.state.ref_locks.insert(key.clone(), expired_lock);
        }

        std::thread::sleep(Duration::from_millis(10));

        let new_lock = LockInfo::new(
            "lock2",
            "repo1",
            "refs/heads/main",
            "holder2",
            Duration::from_secs(30),
        );
        let request = Request::AcquireLock {
            key,
            lock: new_lock,
        };
        let entries = vec![create_normal_entry(1, request)];
        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        match &responses[0] {
            Response::LockAcquired { lock_id } => assert_eq!(lock_id, "lock2"),
            _ => panic!("Expected LockAcquired response"),
        }
    }

    #[tokio::test]
    async fn test_get_log_state_after_full_purge() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![create_blank_entry(1), create_blank_entry(2)];
        store.append_to_log(entries).await.unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 2,
        };
        store.purge_logs_upto(log_id).await.unwrap();

        let log_state = store.get_log_state().await.unwrap();
        assert!(log_state.last_purged_log_id.is_some());
        assert_eq!(log_state.last_purged_log_id.unwrap().index, 2);
        assert!(log_state.last_log_id.is_some());
        assert_eq!(log_state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn test_apply_multiple_node_types() {
        let mut store = ControlPlaneStore::new();

        let cp_node = ExtendedNodeInfo::new("cp1", "192.168.1.1", 8080, NodeType::ControlPlane);
        let md_node = ExtendedNodeInfo::new("md1", "192.168.1.2", 8080, NodeType::Metadata);
        let obj_node = ExtendedNodeInfo::new("obj1", "192.168.1.3", 8080, NodeType::Object);
        let fe_node = ExtendedNodeInfo::new("fe1", "192.168.1.4", 8080, NodeType::Frontend);

        let entries = vec![
            create_normal_entry(1, Request::AddNode { node: cp_node }),
            create_normal_entry(2, Request::AddNode { node: md_node }),
            create_normal_entry(3, Request::AddNode { node: obj_node }),
            create_normal_entry(4, Request::AddNode { node: fe_node }),
        ];

        let responses = store.apply_to_state_machine(&entries).await.unwrap();
        assert_eq!(responses.len(), 4);
        for response in responses {
            assert_eq!(response, Response::NodeAdded);
        }

        let state = store.get_state();
        assert_eq!(state.control_plane_nodes.len(), 1);
        assert_eq!(state.metadata_nodes.len(), 1);
        assert_eq!(state.object_nodes.len(), 1);
        assert_eq!(state.frontend_nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_delete_conflict_logs_all() {
        let mut store = ControlPlaneStore::new();

        let entries = vec![
            create_blank_entry(1),
            create_blank_entry(2),
            create_blank_entry(3),
        ];
        store.append_to_log(entries).await.unwrap();

        let log_id = LogId {
            leader_id: LeaderId::new(1, 1),
            index: 1,
        };
        store.delete_conflict_logs_since(log_id).await.unwrap();

        let logs = store.logs.read();
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_type_config_declaration() {
        let _entry: Entry<TypeConfig> = create_blank_entry(1);
    }

    #[test]
    fn test_node_id_type() {
        let _id: NodeId = 42;
    }
}
