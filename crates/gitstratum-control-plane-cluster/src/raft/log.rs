use openraft::{Entry, RaftLogReader, StorageError};
use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::raft::node::{NodeId, TypeConfig};

pub use crate::raft::node::LogStore;

pub struct LogReader {
    pub(crate) logs: BTreeMap<u64, Entry<TypeConfig>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_reader_new() {
        let reader = LogReader {
            logs: BTreeMap::new(),
        };
        assert!(reader.logs.is_empty());
    }

    #[tokio::test]
    async fn test_log_reader_empty_range() {
        let mut reader = LogReader {
            logs: BTreeMap::new(),
        };
        let entries = reader.try_get_log_entries(0..10).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_log_reader_with_entries() {
        use openraft::LogId;

        let mut logs = BTreeMap::new();

        let entry1 = Entry::<TypeConfig> {
            log_id: LogId {
                leader_id: openraft::LeaderId::new(1, 1),
                index: 1,
            },
            payload: openraft::EntryPayload::Blank,
        };
        let entry2 = Entry::<TypeConfig> {
            log_id: LogId {
                leader_id: openraft::LeaderId::new(1, 1),
                index: 2,
            },
            payload: openraft::EntryPayload::Blank,
        };

        logs.insert(1, entry1);
        logs.insert(2, entry2);

        let mut reader = LogReader { logs };

        let entries = reader.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_log_reader_partial_range() {
        use openraft::LogId;

        let mut logs = BTreeMap::new();

        for i in 1..=5 {
            let entry = Entry::<TypeConfig> {
                log_id: LogId {
                    leader_id: openraft::LeaderId::new(1, 1),
                    index: i,
                },
                payload: openraft::EntryPayload::Blank,
            };
            logs.insert(i, entry);
        }

        let mut reader = LogReader { logs };

        let entries = reader.try_get_log_entries(2..4).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_log_reader_unbounded_end() {
        use openraft::LogId;

        let mut logs = BTreeMap::new();

        for i in 1..=3 {
            let entry = Entry::<TypeConfig> {
                log_id: LogId {
                    leader_id: openraft::LeaderId::new(1, 1),
                    index: i,
                },
                payload: openraft::EntryPayload::Blank,
            };
            logs.insert(i, entry);
        }

        let mut reader = LogReader { logs };

        let entries = reader.try_get_log_entries(2..).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_log_reader_unbounded_start() {
        use openraft::LogId;

        let mut logs = BTreeMap::new();

        for i in 1..=3 {
            let entry = Entry::<TypeConfig> {
                log_id: LogId {
                    leader_id: openraft::LeaderId::new(1, 1),
                    index: i,
                },
                payload: openraft::EntryPayload::Blank,
            };
            logs.insert(i, entry);
        }

        let mut reader = LogReader { logs };

        let entries = reader.try_get_log_entries(..2).await.unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_log_reader_all_entries() {
        use openraft::LogId;

        let mut logs = BTreeMap::new();

        for i in 1..=5 {
            let entry = Entry::<TypeConfig> {
                log_id: LogId {
                    leader_id: openraft::LeaderId::new(1, 1),
                    index: i,
                },
                payload: openraft::EntryPayload::Blank,
            };
            logs.insert(i, entry);
        }

        let mut reader = LogReader { logs };

        let entries = reader.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 5);
    }
}
