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
