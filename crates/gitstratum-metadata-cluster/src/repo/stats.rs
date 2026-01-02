use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use gitstratum_core::RepoId;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RepoStats {
    pub clone_count: u64,
    pub fetch_count: u64,
    pub push_count: u64,
    pub total_refs: u64,
    pub total_commits: u64,
    pub size_bytes: u64,
    pub last_push_timestamp: Option<i64>,
    pub last_fetch_timestamp: Option<i64>,
}

impl RepoStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_clone(&mut self) {
        self.clone_count += 1;
    }

    pub fn record_fetch(&mut self, timestamp: i64) {
        self.fetch_count += 1;
        self.last_fetch_timestamp = Some(timestamp);
    }

    pub fn record_push(&mut self, timestamp: i64) {
        self.push_count += 1;
        self.last_push_timestamp = Some(timestamp);
    }

    pub fn update_refs_count(&mut self, count: u64) {
        self.total_refs = count;
    }

    pub fn update_commits_count(&mut self, count: u64) {
        self.total_commits = count;
    }

    pub fn update_size(&mut self, size_bytes: u64) {
        self.size_bytes = size_bytes;
    }

    pub fn total_operations(&self) -> u64 {
        self.clone_count + self.fetch_count + self.push_count
    }
}

pub struct RepoStatsCollector {
    stats: RwLock<HashMap<RepoId, RepoStats>>,
    pending_updates: RwLock<HashMap<RepoId, RepoStats>>,
    last_flush: RwLock<Instant>,
    flush_interval: Duration,
}

impl RepoStatsCollector {
    pub fn new(flush_interval: Duration) -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
            pending_updates: RwLock::new(HashMap::new()),
            last_flush: RwLock::new(Instant::now()),
            flush_interval,
        }
    }

    pub fn get(&self, repo_id: &RepoId) -> Option<RepoStats> {
        self.stats.read().get(repo_id).cloned()
    }

    pub fn record_clone(&self, repo_id: &RepoId) {
        let mut pending = self.pending_updates.write();
        let stats = pending.entry(repo_id.clone()).or_default();
        stats.record_clone();
        drop(pending);
        self.maybe_flush();
    }

    pub fn record_fetch(&self, repo_id: &RepoId, timestamp: i64) {
        let mut pending = self.pending_updates.write();
        let stats = pending.entry(repo_id.clone()).or_default();
        stats.record_fetch(timestamp);
        drop(pending);
        self.maybe_flush();
    }

    pub fn record_push(&self, repo_id: &RepoId, timestamp: i64) {
        let mut pending = self.pending_updates.write();
        let stats = pending.entry(repo_id.clone()).or_default();
        stats.record_push(timestamp);
        drop(pending);
        self.maybe_flush();
    }

    fn maybe_flush(&self) {
        let last = *self.last_flush.read();
        if last.elapsed() >= self.flush_interval {
            self.flush();
        }
    }

    pub fn flush(&self) {
        let pending = std::mem::take(&mut *self.pending_updates.write());

        if pending.is_empty() {
            return;
        }

        let mut stats = self.stats.write();
        for (repo_id, update) in pending {
            let current = stats.entry(repo_id).or_default();
            current.clone_count += update.clone_count;
            current.fetch_count += update.fetch_count;
            current.push_count += update.push_count;
            if update.last_push_timestamp.is_some() {
                current.last_push_timestamp = update.last_push_timestamp;
            }
            if update.last_fetch_timestamp.is_some() {
                current.last_fetch_timestamp = update.last_fetch_timestamp;
            }
        }

        *self.last_flush.write() = Instant::now();
    }

    pub fn set_stats(&self, repo_id: &RepoId, stats: RepoStats) {
        self.stats.write().insert(repo_id.clone(), stats);
    }

    pub fn delete(&self, repo_id: &RepoId) {
        self.stats.write().remove(repo_id);
        self.pending_updates.write().remove(repo_id);
    }

    pub fn list_all(&self) -> HashMap<RepoId, RepoStats> {
        self.flush();
        self.stats.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repo_stats_default() {
        let stats = RepoStats::new();
        assert_eq!(stats.clone_count, 0);
        assert_eq!(stats.fetch_count, 0);
        assert_eq!(stats.push_count, 0);
        assert_eq!(stats.total_operations(), 0);
    }

    #[test]
    fn test_repo_stats_record_operations() {
        let mut stats = RepoStats::new();

        stats.record_clone();
        assert_eq!(stats.clone_count, 1);

        stats.record_fetch(1000);
        assert_eq!(stats.fetch_count, 1);
        assert_eq!(stats.last_fetch_timestamp, Some(1000));

        stats.record_push(2000);
        assert_eq!(stats.push_count, 1);
        assert_eq!(stats.last_push_timestamp, Some(2000));

        assert_eq!(stats.total_operations(), 3);
    }

    #[test]
    fn test_repo_stats_update_counts() {
        let mut stats = RepoStats::new();

        stats.update_refs_count(100);
        assert_eq!(stats.total_refs, 100);

        stats.update_commits_count(500);
        assert_eq!(stats.total_commits, 500);

        stats.update_size(1024 * 1024);
        assert_eq!(stats.size_bytes, 1024 * 1024);
    }

    #[test]
    fn test_repo_stats_collector() {
        let collector = RepoStatsCollector::new(Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();

        assert!(collector.get(&repo_id).is_none());

        collector.record_clone(&repo_id);
        collector.record_fetch(&repo_id, 1000);
        collector.record_push(&repo_id, 2000);

        collector.flush();

        let stats = collector.get(&repo_id).unwrap();
        assert_eq!(stats.clone_count, 1);
        assert_eq!(stats.fetch_count, 1);
        assert_eq!(stats.push_count, 1);
    }

    #[test]
    fn test_repo_stats_collector_set_stats() {
        let collector = RepoStatsCollector::new(Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();

        let mut stats = RepoStats::new();
        stats.clone_count = 100;
        stats.total_commits = 500;

        collector.set_stats(&repo_id, stats);

        let retrieved = collector.get(&repo_id).unwrap();
        assert_eq!(retrieved.clone_count, 100);
        assert_eq!(retrieved.total_commits, 500);
    }

    #[test]
    fn test_repo_stats_collector_delete() {
        let collector = RepoStatsCollector::new(Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();

        collector.record_clone(&repo_id);
        collector.flush();

        collector.delete(&repo_id);
        assert!(collector.get(&repo_id).is_none());
    }

    #[test]
    fn test_repo_stats_collector_list_all() {
        let collector = RepoStatsCollector::new(Duration::from_secs(60));
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();

        collector.record_clone(&repo1);
        collector.record_clone(&repo2);

        let all = collector.list_all();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key(&repo1));
        assert!(all.contains_key(&repo2));
    }

    #[test]
    fn test_repo_stats_accumulation() {
        let collector = RepoStatsCollector::new(Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();

        collector.record_clone(&repo_id);
        collector.record_clone(&repo_id);
        collector.flush();

        collector.record_clone(&repo_id);
        collector.flush();

        let stats = collector.get(&repo_id).unwrap();
        assert_eq!(stats.clone_count, 3);
    }
}
