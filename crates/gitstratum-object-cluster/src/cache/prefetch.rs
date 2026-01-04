use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use gitstratum_core::Oid;
use parking_lot::RwLock;

pub struct PrefetcherConfig {
    pub max_pending: usize,
    pub batch_size: usize,
}

impl Default for PrefetcherConfig {
    fn default() -> Self {
        Self {
            max_pending: 1000,
            batch_size: 100,
        }
    }
}

struct PendingQueue {
    queue: VecDeque<Oid>,
    set: HashSet<Oid>,
}

impl PendingQueue {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    fn contains(&self, oid: &Oid) -> bool {
        self.set.contains(oid)
    }

    fn push_back(&mut self, oid: Oid) {
        if self.set.insert(oid) {
            self.queue.push_back(oid);
        }
    }

    fn pop_front(&mut self) -> Option<Oid> {
        if let Some(oid) = self.queue.pop_front() {
            self.set.remove(&oid);
            Some(oid)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn clear(&mut self) {
        self.queue.clear();
        self.set.clear();
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Oid) -> bool,
    {
        self.queue.retain(|oid| {
            let keep = f(oid);
            if !keep {
                self.set.remove(oid);
            }
            keep
        });
    }
}

pub struct Prefetcher {
    config: PrefetcherConfig,
    pending: RwLock<PendingQueue>,
    in_flight: RwLock<HashSet<Oid>>,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl Prefetcher {
    pub fn new(config: PrefetcherConfig) -> Self {
        Self {
            config,
            pending: RwLock::new(PendingQueue::new()),
            in_flight: RwLock::new(HashSet::new()),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }

    pub fn schedule(&self, oid: Oid) {
        let in_flight = self.in_flight.read();
        if in_flight.contains(&oid) {
            return;
        }
        drop(in_flight);

        let mut pending = self.pending.write();
        if pending.contains(&oid) {
            return;
        }

        if pending.len() >= self.config.max_pending {
            pending.pop_front();
        }

        pending.push_back(oid);
    }

    pub fn schedule_batch(&self, oids: Vec<Oid>) {
        for oid in oids {
            self.schedule(oid);
        }
    }

    pub fn next_batch(&self) -> Vec<Oid> {
        let candidates = {
            let mut pending = self.pending.write();
            let mut candidates = Vec::with_capacity(self.config.batch_size);
            while candidates.len() < self.config.batch_size {
                if let Some(oid) = pending.pop_front() {
                    candidates.push(oid);
                } else {
                    break;
                }
            }
            candidates
        };

        if candidates.is_empty() {
            return Vec::new();
        }

        let mut in_flight = self.in_flight.write();
        let mut batch = Vec::with_capacity(candidates.len());
        for oid in candidates {
            if !in_flight.contains(&oid) {
                in_flight.insert(oid);
                batch.push(oid);
            }
        }

        batch
    }

    pub fn complete(&self, oid: &Oid, success: bool) {
        let mut in_flight = self.in_flight.write();
        in_flight.remove(oid);

        if success {
            self.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn complete_batch(&self, oids: &[Oid], success: bool) {
        for oid in oids {
            self.complete(oid, success);
        }
    }

    pub fn cancel(&self, oid: &Oid) {
        let mut pending = self.pending.write();
        pending.retain(|o| o != oid);

        let mut in_flight = self.in_flight.write();
        in_flight.remove(oid);
    }

    pub fn clear(&self) {
        self.pending.write().clear();
        self.in_flight.write().clear();
    }

    pub fn pending_count(&self) -> usize {
        self.pending.read().len()
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.read().is_empty() && self.in_flight.read().is_empty()
    }

    pub fn stats(&self) -> PrefetcherStats {
        PrefetcherStats {
            pending: self.pending_count(),
            in_flight: self.in_flight_count(),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

impl Default for Prefetcher {
    fn default() -> Self {
        Self::new(PrefetcherConfig::default())
    }
}

#[derive(Debug, Clone)]
pub struct PrefetcherStats {
    pub pending: usize,
    pub in_flight: usize,
    pub completed: u64,
    pub failed: u64,
}

pub struct RelatedObjectsFinder;

impl RelatedObjectsFinder {
    pub fn find_tree_children(_tree_oid: &Oid) -> Vec<Oid> {
        Vec::new()
    }

    pub fn find_commit_parents(_commit_oid: &Oid) -> Vec<Oid> {
        Vec::new()
    }

    pub fn find_related(_oid: &Oid, _depth: usize) -> Vec<Oid> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetcher_config_default() {
        let config = PrefetcherConfig::default();
        assert_eq!(config.max_pending, 1000);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_prefetcher_schedule() {
        let prefetcher = Prefetcher::default();
        let oid = Oid::hash(b"test");

        prefetcher.schedule(oid);
        assert_eq!(prefetcher.pending_count(), 1);
    }

    #[test]
    fn test_prefetcher_schedule_batch() {
        let prefetcher = Prefetcher::default();
        let oids: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();

        prefetcher.schedule_batch(oids);
        assert_eq!(prefetcher.pending_count(), 5);
    }

    #[test]
    fn test_prefetcher_next_batch() {
        let config = PrefetcherConfig {
            max_pending: 100,
            batch_size: 3,
        };
        let prefetcher = Prefetcher::new(config);

        let oids: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();
        prefetcher.schedule_batch(oids);

        let batch = prefetcher.next_batch();
        assert_eq!(batch.len(), 3);
        assert_eq!(prefetcher.in_flight_count(), 3);
        assert_eq!(prefetcher.pending_count(), 2);
    }

    #[test]
    fn test_prefetcher_complete() {
        let prefetcher = Prefetcher::default();
        let oid = Oid::hash(b"test");

        prefetcher.schedule(oid);
        let batch = prefetcher.next_batch();
        assert_eq!(batch.len(), 1);

        prefetcher.complete(&oid, true);
        assert_eq!(prefetcher.in_flight_count(), 0);

        let stats = prefetcher.stats();
        assert_eq!(stats.completed, 1);
    }

    #[test]
    fn test_prefetcher_complete_failed() {
        let prefetcher = Prefetcher::default();
        let oid = Oid::hash(b"test");

        prefetcher.schedule(oid);
        prefetcher.next_batch();
        prefetcher.complete(&oid, false);

        let stats = prefetcher.stats();
        assert_eq!(stats.failed, 1);
    }

    #[test]
    fn test_prefetcher_cancel() {
        let prefetcher = Prefetcher::default();
        let oid = Oid::hash(b"test");

        prefetcher.schedule(oid);
        assert_eq!(prefetcher.pending_count(), 1);

        prefetcher.cancel(&oid);
        assert_eq!(prefetcher.pending_count(), 0);
    }

    #[test]
    fn test_prefetcher_clear() {
        let prefetcher = Prefetcher::default();
        let oids: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();

        prefetcher.schedule_batch(oids);
        prefetcher.next_batch();
        assert!(!prefetcher.is_empty());

        prefetcher.clear();
        assert!(prefetcher.is_empty());
    }

    #[test]
    fn test_prefetcher_default() {
        let prefetcher = Prefetcher::default();
        assert!(prefetcher.is_empty());
    }

    #[test]
    fn test_prefetcher_max_pending() {
        let config = PrefetcherConfig {
            max_pending: 3,
            batch_size: 10,
        };
        let prefetcher = Prefetcher::new(config);

        for i in 0..5 {
            prefetcher.schedule(Oid::hash(&[i]));
        }

        assert_eq!(prefetcher.pending_count(), 3);
    }

    #[test]
    fn test_prefetcher_no_duplicate() {
        let prefetcher = Prefetcher::default();
        let oid = Oid::hash(b"test");

        prefetcher.schedule(oid);
        prefetcher.schedule(oid);
        assert_eq!(prefetcher.pending_count(), 1);
    }

    #[test]
    fn test_related_objects_finder() {
        let oid = Oid::hash(b"test");
        let children = RelatedObjectsFinder::find_tree_children(&oid);
        assert!(children.is_empty());

        let parents = RelatedObjectsFinder::find_commit_parents(&oid);
        assert!(parents.is_empty());

        let related = RelatedObjectsFinder::find_related(&oid, 2);
        assert!(related.is_empty());
    }
}
