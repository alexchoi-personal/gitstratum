use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use gitstratum_core::Oid;
use parking_lot::RwLock;

pub struct SweepPhase {
    to_delete: RwLock<Vec<Oid>>,
    objects_examined: AtomicU64,
    objects_deleted: AtomicU64,
    bytes_freed: AtomicU64,
}

impl SweepPhase {
    pub fn new() -> Self {
        Self {
            to_delete: RwLock::new(Vec::new()),
            objects_examined: AtomicU64::new(0),
            objects_deleted: AtomicU64::new(0),
            bytes_freed: AtomicU64::new(0),
        }
    }

    pub fn examine(&self, oid: Oid, marked: &HashSet<Oid>) {
        self.objects_examined.fetch_add(1, Ordering::Relaxed);
        if !marked.contains(&oid) {
            self.to_delete.write().push(oid);
        }
    }

    pub fn examine_batch(&self, oids: &[Oid], marked: &HashSet<Oid>) {
        let mut to_delete = self.to_delete.write();
        for oid in oids {
            self.objects_examined.fetch_add(1, Ordering::Relaxed);
            if !marked.contains(oid) {
                to_delete.push(*oid);
            }
        }
    }

    pub fn get_deletions(&self) -> Vec<Oid> {
        self.to_delete.read().clone()
    }

    pub fn deletions_count(&self) -> usize {
        self.to_delete.read().len()
    }

    pub fn record_deletion(&self, bytes: u64) {
        self.objects_deleted.fetch_add(1, Ordering::Relaxed);
        self.bytes_freed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_deletions(&self, count: u64, bytes: u64) {
        self.objects_deleted.fetch_add(count, Ordering::Relaxed);
        self.bytes_freed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn clear(&self) {
        self.to_delete.write().clear();
        self.objects_examined.store(0, Ordering::Relaxed);
        self.objects_deleted.store(0, Ordering::Relaxed);
        self.bytes_freed.store(0, Ordering::Relaxed);
    }

    pub fn stats(&self) -> SweepPhaseStats {
        SweepPhaseStats {
            objects_examined: self.objects_examined.load(Ordering::Relaxed),
            objects_deleted: self.objects_deleted.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
            pending_deletions: self.deletions_count(),
        }
    }
}

impl Default for SweepPhase {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct SweepPhaseStats {
    pub objects_examined: u64,
    pub objects_deleted: u64,
    pub bytes_freed: u64,
    pub pending_deletions: usize,
}

pub struct SweepConfig {
    pub batch_size: usize,
    pub delete_delay_ms: u64,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            delete_delay_ms: 0,
        }
    }
}

pub struct Sweeper {
    config: SweepConfig,
    sweep_phase: SweepPhase,
}

impl Sweeper {
    pub fn new(config: SweepConfig) -> Self {
        Self {
            config,
            sweep_phase: SweepPhase::new(),
        }
    }

    pub fn identify_garbage(&self, all_oids: &[Oid], marked: &HashSet<Oid>) {
        for chunk in all_oids.chunks(self.config.batch_size) {
            self.sweep_phase.examine_batch(chunk, marked);
        }
    }

    pub fn get_garbage(&self) -> Vec<Oid> {
        self.sweep_phase.get_deletions()
    }

    pub fn garbage_count(&self) -> usize {
        self.sweep_phase.deletions_count()
    }

    pub fn record_deletions(&self, count: u64, bytes: u64) {
        self.sweep_phase.record_deletions(count, bytes);
    }

    pub fn clear(&self) {
        self.sweep_phase.clear();
    }

    pub fn stats(&self) -> SweepPhaseStats {
        self.sweep_phase.stats()
    }

    pub fn batch_size(&self) -> usize {
        self.config.batch_size
    }
}

impl Default for Sweeper {
    fn default() -> Self {
        Self::new(SweepConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sweep_phase_new() {
        let phase = SweepPhase::new();
        assert_eq!(phase.deletions_count(), 0);
    }

    #[test]
    fn test_examine_unmarked() {
        let phase = SweepPhase::new();
        let marked: HashSet<Oid> = HashSet::new();
        let oid = Oid::hash(b"test");

        phase.examine(oid, &marked);

        assert_eq!(phase.deletions_count(), 1);
        assert!(phase.get_deletions().contains(&oid));
    }

    #[test]
    fn test_examine_marked() {
        let phase = SweepPhase::new();
        let oid = Oid::hash(b"test");
        let marked: HashSet<Oid> = vec![oid].into_iter().collect();

        phase.examine(oid, &marked);

        assert_eq!(phase.deletions_count(), 0);
    }

    #[test]
    fn test_examine_batch() {
        let phase = SweepPhase::new();
        let oids: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();
        let marked: HashSet<Oid> = vec![oids[0], oids[2]].into_iter().collect();

        phase.examine_batch(&oids, &marked);

        assert_eq!(phase.deletions_count(), 3);
    }

    #[test]
    fn test_record_deletion() {
        let phase = SweepPhase::new();
        phase.record_deletion(1024);

        let stats = phase.stats();
        assert_eq!(stats.objects_deleted, 1);
        assert_eq!(stats.bytes_freed, 1024);
    }

    #[test]
    fn test_record_deletions() {
        let phase = SweepPhase::new();
        phase.record_deletions(5, 5120);

        let stats = phase.stats();
        assert_eq!(stats.objects_deleted, 5);
        assert_eq!(stats.bytes_freed, 5120);
    }

    #[test]
    fn test_clear() {
        let phase = SweepPhase::new();
        let oid = Oid::hash(b"test");
        let marked: HashSet<Oid> = HashSet::new();

        phase.examine(oid, &marked);
        phase.record_deletion(1024);
        assert_eq!(phase.deletions_count(), 1);

        phase.clear();
        assert_eq!(phase.deletions_count(), 0);
        let stats = phase.stats();
        assert_eq!(stats.objects_deleted, 0);
    }

    #[test]
    fn test_sweep_phase_default() {
        let phase = SweepPhase::default();
        assert_eq!(phase.deletions_count(), 0);
    }

    #[test]
    fn test_sweep_config_default() {
        let config = SweepConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.delete_delay_ms, 0);
    }

    #[test]
    fn test_sweeper_identify_garbage() {
        let sweeper = Sweeper::default();
        let all_oids: Vec<Oid> = (0..10).map(|i| Oid::hash(&[i])).collect();
        let marked: HashSet<Oid> = all_oids[0..5].iter().copied().collect();

        sweeper.identify_garbage(&all_oids, &marked);

        assert_eq!(sweeper.garbage_count(), 5);
    }

    #[test]
    fn test_sweeper_get_garbage() {
        let sweeper = Sweeper::default();
        let all_oids: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();
        let marked: HashSet<Oid> = HashSet::new();

        sweeper.identify_garbage(&all_oids, &marked);

        let garbage = sweeper.get_garbage();
        assert_eq!(garbage.len(), 5);
    }

    #[test]
    fn test_sweeper_record_deletions() {
        let sweeper = Sweeper::default();
        sweeper.record_deletions(10, 10240);

        let stats = sweeper.stats();
        assert_eq!(stats.objects_deleted, 10);
        assert_eq!(stats.bytes_freed, 10240);
    }

    #[test]
    fn test_sweeper_batch_size() {
        let config = SweepConfig {
            batch_size: 500,
            delete_delay_ms: 0,
        };
        let sweeper = Sweeper::new(config);
        assert_eq!(sweeper.batch_size(), 500);
    }

    #[test]
    fn test_sweeper_default() {
        let sweeper = Sweeper::default();
        assert_eq!(sweeper.batch_size(), 1000);
    }
}
