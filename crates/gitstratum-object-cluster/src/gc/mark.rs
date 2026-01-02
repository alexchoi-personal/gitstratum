use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use gitstratum_core::Oid;
use parking_lot::RwLock;

pub struct MarkPhase {
    marked: RwLock<HashSet<Oid>>,
    objects_scanned: AtomicU64,
    objects_marked: AtomicU64,
}

impl MarkPhase {
    pub fn new() -> Self {
        Self {
            marked: RwLock::new(HashSet::new()),
            objects_scanned: AtomicU64::new(0),
            objects_marked: AtomicU64::new(0),
        }
    }

    pub fn mark(&self, oid: Oid) {
        self.objects_scanned.fetch_add(1, Ordering::Relaxed);
        let mut marked = self.marked.write();
        if marked.insert(oid) {
            self.objects_marked.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn mark_batch(&self, oids: &[Oid]) {
        let mut marked = self.marked.write();
        for oid in oids {
            self.objects_scanned.fetch_add(1, Ordering::Relaxed);
            if marked.insert(*oid) {
                self.objects_marked.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn is_marked(&self, oid: &Oid) -> bool {
        self.marked.read().contains(oid)
    }

    pub fn marked_count(&self) -> usize {
        self.marked.read().len()
    }

    pub fn get_marked(&self) -> HashSet<Oid> {
        self.marked.read().clone()
    }

    pub fn clear(&self) {
        self.marked.write().clear();
        self.objects_scanned.store(0, Ordering::Relaxed);
        self.objects_marked.store(0, Ordering::Relaxed);
    }

    pub fn stats(&self) -> MarkPhaseStats {
        MarkPhaseStats {
            objects_scanned: self.objects_scanned.load(Ordering::Relaxed),
            objects_marked: self.objects_marked.load(Ordering::Relaxed),
        }
    }
}

impl Default for MarkPhase {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct MarkPhaseStats {
    pub objects_scanned: u64,
    pub objects_marked: u64,
}

pub trait RootProvider {
    fn get_roots(&self) -> Vec<Oid>;
}

pub struct StaticRootProvider {
    roots: Vec<Oid>,
}

impl StaticRootProvider {
    pub fn new(roots: Vec<Oid>) -> Self {
        Self { roots }
    }
}

impl RootProvider for StaticRootProvider {
    fn get_roots(&self) -> Vec<Oid> {
        self.roots.clone()
    }
}

pub struct MarkWalker<R: RootProvider> {
    root_provider: R,
    mark_phase: MarkPhase,
}

impl<R: RootProvider> MarkWalker<R> {
    pub fn new(root_provider: R) -> Self {
        Self {
            root_provider,
            mark_phase: MarkPhase::new(),
        }
    }

    pub fn run(&self) {
        let roots = self.root_provider.get_roots();
        self.mark_phase.mark_batch(&roots);
    }

    pub fn get_marked(&self) -> HashSet<Oid> {
        self.mark_phase.get_marked()
    }

    pub fn marked_count(&self) -> usize {
        self.mark_phase.marked_count()
    }

    pub fn stats(&self) -> MarkPhaseStats {
        self.mark_phase.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_phase_new() {
        let phase = MarkPhase::new();
        assert_eq!(phase.marked_count(), 0);
    }

    #[test]
    fn test_mark_single() {
        let phase = MarkPhase::new();
        let oid = Oid::hash(b"test");

        phase.mark(oid);
        assert!(phase.is_marked(&oid));
        assert_eq!(phase.marked_count(), 1);
    }

    #[test]
    fn test_mark_duplicate() {
        let phase = MarkPhase::new();
        let oid = Oid::hash(b"test");

        phase.mark(oid);
        phase.mark(oid);
        assert_eq!(phase.marked_count(), 1);

        let stats = phase.stats();
        assert_eq!(stats.objects_scanned, 2);
        assert_eq!(stats.objects_marked, 1);
    }

    #[test]
    fn test_mark_batch() {
        let phase = MarkPhase::new();
        let oids: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();

        phase.mark_batch(&oids);
        assert_eq!(phase.marked_count(), 5);
    }

    #[test]
    fn test_is_marked() {
        let phase = MarkPhase::new();
        let oid = Oid::hash(b"test");
        let other = Oid::hash(b"other");

        assert!(!phase.is_marked(&oid));
        phase.mark(oid);
        assert!(phase.is_marked(&oid));
        assert!(!phase.is_marked(&other));
    }

    #[test]
    fn test_get_marked() {
        let phase = MarkPhase::new();
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");

        phase.mark(oid1);
        phase.mark(oid2);

        let marked = phase.get_marked();
        assert!(marked.contains(&oid1));
        assert!(marked.contains(&oid2));
    }

    #[test]
    fn test_clear() {
        let phase = MarkPhase::new();
        let oid = Oid::hash(b"test");

        phase.mark(oid);
        assert_eq!(phase.marked_count(), 1);

        phase.clear();
        assert_eq!(phase.marked_count(), 0);
        assert!(!phase.is_marked(&oid));
    }

    #[test]
    fn test_mark_phase_default() {
        let phase = MarkPhase::default();
        assert_eq!(phase.marked_count(), 0);
    }

    #[test]
    fn test_static_root_provider() {
        let roots: Vec<Oid> = (0..3).map(|i| Oid::hash(&[i])).collect();
        let provider = StaticRootProvider::new(roots.clone());

        let retrieved = provider.get_roots();
        assert_eq!(retrieved.len(), 3);
        assert_eq!(retrieved, roots);
    }

    #[test]
    fn test_mark_walker() {
        let roots: Vec<Oid> = (0..3).map(|i| Oid::hash(&[i])).collect();
        let provider = StaticRootProvider::new(roots.clone());
        let walker = MarkWalker::new(provider);

        walker.run();

        assert_eq!(walker.marked_count(), 3);
        let marked = walker.get_marked();
        for root in &roots {
            assert!(marked.contains(root));
        }
    }

    #[test]
    fn test_mark_walker_stats() {
        let roots: Vec<Oid> = (0..5).map(|i| Oid::hash(&[i])).collect();
        let provider = StaticRootProvider::new(roots);
        let walker = MarkWalker::new(provider);

        walker.run();

        let stats = walker.stats();
        assert_eq!(stats.objects_scanned, 5);
        assert_eq!(stats.objects_marked, 5);
    }
}
