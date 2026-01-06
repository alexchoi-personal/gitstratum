use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;

use gitstratum_core::{Oid, RefName, RepoId};

pub struct RefCache {
    cache: RwLock<LruCache<(RepoId, RefName), CacheEntry>>,
    repo_index: RwLock<HashMap<RepoId, HashSet<RefName>>>,
    ttl: Duration,
}

struct CacheEntry {
    oid: Option<Oid>,
    inserted_at: Instant,
}

impl RefCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            cache: RwLock::new(LruCache::new(cap)),
            repo_index: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    pub fn get(&self, repo_id: &RepoId, ref_name: &RefName) -> Option<Option<Oid>> {
        let key = (repo_id.clone(), ref_name.clone());
        let mut cache = self.cache.write();

        if let Some(entry) = cache.get(&key) {
            if entry.inserted_at.elapsed() <= self.ttl {
                return Some(entry.oid);
            }
            cache.pop(&key);
            drop(cache);
            self.remove_from_index(repo_id, ref_name);
        }

        None
    }

    pub fn put(&self, repo_id: &RepoId, ref_name: &RefName, oid: Option<Oid>) {
        let key = (repo_id.clone(), ref_name.clone());
        let entry = CacheEntry {
            oid,
            inserted_at: Instant::now(),
        };
        let mut cache = self.cache.write();
        let mut repo_index = self.repo_index.write();
        cache.put(key, entry);
        repo_index
            .entry(repo_id.clone())
            .or_default()
            .insert(ref_name.clone());
    }

    pub fn invalidate(&self, repo_id: &RepoId, ref_name: &RefName) {
        let key = (repo_id.clone(), ref_name.clone());
        let mut cache = self.cache.write();
        let mut repo_index = self.repo_index.write();
        cache.pop(&key);
        if let Some(refs) = repo_index.get_mut(repo_id) {
            refs.remove(ref_name);
            if refs.is_empty() {
                repo_index.remove(repo_id);
            }
        }
    }

    pub fn invalidate_repo(&self, repo_id: &RepoId) {
        let mut cache = self.cache.write();
        let mut repo_index = self.repo_index.write();

        if let Some(ref_names) = repo_index.remove(repo_id) {
            for ref_name in ref_names {
                cache.pop(&(repo_id.clone(), ref_name));
            }
        }
    }

    pub fn clear(&self) {
        self.cache.write().clear();
        self.repo_index.write().clear();
    }

    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn remove_from_index(&self, repo_id: &RepoId, ref_name: &RefName) {
        let mut index = self.repo_index.write();
        if let Some(refs) = index.get_mut(repo_id) {
            refs.remove(ref_name);
            if refs.is_empty() {
                index.remove(repo_id);
            }
        }
    }
}

pub struct HotRefsCache {
    hot_refs: RwLock<HashMap<RepoId, Vec<(RefName, Oid)>>>,
    last_updated: RwLock<HashMap<RepoId, Instant>>,
    ttl: Duration,
}

impl HotRefsCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            hot_refs: RwLock::new(HashMap::new()),
            last_updated: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    pub fn get_hot_refs(&self, repo_id: &RepoId) -> Option<Vec<(RefName, Oid)>> {
        let last_updated = self.last_updated.read();
        if let Some(ts) = last_updated.get(repo_id) {
            if ts.elapsed() <= self.ttl {
                return self.hot_refs.read().get(repo_id).cloned();
            }
        }
        None
    }

    pub fn set_hot_refs(&self, repo_id: &RepoId, refs: Vec<(RefName, Oid)>) {
        self.hot_refs.write().insert(repo_id.clone(), refs);
        self.last_updated
            .write()
            .insert(repo_id.clone(), Instant::now());
    }

    pub fn invalidate(&self, repo_id: &RepoId) {
        self.hot_refs.write().remove(repo_id);
        self.last_updated.write().remove(repo_id);
    }

    pub fn clear(&self) {
        self.hot_refs.write().clear();
        self.last_updated.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_cache_get_put() {
        let cache = RefCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        assert!(cache.get(&repo_id, &ref_name).is_none());

        cache.put(&repo_id, &ref_name, Some(oid));
        assert_eq!(cache.get(&repo_id, &ref_name), Some(Some(oid)));
    }

    #[test]
    fn test_ref_cache_put_none() {
        let cache = RefCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();

        cache.put(&repo_id, &ref_name, None);
        assert_eq!(cache.get(&repo_id, &ref_name), Some(None));
    }

    #[test]
    fn test_ref_cache_ttl_expiration() {
        let cache = RefCache::new(100, Duration::from_millis(1));
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        cache.put(&repo_id, &ref_name, Some(oid));

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get(&repo_id, &ref_name).is_none());
    }

    #[test]
    fn test_ref_cache_invalidate() {
        let cache = RefCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        cache.put(&repo_id, &ref_name, Some(oid));
        cache.invalidate(&repo_id, &ref_name);

        assert!(cache.get(&repo_id, &ref_name).is_none());
    }

    #[test]
    fn test_ref_cache_invalidate_repo() {
        let cache = RefCache::new(100, Duration::from_secs(60));
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();
        let ref1 = RefName::new("refs/heads/main").unwrap();
        let ref2 = RefName::new("refs/heads/feature").unwrap();
        let oid = Oid::hash(b"commit");

        cache.put(&repo1, &ref1, Some(oid));
        cache.put(&repo1, &ref2, Some(oid));
        cache.put(&repo2, &ref1, Some(oid));

        cache.invalidate_repo(&repo1);

        assert!(cache.get(&repo1, &ref1).is_none());
        assert!(cache.get(&repo1, &ref2).is_none());
        assert_eq!(cache.get(&repo2, &ref1), Some(Some(oid)));
    }

    #[test]
    fn test_ref_cache_clear() {
        let cache = RefCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        cache.put(&repo_id, &ref_name, Some(oid));
        cache.clear();

        assert!(cache.get(&repo_id, &ref_name).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_ref_cache_len() {
        let cache = RefCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref1 = RefName::new("refs/heads/main").unwrap();
        let ref2 = RefName::new("refs/heads/feature").unwrap();
        let oid = Oid::hash(b"commit");

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());

        cache.put(&repo_id, &ref1, Some(oid));
        assert_eq!(cache.len(), 1);

        cache.put(&repo_id, &ref2, Some(oid));
        assert_eq!(cache.len(), 2);
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_hot_refs_cache_get_set() {
        let cache = HotRefsCache::new(Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();

        assert!(cache.get_hot_refs(&repo_id).is_none());

        let refs = vec![
            (
                RefName::new("refs/heads/main").unwrap(),
                Oid::hash(b"commit1"),
            ),
            (
                RefName::new("refs/heads/dev").unwrap(),
                Oid::hash(b"commit2"),
            ),
        ];

        cache.set_hot_refs(&repo_id, refs.clone());
        let cached = cache.get_hot_refs(&repo_id).unwrap();
        assert_eq!(cached.len(), 2);
    }

    #[test]
    fn test_hot_refs_cache_ttl_expiration() {
        let cache = HotRefsCache::new(Duration::from_millis(1));
        let repo_id = RepoId::new("test/repo").unwrap();

        let refs = vec![(
            RefName::new("refs/heads/main").unwrap(),
            Oid::hash(b"commit"),
        )];
        cache.set_hot_refs(&repo_id, refs);

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get_hot_refs(&repo_id).is_none());
    }

    #[test]
    fn test_hot_refs_cache_invalidate() {
        let cache = HotRefsCache::new(Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();

        let refs = vec![(
            RefName::new("refs/heads/main").unwrap(),
            Oid::hash(b"commit"),
        )];
        cache.set_hot_refs(&repo_id, refs);
        cache.invalidate(&repo_id);

        assert!(cache.get_hot_refs(&repo_id).is_none());
    }

    #[test]
    fn test_hot_refs_cache_clear() {
        let cache = HotRefsCache::new(Duration::from_secs(60));
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();

        let refs = vec![(
            RefName::new("refs/heads/main").unwrap(),
            Oid::hash(b"commit"),
        )];
        cache.set_hot_refs(&repo1, refs.clone());
        cache.set_hot_refs(&repo2, refs);

        cache.clear();

        assert!(cache.get_hot_refs(&repo1).is_none());
        assert!(cache.get_hot_refs(&repo2).is_none());
    }
}
