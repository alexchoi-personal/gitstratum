use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;

use gitstratum_core::{Oid, RepoId};

pub struct GraphCache {
    ancestry_cache: RwLock<LruCache<(RepoId, Oid, Oid), bool>>,
    reachable_cache: RwLock<LruCache<(RepoId, Oid), Arc<HashSet<Oid>>>>,
    merge_base_cache: RwLock<LruCache<(RepoId, Vec<Oid>), Option<Oid>>>,
    entry_ttl: Duration,
    timestamps: RwLock<HashMap<CacheKey, Instant>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    Ancestry(RepoId, Oid, Oid),
    Reachable(RepoId, Oid),
    MergeBase(RepoId, Vec<Oid>),
}

impl GraphCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            ancestry_cache: RwLock::new(LruCache::new(cap)),
            reachable_cache: RwLock::new(LruCache::new(cap)),
            merge_base_cache: RwLock::new(LruCache::new(cap)),
            entry_ttl: ttl,
            timestamps: RwLock::new(HashMap::new()),
        }
    }

    fn is_expired(&self, key: &CacheKey) -> bool {
        let timestamps = self.timestamps.read();
        if let Some(ts) = timestamps.get(key) {
            ts.elapsed() > self.entry_ttl
        } else {
            true
        }
    }

    fn record_timestamp(&self, key: CacheKey) {
        let mut timestamps = self.timestamps.write();
        timestamps.insert(key, Instant::now());
    }

    pub fn get_ancestry(&self, repo_id: &RepoId, ancestor: &Oid, descendant: &Oid) -> Option<bool> {
        let cache_key = CacheKey::Ancestry(repo_id.clone(), *ancestor, *descendant);
        if self.is_expired(&cache_key) {
            return None;
        }

        let key = (repo_id.clone(), *ancestor, *descendant);
        let mut cache = self.ancestry_cache.write();
        cache.get(&key).copied()
    }

    pub fn put_ancestry(&self, repo_id: &RepoId, ancestor: &Oid, descendant: &Oid, is_ancestor: bool) {
        let key = (repo_id.clone(), *ancestor, *descendant);
        let cache_key = CacheKey::Ancestry(repo_id.clone(), *ancestor, *descendant);

        let mut cache = self.ancestry_cache.write();
        cache.put(key, is_ancestor);
        self.record_timestamp(cache_key);
    }

    pub fn get_reachable(&self, repo_id: &RepoId, from: &Oid) -> Option<Arc<HashSet<Oid>>> {
        let cache_key = CacheKey::Reachable(repo_id.clone(), *from);
        if self.is_expired(&cache_key) {
            return None;
        }

        let key = (repo_id.clone(), *from);
        let mut cache = self.reachable_cache.write();
        cache.get(&key).cloned()
    }

    pub fn put_reachable(&self, repo_id: &RepoId, from: &Oid, reachable: HashSet<Oid>) {
        let key = (repo_id.clone(), *from);
        let cache_key = CacheKey::Reachable(repo_id.clone(), *from);

        let mut cache = self.reachable_cache.write();
        cache.put(key, Arc::new(reachable));
        self.record_timestamp(cache_key);
    }

    pub fn get_merge_base(&self, repo_id: &RepoId, commits: &[Oid]) -> Option<Option<Oid>> {
        let cache_key = CacheKey::MergeBase(repo_id.clone(), commits.to_vec());
        if self.is_expired(&cache_key) {
            return None;
        }

        let key = (repo_id.clone(), commits.to_vec());
        let mut cache = self.merge_base_cache.write();
        cache.get(&key).copied()
    }

    pub fn put_merge_base(&self, repo_id: &RepoId, commits: &[Oid], merge_base: Option<Oid>) {
        let key = (repo_id.clone(), commits.to_vec());
        let cache_key = CacheKey::MergeBase(repo_id.clone(), commits.to_vec());

        let mut cache = self.merge_base_cache.write();
        cache.put(key, merge_base);
        self.record_timestamp(cache_key);
    }

    pub fn invalidate_repo(&self, repo_id: &RepoId) {
        {
            let mut cache = self.ancestry_cache.write();
            let keys_to_remove: Vec<_> = cache
                .iter()
                .filter(|((r, _, _), _)| r == repo_id)
                .map(|(k, _)| k.clone())
                .collect();
            for key in keys_to_remove {
                cache.pop(&key);
            }
        }

        {
            let mut cache = self.reachable_cache.write();
            let keys_to_remove: Vec<_> = cache
                .iter()
                .filter(|((r, _), _)| r == repo_id)
                .map(|(k, _)| k.clone())
                .collect();
            for key in keys_to_remove {
                cache.pop(&key);
            }
        }

        {
            let mut cache = self.merge_base_cache.write();
            let keys_to_remove: Vec<_> = cache
                .iter()
                .filter(|((r, _), _)| r == repo_id)
                .map(|(k, _)| k.clone())
                .collect();
            for key in keys_to_remove {
                cache.pop(&key);
            }
        }

        {
            let mut timestamps = self.timestamps.write();
            timestamps.retain(|k, _| match k {
                CacheKey::Ancestry(r, _, _) => r != repo_id,
                CacheKey::Reachable(r, _) => r != repo_id,
                CacheKey::MergeBase(r, _) => r != repo_id,
            });
        }
    }

    pub fn clear(&self) {
        self.ancestry_cache.write().clear();
        self.reachable_cache.write().clear();
        self.merge_base_cache.write().clear();
        self.timestamps.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ancestry_cache() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        assert!(cache.get_ancestry(&repo_id, &oid1, &oid2).is_none());

        cache.put_ancestry(&repo_id, &oid1, &oid2, true);
        assert_eq!(cache.get_ancestry(&repo_id, &oid1, &oid2), Some(true));
    }

    #[test]
    fn test_reachable_cache() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let from = Oid::hash(b"from");

        assert!(cache.get_reachable(&repo_id, &from).is_none());

        let mut reachable = HashSet::new();
        reachable.insert(Oid::hash(b"oid1"));
        reachable.insert(Oid::hash(b"oid2"));

        cache.put_reachable(&repo_id, &from, reachable.clone());

        let cached = cache.get_reachable(&repo_id, &from).unwrap();
        assert_eq!(*cached, reachable);
    }

    #[test]
    fn test_merge_base_cache() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let commits = vec![Oid::hash(b"oid1"), Oid::hash(b"oid2")];
        let merge_base = Oid::hash(b"base");

        assert!(cache.get_merge_base(&repo_id, &commits).is_none());

        cache.put_merge_base(&repo_id, &commits, Some(merge_base));
        assert_eq!(cache.get_merge_base(&repo_id, &commits), Some(Some(merge_base)));
    }

    #[test]
    fn test_invalidate_repo() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        cache.put_ancestry(&repo1, &oid1, &oid2, true);
        cache.put_ancestry(&repo2, &oid1, &oid2, false);

        cache.invalidate_repo(&repo1);

        assert!(cache.get_ancestry(&repo1, &oid1, &oid2).is_none());
        assert_eq!(cache.get_ancestry(&repo2, &oid1, &oid2), Some(false));
    }

    #[test]
    fn test_clear() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        cache.put_ancestry(&repo_id, &oid1, &oid2, true);

        cache.clear();

        assert!(cache.get_ancestry(&repo_id, &oid1, &oid2).is_none());
    }

    #[test]
    fn test_ttl_expiration() {
        let cache = GraphCache::new(100, Duration::from_millis(1));
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        cache.put_ancestry(&repo_id, &oid1, &oid2, true);

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get_ancestry(&repo_id, &oid1, &oid2).is_none());
    }
}
