use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;

use gitstratum_core::{Oid, RepoId};

type RepoKey = Arc<str>;
type AncestryKey = (RepoKey, Oid, Oid);
type ReachableKey = (RepoKey, Oid);
type MergeBaseKey = (RepoKey, Vec<Oid>);
type ReachableCacheValue = (Arc<HashSet<Oid>>, Instant);

pub struct GraphCache {
    ancestry_cache: RwLock<LruCache<AncestryKey, (bool, Instant)>>,
    reachable_cache: RwLock<LruCache<ReachableKey, ReachableCacheValue>>,
    merge_base_cache: RwLock<LruCache<MergeBaseKey, (Option<Oid>, Instant)>>,
    entry_ttl: Duration,
    repo_keys: RwLock<HashMap<Arc<str>, Arc<str>>>,
    repo_index: RwLock<HashMap<Arc<str>, Vec<CacheKeyRef>>>,
}

#[derive(Clone, Debug)]
enum CacheKeyRef {
    Ancestry(Oid, Oid),
    Reachable(Oid),
    MergeBase(Vec<Oid>),
}

impl GraphCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            ancestry_cache: RwLock::new(LruCache::new(cap)),
            reachable_cache: RwLock::new(LruCache::new(cap)),
            merge_base_cache: RwLock::new(LruCache::new(cap)),
            entry_ttl: ttl,
            repo_keys: RwLock::new(HashMap::new()),
            repo_index: RwLock::new(HashMap::new()),
        }
    }

    fn get_or_intern_repo_key(&self, repo_id: &RepoId) -> Arc<str> {
        let repo_str = repo_id.as_str();
        {
            let keys = self.repo_keys.read();
            if let Some(key) = keys.get(repo_str) {
                return Arc::clone(key);
            }
        }
        let mut keys = self.repo_keys.write();
        if let Some(key) = keys.get(repo_str) {
            return Arc::clone(key);
        }
        let key: Arc<str> = repo_str.into();
        keys.insert(Arc::clone(&key), Arc::clone(&key));
        key
    }

    pub fn get_ancestry(&self, repo_id: &RepoId, ancestor: &Oid, descendant: &Oid) -> Option<bool> {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let key = (repo_key, *ancestor, *descendant);
        let cache = self.ancestry_cache.read();
        if let Some((value, timestamp)) = cache.peek(&key) {
            if timestamp.elapsed() <= self.entry_ttl {
                return Some(*value);
            }
        }
        None
    }

    pub fn put_ancestry(
        &self,
        repo_id: &RepoId,
        ancestor: &Oid,
        descendant: &Oid,
        is_ancestor: bool,
    ) {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let key = (Arc::clone(&repo_key), *ancestor, *descendant);
        let mut cache = self.ancestry_cache.write();
        cache.put(key, (is_ancestor, Instant::now()));

        let mut index = self.repo_index.write();
        index
            .entry(repo_key)
            .or_default()
            .push(CacheKeyRef::Ancestry(*ancestor, *descendant));
    }

    pub fn get_reachable(&self, repo_id: &RepoId, from: &Oid) -> Option<Arc<HashSet<Oid>>> {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let key = (repo_key, *from);
        let cache = self.reachable_cache.read();
        if let Some((value, timestamp)) = cache.peek(&key) {
            if timestamp.elapsed() <= self.entry_ttl {
                return Some(Arc::clone(value));
            }
        }
        None
    }

    pub fn put_reachable(&self, repo_id: &RepoId, from: &Oid, reachable: HashSet<Oid>) {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let key = (Arc::clone(&repo_key), *from);
        let mut cache = self.reachable_cache.write();
        cache.put(key, (Arc::new(reachable), Instant::now()));

        let mut index = self.repo_index.write();
        index
            .entry(repo_key)
            .or_default()
            .push(CacheKeyRef::Reachable(*from));
    }

    pub fn get_merge_base(&self, repo_id: &RepoId, commits: &[Oid]) -> Option<Option<Oid>> {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let key = (repo_key, commits.to_vec());
        let cache = self.merge_base_cache.read();
        if let Some((value, timestamp)) = cache.peek(&key) {
            if timestamp.elapsed() <= self.entry_ttl {
                return Some(*value);
            }
        }
        None
    }

    pub fn put_merge_base(&self, repo_id: &RepoId, commits: &[Oid], merge_base: Option<Oid>) {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let key = (Arc::clone(&repo_key), commits.to_vec());
        let mut cache = self.merge_base_cache.write();
        cache.put(key, (merge_base, Instant::now()));

        let mut index = self.repo_index.write();
        index
            .entry(repo_key)
            .or_default()
            .push(CacheKeyRef::MergeBase(commits.to_vec()));
    }

    pub fn invalidate_repo(&self, repo_id: &RepoId) {
        let repo_key = self.get_or_intern_repo_key(repo_id);
        let keys = {
            let mut index = self.repo_index.write();
            index.remove(&repo_key).unwrap_or_default()
        };

        if keys.is_empty() {
            return;
        }

        let mut ancestry_keys = Vec::new();
        let mut reachable_keys = Vec::new();
        let mut merge_base_keys = Vec::new();

        for key_ref in keys {
            match key_ref {
                CacheKeyRef::Ancestry(ancestor, descendant) => {
                    ancestry_keys.push((ancestor, descendant));
                }
                CacheKeyRef::Reachable(from) => {
                    reachable_keys.push(from);
                }
                CacheKeyRef::MergeBase(commits) => {
                    merge_base_keys.push(commits);
                }
            }
        }

        if !ancestry_keys.is_empty() {
            let mut cache = self.ancestry_cache.write();
            for (ancestor, descendant) in ancestry_keys {
                cache.pop(&(Arc::clone(&repo_key), ancestor, descendant));
            }
        }

        if !reachable_keys.is_empty() {
            let mut cache = self.reachable_cache.write();
            for from in reachable_keys {
                cache.pop(&(Arc::clone(&repo_key), from));
            }
        }

        if !merge_base_keys.is_empty() {
            let mut cache = self.merge_base_cache.write();
            for commits in merge_base_keys {
                cache.pop(&(Arc::clone(&repo_key), commits));
            }
        }
    }

    pub fn clear(&self) {
        self.ancestry_cache.write().clear();
        self.reachable_cache.write().clear();
        self.merge_base_cache.write().clear();
        self.repo_keys.write().clear();
        self.repo_index.write().clear();
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
        assert_eq!(
            cache.get_merge_base(&repo_id, &commits),
            Some(Some(merge_base))
        );
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

    #[test]
    fn test_invalidate_repo_all_cache_types() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");
        let from = Oid::hash(b"from");
        let commits = vec![Oid::hash(b"c1"), Oid::hash(b"c2")];
        let merge_base = Oid::hash(b"base");

        cache.put_ancestry(&repo1, &oid1, &oid2, true);
        cache.put_ancestry(&repo2, &oid1, &oid2, false);

        let mut reachable = HashSet::new();
        reachable.insert(oid1);
        cache.put_reachable(&repo1, &from, reachable.clone());
        cache.put_reachable(&repo2, &from, reachable);

        cache.put_merge_base(&repo1, &commits, Some(merge_base));
        cache.put_merge_base(&repo2, &commits, Some(merge_base));

        cache.invalidate_repo(&repo1);

        assert!(cache.get_ancestry(&repo1, &oid1, &oid2).is_none());
        assert_eq!(cache.get_ancestry(&repo2, &oid1, &oid2), Some(false));
        assert!(cache.get_reachable(&repo1, &from).is_none());
        assert!(cache.get_reachable(&repo2, &from).is_some());
        assert!(cache.get_merge_base(&repo1, &commits).is_none());
        assert_eq!(
            cache.get_merge_base(&repo2, &commits),
            Some(Some(merge_base))
        );
    }

    #[test]
    fn test_ttl_expiration_reachable() {
        let cache = GraphCache::new(100, Duration::from_millis(1));
        let repo_id = RepoId::new("test/repo").unwrap();
        let from = Oid::hash(b"from");

        let mut reachable = HashSet::new();
        reachable.insert(Oid::hash(b"oid1"));
        cache.put_reachable(&repo_id, &from, reachable);

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get_reachable(&repo_id, &from).is_none());
    }

    #[test]
    fn test_ttl_expiration_merge_base() {
        let cache = GraphCache::new(100, Duration::from_millis(1));
        let repo_id = RepoId::new("test/repo").unwrap();
        let commits = vec![Oid::hash(b"c1"), Oid::hash(b"c2")];
        let merge_base = Oid::hash(b"base");

        cache.put_merge_base(&repo_id, &commits, Some(merge_base));

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get_merge_base(&repo_id, &commits).is_none());
    }

    #[test]
    fn test_merge_base_none() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let commits = vec![Oid::hash(b"c1"), Oid::hash(b"c2")];

        cache.put_merge_base(&repo_id, &commits, None);
        assert_eq!(cache.get_merge_base(&repo_id, &commits), Some(None));
    }

    #[test]
    fn test_clear_all_caches() {
        let cache = GraphCache::new(100, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");
        let from = Oid::hash(b"from");
        let commits = vec![Oid::hash(b"c1")];

        cache.put_ancestry(&repo_id, &oid1, &oid2, true);
        let mut reachable = HashSet::new();
        reachable.insert(oid1);
        cache.put_reachable(&repo_id, &from, reachable);
        cache.put_merge_base(&repo_id, &commits, Some(Oid::hash(b"base")));

        cache.clear();

        assert!(cache.get_ancestry(&repo_id, &oid1, &oid2).is_none());
        assert!(cache.get_reachable(&repo_id, &from).is_none());
        assert!(cache.get_merge_base(&repo_id, &commits).is_none());
    }

    #[test]
    fn test_graph_cache_new_with_zero_capacity() {
        let cache = GraphCache::new(0, Duration::from_secs(60));
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        cache.put_ancestry(&repo_id, &oid1, &oid2, true);
        assert!(cache.get_ancestry(&repo_id, &oid1, &oid2).is_some());
    }
}
