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

#[derive(Clone, Debug)]
enum CacheKeyRef {
    Ancestry(Oid, Oid),
    Reachable(Oid),
    MergeBase(Vec<Oid>),
}

struct GraphCacheState {
    ancestry_cache: LruCache<AncestryKey, (bool, Instant)>,
    reachable_cache: LruCache<ReachableKey, ReachableCacheValue>,
    merge_base_cache: LruCache<MergeBaseKey, (Option<Oid>, Instant)>,
    repo_keys: HashMap<Arc<str>, Arc<str>>,
    repo_index: HashMap<Arc<str>, Vec<CacheKeyRef>>,
}

impl GraphCacheState {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            ancestry_cache: LruCache::new(capacity),
            reachable_cache: LruCache::new(capacity),
            merge_base_cache: LruCache::new(capacity),
            repo_keys: HashMap::new(),
            repo_index: HashMap::new(),
        }
    }

    fn get_or_intern_repo_key(&mut self, repo_id: &RepoId) -> Arc<str> {
        let repo_str = repo_id.as_str();
        if let Some(key) = self.repo_keys.get(repo_str) {
            return Arc::clone(key);
        }
        let key: Arc<str> = repo_str.into();
        self.repo_keys.insert(Arc::clone(&key), Arc::clone(&key));
        key
    }
}

pub struct GraphCache {
    state: RwLock<GraphCacheState>,
    entry_ttl: Duration,
}

impl GraphCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            state: RwLock::new(GraphCacheState::new(cap)),
            entry_ttl: ttl,
        }
    }

    pub fn get_ancestry(&self, repo_id: &RepoId, ancestor: &Oid, descendant: &Oid) -> Option<bool> {
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let key = (repo_key, *ancestor, *descendant);
        if let Some((value, timestamp)) = state.ancestry_cache.peek(&key) {
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
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let key = (Arc::clone(&repo_key), *ancestor, *descendant);
        state.ancestry_cache.put(key, (is_ancestor, Instant::now()));
        state
            .repo_index
            .entry(repo_key)
            .or_default()
            .push(CacheKeyRef::Ancestry(*ancestor, *descendant));
    }

    pub fn get_reachable(&self, repo_id: &RepoId, from: &Oid) -> Option<Arc<HashSet<Oid>>> {
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let key = (repo_key, *from);
        if let Some((value, timestamp)) = state.reachable_cache.peek(&key) {
            if timestamp.elapsed() <= self.entry_ttl {
                return Some(Arc::clone(value));
            }
        }
        None
    }

    pub fn put_reachable(&self, repo_id: &RepoId, from: &Oid, reachable: HashSet<Oid>) {
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let key = (Arc::clone(&repo_key), *from);
        state
            .reachable_cache
            .put(key, (Arc::new(reachable), Instant::now()));
        state
            .repo_index
            .entry(repo_key)
            .or_default()
            .push(CacheKeyRef::Reachable(*from));
    }

    pub fn get_merge_base(&self, repo_id: &RepoId, commits: &[Oid]) -> Option<Option<Oid>> {
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let key = (repo_key, commits.to_vec());
        if let Some((value, timestamp)) = state.merge_base_cache.peek(&key) {
            if timestamp.elapsed() <= self.entry_ttl {
                return Some(*value);
            }
        }
        None
    }

    pub fn put_merge_base(&self, repo_id: &RepoId, commits: &[Oid], merge_base: Option<Oid>) {
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let key = (Arc::clone(&repo_key), commits.to_vec());
        state
            .merge_base_cache
            .put(key, (merge_base, Instant::now()));
        state
            .repo_index
            .entry(repo_key)
            .or_default()
            .push(CacheKeyRef::MergeBase(commits.to_vec()));
    }

    pub fn invalidate_repo(&self, repo_id: &RepoId) {
        let mut state = self.state.write();
        let repo_key = state.get_or_intern_repo_key(repo_id);
        let keys = state.repo_index.remove(&repo_key).unwrap_or_default();

        if keys.is_empty() {
            return;
        }

        for key_ref in keys {
            match key_ref {
                CacheKeyRef::Ancestry(ancestor, descendant) => {
                    state
                        .ancestry_cache
                        .pop(&(Arc::clone(&repo_key), ancestor, descendant));
                }
                CacheKeyRef::Reachable(from) => {
                    state.reachable_cache.pop(&(Arc::clone(&repo_key), from));
                }
                CacheKeyRef::MergeBase(commits) => {
                    state
                        .merge_base_cache
                        .pop(&(Arc::clone(&repo_key), commits));
                }
            }
        }
    }

    pub fn clear(&self) {
        let mut state = self.state.write();
        state.ancestry_cache.clear();
        state.reachable_cache.clear();
        state.merge_base_cache.clear();
        state.repo_keys.clear();
        state.repo_index.clear();
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
