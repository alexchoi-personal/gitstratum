use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::RwLock;
use rocksdb::{ColumnFamily, Options, DB};

use gitstratum_core::{Commit, Oid, RefName, RepoId, Tree};

use crate::error::{MetadataStoreError, Result};
use crate::store::column_families::{
    create_cf_descriptors, CF_COMMITS, CF_REFS, CF_REPOS, CF_TREES,
};

const DEFAULT_CACHE_SIZE: usize = 10000;

type CacheKey = (Arc<str>, Oid);

pub struct MetadataStore {
    db: Arc<DB>,
    commit_cache: RwLock<LruCache<CacheKey, Commit>>,
    tree_cache: RwLock<LruCache<CacheKey, Tree>>,
}

impl MetadataStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_cache_size(path, DEFAULT_CACHE_SIZE)
    }

    pub fn open_with_cache_size<P: AsRef<Path>>(path: P, cache_size: usize) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = create_cf_descriptors();
        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)?;

        let cache_size = NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(1).unwrap());

        Ok(Self {
            db: Arc::new(db),
            commit_cache: RwLock::new(LruCache::new(cache_size)),
            tree_cache: RwLock::new(LruCache::new(cache_size)),
        })
    }

    fn cf_repos(&self) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(CF_REPOS)
            .ok_or_else(|| MetadataStoreError::ColumnFamilyNotFound(CF_REPOS.to_string()))
    }

    fn cf_refs(&self) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(CF_REFS)
            .ok_or_else(|| MetadataStoreError::ColumnFamilyNotFound(CF_REFS.to_string()))
    }

    fn cf_commits(&self) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(CF_COMMITS)
            .ok_or_else(|| MetadataStoreError::ColumnFamilyNotFound(CF_COMMITS.to_string()))
    }

    fn cf_trees(&self) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(CF_TREES)
            .ok_or_else(|| MetadataStoreError::ColumnFamilyNotFound(CF_TREES.to_string()))
    }

    fn repo_key(repo_id: &RepoId) -> Vec<u8> {
        repo_id.as_str().as_bytes().to_vec()
    }

    fn ref_key(repo_id: &RepoId, ref_name: &RefName) -> Vec<u8> {
        let repo = repo_id.as_str();
        let refn = ref_name.as_str();
        let mut key = Vec::with_capacity(repo.len() + 5 + refn.len());
        key.extend_from_slice(repo.as_bytes());
        key.extend_from_slice(b"/ref/");
        key.extend_from_slice(refn.as_bytes());
        key
    }

    fn commit_key(repo_id: &RepoId, oid: &Oid) -> Vec<u8> {
        let repo = repo_id.as_str();
        let mut key = Vec::with_capacity(repo.len() + 8 + 64);
        key.extend_from_slice(repo.as_bytes());
        key.extend_from_slice(b"/commit/");
        oid.write_hex(&mut key);
        key
    }

    fn tree_key(repo_id: &RepoId, oid: &Oid) -> Vec<u8> {
        let repo = repo_id.as_str();
        let mut key = Vec::with_capacity(repo.len() + 6 + 64);
        key.extend_from_slice(repo.as_bytes());
        key.extend_from_slice(b"/tree/");
        oid.write_hex(&mut key);
        key
    }

    pub fn create_repo(&self, repo_id: &RepoId) -> Result<()> {
        let key = Self::repo_key(repo_id);
        if self.db.get_cf(self.cf_repos()?, &key)?.is_some() {
            return Err(MetadataStoreError::RepoAlreadyExists(
                repo_id.as_str().to_string(),
            ));
        }
        self.db.put_cf(self.cf_repos()?, &key, b"1")?;
        Ok(())
    }

    pub fn delete_repo(&self, repo_id: &RepoId) -> Result<()> {
        let key = Self::repo_key(repo_id);
        if self.db.get_cf(self.cf_repos()?, &key)?.is_none() {
            return Err(MetadataStoreError::RepoNotFound(
                repo_id.as_str().to_string(),
            ));
        }

        let prefix = format!("{}/", repo_id.as_str());
        let prefix_bytes = prefix.as_bytes();

        let cf_refs = self.cf_refs()?;
        let refs_to_delete: Vec<_> = self
            .db
            .prefix_iterator_cf(cf_refs, prefix_bytes)
            .take_while(|item| {
                if let Ok((k, _)) = item {
                    k.starts_with(prefix_bytes)
                } else {
                    false
                }
            })
            .filter_map(|item| item.ok().map(|(k, _)| k.to_vec()))
            .collect();

        for k in refs_to_delete {
            self.db.delete_cf(cf_refs, &k)?;
        }

        let cf_commits = self.cf_commits()?;
        let commits_to_delete: Vec<_> = self
            .db
            .prefix_iterator_cf(cf_commits, prefix_bytes)
            .take_while(|item| {
                if let Ok((k, _)) = item {
                    k.starts_with(prefix_bytes)
                } else {
                    false
                }
            })
            .filter_map(|item| item.ok().map(|(k, _)| k.to_vec()))
            .collect();

        for k in commits_to_delete {
            self.db.delete_cf(cf_commits, &k)?;
        }

        let cf_trees = self.cf_trees()?;
        let trees_to_delete: Vec<_> = self
            .db
            .prefix_iterator_cf(cf_trees, prefix_bytes)
            .take_while(|item| {
                if let Ok((k, _)) = item {
                    k.starts_with(prefix_bytes)
                } else {
                    false
                }
            })
            .filter_map(|item| item.ok().map(|(k, _)| k.to_vec()))
            .collect();

        for k in trees_to_delete {
            self.db.delete_cf(cf_trees, &k)?;
        }

        self.db.delete_cf(self.cf_repos()?, &key)?;
        Ok(())
    }

    pub fn list_repos(
        &self,
        prefix: &str,
        limit: usize,
        cursor: &str,
    ) -> Result<(Vec<RepoId>, Option<String>)> {
        let mut repos = Vec::new();
        let prefix_bytes = prefix.as_bytes();
        let cursor_bytes = cursor.as_bytes();

        let cf_repos = self.cf_repos()?;
        let iter = if cursor.is_empty() {
            self.db.prefix_iterator_cf(cf_repos, prefix_bytes)
        } else {
            self.db.prefix_iterator_cf(cf_repos, cursor_bytes)
        };

        for item in iter {
            let (k, _) = item?;
            if !k.starts_with(prefix_bytes) {
                break;
            }
            if !cursor.is_empty() && k.as_ref() <= cursor_bytes {
                continue;
            }
            let repo_str = String::from_utf8_lossy(&k).to_string();
            if let Ok(repo_id) = RepoId::new(&repo_str) {
                repos.push(repo_id);
                if repos.len() >= limit {
                    break;
                }
            }
        }

        let next_cursor = if repos.len() >= limit {
            repos.last().map(|r| r.as_str().to_string())
        } else {
            None
        };

        Ok((repos, next_cursor))
    }

    pub fn repo_exists(&self, repo_id: &RepoId) -> Result<bool> {
        let key = Self::repo_key(repo_id);
        Ok(self.db.get_cf(self.cf_repos()?, &key)?.is_some())
    }

    pub fn get_ref(&self, repo_id: &RepoId, ref_name: &RefName) -> Result<Option<Oid>> {
        let key = Self::ref_key(repo_id, ref_name);
        match self.db.get_cf(self.cf_refs()?, &key)? {
            Some(v) => {
                let oid = Oid::from_slice(&v)
                    .map_err(|e| MetadataStoreError::Deserialization(e.to_string()))?;
                Ok(Some(oid))
            }
            None => Ok(None),
        }
    }

    pub fn list_refs(&self, repo_id: &RepoId, prefix: &str) -> Result<Vec<(RefName, Oid)>> {
        let key_prefix = format!("{}/ref/{}", repo_id.as_str(), prefix);
        let prefix_bytes = key_prefix.as_bytes();
        let repo_prefix = format!("{}/ref/", repo_id.as_str());
        let repo_prefix_len = repo_prefix.len();

        let mut refs = Vec::new();

        for item in self.db.prefix_iterator_cf(self.cf_refs()?, prefix_bytes) {
            let (k, v) = item?;
            if !k.starts_with(prefix_bytes) {
                break;
            }
            let key_str = String::from_utf8_lossy(&k);
            if key_str.len() > repo_prefix_len {
                let ref_str = &key_str[repo_prefix_len..];
                if let Ok(ref_name) = RefName::new(ref_str) {
                    if let Ok(oid) = Oid::from_slice(&v) {
                        refs.push((ref_name, oid));
                    }
                }
            }
        }

        Ok(refs)
    }

    pub fn update_ref(
        &self,
        repo_id: &RepoId,
        ref_name: &RefName,
        old_target: Option<&Oid>,
        new_target: &Oid,
        force: bool,
    ) -> Result<()> {
        let key = Self::ref_key(repo_id, ref_name);
        let cf_refs = self.cf_refs()?;

        if !force {
            let current = self.db.get_cf(cf_refs, &key)?;
            match (old_target, current) {
                (Some(expected), Some(actual)) => {
                    let actual_oid = Oid::from_slice(&actual)
                        .map_err(|e| MetadataStoreError::Deserialization(e.to_string()))?;
                    if expected != &actual_oid {
                        return Err(MetadataStoreError::CasFailed {
                            expected: expected.to_hex(),
                            found: actual_oid.to_hex(),
                        });
                    }
                }
                (Some(expected), None) => {
                    if !expected.is_zero() {
                        return Err(MetadataStoreError::CasFailed {
                            expected: expected.to_hex(),
                            found: "none".to_string(),
                        });
                    }
                }
                (None, Some(actual)) => {
                    let actual_oid = Oid::from_slice(&actual)
                        .map_err(|e| MetadataStoreError::Deserialization(e.to_string()))?;
                    return Err(MetadataStoreError::CasFailed {
                        expected: "none".to_string(),
                        found: actual_oid.to_hex(),
                    });
                }
                (None, None) => {}
            }
        }

        self.db.put_cf(cf_refs, &key, new_target.as_bytes())?;
        Ok(())
    }

    pub fn delete_ref(&self, repo_id: &RepoId, ref_name: &RefName) -> Result<()> {
        let key = Self::ref_key(repo_id, ref_name);
        self.db.delete_cf(self.cf_refs()?, &key)?;
        Ok(())
    }

    pub fn get_commit(&self, repo_id: &RepoId, oid: &Oid) -> Result<Option<Commit>> {
        let cache_key: CacheKey = (Arc::from(repo_id.as_str()), *oid);

        {
            let cache = self.commit_cache.read();
            if let Some(commit) = cache.peek(&cache_key) {
                return Ok(Some(commit.clone()));
            }
        }

        let key = Self::commit_key(repo_id, oid);
        match self.db.get_cf(self.cf_commits()?, &key)? {
            Some(v) => {
                let commit: Commit = bincode::deserialize(&v)?;
                {
                    let mut cache = self.commit_cache.write();
                    cache.put(cache_key, commit.clone());
                }
                Ok(Some(commit))
            }
            None => Ok(None),
        }
    }

    pub fn put_commit(&self, repo_id: &RepoId, commit: &Commit) -> Result<()> {
        let key = Self::commit_key(repo_id, &commit.oid);
        let value = bincode::serialize(commit)?;
        self.db.put_cf(self.cf_commits()?, &key, &value)?;

        let cache_key: CacheKey = (Arc::from(repo_id.as_str()), commit.oid);
        let mut cache = self.commit_cache.write();
        cache.put(cache_key, commit.clone());

        Ok(())
    }

    pub fn get_tree(&self, repo_id: &RepoId, oid: &Oid) -> Result<Option<Tree>> {
        let cache_key: CacheKey = (Arc::from(repo_id.as_str()), *oid);

        {
            let cache = self.tree_cache.read();
            if let Some(tree) = cache.peek(&cache_key) {
                return Ok(Some(tree.clone()));
            }
        }

        let key = Self::tree_key(repo_id, oid);
        match self.db.get_cf(self.cf_trees()?, &key)? {
            Some(v) => {
                let tree: Tree = bincode::deserialize(&v)?;
                {
                    let mut cache = self.tree_cache.write();
                    cache.put(cache_key, tree.clone());
                }
                Ok(Some(tree))
            }
            None => Ok(None),
        }
    }

    pub fn put_tree(&self, repo_id: &RepoId, tree: &Tree) -> Result<()> {
        let key = Self::tree_key(repo_id, &tree.oid);
        let value = bincode::serialize(tree)?;
        self.db.put_cf(self.cf_trees()?, &key, &value)?;

        let cache_key: CacheKey = (Arc::from(repo_id.as_str()), tree.oid);
        let mut cache = self.tree_cache.write();
        cache.put(cache_key, tree.clone());

        Ok(())
    }

    pub fn clear_cache(&self) {
        self.commit_cache.write().clear();
        self.tree_cache.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Signature;
    use tempfile::TempDir;

    fn create_test_store() -> (MetadataStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::open(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[test]
    fn test_open_default_cache_size() {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::open(temp_dir.path()).unwrap();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();
        assert!(store.repo_exists(&repo_id).unwrap());
    }

    #[test]
    fn test_open_zero_cache_size() {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::open_with_cache_size(temp_dir.path(), 0).unwrap();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();
        assert!(store.repo_exists(&repo_id).unwrap());
    }

    #[test]
    fn test_create_repo_already_exists() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();

        store.create_repo(&repo_id).unwrap();
        let result = store.create_repo(&repo_id);
        assert!(matches!(
            result,
            Err(MetadataStoreError::RepoAlreadyExists(_))
        ));
    }

    #[test]
    fn test_delete_repo() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();

        store.create_repo(&repo_id).unwrap();
        assert!(store.repo_exists(&repo_id).unwrap());

        store.delete_repo(&repo_id).unwrap();
        assert!(!store.repo_exists(&repo_id).unwrap());
    }

    #[test]
    fn test_delete_repo_not_found() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("nonexistent/repo").unwrap();

        let result = store.delete_repo(&repo_id);
        assert!(matches!(result, Err(MetadataStoreError::RepoNotFound(_))));
    }

    #[test]
    fn test_list_repos() {
        let (store, _dir) = create_test_store();

        let repo1 = RepoId::new("org/repo1").unwrap();
        let repo2 = RepoId::new("org/repo2").unwrap();
        let repo3 = RepoId::new("other/repo").unwrap();

        store.create_repo(&repo1).unwrap();
        store.create_repo(&repo2).unwrap();
        store.create_repo(&repo3).unwrap();

        let (repos, cursor) = store.list_repos("org/", 10, "").unwrap();
        assert_eq!(repos.len(), 2);
        assert!(cursor.is_none());
    }

    #[test]
    fn test_list_repos_with_limit() {
        let (store, _dir) = create_test_store();

        let repo1 = RepoId::new("org/repo1").unwrap();
        let repo2 = RepoId::new("org/repo2").unwrap();

        store.create_repo(&repo1).unwrap();
        store.create_repo(&repo2).unwrap();

        let (repos, cursor) = store.list_repos("org/", 1, "").unwrap();
        assert_eq!(repos.len(), 1);
        assert!(cursor.is_some());

        let (repos2, _) = store.list_repos("org/", 1, &cursor.unwrap()).unwrap();
        assert_eq!(repos2.len(), 1);
    }

    #[test]
    fn test_ref_operations() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit1");

        assert!(store.get_ref(&repo_id, &ref_name).unwrap().is_none());

        store
            .update_ref(&repo_id, &ref_name, None, &oid, true)
            .unwrap();
        assert_eq!(store.get_ref(&repo_id, &ref_name).unwrap(), Some(oid));

        store.delete_ref(&repo_id, &ref_name).unwrap();
        assert!(store.get_ref(&repo_id, &ref_name).unwrap().is_none());
    }

    #[test]
    fn test_update_ref_cas_match() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");

        store
            .update_ref(&repo_id, &ref_name, None, &oid1, false)
            .unwrap();

        store
            .update_ref(&repo_id, &ref_name, Some(&oid1), &oid2, false)
            .unwrap();
        assert_eq!(store.get_ref(&repo_id, &ref_name).unwrap(), Some(oid2));
    }

    #[test]
    fn test_update_ref_cas_mismatch() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");
        let wrong_oid = Oid::hash(b"wrong");

        store
            .update_ref(&repo_id, &ref_name, None, &oid1, true)
            .unwrap();

        let result = store.update_ref(&repo_id, &ref_name, Some(&wrong_oid), &oid2, false);
        assert!(matches!(result, Err(MetadataStoreError::CasFailed { .. })));
    }

    #[test]
    fn test_update_ref_cas_expected_none() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");

        store
            .update_ref(&repo_id, &ref_name, None, &oid1, true)
            .unwrap();

        let result = store.update_ref(&repo_id, &ref_name, None, &oid2, false);
        assert!(matches!(result, Err(MetadataStoreError::CasFailed { .. })));
    }

    #[test]
    fn test_update_ref_cas_expected_nonzero_actual_none() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let expected = Oid::hash(b"expected");
        let new_oid = Oid::hash(b"new");

        let result = store.update_ref(&repo_id, &ref_name, Some(&expected), &new_oid, false);
        assert!(matches!(result, Err(MetadataStoreError::CasFailed { .. })));
    }

    #[test]
    fn test_update_ref_cas_zero_expected_when_none() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let new_oid = Oid::hash(b"new");

        store
            .update_ref(&repo_id, &ref_name, Some(&Oid::ZERO), &new_oid, false)
            .unwrap();
        assert_eq!(store.get_ref(&repo_id, &ref_name).unwrap(), Some(new_oid));
    }

    #[test]
    fn test_list_refs() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let main = RefName::new("refs/heads/main").unwrap();
        let feature = RefName::new("refs/heads/feature").unwrap();
        let tag = RefName::new("refs/tags/v1.0").unwrap();

        let oid = Oid::hash(b"commit");
        store.update_ref(&repo_id, &main, None, &oid, true).unwrap();
        store
            .update_ref(&repo_id, &feature, None, &oid, true)
            .unwrap();
        store.update_ref(&repo_id, &tag, None, &oid, true).unwrap();

        let refs = store.list_refs(&repo_id, "refs/heads/").unwrap();
        assert_eq!(refs.len(), 2);

        let refs = store.list_refs(&repo_id, "refs/tags/").unwrap();
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn test_commit_operations() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let commit = Commit {
            oid: Oid::hash(b"commit1"),
            tree: Oid::hash(b"tree1"),
            parents: vec![],
            message: "Initial commit".to_string(),
            author: Signature::new("Test Author", "test@example.com", 0, "+0000"),
            committer: Signature::new("Test Committer", "test@example.com", 0, "+0000"),
        };

        assert!(store.get_commit(&repo_id, &commit.oid).unwrap().is_none());

        store.put_commit(&repo_id, &commit).unwrap();

        let retrieved = store.get_commit(&repo_id, &commit.oid).unwrap().unwrap();
        assert_eq!(retrieved.message, "Initial commit");

        let cached = store.get_commit(&repo_id, &commit.oid).unwrap().unwrap();
        assert_eq!(cached.message, "Initial commit");
    }

    #[test]
    fn test_tree_operations() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let tree = Tree {
            oid: Oid::hash(b"tree1"),
            entries: vec![],
        };

        assert!(store.get_tree(&repo_id, &tree.oid).unwrap().is_none());

        store.put_tree(&repo_id, &tree).unwrap();

        let retrieved = store.get_tree(&repo_id, &tree.oid).unwrap().unwrap();
        assert_eq!(retrieved.oid, tree.oid);

        let cached = store.get_tree(&repo_id, &tree.oid).unwrap().unwrap();
        assert_eq!(cached.oid, tree.oid);
    }

    #[test]
    fn test_clear_cache() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let commit = Commit {
            oid: Oid::hash(b"commit1"),
            tree: Oid::hash(b"tree1"),
            parents: vec![],
            message: "Test".to_string(),
            author: Signature::new("Test", "test@example.com", 0, "+0000"),
            committer: Signature::new("Test", "test@example.com", 0, "+0000"),
        };
        store.put_commit(&repo_id, &commit).unwrap();

        store.clear_cache();

        let retrieved = store.get_commit(&repo_id, &commit.oid).unwrap().unwrap();
        assert_eq!(retrieved.message, "Test");
    }

    #[test]
    fn test_delete_repo_with_data() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit1");
        store
            .update_ref(&repo_id, &ref_name, None, &oid, true)
            .unwrap();

        let commit = Commit {
            oid: Oid::hash(b"commit1"),
            tree: Oid::hash(b"tree1"),
            parents: vec![],
            message: "Test".to_string(),
            author: Signature::new("Test", "test@example.com", 0, "+0000"),
            committer: Signature::new("Test", "test@example.com", 0, "+0000"),
        };
        store.put_commit(&repo_id, &commit).unwrap();

        let tree = Tree {
            oid: Oid::hash(b"tree1"),
            entries: vec![],
        };
        store.put_tree(&repo_id, &tree).unwrap();

        store.delete_repo(&repo_id).unwrap();

        assert!(!store.repo_exists(&repo_id).unwrap());
        assert!(store.get_ref(&repo_id, &ref_name).unwrap().is_none());
    }
}
