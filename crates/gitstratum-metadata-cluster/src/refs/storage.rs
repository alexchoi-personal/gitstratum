use gitstratum_core::{Oid, RefName, RepoId};

use crate::error::Result;
use crate::store::MetadataStore;

pub struct RefStorage<'a> {
    store: &'a MetadataStore,
}

impl<'a> RefStorage<'a> {
    pub fn new(store: &'a MetadataStore) -> Self {
        Self { store }
    }

    pub fn get(&self, repo_id: &RepoId, ref_name: &RefName) -> Result<Option<Oid>> {
        self.store.get_ref(repo_id, ref_name)
    }

    pub fn list(&self, repo_id: &RepoId, prefix: &str) -> Result<Vec<(RefName, Oid)>> {
        self.store.list_refs(repo_id, prefix)
    }

    pub fn list_heads(&self, repo_id: &RepoId) -> Result<Vec<(RefName, Oid)>> {
        self.list(repo_id, "refs/heads/")
    }

    pub fn list_tags(&self, repo_id: &RepoId) -> Result<Vec<(RefName, Oid)>> {
        self.list(repo_id, "refs/tags/")
    }

    pub fn list_remotes(&self, repo_id: &RepoId, remote: &str) -> Result<Vec<(RefName, Oid)>> {
        let prefix = format!("refs/remotes/{}/", remote);
        self.list(repo_id, &prefix)
    }

    pub fn get_head(&self, repo_id: &RepoId) -> Result<Option<Oid>> {
        let head_ref = RefName::new("HEAD").expect("HEAD is a valid ref name");
        self.get(repo_id, &head_ref)
    }

    pub fn get_main(&self, repo_id: &RepoId) -> Result<Option<Oid>> {
        let main_ref =
            RefName::new("refs/heads/main").expect("refs/heads/main is a valid ref name");
        if let Some(oid) = self.get(repo_id, &main_ref)? {
            return Ok(Some(oid));
        }

        let master_ref =
            RefName::new("refs/heads/master").expect("refs/heads/master is a valid ref name");
        self.get(repo_id, &master_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (MetadataStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::open(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[test]
    fn test_ref_storage_get() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_storage = RefStorage::new(&store);
        let ref_name = RefName::new("refs/heads/main").unwrap();

        assert!(ref_storage.get(&repo_id, &ref_name).unwrap().is_none());

        let oid = Oid::hash(b"commit1");
        store
            .update_ref(&repo_id, &ref_name, None, &oid, true)
            .unwrap();

        assert_eq!(ref_storage.get(&repo_id, &ref_name).unwrap(), Some(oid));
    }

    #[test]
    fn test_list_heads() {
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

        let ref_storage = RefStorage::new(&store);
        let heads = ref_storage.list_heads(&repo_id).unwrap();
        assert_eq!(heads.len(), 2);
    }

    #[test]
    fn test_list_tags() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let main = RefName::new("refs/heads/main").unwrap();
        let tag1 = RefName::new("refs/tags/v1.0").unwrap();
        let tag2 = RefName::new("refs/tags/v2.0").unwrap();

        let oid = Oid::hash(b"commit");
        store.update_ref(&repo_id, &main, None, &oid, true).unwrap();
        store.update_ref(&repo_id, &tag1, None, &oid, true).unwrap();
        store.update_ref(&repo_id, &tag2, None, &oid, true).unwrap();

        let ref_storage = RefStorage::new(&store);
        let tags = ref_storage.list_tags(&repo_id).unwrap();
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_get_main_with_main_branch() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let main = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");
        store.update_ref(&repo_id, &main, None, &oid, true).unwrap();

        let ref_storage = RefStorage::new(&store);
        assert_eq!(ref_storage.get_main(&repo_id).unwrap(), Some(oid));
    }

    #[test]
    fn test_get_main_with_master_branch() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let master = RefName::new("refs/heads/master").unwrap();
        let oid = Oid::hash(b"commit");
        store
            .update_ref(&repo_id, &master, None, &oid, true)
            .unwrap();

        let ref_storage = RefStorage::new(&store);
        assert_eq!(ref_storage.get_main(&repo_id).unwrap(), Some(oid));
    }

    #[test]
    fn test_get_main_none() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_storage = RefStorage::new(&store);
        assert!(ref_storage.get_main(&repo_id).unwrap().is_none());
    }

    #[test]
    fn test_list_remotes() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let origin_main = RefName::new("refs/remotes/origin/main").unwrap();
        let origin_dev = RefName::new("refs/remotes/origin/dev").unwrap();
        let upstream_main = RefName::new("refs/remotes/upstream/main").unwrap();

        let oid = Oid::hash(b"commit");
        store
            .update_ref(&repo_id, &origin_main, None, &oid, true)
            .unwrap();
        store
            .update_ref(&repo_id, &origin_dev, None, &oid, true)
            .unwrap();
        store
            .update_ref(&repo_id, &upstream_main, None, &oid, true)
            .unwrap();

        let ref_storage = RefStorage::new(&store);
        let origin_refs = ref_storage.list_remotes(&repo_id, "origin").unwrap();
        assert_eq!(origin_refs.len(), 2);

        let upstream_refs = ref_storage.list_remotes(&repo_id, "upstream").unwrap();
        assert_eq!(upstream_refs.len(), 1);
    }

    #[test]
    fn test_get_head() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_storage = RefStorage::new(&store);
        assert!(ref_storage.get_head(&repo_id).unwrap().is_none());

        let head = RefName::new("HEAD").unwrap();
        let oid = Oid::hash(b"commit");
        store.update_ref(&repo_id, &head, None, &oid, true).unwrap();

        assert_eq!(ref_storage.get_head(&repo_id).unwrap(), Some(oid));
    }
}
