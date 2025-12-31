use gitstratum_core::{Oid, RefName, RepoId};

use crate::error::{MetadataStoreError, Result};
use crate::store::MetadataStore;

#[derive(Debug, Clone)]
pub struct RefUpdate {
    pub ref_name: RefName,
    pub old_target: Option<Oid>,
    pub new_target: Oid,
    pub force: bool,
}

impl RefUpdate {
    pub fn new(ref_name: RefName, new_target: Oid) -> Self {
        Self {
            ref_name,
            old_target: None,
            new_target,
            force: false,
        }
    }

    pub fn with_old_target(mut self, old_target: Oid) -> Self {
        self.old_target = Some(old_target);
        self
    }

    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    pub fn create(ref_name: RefName, target: Oid) -> Self {
        Self {
            ref_name,
            old_target: Some(Oid::ZERO),
            new_target: target,
            force: false,
        }
    }
}

#[derive(Debug)]
pub struct RefUpdateResult {
    pub ref_name: RefName,
    pub success: bool,
    pub error: Option<MetadataStoreError>,
}

pub struct AtomicRefUpdater<'a> {
    store: &'a MetadataStore,
}

impl<'a> AtomicRefUpdater<'a> {
    pub fn new(store: &'a MetadataStore) -> Self {
        Self { store }
    }

    pub fn update_single(&self, repo_id: &RepoId, update: &RefUpdate) -> Result<()> {
        self.store.update_ref(
            repo_id,
            &update.ref_name,
            update.old_target.as_ref(),
            &update.new_target,
            update.force,
        )
    }

    pub fn update_batch(&self, repo_id: &RepoId, updates: &[RefUpdate]) -> Vec<RefUpdateResult> {
        let mut results = Vec::with_capacity(updates.len());

        for update in updates {
            let result = self.update_single(repo_id, update);
            results.push(RefUpdateResult {
                ref_name: update.ref_name.clone(),
                success: result.is_ok(),
                error: result.err(),
            });
        }

        results
    }

    pub fn delete_ref(&self, repo_id: &RepoId, ref_name: &RefName) -> Result<()> {
        self.store.delete_ref(repo_id, ref_name)
    }

    pub fn delete_refs(&self, repo_id: &RepoId, ref_names: &[RefName]) -> Vec<RefUpdateResult> {
        let mut results = Vec::with_capacity(ref_names.len());

        for ref_name in ref_names {
            let result = self.store.delete_ref(repo_id, ref_name);
            results.push(RefUpdateResult {
                ref_name: ref_name.clone(),
                success: result.is_ok(),
                error: result.err(),
            });
        }

        results
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
    fn test_ref_update_new() {
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");
        let update = RefUpdate::new(ref_name.clone(), oid);

        assert_eq!(update.ref_name, ref_name);
        assert!(update.old_target.is_none());
        assert_eq!(update.new_target, oid);
        assert!(!update.force);
    }

    #[test]
    fn test_ref_update_with_old_target() {
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let old_oid = Oid::hash(b"old");
        let new_oid = Oid::hash(b"new");

        let update = RefUpdate::new(ref_name, new_oid).with_old_target(old_oid);

        assert_eq!(update.old_target, Some(old_oid));
    }

    #[test]
    fn test_ref_update_with_force() {
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let update = RefUpdate::new(ref_name, oid).with_force(true);

        assert!(update.force);
    }

    #[test]
    fn test_ref_update_create() {
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let update = RefUpdate::create(ref_name, oid);

        assert_eq!(update.old_target, Some(Oid::ZERO));
    }

    #[test]
    fn test_update_single() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let updater = AtomicRefUpdater::new(&store);
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let update = RefUpdate::new(ref_name.clone(), oid).with_force(true);
        updater.update_single(&repo_id, &update).unwrap();

        assert_eq!(store.get_ref(&repo_id, &ref_name).unwrap(), Some(oid));
    }

    #[test]
    fn test_update_batch() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let updater = AtomicRefUpdater::new(&store);

        let updates = vec![
            RefUpdate::new(
                RefName::new("refs/heads/main").unwrap(),
                Oid::hash(b"commit1"),
            )
            .with_force(true),
            RefUpdate::new(
                RefName::new("refs/heads/feature").unwrap(),
                Oid::hash(b"commit2"),
            )
            .with_force(true),
        ];

        let results = updater.update_batch(&repo_id, &updates);

        assert_eq!(results.len(), 2);
        assert!(results[0].success);
        assert!(results[1].success);
    }

    #[test]
    fn test_delete_ref() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");
        store
            .update_ref(&repo_id, &ref_name, None, &oid, true)
            .unwrap();

        let updater = AtomicRefUpdater::new(&store);
        updater.delete_ref(&repo_id, &ref_name).unwrap();

        assert!(store.get_ref(&repo_id, &ref_name).unwrap().is_none());
    }

    #[test]
    fn test_delete_refs() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref1 = RefName::new("refs/heads/main").unwrap();
        let ref2 = RefName::new("refs/heads/feature").unwrap();
        let oid = Oid::hash(b"commit");

        store.update_ref(&repo_id, &ref1, None, &oid, true).unwrap();
        store.update_ref(&repo_id, &ref2, None, &oid, true).unwrap();

        let updater = AtomicRefUpdater::new(&store);
        let results = updater.delete_refs(&repo_id, &[ref1.clone(), ref2.clone()]);

        assert_eq!(results.len(), 2);
        assert!(results[0].success);
        assert!(results[1].success);

        assert!(store.get_ref(&repo_id, &ref1).unwrap().is_none());
        assert!(store.get_ref(&repo_id, &ref2).unwrap().is_none());
    }

    #[test]
    fn test_update_batch_partial_failure() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid1 = Oid::hash(b"commit1");
        store
            .update_ref(&repo_id, &ref_name, None, &oid1, true)
            .unwrap();

        let updater = AtomicRefUpdater::new(&store);

        let updates = vec![
            RefUpdate::new(ref_name.clone(), Oid::hash(b"commit2"))
                .with_old_target(Oid::hash(b"wrong")),
            RefUpdate::new(
                RefName::new("refs/heads/feature").unwrap(),
                Oid::hash(b"commit3"),
            )
            .with_force(true),
        ];

        let results = updater.update_batch(&repo_id, &updates);

        assert!(!results[0].success);
        assert!(results[0].error.is_some());
        assert!(results[1].success);
    }
}
