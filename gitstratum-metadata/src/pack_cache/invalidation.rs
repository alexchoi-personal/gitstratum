use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;

use gitstratum_core::{Oid, RefName, RepoId};

use crate::pack_cache::storage::{PackCacheKey, PackCacheStorage};

#[derive(Debug, Clone)]
pub enum InvalidationEvent {
    RefUpdate {
        repo_id: RepoId,
        ref_name: RefName,
        old_target: Option<Oid>,
        new_target: Oid,
    },
    RefDelete {
        repo_id: RepoId,
        ref_name: RefName,
    },
    RepoDelete {
        repo_id: RepoId,
    },
    ObjectUpdate {
        repo_id: RepoId,
        oid: Oid,
    },
}

pub struct InvalidationTracker {
    ref_to_packs: RwLock<HashMap<(RepoId, RefName), HashSet<PackCacheKey>>>,
    object_to_packs: RwLock<HashMap<(RepoId, Oid), HashSet<PackCacheKey>>>,
}

impl InvalidationTracker {
    pub fn new() -> Self {
        Self {
            ref_to_packs: RwLock::new(HashMap::new()),
            object_to_packs: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_pack(&self, key: &PackCacheKey, refs: &[(RefName, Oid)], objects: &[Oid]) {
        let repo_id = &key.repo_id;

        let mut ref_to_packs = self.ref_to_packs.write();
        for (ref_name, _) in refs {
            ref_to_packs
                .entry((repo_id.clone(), ref_name.clone()))
                .or_default()
                .insert(key.clone());
        }

        let mut object_to_packs = self.object_to_packs.write();
        for oid in objects {
            object_to_packs
                .entry((repo_id.clone(), *oid))
                .or_default()
                .insert(key.clone());
        }
    }

    pub fn unregister_pack(&self, key: &PackCacheKey) {
        let mut ref_to_packs = self.ref_to_packs.write();
        for packs in ref_to_packs.values_mut() {
            packs.remove(key);
        }

        let mut object_to_packs = self.object_to_packs.write();
        for packs in object_to_packs.values_mut() {
            packs.remove(key);
        }
    }

    pub fn get_affected_packs(&self, event: &InvalidationEvent) -> HashSet<PackCacheKey> {
        match event {
            InvalidationEvent::RefUpdate { repo_id, ref_name, .. } => {
                let ref_to_packs = self.ref_to_packs.read();
                ref_to_packs
                    .get(&(repo_id.clone(), ref_name.clone()))
                    .cloned()
                    .unwrap_or_default()
            }
            InvalidationEvent::RefDelete { repo_id, ref_name } => {
                let ref_to_packs = self.ref_to_packs.read();
                ref_to_packs
                    .get(&(repo_id.clone(), ref_name.clone()))
                    .cloned()
                    .unwrap_or_default()
            }
            InvalidationEvent::RepoDelete { repo_id } => {
                let mut affected = HashSet::new();

                let ref_to_packs = self.ref_to_packs.read();
                for ((r, _), packs) in ref_to_packs.iter() {
                    if r == repo_id {
                        affected.extend(packs.clone());
                    }
                }

                affected
            }
            InvalidationEvent::ObjectUpdate { repo_id, oid } => {
                let object_to_packs = self.object_to_packs.read();
                object_to_packs
                    .get(&(repo_id.clone(), *oid))
                    .cloned()
                    .unwrap_or_default()
            }
        }
    }

    pub fn clear_repo(&self, repo_id: &RepoId) {
        {
            let mut ref_to_packs = self.ref_to_packs.write();
            ref_to_packs.retain(|(r, _), _| r != repo_id);
        }

        {
            let mut object_to_packs = self.object_to_packs.write();
            object_to_packs.retain(|(r, _), _| r != repo_id);
        }
    }

    pub fn clear(&self) {
        self.ref_to_packs.write().clear();
        self.object_to_packs.write().clear();
    }
}

impl Default for InvalidationTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CacheInvalidator {
    storage: std::sync::Arc<PackCacheStorage>,
    tracker: InvalidationTracker,
}

impl CacheInvalidator {
    pub fn new(storage: std::sync::Arc<PackCacheStorage>) -> Self {
        Self {
            storage,
            tracker: InvalidationTracker::new(),
        }
    }

    pub fn register_pack(&self, key: &PackCacheKey, refs: &[(RefName, Oid)], objects: &[Oid]) {
        self.tracker.register_pack(key, refs, objects);
    }

    pub fn invalidate(&self, event: &InvalidationEvent) -> usize {
        let affected = self.tracker.get_affected_packs(event);
        let mut count = 0;

        for key in &affected {
            if self.storage.remove(key) {
                self.tracker.unregister_pack(key);
                count += 1;
            }
        }

        if let InvalidationEvent::RepoDelete { repo_id } = event {
            self.storage.remove_by_repo(repo_id);
            self.tracker.clear_repo(repo_id);
        }

        count
    }

    pub fn clear(&self) {
        self.storage.clear();
        self.tracker.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidation_tracker_register_unregister() {
        let tracker = InvalidationTracker::new();
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let key = PackCacheKey::new(repo_id.clone(), vec![oid], vec![]);

        tracker.register_pack(&key, &[(ref_name.clone(), oid)], &[oid]);

        let event = InvalidationEvent::RefUpdate {
            repo_id: repo_id.clone(),
            ref_name: ref_name.clone(),
            old_target: None,
            new_target: oid,
        };

        let affected = tracker.get_affected_packs(&event);
        assert!(affected.contains(&key));

        tracker.unregister_pack(&key);
        let affected = tracker.get_affected_packs(&event);
        assert!(!affected.contains(&key));
    }

    #[test]
    fn test_invalidation_tracker_ref_delete() {
        let tracker = InvalidationTracker::new();
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let key = PackCacheKey::new(repo_id.clone(), vec![oid], vec![]);
        tracker.register_pack(&key, &[(ref_name.clone(), oid)], &[]);

        let event = InvalidationEvent::RefDelete {
            repo_id,
            ref_name,
        };

        let affected = tracker.get_affected_packs(&event);
        assert!(affected.contains(&key));
    }

    #[test]
    fn test_invalidation_tracker_repo_delete() {
        let tracker = InvalidationTracker::new();
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let key1 = PackCacheKey::new(repo_id.clone(), vec![oid], vec![]);
        let key2 = PackCacheKey::new(repo_id.clone(), vec![Oid::hash(b"other")], vec![]);

        tracker.register_pack(&key1, &[(ref_name.clone(), oid)], &[]);
        tracker.register_pack(&key2, &[], &[]);

        let event = InvalidationEvent::RepoDelete {
            repo_id: repo_id.clone(),
        };

        let affected = tracker.get_affected_packs(&event);
        assert!(affected.contains(&key1));
    }

    #[test]
    fn test_invalidation_tracker_object_update() {
        let tracker = InvalidationTracker::new();
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid = Oid::hash(b"object");

        let key = PackCacheKey::new(repo_id.clone(), vec![oid], vec![]);
        tracker.register_pack(&key, &[], &[oid]);

        let event = InvalidationEvent::ObjectUpdate {
            repo_id,
            oid,
        };

        let affected = tracker.get_affected_packs(&event);
        assert!(affected.contains(&key));
    }

    #[test]
    fn test_invalidation_tracker_clear_repo() {
        let tracker = InvalidationTracker::new();
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let key1 = PackCacheKey::new(repo1.clone(), vec![oid], vec![]);
        let key2 = PackCacheKey::new(repo2.clone(), vec![oid], vec![]);

        tracker.register_pack(&key1, &[(ref_name.clone(), oid)], &[]);
        tracker.register_pack(&key2, &[(ref_name.clone(), oid)], &[]);

        tracker.clear_repo(&repo1);

        let event1 = InvalidationEvent::RefUpdate {
            repo_id: repo1,
            ref_name: ref_name.clone(),
            old_target: None,
            new_target: oid,
        };
        let event2 = InvalidationEvent::RefUpdate {
            repo_id: repo2,
            ref_name,
            old_target: None,
            new_target: oid,
        };

        assert!(tracker.get_affected_packs(&event1).is_empty());
        assert!(!tracker.get_affected_packs(&event2).is_empty());
    }

    #[test]
    fn test_invalidation_tracker_clear() {
        let tracker = InvalidationTracker::new();
        let repo_id = RepoId::new("test/repo").unwrap();
        let ref_name = RefName::new("refs/heads/main").unwrap();
        let oid = Oid::hash(b"commit");

        let key = PackCacheKey::new(repo_id.clone(), vec![oid], vec![]);
        tracker.register_pack(&key, &[(ref_name.clone(), oid)], &[oid]);

        tracker.clear();

        let event = InvalidationEvent::RefUpdate {
            repo_id,
            ref_name,
            old_target: None,
            new_target: oid,
        };
        assert!(tracker.get_affected_packs(&event).is_empty());
    }
}
