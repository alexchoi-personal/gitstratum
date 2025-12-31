use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use gitstratum_core::{Oid, RepoId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackCacheEntry {
    pub pack_id: String,
    pub objects: Vec<Oid>,
    pub size_bytes: u64,
    pub created_at: i64,
    pub expires_at: i64,
    pub hit_count: u64,
}

impl PackCacheEntry {
    pub fn new(pack_id: String, objects: Vec<Oid>, size_bytes: u64, ttl_secs: i64) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            pack_id,
            objects,
            size_bytes,
            created_at: now,
            expires_at: now + ttl_secs,
            hit_count: 0,
        }
    }

    pub fn is_expired(&self) -> bool {
        chrono::Utc::now().timestamp() > self.expires_at
    }

    pub fn record_hit(&mut self) {
        self.hit_count += 1;
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PackCacheKey {
    pub repo_id: RepoId,
    pub wants: Vec<Oid>,
    pub haves: Vec<Oid>,
}

impl PackCacheKey {
    pub fn new(repo_id: RepoId, wants: Vec<Oid>, haves: Vec<Oid>) -> Self {
        let mut wants = wants;
        let mut haves = haves;
        wants.sort();
        haves.sort();
        Self {
            repo_id,
            wants,
            haves,
        }
    }

    pub fn cache_id(&self) -> String {
        let wants_str: Vec<String> = self.wants.iter().map(|o| o.to_hex()).collect();
        let haves_str: Vec<String> = self.haves.iter().map(|o| o.to_hex()).collect();
        format!(
            "{}:{}:{}",
            self.repo_id.as_str(),
            wants_str.join(","),
            haves_str.join(",")
        )
    }
}

pub struct PackCacheStorage {
    entries: RwLock<HashMap<PackCacheKey, PackCacheEntry>>,
    max_size_bytes: u64,
    current_size_bytes: RwLock<u64>,
}

impl PackCacheStorage {
    pub fn new(max_size_bytes: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_size_bytes,
            current_size_bytes: RwLock::new(0),
        }
    }

    pub fn get(&self, key: &PackCacheKey) -> Option<PackCacheEntry> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                let size = entry.size_bytes;
                entries.remove(key);
                *self.current_size_bytes.write() -= size;
                return None;
            }
            entry.record_hit();
            return Some(entry.clone());
        }
        None
    }

    pub fn put(&self, key: PackCacheKey, entry: PackCacheEntry) -> bool {
        if entry.size_bytes > self.max_size_bytes {
            return false;
        }

        self.evict_if_needed(entry.size_bytes);

        let size = entry.size_bytes;
        self.entries.write().insert(key, entry);
        *self.current_size_bytes.write() += size;
        true
    }

    fn evict_if_needed(&self, needed_bytes: u64) {
        let current = *self.current_size_bytes.read();
        if current + needed_bytes <= self.max_size_bytes {
            return;
        }

        let mut entries = self.entries.write();
        let now = chrono::Utc::now().timestamp();

        let expired_keys: Vec<PackCacheKey> = entries
            .iter()
            .filter(|(_, e)| e.expires_at < now)
            .map(|(k, _)| k.clone())
            .collect();

        let mut freed = 0u64;
        for key in expired_keys {
            if let Some(entry) = entries.remove(&key) {
                freed += entry.size_bytes;
            }
        }
        *self.current_size_bytes.write() -= freed;

        if *self.current_size_bytes.read() + needed_bytes > self.max_size_bytes {
            let mut by_hits: Vec<(PackCacheKey, u64, u64)> = entries
                .iter()
                .map(|(k, e)| (k.clone(), e.hit_count, e.size_bytes))
                .collect();
            by_hits.sort_by_key(|(_, hits, _)| *hits);

            let mut additional_freed = 0u64;
            let target = (self.max_size_bytes as f64 * 0.8) as u64;

            for (key, _, size) in by_hits {
                if *self.current_size_bytes.read() - additional_freed <= target {
                    break;
                }
                entries.remove(&key);
                additional_freed += size;
            }
            *self.current_size_bytes.write() -= additional_freed;
        }
    }

    pub fn remove(&self, key: &PackCacheKey) -> bool {
        if let Some(entry) = self.entries.write().remove(key) {
            *self.current_size_bytes.write() -= entry.size_bytes;
            true
        } else {
            false
        }
    }

    pub fn remove_by_repo(&self, repo_id: &RepoId) {
        let mut entries = self.entries.write();
        let keys_to_remove: Vec<PackCacheKey> = entries
            .keys()
            .filter(|k| &k.repo_id == repo_id)
            .cloned()
            .collect();

        let mut freed = 0u64;
        for key in keys_to_remove {
            if let Some(entry) = entries.remove(&key) {
                freed += entry.size_bytes;
            }
        }
        *self.current_size_bytes.write() -= freed;
    }

    pub fn current_size(&self) -> u64 {
        *self.current_size_bytes.read()
    }

    pub fn entry_count(&self) -> usize {
        self.entries.read().len()
    }

    pub fn clear(&self) {
        self.entries.write().clear();
        *self.current_size_bytes.write() = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_cache_entry_new() {
        let entry = PackCacheEntry::new(
            "pack-123".to_string(),
            vec![Oid::hash(b"obj1"), Oid::hash(b"obj2")],
            1024,
            3600,
        );

        assert_eq!(entry.pack_id, "pack-123");
        assert_eq!(entry.objects.len(), 2);
        assert_eq!(entry.size_bytes, 1024);
        assert_eq!(entry.hit_count, 0);
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_pack_cache_entry_expired() {
        let mut entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 0);
        entry.expires_at = entry.created_at - 1;
        assert!(entry.is_expired());
    }

    #[test]
    fn test_pack_cache_entry_record_hit() {
        let mut entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 3600);
        assert_eq!(entry.hit_count, 0);

        entry.record_hit();
        assert_eq!(entry.hit_count, 1);

        entry.record_hit();
        assert_eq!(entry.hit_count, 2);
    }

    #[test]
    fn test_pack_cache_key() {
        let repo_id = RepoId::new("test/repo").unwrap();
        let wants = vec![Oid::hash(b"want1"), Oid::hash(b"want2")];
        let haves = vec![Oid::hash(b"have1")];

        let key = PackCacheKey::new(repo_id, wants, haves);
        assert!(!key.cache_id().is_empty());
    }

    #[test]
    fn test_pack_cache_key_sorted() {
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        let key1 = PackCacheKey::new(repo_id.clone(), vec![oid1, oid2], vec![]);
        let key2 = PackCacheKey::new(repo_id, vec![oid2, oid1], vec![]);

        assert_eq!(key1.cache_id(), key2.cache_id());
    }

    #[test]
    fn test_pack_cache_storage_put_get() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);

        let entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 3600);

        assert!(storage.get(&key).is_none());
        assert!(storage.put(key.clone(), entry));

        let retrieved = storage.get(&key).unwrap();
        assert_eq!(retrieved.pack_id, "pack-123");
        assert_eq!(retrieved.hit_count, 1);
    }

    #[test]
    fn test_pack_cache_storage_remove() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);

        let entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 3600);
        storage.put(key.clone(), entry);

        assert!(storage.remove(&key));
        assert!(storage.get(&key).is_none());
    }

    #[test]
    fn test_pack_cache_storage_remove_by_repo() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo1 = RepoId::new("test/repo1").unwrap();
        let repo2 = RepoId::new("test/repo2").unwrap();

        let key1 = PackCacheKey::new(repo1.clone(), vec![Oid::hash(b"want")], vec![]);
        let key2 = PackCacheKey::new(repo2.clone(), vec![Oid::hash(b"want")], vec![]);

        storage.put(
            key1.clone(),
            PackCacheEntry::new("pack-1".to_string(), vec![], 1024, 3600),
        );
        storage.put(
            key2.clone(),
            PackCacheEntry::new("pack-2".to_string(), vec![], 1024, 3600),
        );

        storage.remove_by_repo(&repo1);

        assert!(storage.get(&key1).is_none());
        assert!(storage.get(&key2).is_some());
    }

    #[test]
    fn test_pack_cache_storage_size_tracking() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();

        assert_eq!(storage.current_size(), 0);
        assert_eq!(storage.entry_count(), 0);

        let key1 = PackCacheKey::new(repo_id.clone(), vec![Oid::hash(b"want1")], vec![]);
        let key2 = PackCacheKey::new(repo_id, vec![Oid::hash(b"want2")], vec![]);

        storage.put(
            key1.clone(),
            PackCacheEntry::new("pack-1".to_string(), vec![], 1024, 3600),
        );
        assert_eq!(storage.current_size(), 1024);
        assert_eq!(storage.entry_count(), 1);

        storage.put(
            key2,
            PackCacheEntry::new("pack-2".to_string(), vec![], 2048, 3600),
        );
        assert_eq!(storage.current_size(), 3072);
        assert_eq!(storage.entry_count(), 2);

        storage.remove(&key1);
        assert_eq!(storage.current_size(), 2048);
    }

    #[test]
    fn test_pack_cache_storage_clear() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();

        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);
        storage.put(
            key,
            PackCacheEntry::new("pack-1".to_string(), vec![], 1024, 3600),
        );

        storage.clear();
        assert_eq!(storage.current_size(), 0);
        assert_eq!(storage.entry_count(), 0);
    }

    #[test]
    fn test_pack_cache_storage_too_large() {
        let storage = PackCacheStorage::new(1024);
        let repo_id = RepoId::new("test/repo").unwrap();

        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);
        let entry = PackCacheEntry::new("pack-1".to_string(), vec![], 2048, 3600);

        assert!(!storage.put(key, entry));
    }

    #[test]
    fn test_pack_cache_storage_get_expired() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);

        let mut entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 3600);
        entry.expires_at = entry.created_at - 1;
        storage.put(key.clone(), entry);

        assert_eq!(storage.current_size(), 1024);
        assert!(storage.get(&key).is_none());
        assert_eq!(storage.current_size(), 0);
    }

    #[test]
    fn test_pack_cache_storage_remove_nonexistent() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);

        assert!(!storage.remove(&key));
    }

    #[test]
    fn test_pack_cache_storage_evict_expired_on_put() {
        let storage = PackCacheStorage::new(2000);
        let repo_id = RepoId::new("test/repo").unwrap();

        let key1 = PackCacheKey::new(repo_id.clone(), vec![Oid::hash(b"want1")], vec![]);
        let mut entry1 = PackCacheEntry::new("pack-1".to_string(), vec![], 1000, 3600);
        entry1.expires_at = entry1.created_at - 1;
        storage.put(key1.clone(), entry1);

        let key2 = PackCacheKey::new(repo_id, vec![Oid::hash(b"want2")], vec![]);
        let entry2 = PackCacheEntry::new("pack-2".to_string(), vec![], 1500, 3600);
        assert!(storage.put(key2.clone(), entry2));

        assert!(storage.get(&key1).is_none());
        assert!(storage.get(&key2).is_some());
    }

    #[test]
    fn test_pack_cache_key_with_haves() {
        let repo_id = RepoId::new("test/repo").unwrap();
        let wants = vec![Oid::hash(b"want1")];
        let haves = vec![Oid::hash(b"have1"), Oid::hash(b"have2")];

        let key = PackCacheKey::new(repo_id, wants, haves);
        let cache_id = key.cache_id();
        assert!(cache_id.contains("test/repo"));
        assert!(cache_id.contains(","));
    }

    #[test]
    fn test_pack_cache_key_empty_haves() {
        let repo_id = RepoId::new("test/repo").unwrap();
        let wants = vec![Oid::hash(b"want1")];
        let haves: Vec<Oid> = vec![];

        let key = PackCacheKey::new(repo_id, wants, haves);
        let cache_id = key.cache_id();
        assert!(cache_id.ends_with(":"));
    }

    #[test]
    fn test_pack_cache_key_debug_clone() {
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);
        let cloned = key.clone();
        assert_eq!(key, cloned);
        assert!(format!("{:?}", key).contains("PackCacheKey"));
    }

    #[test]
    fn test_pack_cache_entry_debug_clone() {
        let entry =
            PackCacheEntry::new("pack-123".to_string(), vec![Oid::hash(b"obj")], 1024, 3600);
        let cloned = entry.clone();
        assert_eq!(cloned.pack_id, entry.pack_id);
        assert_eq!(cloned.size_bytes, entry.size_bytes);
        assert_eq!(cloned.objects.len(), entry.objects.len());
        assert!(format!("{:?}", entry).contains("pack-123"));
    }

    #[test]
    fn test_pack_cache_storage_remove_by_repo_empty() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();

        storage.remove_by_repo(&repo_id);
        assert_eq!(storage.entry_count(), 0);
    }

    #[test]
    fn test_pack_cache_key_hash_equality() {
        use std::collections::HashSet;

        let repo_id = RepoId::new("test/repo").unwrap();
        let oid1 = Oid::hash(b"oid1");
        let oid2 = Oid::hash(b"oid2");

        let key1 = PackCacheKey::new(repo_id.clone(), vec![oid1, oid2], vec![]);
        let key2 = PackCacheKey::new(repo_id, vec![oid2, oid1], vec![]);

        let mut set = HashSet::new();
        set.insert(key1);
        assert!(set.contains(&key2));
    }

    #[test]
    fn test_pack_cache_storage_no_eviction_when_space_available() {
        let storage = PackCacheStorage::new(5000);
        let repo_id = RepoId::new("test/repo").unwrap();

        let key1 = PackCacheKey::new(repo_id.clone(), vec![Oid::hash(b"want1")], vec![]);
        let entry1 = PackCacheEntry::new("pack-1".to_string(), vec![], 1000, 3600);
        storage.put(key1.clone(), entry1);

        let key2 = PackCacheKey::new(repo_id, vec![Oid::hash(b"want2")], vec![]);
        let entry2 = PackCacheEntry::new("pack-2".to_string(), vec![], 1000, 3600);
        storage.put(key2.clone(), entry2);

        assert!(storage.get(&key1).is_some());
        assert!(storage.get(&key2).is_some());
        assert_eq!(storage.entry_count(), 2);
    }
}
