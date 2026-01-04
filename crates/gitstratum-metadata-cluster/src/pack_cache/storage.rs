use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;
use serde::{Deserialize, Serialize};

use gitstratum_core::{Oid, RepoId};

#[derive(Debug, Serialize, Deserialize)]
pub struct PackCacheEntry {
    pub pack_id: String,
    pub objects: Vec<Oid>,
    pub size_bytes: u64,
    pub created_at: i64,
    pub expires_at: i64,
    #[serde(skip, default)]
    hit_count: AtomicU64,
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
            hit_count: AtomicU64::new(0),
        }
    }

    pub fn is_expired(&self) -> bool {
        chrono::Utc::now().timestamp() > self.expires_at
    }

    pub fn record_hit(&self) {
        self.hit_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
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
        use std::fmt::Write;
        let mut result = String::with_capacity(
            self.repo_id.as_str().len() + 2 + (self.wants.len() + self.haves.len()) * 41,
        );
        result.push_str(self.repo_id.as_str());
        result.push(':');
        for (i, oid) in self.wants.iter().enumerate() {
            if i > 0 {
                result.push(',');
            }
            let _ = write!(result, "{}", oid.to_hex());
        }
        result.push(':');
        for (i, oid) in self.haves.iter().enumerate() {
            if i > 0 {
                result.push(',');
            }
            let _ = write!(result, "{}", oid.to_hex());
        }
        result
    }
}

pub struct PackCacheStorage {
    cache: Cache<PackCacheKey, Arc<PackCacheEntry>>,
    max_size_bytes: u64,
    current_size_bytes: AtomicU64,
    default_ttl: Duration,
}

impl PackCacheStorage {
    pub fn new(max_size_bytes: u64) -> Self {
        Self::with_ttl(max_size_bytes, Duration::from_secs(3600))
    }

    pub fn with_ttl(max_size_bytes: u64, default_ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(10000)
            .time_to_live(default_ttl)
            .weigher(|_key: &PackCacheKey, value: &Arc<PackCacheEntry>| -> u32 {
                value.size_bytes.min(u32::MAX as u64) as u32
            })
            .build();

        Self {
            cache,
            max_size_bytes,
            current_size_bytes: AtomicU64::new(0),
            default_ttl,
        }
    }

    pub fn get(&self, key: &PackCacheKey) -> Option<Arc<PackCacheEntry>> {
        let entry = self.cache.get(key)?;
        entry.record_hit();
        Some(entry)
    }

    pub fn put(&self, key: PackCacheKey, entry: PackCacheEntry) -> bool {
        if entry.size_bytes > self.max_size_bytes {
            return false;
        }

        let size = entry.size_bytes;
        let arc_entry = Arc::new(entry);

        if let Some(old) = self.cache.get(&key) {
            self.current_size_bytes
                .fetch_sub(old.size_bytes, Ordering::Relaxed);
        }

        self.cache.insert(key, arc_entry);
        self.current_size_bytes.fetch_add(size, Ordering::Relaxed);
        true
    }

    pub fn remove(&self, key: &PackCacheKey) -> bool {
        if let Some(entry) = self.cache.remove(key) {
            self.current_size_bytes
                .fetch_sub(entry.size_bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn remove_by_repo(&self, repo_id: &RepoId) {
        let keys_to_remove: Vec<Arc<PackCacheKey>> = self
            .cache
            .iter()
            .filter(|(k, _)| &k.repo_id == repo_id)
            .map(|(k, _)| k)
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = self.cache.remove(key.as_ref()) {
                self.current_size_bytes
                    .fetch_sub(entry.size_bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn current_size(&self) -> u64 {
        self.current_size_bytes.load(Ordering::Relaxed)
    }

    pub fn entry_count(&self) -> usize {
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize
    }

    pub fn clear(&self) {
        self.cache.invalidate_all();
        self.current_size_bytes.store(0, Ordering::Relaxed);
    }

    pub fn default_ttl(&self) -> Duration {
        self.default_ttl
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
        assert_eq!(entry.get_hit_count(), 0);
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
        let entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 3600);
        assert_eq!(entry.get_hit_count(), 0);

        entry.record_hit();
        assert_eq!(entry.get_hit_count(), 1);

        entry.record_hit();
        assert_eq!(entry.get_hit_count(), 2);
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
        assert_eq!(retrieved.get_hit_count(), 1);
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
    fn test_pack_cache_storage_ttl_expiration() {
        let storage = PackCacheStorage::with_ttl(1024 * 1024, Duration::from_millis(50));
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);

        let entry = PackCacheEntry::new("pack-123".to_string(), vec![], 1024, 3600);
        storage.put(key.clone(), entry);

        assert!(storage.get(&key).is_some());

        std::thread::sleep(Duration::from_millis(100));

        assert!(storage.get(&key).is_none());
    }

    #[test]
    fn test_pack_cache_storage_remove_nonexistent() {
        let storage = PackCacheStorage::new(1024 * 1024);
        let repo_id = RepoId::new("test/repo").unwrap();
        let key = PackCacheKey::new(repo_id, vec![Oid::hash(b"want")], vec![]);

        assert!(!storage.remove(&key));
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
    fn test_pack_cache_entry_debug() {
        let entry =
            PackCacheEntry::new("pack-123".to_string(), vec![Oid::hash(b"obj")], 1024, 3600);
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

    #[test]
    fn test_pack_cache_storage_default_ttl() {
        let storage = PackCacheStorage::new(1024);
        assert_eq!(storage.default_ttl(), Duration::from_secs(3600));

        let custom_storage = PackCacheStorage::with_ttl(1024, Duration::from_secs(7200));
        assert_eq!(custom_storage.default_ttl(), Duration::from_secs(7200));
    }
}
