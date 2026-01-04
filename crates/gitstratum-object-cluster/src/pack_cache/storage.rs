use bytes::Bytes;
use gitstratum_core::Oid;
use lru::LruCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

use crate::error::Result;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackCacheKey {
    pub repo_id: String,
    pub ref_name: String,
    pub depth: u32,
}

impl PackCacheKey {
    pub fn new(repo_id: impl Into<String>, ref_name: impl Into<String>, depth: u32) -> Self {
        Self {
            repo_id: repo_id.into(),
            ref_name: ref_name.into(),
            depth,
        }
    }

    pub fn to_string_key(&self) -> String {
        format!("{}:{}:{}", self.repo_id, self.ref_name, self.depth)
    }

    pub fn from_string_key(key: &str) -> Option<Self> {
        let parts: Vec<&str> = key.splitn(3, ':').collect();
        if parts.len() != 3 {
            return None;
        }
        let depth = parts[2].parse().ok()?;
        Some(Self {
            repo_id: parts[0].to_string(),
            ref_name: parts[1].to_string(),
            depth,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PackData {
    pub data: Bytes,
    pub object_count: u32,
    pub total_size: u64,
    pub tip_oid: Oid,
    pub created_at: Instant,
}

impl PackData {
    pub fn new(data: Bytes, object_count: u32, total_size: u64, tip_oid: Oid) -> Self {
        Self {
            data,
            object_count,
            total_size,
            tip_oid,
            created_at: Instant::now(),
        }
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.age() > ttl
    }
}

struct CacheEntry {
    pack: PackData,
    access_count: u64,
}

pub struct PackCache {
    cache: Mutex<LruCache<PackCacheKey, CacheEntry>>,
    max_size_bytes: u64,
    current_size_bytes: AtomicU64,
    default_ttl: Duration,
}

impl PackCache {
    pub fn new(max_size_bytes: u64, default_ttl: Duration) -> Self {
        let cap = NonZeroUsize::new(10000).unwrap();
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            max_size_bytes,
            current_size_bytes: AtomicU64::new(0),
            default_ttl,
        }
    }

    #[instrument(skip(self, pack))]
    pub fn put(&self, key: PackCacheKey, pack: PackData) -> Result<()> {
        let pack_size = pack.data.len() as u64;

        let mut cache = self.cache.lock();

        if let Some(old_entry) = cache.pop(&key) {
            self.current_size_bytes
                .fetch_sub(old_entry.pack.data.len() as u64, Ordering::Relaxed);
        }

        self.evict_for_size(&mut cache, pack_size);

        let entry = CacheEntry {
            pack,
            access_count: 0,
        };

        cache.put(key.clone(), entry);
        self.current_size_bytes
            .fetch_add(pack_size, Ordering::Relaxed);

        debug!(key = %key.to_string_key(), size = pack_size, "cached pack");
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn get(&self, key: &PackCacheKey) -> Option<PackData> {
        let mut cache = self.cache.lock();

        if let Some(entry) = cache.get_mut(key) {
            if entry.pack.is_expired(self.default_ttl) {
                let size = entry.pack.data.len() as u64;
                cache.pop(key);
                self.current_size_bytes.fetch_sub(size, Ordering::Relaxed);
                return None;
            }

            entry.access_count += 1;
            debug!(key = %key.to_string_key(), "cache hit");
            return Some(entry.pack.clone());
        }

        None
    }

    #[instrument(skip(self))]
    pub fn remove(&self, key: &PackCacheKey) -> Option<PackData> {
        let mut cache = self.cache.lock();
        if let Some(entry) = cache.pop(key) {
            self.current_size_bytes
                .fetch_sub(entry.pack.data.len() as u64, Ordering::Relaxed);
            debug!(key = %key.to_string_key(), "removed pack from cache");
            Some(entry.pack)
        } else {
            None
        }
    }

    pub fn contains(&self, key: &PackCacheKey) -> bool {
        let cache = self.cache.lock();
        if let Some(entry) = cache.peek(key) {
            !entry.pack.is_expired(self.default_ttl)
        } else {
            false
        }
    }

    pub fn invalidate_repo(&self, repo_id: &str) {
        let mut cache = self.cache.lock();
        let keys_to_remove: Vec<PackCacheKey> = cache
            .iter()
            .filter(|(k, _)| k.repo_id == repo_id)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = cache.pop(&key) {
                self.current_size_bytes
                    .fetch_sub(entry.pack.data.len() as u64, Ordering::Relaxed);
            }
        }
    }

    pub fn invalidate_ref(&self, repo_id: &str, ref_name: &str) {
        let mut cache = self.cache.lock();
        let keys_to_remove: Vec<PackCacheKey> = cache
            .iter()
            .filter(|(k, _)| k.repo_id == repo_id && k.ref_name == ref_name)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = cache.pop(&key) {
                self.current_size_bytes
                    .fetch_sub(entry.pack.data.len() as u64, Ordering::Relaxed);
            }
        }
    }

    fn evict_for_size(&self, cache: &mut LruCache<PackCacheKey, CacheEntry>, additional_size: u64) {
        let now = Instant::now();
        loop {
            if let Some((_, entry)) = cache.peek_lru() {
                if now.duration_since(entry.pack.created_at) > self.default_ttl {
                    if let Some((_, evicted)) = cache.pop_lru() {
                        self.current_size_bytes
                            .fetch_sub(evicted.pack.data.len() as u64, Ordering::Relaxed);
                        continue;
                    }
                }
            }
            break;
        }

        while self.current_size_bytes.load(Ordering::Relaxed) + additional_size
            > self.max_size_bytes
        {
            if let Some((_, evicted)) = cache.pop_lru() {
                self.current_size_bytes
                    .fetch_sub(evicted.pack.data.len() as u64, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    pub fn cleanup_expired(&self) -> u64 {
        let mut cache = self.cache.lock();
        let now = Instant::now();
        let mut freed = 0u64;

        loop {
            if let Some((_, entry)) = cache.peek_lru() {
                if now.duration_since(entry.pack.created_at) > self.default_ttl {
                    if let Some((_, evicted)) = cache.pop_lru() {
                        let size = evicted.pack.data.len() as u64;
                        self.current_size_bytes.fetch_sub(size, Ordering::Relaxed);
                        freed += size;
                        continue;
                    }
                }
            }
            break;
        }

        freed
    }

    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.lock();
        let mut total_entries = 0;
        let mut total_objects = 0u64;
        let mut total_accesses = 0u64;

        for (_, entry) in cache.iter() {
            total_entries += 1;
            total_objects += entry.pack.object_count as u64;
            total_accesses += entry.access_count;
        }

        CacheStats {
            entry_count: total_entries,
            total_size_bytes: self.current_size_bytes.load(Ordering::Relaxed),
            max_size_bytes: self.max_size_bytes,
            object_count: total_objects,
            total_accesses,
        }
    }

    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
        self.current_size_bytes.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entry_count: usize,
    pub total_size_bytes: u64,
    pub max_size_bytes: u64,
    pub object_count: u64,
    pub total_accesses: u64,
}

impl CacheStats {
    pub fn utilization(&self) -> f64 {
        if self.max_size_bytes == 0 {
            return 0.0;
        }
        self.total_size_bytes as f64 / self.max_size_bytes as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_pack(size: usize) -> PackData {
        PackData::new(
            Bytes::from(vec![0u8; size]),
            10,
            size as u64,
            Oid::hash(b"test"),
        )
    }

    #[test]
    fn test_pack_cache_key() {
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        assert_eq!(key.repo_id, "repo1");
        assert_eq!(key.ref_name, "refs/heads/main");
        assert_eq!(key.depth, 50);

        let string_key = key.to_string_key();
        assert_eq!(string_key, "repo1:refs/heads/main:50");

        let restored = PackCacheKey::from_string_key(&string_key).unwrap();
        assert_eq!(restored.repo_id, key.repo_id);
        assert_eq!(restored.ref_name, key.ref_name);
        assert_eq!(restored.depth, key.depth);
    }

    #[test]
    fn test_pack_cache_key_from_string_invalid() {
        assert!(PackCacheKey::from_string_key("invalid").is_none());
        assert!(PackCacheKey::from_string_key("a:b").is_none());
        assert!(PackCacheKey::from_string_key("a:b:notanumber").is_none());
    }

    #[test]
    fn test_pack_data_age() {
        let pack = create_test_pack(100);
        std::thread::sleep(Duration::from_millis(10));
        assert!(pack.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_pack_data_is_expired() {
        let pack = create_test_pack(100);
        assert!(!pack.is_expired(Duration::from_secs(1)));

        std::thread::sleep(Duration::from_millis(50));
        assert!(pack.is_expired(Duration::from_millis(10)));
    }

    #[test]
    fn test_put_get() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        let pack = create_test_pack(100);

        cache.put(key.clone(), pack.clone()).unwrap();

        let retrieved = cache.get(&key).unwrap();
        assert_eq!(retrieved.data.len(), pack.data.len());
        assert_eq!(retrieved.object_count, pack.object_count);
    }

    #[test]
    fn test_get_nonexistent() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_contains() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);

        assert!(!cache.contains(&key));

        let pack = create_test_pack(100);
        cache.put(key.clone(), pack).unwrap();

        assert!(cache.contains(&key));
    }

    #[test]
    fn test_remove() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        let pack = create_test_pack(100);

        cache.put(key.clone(), pack).unwrap();
        assert!(cache.contains(&key));

        let removed = cache.remove(&key);
        assert!(removed.is_some());
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_remove_nonexistent() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        assert!(cache.remove(&key).is_none());
    }

    #[test]
    fn test_invalidate_repo() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));

        let key1 = PackCacheKey::new("repo1", "refs/heads/main", 50);
        let key2 = PackCacheKey::new("repo1", "refs/heads/dev", 50);
        let key3 = PackCacheKey::new("repo2", "refs/heads/main", 50);

        cache.put(key1.clone(), create_test_pack(100)).unwrap();
        cache.put(key2.clone(), create_test_pack(100)).unwrap();
        cache.put(key3.clone(), create_test_pack(100)).unwrap();

        cache.invalidate_repo("repo1");

        assert!(!cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_invalidate_ref() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));

        let key1 = PackCacheKey::new("repo1", "refs/heads/main", 50);
        let key2 = PackCacheKey::new("repo1", "refs/heads/main", 100);
        let key3 = PackCacheKey::new("repo1", "refs/heads/dev", 50);

        cache.put(key1.clone(), create_test_pack(100)).unwrap();
        cache.put(key2.clone(), create_test_pack(100)).unwrap();
        cache.put(key3.clone(), create_test_pack(100)).unwrap();

        cache.invalidate_ref("repo1", "refs/heads/main");

        assert!(!cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_eviction() {
        let cache = PackCache::new(250, Duration::from_secs(300));

        let key1 = PackCacheKey::new("repo1", "ref1", 50);
        let key2 = PackCacheKey::new("repo1", "ref2", 50);
        let key3 = PackCacheKey::new("repo1", "ref3", 50);

        cache.put(key1.clone(), create_test_pack(100)).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        cache.put(key2.clone(), create_test_pack(100)).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        cache.put(key3.clone(), create_test_pack(100)).unwrap();

        assert!(!cache.contains(&key1));
    }

    #[test]
    fn test_cleanup_expired() {
        let cache = PackCache::new(1024 * 1024, Duration::from_millis(10));

        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        cache.put(key.clone(), create_test_pack(100)).unwrap();

        std::thread::sleep(Duration::from_millis(20));

        let freed = cache.cleanup_expired();
        assert!(freed > 0);
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_stats() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.total_size_bytes, 0);

        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        cache.put(key.clone(), create_test_pack(100)).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.total_size_bytes, 100);

        cache.get(&key);
        cache.get(&key);

        let stats = cache.stats();
        assert_eq!(stats.total_accesses, 2);
    }

    #[test]
    fn test_stats_utilization() {
        let stats = CacheStats {
            entry_count: 10,
            total_size_bytes: 500,
            max_size_bytes: 1000,
            object_count: 100,
            total_accesses: 50,
        };

        assert!((stats.utilization() - 0.5).abs() < 0.001);

        let empty_stats = CacheStats {
            entry_count: 0,
            total_size_bytes: 0,
            max_size_bytes: 0,
            object_count: 0,
            total_accesses: 0,
        };

        assert_eq!(empty_stats.utilization(), 0.0);
    }

    #[test]
    fn test_clear() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));

        let key1 = PackCacheKey::new("repo1", "ref1", 50);
        let key2 = PackCacheKey::new("repo2", "ref2", 50);

        cache.put(key1.clone(), create_test_pack(100)).unwrap();
        cache.put(key2.clone(), create_test_pack(100)).unwrap();

        assert_eq!(cache.stats().entry_count, 2);

        cache.clear();

        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(cache.stats().total_size_bytes, 0);
    }

    #[test]
    fn test_overwrite_existing() {
        let cache = PackCache::new(1024 * 1024, Duration::from_secs(300));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);

        cache.put(key.clone(), create_test_pack(100)).unwrap();
        assert_eq!(cache.stats().total_size_bytes, 100);

        cache.put(key.clone(), create_test_pack(200)).unwrap();
        assert_eq!(cache.stats().total_size_bytes, 200);
        assert_eq!(cache.stats().entry_count, 1);
    }

    #[test]
    fn test_get_expired_entry() {
        let cache = PackCache::new(1024 * 1024, Duration::from_millis(10));
        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);

        cache.put(key.clone(), create_test_pack(100)).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().entry_count, 0);
    }
}
