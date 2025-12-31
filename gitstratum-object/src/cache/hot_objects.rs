use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use gitstratum_core::{Blob, Oid};
use parking_lot::RwLock;

#[derive(Clone)]
struct CacheEntry {
    blob: Blob,
    last_access: Instant,
    access_count: u64,
    size: usize,
}

pub struct HotObjectsCacheConfig {
    pub max_entries: usize,
    pub max_size_bytes: usize,
}

impl Default for HotObjectsCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            max_size_bytes: 256 * 1024 * 1024,
        }
    }
}

pub struct HotObjectsCache {
    config: HotObjectsCacheConfig,
    entries: RwLock<HashMap<Oid, CacheEntry>>,
    current_size: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl HotObjectsCache {
    pub fn new(config: HotObjectsCacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            current_size: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, oid: &Oid) -> Option<Blob> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(oid) {
            entry.last_access = Instant::now();
            entry.access_count += 1;
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.blob.clone());
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn put(&self, blob: Blob) {
        let size = blob.data.len();

        self.evict_if_needed(size);

        let entry = CacheEntry {
            blob: blob.clone(),
            last_access: Instant::now(),
            access_count: 1,
            size,
        };

        let oid = blob.oid;
        let mut entries = self.entries.write();
        if let Some(old) = entries.insert(oid, entry) {
            self.current_size
                .fetch_sub(old.size as u64, Ordering::Relaxed);
        }
        self.current_size.fetch_add(size as u64, Ordering::Relaxed);
    }

    fn evict_if_needed(&self, incoming_size: usize) {
        let current = self.current_size.load(Ordering::Relaxed) as usize;
        let entries_count = self.entries.read().len();

        if current + incoming_size <= self.config.max_size_bytes
            && entries_count < self.config.max_entries
        {
            return;
        }

        let mut entries = self.entries.write();

        while (self.current_size.load(Ordering::Relaxed) as usize + incoming_size
            > self.config.max_size_bytes)
            || entries.len() >= self.config.max_entries
        {
            let lru = entries
                .iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| *k);

            if let Some(oid) = lru {
                if let Some(entry) = entries.remove(&oid) {
                    self.current_size
                        .fetch_sub(entry.size as u64, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }
    }

    pub fn remove(&self, oid: &Oid) -> Option<Blob> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(oid) {
            self.current_size
                .fetch_sub(entry.size as u64, Ordering::Relaxed);
            Some(entry.blob)
        } else {
            None
        }
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        self.entries.read().contains_key(oid)
    }

    pub fn clear(&self) {
        let mut entries = self.entries.write();
        entries.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.current_size.load(Ordering::Relaxed) as usize
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn stats(&self) -> HotObjectsCacheStats {
        HotObjectsCacheStats {
            entries: self.len(),
            size_bytes: self.size_bytes(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
        }
    }
}

impl Default for HotObjectsCache {
    fn default() -> Self {
        Self::new(HotObjectsCacheConfig::default())
    }
}

#[derive(Debug, Clone)]
pub struct HotObjectsCacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blob(data: &[u8]) -> Blob {
        Blob::new(data.to_vec())
    }

    #[test]
    fn test_hot_objects_cache_config_default() {
        let config = HotObjectsCacheConfig::default();
        assert_eq!(config.max_entries, 10000);
        assert_eq!(config.max_size_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = HotObjectsCache::default();
        let blob = create_test_blob(b"hello world");

        cache.put(blob.clone());
        let retrieved = cache.get(&blob.oid);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.as_ref(), b"hello world");
    }

    #[test]
    fn test_cache_miss() {
        let cache = HotObjectsCache::default();
        let oid = Oid::hash(b"nonexistent");

        let result = cache.get(&oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_remove() {
        let cache = HotObjectsCache::default();
        let blob = create_test_blob(b"test data");

        cache.put(blob.clone());
        let removed = cache.remove(&blob.oid);
        assert!(removed.is_some());

        let result = cache.get(&blob.oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_contains() {
        let cache = HotObjectsCache::default();
        let blob = create_test_blob(b"test");

        assert!(!cache.contains(&blob.oid));
        cache.put(blob.clone());
        assert!(cache.contains(&blob.oid));
    }

    #[test]
    fn test_cache_clear() {
        let cache = HotObjectsCache::default();
        let blob = create_test_blob(b"test");

        cache.put(blob);
        assert!(!cache.is_empty());

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.size_bytes(), 0);
    }

    #[test]
    fn test_cache_len() {
        let cache = HotObjectsCache::default();
        assert_eq!(cache.len(), 0);

        let blob = create_test_blob(b"test");
        cache.put(blob);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_eviction() {
        let config = HotObjectsCacheConfig {
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
        };
        let cache = HotObjectsCache::new(config);

        for i in 0..3 {
            let blob = create_test_blob(&[i; 10]);
            cache.put(blob);
        }

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_stats() {
        let cache = HotObjectsCache::default();
        let blob = create_test_blob(b"test data");

        cache.put(blob.clone());
        cache.get(&blob.oid);
        cache.get(&Oid::hash(b"miss"));

        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate, 0.5);
    }

    #[test]
    fn test_cache_default() {
        let cache = HotObjectsCache::default();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_hit_rate_no_access() {
        let cache = HotObjectsCache::default();
        assert_eq!(cache.hit_rate(), 0.0);
    }

    #[test]
    fn test_cache_size_bytes() {
        let cache = HotObjectsCache::default();
        let blob = create_test_blob(b"12345678901234567890");
        cache.put(blob);
        assert_eq!(cache.size_bytes(), 20);
    }
}
