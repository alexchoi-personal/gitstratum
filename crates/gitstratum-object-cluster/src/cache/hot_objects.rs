use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use gitstratum_core::{Blob, Oid};
use lru::LruCache;
use parking_lot::Mutex;

use crate::error::{ObjectStoreError, Result};

struct CacheEntry {
    blob: Arc<Blob>,
    size: usize,
}

pub struct HotObjectsCacheConfig {
    pub max_entries: usize,
    pub max_size_bytes: usize,
    pub eviction_batch_size: usize,
}

impl Default for HotObjectsCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            max_size_bytes: 256 * 1024 * 1024,
            eviction_batch_size: 100,
        }
    }
}

pub struct HotObjectsCache {
    config: HotObjectsCacheConfig,
    entries: Mutex<LruCache<Oid, CacheEntry>>,
    current_size: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl HotObjectsCache {
    pub fn new(config: HotObjectsCacheConfig) -> Result<Self> {
        let capacity = NonZeroUsize::new(config.max_entries).ok_or_else(|| {
            ObjectStoreError::InvalidArgument("max_entries must be greater than 0".to_string())
        })?;
        Ok(Self {
            entries: Mutex::new(LruCache::new(capacity)),
            config,
            current_size: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    pub fn get(&self, oid: &Oid) -> Option<Arc<Blob>> {
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.get(oid) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(Arc::clone(&entry.blob));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn put(&self, blob: Blob) {
        let size = blob.data.len();
        let oid = blob.oid;

        let entry = CacheEntry {
            blob: Arc::new(blob),
            size,
        };

        self.evict_if_needed(size);

        let mut entries = self.entries.lock();

        if let Some(old) = entries.peek(&oid) {
            self.current_size
                .fetch_sub(old.size as u64, Ordering::Relaxed);
        }

        entries.put(oid, entry);
        self.current_size.fetch_add(size as u64, Ordering::Relaxed);
    }

    fn evict_if_needed(&self, incoming_size: usize) {
        let current = self.current_size.load(Ordering::Relaxed) as usize;
        if current + incoming_size <= self.config.max_size_bytes {
            return;
        }

        let mut evicted_count = 0;
        let batch_size = self.config.eviction_batch_size;

        while self.current_size.load(Ordering::Relaxed) as usize + incoming_size
            > self.config.max_size_bytes
            && evicted_count < batch_size
        {
            let mut entries = self.entries.lock();
            let mut batch_freed = 0usize;
            let mut local_evicted = 0;

            while batch_freed < incoming_size && local_evicted < 10 {
                if let Some((_, entry)) = entries.pop_lru() {
                    batch_freed += entry.size;
                    local_evicted += 1;
                } else {
                    break;
                }
            }

            self.current_size
                .fetch_sub(batch_freed as u64, Ordering::Relaxed);
            evicted_count += local_evicted;

            if local_evicted == 0 {
                break;
            }
        }
    }

    pub fn remove(&self, oid: &Oid) -> Option<Arc<Blob>> {
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.pop(oid) {
            self.current_size
                .fetch_sub(entry.size as u64, Ordering::Relaxed);
            Some(entry.blob)
        } else {
            None
        }
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        self.entries.lock().contains(oid)
    }

    pub fn clear(&self) {
        let mut entries = self.entries.lock();
        entries.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
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
        Self::new(HotObjectsCacheConfig::default()).expect("default config should always be valid")
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
    fn test_cache_eviction_by_count() {
        let config = HotObjectsCacheConfig {
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
            eviction_batch_size: 100,
        };
        let cache = HotObjectsCache::new(config).unwrap();

        for i in 0..3u8 {
            let blob = create_test_blob(&[i; 10]);
            cache.put(blob);
        }

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_eviction_by_size() {
        let config = HotObjectsCacheConfig {
            max_entries: 100,
            max_size_bytes: 50,
            eviction_batch_size: 100,
        };
        let cache = HotObjectsCache::new(config).unwrap();

        for i in 0..5u8 {
            let blob = create_test_blob(&[i; 20]);
            cache.put(blob);
        }

        assert!(cache.size_bytes() <= 50);
    }

    #[test]
    fn test_cache_lru_order() {
        let config = HotObjectsCacheConfig {
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
            eviction_batch_size: 100,
        };
        let cache = HotObjectsCache::new(config).unwrap();

        let blob1 = create_test_blob(b"first");
        let blob2 = create_test_blob(b"second");
        let blob3 = create_test_blob(b"third");

        cache.put(blob1.clone());
        cache.put(blob2.clone());

        cache.get(&blob1.oid);

        cache.put(blob3.clone());

        assert!(cache.contains(&blob1.oid));
        assert!(!cache.contains(&blob2.oid));
        assert!(cache.contains(&blob3.oid));
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

    #[test]
    fn test_cache_update_existing() {
        let cache = HotObjectsCache::default();
        let blob1 = create_test_blob(b"first version");
        let oid = blob1.oid;

        cache.put(blob1);
        assert_eq!(cache.size_bytes(), 13);

        let blob2 = Blob {
            oid,
            data: bytes::Bytes::from_static(b"second version longer"),
        };
        cache.put(blob2);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.size_bytes(), 21);
    }

    #[test]
    fn test_cache_new_zero_max_entries_fails() {
        let config = HotObjectsCacheConfig {
            max_entries: 0,
            max_size_bytes: 1024,
            eviction_batch_size: 100,
        };
        let result = HotObjectsCache::new(config);
        assert!(result.is_err());
    }
}
