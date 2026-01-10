use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use gitstratum_core::{Blob, Oid};
use moka::sync::Cache;

use crate::error::{ObjectStoreError, Result};

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
    cache: Cache<Oid, Arc<Blob>>,
    current_size: AtomicU64,
    max_size_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl HotObjectsCache {
    pub fn new(config: HotObjectsCacheConfig) -> Result<Self> {
        if config.max_entries == 0 {
            return Err(ObjectStoreError::InvalidArgument(
                "max_entries must be greater than 0".to_string(),
            ));
        }

        let cache = Cache::builder()
            .max_capacity(config.max_entries as u64)
            .weigher(|_key: &Oid, value: &Arc<Blob>| -> u32 {
                value.data.len().min(u32::MAX as usize) as u32
            })
            .build();

        Ok(Self {
            cache,
            current_size: AtomicU64::new(0),
            max_size_bytes: config.max_size_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    pub fn get(&self, oid: &Oid) -> Option<Arc<Blob>> {
        if let Some(blob) = self.cache.get(oid) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(blob)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn put(&self, blob: Blob) {
        let size = blob.data.len();
        let oid = blob.oid;

        if let Some(old) = self.cache.get(&oid) {
            self.current_size
                .fetch_sub(old.data.len() as u64, Ordering::Relaxed);
        }

        let arc_blob = Arc::new(blob);
        self.cache.insert(oid, arc_blob);
        self.current_size.fetch_add(size as u64, Ordering::Relaxed);
    }

    pub fn remove(&self, oid: &Oid) -> Option<Arc<Blob>> {
        if let Some(blob) = self.cache.remove(oid) {
            self.current_size
                .fetch_sub(blob.data.len() as u64, Ordering::Relaxed);
            Some(blob)
        } else {
            None
        }
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        self.cache.contains_key(oid)
    }

    pub fn clear(&self) {
        self.cache.invalidate_all();
        self.current_size.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size_bytes(&self) -> usize {
        self.current_size.load(Ordering::Relaxed) as usize
    }

    pub fn max_size_bytes(&self) -> usize {
        self.max_size_bytes
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
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        let blob = create_test_blob(b"hello world");

        cache.put(blob.clone());
        let retrieved = cache.get(&blob.oid);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.as_ref(), b"hello world");
    }

    #[test]
    fn test_cache_miss() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        let oid = Oid::hash(b"nonexistent");

        let result = cache.get(&oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_remove() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        let blob = create_test_blob(b"test data");

        cache.put(blob.clone());
        let removed = cache.remove(&blob.oid);
        assert!(removed.is_some());

        let result = cache.get(&blob.oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_contains() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        let blob = create_test_blob(b"test");

        assert!(!cache.contains(&blob.oid));
        cache.put(blob.clone());
        assert!(cache.contains(&blob.oid));
    }

    #[test]
    fn test_cache_clear() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        let blob = create_test_blob(b"test");

        cache.put(blob);
        assert!(!cache.is_empty());

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.size_bytes(), 0);
    }

    #[test]
    fn test_cache_len() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        assert_eq!(cache.len(), 0);

        let blob = create_test_blob(b"test");
        cache.put(blob);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_stats() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
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
    fn test_cache_hit_rate_no_access() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        assert_eq!(cache.hit_rate(), 0.0);
    }

    #[test]
    fn test_cache_size_bytes() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
        let blob = create_test_blob(b"12345678901234567890");
        cache.put(blob);
        assert_eq!(cache.size_bytes(), 20);
    }

    #[test]
    fn test_cache_update_existing() {
        let cache = HotObjectsCache::new(HotObjectsCacheConfig::default()).unwrap();
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
        };
        let result = HotObjectsCache::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_cache_max_size_bytes() {
        let config = HotObjectsCacheConfig {
            max_entries: 100,
            max_size_bytes: 512,
        };
        let cache = HotObjectsCache::new(config).unwrap();
        assert_eq!(cache.max_size_bytes(), 512);
    }
}
