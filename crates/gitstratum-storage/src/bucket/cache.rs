use super::disk::DiskBucket;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct BucketCache {
    cache: Mutex<LruCache<u32, Box<DiskBucket>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BucketCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, bucket_id: u32) -> Option<Box<DiskBucket>> {
        let mut cache = self.cache.lock();
        if let Some(bucket) = cache.get(&bucket_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(bucket.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn put(&self, bucket_id: u32, bucket: DiskBucket) {
        let mut cache = self.cache.lock();
        cache.put(bucket_id, Box::new(bucket));
    }

    pub fn invalidate(&self, bucket_id: u32) {
        let mut cache = self.cache.lock();
        cache.pop(&bucket_id);
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

    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }

    pub fn clear(&self) {
        self.cache.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_new() {
        let cache = BucketCache::new(16);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.hit_rate(), 0.0);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = BucketCache::new(16);
        let bucket = DiskBucket::new();

        cache.put(0, bucket);
        assert_eq!(cache.len(), 1);

        let retrieved = cache.get(0);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = BucketCache::new(16);

        let retrieved = cache.get(0);
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = BucketCache::new(16);
        let bucket = DiskBucket::new();

        cache.put(0, bucket);
        assert_eq!(cache.len(), 1);

        cache.invalidate(0);
        assert_eq!(cache.len(), 0);

        let retrieved = cache.get(0);
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_cache_hit_rate() {
        let cache = BucketCache::new(16);
        let bucket = DiskBucket::new();

        cache.put(0, bucket);

        cache.get(0);
        cache.get(0);
        cache.get(1);

        let rate = cache.hit_rate();
        assert!((rate - 0.6666).abs() < 0.01);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = BucketCache::new(2);

        cache.put(0, DiskBucket::new());
        cache.put(1, DiskBucket::new());
        cache.put(2, DiskBucket::new());

        assert_eq!(cache.len(), 2);
        assert!(cache.get(0).is_none());
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
    }

    #[test]
    fn test_cache_clear() {
        let cache = BucketCache::new(16);
        cache.put(0, DiskBucket::new());
        cache.put(1, DiskBucket::new());

        cache.clear();
        assert_eq!(cache.len(), 0);
    }
}
