use super::disk::DiskBucket;
use moka::sync::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct BucketCache {
    cache: Cache<u32, Arc<DiskBucket>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BucketCache {
    pub fn new(capacity: usize) -> Self {
        let cache = Cache::builder().max_capacity(capacity as u64).build();

        Self {
            cache,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, bucket_id: u32) -> Option<Arc<DiskBucket>> {
        if let Some(bucket) = self.cache.get(&bucket_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(bucket)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn put(&self, bucket_id: u32, bucket: DiskBucket) {
        self.cache.insert(bucket_id, Arc::new(bucket));
    }

    pub fn invalidate(&self, bucket_id: u32) {
        self.cache.invalidate(&bucket_id);
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
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.cache.invalidate_all();
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
        cache.cache.run_pending_tasks();
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

        cache.cache.run_pending_tasks();
        assert_eq!(cache.len(), 2);

        let mut present_count = 0;
        if cache.get(0).is_some() {
            present_count += 1;
        }
        if cache.get(1).is_some() {
            present_count += 1;
        }
        if cache.get(2).is_some() {
            present_count += 1;
        }
        assert_eq!(present_count, 2);
    }

    #[test]
    fn test_cache_clear() {
        let cache = BucketCache::new(16);
        cache.put(0, DiskBucket::new());
        cache.put(1, DiskBucket::new());

        cache.clear();
        cache.cache.run_pending_tasks();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_is_empty() {
        let cache = BucketCache::new(16);
        assert!(cache.is_empty());

        cache.put(0, DiskBucket::new());
        cache.cache.run_pending_tasks();
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_cache_concurrent_puts() {
        use std::thread;

        let cache = Arc::new(BucketCache::new(100));
        let mut handles = vec![];

        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for j in 0..10 {
                    let bucket_id = i * 10 + j;
                    cache_clone.put(bucket_id, DiskBucket::new());
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        cache.cache.run_pending_tasks();
        assert_eq!(cache.len(), 100);
    }

    #[test]
    fn test_cache_concurrent_gets() {
        use std::thread;

        let cache = Arc::new(BucketCache::new(100));

        for i in 0..50 {
            cache.put(i, DiskBucket::new());
        }
        cache.cache.run_pending_tasks();

        let mut handles = vec![];

        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    let _ = cache_clone.get(i);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let hits = cache.hits.load(Ordering::Relaxed);
        assert_eq!(hits, 500);
    }

    #[test]
    fn test_cache_put_overwrites() {
        let cache = BucketCache::new(16);

        cache.put(0, DiskBucket::new());
        cache.cache.run_pending_tasks();
        let first = cache.get(0).expect("Should exist");

        cache.put(0, DiskBucket::new());
        cache.cache.run_pending_tasks();
        let second = cache.get(0).expect("Should exist");

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_invalidate_nonexistent() {
        let cache = BucketCache::new(16);

        cache.invalidate(999);
        cache.cache.run_pending_tasks();

        assert!(cache.is_empty());
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn test_cache_zero_capacity() {
        let cache = BucketCache::new(0);

        cache.put(0, DiskBucket::new());
        cache.cache.run_pending_tasks();

        assert_eq!(cache.len(), 0);
        assert!(cache.get(0).is_none());
    }

    #[test]
    fn test_cache_stats_after_operations() {
        let cache = BucketCache::new(16);

        assert_eq!(cache.hits.load(Ordering::Relaxed), 0);
        assert_eq!(cache.misses.load(Ordering::Relaxed), 0);
        assert_eq!(cache.hit_rate(), 0.0);

        cache.get(0);
        assert_eq!(cache.misses.load(Ordering::Relaxed), 1);
        assert_eq!(cache.hit_rate(), 0.0);

        cache.put(0, DiskBucket::new());
        cache.get(0);
        assert_eq!(cache.hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.misses.load(Ordering::Relaxed), 1);
        assert_eq!(cache.hit_rate(), 0.5);

        cache.get(0);
        cache.get(0);
        assert_eq!(cache.hits.load(Ordering::Relaxed), 3);
        assert_eq!(cache.misses.load(Ordering::Relaxed), 1);
        assert_eq!(cache.hit_rate(), 0.75);
    }
}
