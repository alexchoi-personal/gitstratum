#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use gitstratum_core::Oid;
use lru::LruCache;
use parking_lot::RwLock;

use super::storage::StoredDelta;

struct CacheEntry {
    delta: StoredDelta,
    inserted_at: Instant,
}

pub struct DeltaCacheConfig {
    pub max_entries: usize,
    pub max_size_bytes: usize,
    pub ttl: Duration,
}

impl Default for DeltaCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            max_size_bytes: 100 * 1024 * 1024,
            ttl: Duration::from_secs(3600),
        }
    }
}

pub struct DeltaCache {
    config: DeltaCacheConfig,
    entries: RwLock<LruCache<(Oid, Oid), CacheEntry>>,
    current_size: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl DeltaCache {
    pub fn new(config: DeltaCacheConfig) -> Self {
        let cap = NonZeroUsize::new(config.max_entries).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            entries: RwLock::new(LruCache::new(cap)),
            config,
            current_size: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, base_oid: &Oid, target_oid: &Oid) -> Option<StoredDelta> {
        let key = (*base_oid, *target_oid);
        let mut entries = self.entries.write();

        if let Some(entry) = entries.get(&key) {
            if entry.inserted_at.elapsed() <= self.config.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.delta.clone());
            }
            let size = entry.delta.size as u64;
            entries.pop(&key);
            self.current_size.fetch_sub(size, Ordering::Relaxed);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn put(&self, delta: StoredDelta) {
        let key = (delta.base_oid, delta.target_oid);
        let size = delta.size;

        let mut entries = self.entries.write();

        if let Some(old) = entries.pop(&key) {
            self.current_size
                .fetch_sub(old.delta.size as u64, Ordering::Relaxed);
        }

        self.evict_for_size(&mut entries, size);

        let entry = CacheEntry {
            delta,
            inserted_at: Instant::now(),
        };

        entries.put(key, entry);
        self.current_size.fetch_add(size as u64, Ordering::Relaxed);
    }

    fn evict_for_size(&self, entries: &mut LruCache<(Oid, Oid), CacheEntry>, incoming_size: usize) {
        let now = Instant::now();
        loop {
            if let Some((_, entry)) = entries.peek_lru() {
                if now.duration_since(entry.inserted_at) > self.config.ttl {
                    if let Some((_, evicted)) = entries.pop_lru() {
                        self.current_size
                            .fetch_sub(evicted.delta.size as u64, Ordering::Relaxed);
                        continue;
                    }
                }
            }
            break;
        }

        while self.current_size.load(Ordering::Relaxed) as usize + incoming_size
            > self.config.max_size_bytes
        {
            if let Some((_, evicted)) = entries.pop_lru() {
                self.current_size
                    .fetch_sub(evicted.delta.size as u64, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    pub fn remove(&self, base_oid: &Oid, target_oid: &Oid) -> Option<StoredDelta> {
        let key = (*base_oid, *target_oid);
        let mut entries = self.entries.write();
        if let Some(entry) = entries.pop(&key) {
            self.current_size
                .fetch_sub(entry.delta.size as u64, Ordering::Relaxed);
            Some(entry.delta)
        } else {
            None
        }
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

    pub fn stats(&self) -> DeltaCacheStats {
        DeltaCacheStats {
            entries: self.len(),
            size_bytes: self.size_bytes(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
        }
    }
}

impl Default for DeltaCache {
    fn default() -> Self {
        Self::new(DeltaCacheConfig::default())
    }
}

#[derive(Debug, Clone)]
pub struct DeltaCacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::super::compute::{Delta, DeltaInstruction};
    use super::*;

    fn create_test_stored_delta(base: &[u8], target: &[u8]) -> StoredDelta {
        let delta = Delta {
            base_oid: Oid::hash(base),
            target_oid: Oid::hash(target),
            instructions: vec![DeltaInstruction::Insert {
                data: target.to_vec(),
            }],
        };
        StoredDelta::from_delta(&delta).unwrap()
    }

    #[test]
    fn test_delta_cache_config_default() {
        let config = DeltaCacheConfig::default();
        assert_eq!(config.max_entries, 10000);
        assert_eq!(config.max_size_bytes, 100 * 1024 * 1024);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = DeltaCache::default();
        let delta = create_test_stored_delta(b"base", b"target");

        cache.put(delta.clone());
        let retrieved = cache.get(&delta.base_oid, &delta.target_oid);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = DeltaCache::default();
        let base_oid = Oid::hash(b"nonexistent");
        let target_oid = Oid::hash(b"also nonexistent");

        let result = cache.get(&base_oid, &target_oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_remove() {
        let cache = DeltaCache::default();
        let delta = create_test_stored_delta(b"base", b"target");

        cache.put(delta.clone());
        let removed = cache.remove(&delta.base_oid, &delta.target_oid);
        assert!(removed.is_some());

        let result = cache.get(&delta.base_oid, &delta.target_oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = DeltaCache::default();
        let delta = create_test_stored_delta(b"base", b"target");

        cache.put(delta);
        assert!(!cache.is_empty());

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.size_bytes(), 0);
    }

    #[test]
    fn test_cache_len() {
        let cache = DeltaCache::default();
        assert_eq!(cache.len(), 0);

        let delta = create_test_stored_delta(b"base", b"target");
        cache.put(delta);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_eviction() {
        let config = DeltaCacheConfig {
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
            ttl: Duration::from_secs(3600),
        };
        let cache = DeltaCache::new(config);

        for i in 0..3 {
            let delta = create_test_stored_delta(&[i], &[i + 10]);
            cache.put(delta);
        }

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_stats() {
        let cache = DeltaCache::default();
        let delta = create_test_stored_delta(b"base", b"target");

        cache.put(delta.clone());
        cache.get(&delta.base_oid, &delta.target_oid);
        cache.get(&Oid::hash(b"miss"), &Oid::hash(b"miss2"));

        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate, 0.5);
    }

    #[test]
    fn test_cache_default() {
        let cache = DeltaCache::default();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_hit_rate_no_access() {
        let cache = DeltaCache::default();
        assert_eq!(cache.hit_rate(), 0.0);
    }

    #[test]
    fn test_cache_ttl_expiration_on_get() {
        let config = DeltaCacheConfig {
            max_entries: 100,
            max_size_bytes: 1024 * 1024,
            ttl: Duration::from_millis(1),
        };
        let cache = DeltaCache::new(config);
        let delta = create_test_stored_delta(b"base", b"target");

        cache.put(delta.clone());
        std::thread::sleep(Duration::from_millis(10));

        let result = cache.get(&delta.base_oid, &delta.target_oid);
        assert!(result.is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_put_replace_existing() {
        let cache = DeltaCache::default();
        let delta1 = create_test_stored_delta(b"base", b"target");
        let delta2 = create_test_stored_delta(b"base", b"target");

        cache.put(delta1);
        let size_before = cache.size_bytes();
        cache.put(delta2);
        let size_after = cache.size_bytes();

        assert_eq!(cache.len(), 1);
        assert_eq!(size_before, size_after);
    }

    #[test]
    fn test_cache_eviction_by_size() {
        let config = DeltaCacheConfig {
            max_entries: 100,
            max_size_bytes: 100,
            ttl: Duration::from_secs(3600),
        };
        let cache = DeltaCache::new(config);

        let delta1 = create_test_stored_delta(b"base1", b"target1_large_data");
        let delta2 = create_test_stored_delta(b"base2", b"target2_large_data");

        cache.put(delta1);
        cache.put(delta2);

        assert!(cache.size_bytes() <= 100);
    }

    #[test]
    fn test_cache_remove_nonexistent() {
        let cache = DeltaCache::default();
        let result = cache.remove(&Oid::hash(b"not"), &Oid::hash(b"there"));
        assert!(result.is_none());
    }

    #[test]
    fn test_delta_cache_stats_struct() {
        let stats = DeltaCacheStats {
            entries: 10,
            size_bytes: 1024,
            hits: 50,
            misses: 10,
            hit_rate: 0.833,
        };
        assert_eq!(stats.entries, 10);
        assert_eq!(stats.size_bytes, 1024);
        assert_eq!(stats.hits, 50);
        assert_eq!(stats.misses, 10);
    }

    #[test]
    fn test_cache_multiple_get_updates_access() {
        let cache = DeltaCache::default();
        let delta = create_test_stored_delta(b"base", b"target");

        cache.put(delta.clone());

        for _ in 0..5 {
            let _ = cache.get(&delta.base_oid, &delta.target_oid);
        }

        let stats = cache.stats();
        assert_eq!(stats.hits, 5);
    }
}
