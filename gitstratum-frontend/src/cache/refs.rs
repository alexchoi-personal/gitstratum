use gitstratum_core::Oid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct RefCacheConfig {
    pub ttl: Duration,
    pub max_entries: usize,
    pub max_refs_per_repo: usize,
}

impl RefCacheConfig {
    pub fn new() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            max_entries: 10000,
            max_refs_per_repo: 1000,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    pub fn with_max_refs_per_repo(mut self, max: usize) -> Self {
        self.max_refs_per_repo = max;
        self
    }
}

impl Default for RefCacheConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct CachedRefs {
    refs: Vec<(String, Oid)>,
    cached_at: Instant,
}

pub struct RefCache {
    config: RefCacheConfig,
    cache: Arc<RwLock<HashMap<String, CachedRefs>>>,
}

impl RefCache {
    pub fn new(config: RefCacheConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, repo_id: &str) -> Option<Vec<(String, Oid)>> {
        let cache = self.cache.read().await;
        if let Some(cached) = cache.get(repo_id) {
            if cached.cached_at.elapsed() < self.config.ttl {
                return Some(cached.refs.clone());
            }
        }
        None
    }

    pub async fn set(&self, repo_id: &str, refs: Vec<(String, Oid)>) {
        let mut cache = self.cache.write().await;

        if cache.len() >= self.config.max_entries && !cache.contains_key(repo_id) {
            self.evict_oldest(&mut cache);
        }

        let truncated = if refs.len() > self.config.max_refs_per_repo {
            refs.into_iter().take(self.config.max_refs_per_repo).collect()
        } else {
            refs
        };

        cache.insert(
            repo_id.to_string(),
            CachedRefs {
                refs: truncated,
                cached_at: Instant::now(),
            },
        );
    }

    pub async fn invalidate(&self, repo_id: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(repo_id);
    }

    pub async fn invalidate_all(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }

    pub async fn cleanup_expired(&self) {
        let mut cache = self.cache.write().await;
        let ttl = self.config.ttl;
        cache.retain(|_, v| v.cached_at.elapsed() < ttl);
    }

    pub async fn len(&self) -> usize {
        self.cache.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.cache.read().await.is_empty()
    }

    fn evict_oldest(&self, cache: &mut HashMap<String, CachedRefs>) {
        if let Some((oldest_key, _)) = cache
            .iter()
            .min_by_key(|(_, v)| v.cached_at)
            .map(|(k, v)| (k.clone(), v.cached_at))
        {
            cache.remove(&oldest_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_cache_config_new() {
        let config = RefCacheConfig::new();
        assert_eq!(config.ttl, Duration::from_secs(30));
        assert_eq!(config.max_entries, 10000);
        assert_eq!(config.max_refs_per_repo, 1000);
    }

    #[test]
    fn test_ref_cache_config_default() {
        let config = RefCacheConfig::default();
        assert_eq!(config.ttl, Duration::from_secs(30));
    }

    #[test]
    fn test_ref_cache_config_builders() {
        let config = RefCacheConfig::new()
            .with_ttl(Duration::from_secs(60))
            .with_max_entries(5000)
            .with_max_refs_per_repo(500);

        assert_eq!(config.ttl, Duration::from_secs(60));
        assert_eq!(config.max_entries, 5000);
        assert_eq!(config.max_refs_per_repo, 500);
    }

    #[tokio::test]
    async fn test_ref_cache_get_set() {
        let config = RefCacheConfig::new();
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        let refs = vec![("refs/heads/main".to_string(), oid)];

        cache.set("repo-1", refs.clone()).await;

        let cached = cache.get("repo-1").await.unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].0, "refs/heads/main");
    }

    #[tokio::test]
    async fn test_ref_cache_miss() {
        let config = RefCacheConfig::new();
        let cache = RefCache::new(config);

        let result = cache.get("nonexistent").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ref_cache_expire() {
        let config = RefCacheConfig::new().with_ttl(Duration::from_millis(1));
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        let refs = vec![("refs/heads/main".to_string(), oid)];

        cache.set("repo-1", refs).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let result = cache.get("repo-1").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ref_cache_invalidate() {
        let config = RefCacheConfig::new();
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        let refs = vec![("refs/heads/main".to_string(), oid)];

        cache.set("repo-1", refs).await;
        cache.invalidate("repo-1").await;

        let result = cache.get("repo-1").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ref_cache_invalidate_all() {
        let config = RefCacheConfig::new();
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        cache.set("repo-1", vec![("refs/heads/main".to_string(), oid)]).await;
        cache.set("repo-2", vec![("refs/heads/main".to_string(), oid)]).await;

        cache.invalidate_all().await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_ref_cache_cleanup_expired() {
        let config = RefCacheConfig::new().with_ttl(Duration::from_millis(1));
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        cache.set("repo-1", vec![("refs/heads/main".to_string(), oid)]).await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        cache.cleanup_expired().await;

        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_ref_cache_max_entries() {
        let config = RefCacheConfig::new().with_max_entries(2);
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        cache.set("repo-1", vec![("refs/heads/main".to_string(), oid)]).await;
        cache.set("repo-2", vec![("refs/heads/main".to_string(), oid)]).await;
        cache.set("repo-3", vec![("refs/heads/main".to_string(), oid)]).await;

        assert_eq!(cache.len().await, 2);
    }

    #[tokio::test]
    async fn test_ref_cache_truncate_refs() {
        let config = RefCacheConfig::new().with_max_refs_per_repo(1);
        let cache = RefCache::new(config);

        let oid = Oid::hash(b"test");
        let refs = vec![
            ("refs/heads/main".to_string(), oid),
            ("refs/heads/feature".to_string(), oid),
        ];

        cache.set("repo-1", refs).await;

        let cached = cache.get("repo-1").await.unwrap();
        assert_eq!(cached.len(), 1);
    }
}
