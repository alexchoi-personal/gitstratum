use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{debug, info, instrument};

use super::PackCache;

#[derive(Debug, Clone)]
pub struct TtlConfig {
    pub default_ttl: Duration,
    pub hot_repo_ttl: Duration,
    pub cleanup_interval: Duration,
}

impl TtlConfig {
    pub fn new(default_ttl: Duration, hot_repo_ttl: Duration, cleanup_interval: Duration) -> Self {
        Self {
            default_ttl,
            hot_repo_ttl,
            cleanup_interval,
        }
    }
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(300),
            hot_repo_ttl: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

pub struct TtlManager {
    config: TtlConfig,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl TtlManager {
    pub fn new(config: TtlConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            config,
            shutdown_tx,
            shutdown_rx,
        }
    }

    #[instrument(skip(self, cache))]
    pub async fn start(&self, cache: Arc<PackCache>) {
        let mut interval = interval(self.config.cleanup_interval);
        let mut shutdown_rx = self.shutdown_rx.clone();

        info!(
            interval_secs = self.config.cleanup_interval.as_secs(),
            "starting TTL manager"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let freed = cache.cleanup_expired();
                    if freed > 0 {
                        debug!(freed_bytes = freed, "cleaned up expired entries");
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("TTL manager shutting down");
                        break;
                    }
                }
            }
        }
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn config(&self) -> &TtlConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack_cache::{PackCacheKey, PackData};
    use bytes::Bytes;
    use gitstratum_core::Oid;

    fn create_test_pack(size: usize) -> PackData {
        PackData::new(
            Bytes::from(vec![0u8; size]),
            10,
            size as u64,
            Oid::hash(b"test"),
        )
    }

    #[test]
    fn test_ttl_config_default() {
        let config = TtlConfig::default();
        assert_eq!(config.default_ttl, Duration::from_secs(300));
        assert_eq!(config.hot_repo_ttl, Duration::from_secs(600));
        assert_eq!(config.cleanup_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_ttl_config_new() {
        let config = TtlConfig::new(
            Duration::from_secs(100),
            Duration::from_secs(200),
            Duration::from_secs(30),
        );
        assert_eq!(config.default_ttl, Duration::from_secs(100));
        assert_eq!(config.hot_repo_ttl, Duration::from_secs(200));
        assert_eq!(config.cleanup_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_ttl_manager_new() {
        let config = TtlConfig::default();
        let manager = TtlManager::new(config.clone());
        assert_eq!(manager.config().default_ttl, config.default_ttl);
    }

    #[tokio::test]
    async fn test_ttl_manager_cleanup() {
        let config = TtlConfig::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(25),
        );

        let cache = Arc::new(PackCache::new(1024 * 1024, config.default_ttl));
        let manager = TtlManager::new(config);

        let key = PackCacheKey::new("repo1", "refs/heads/main", 50);
        cache.put(key.clone(), create_test_pack(100)).unwrap();

        assert!(cache.contains(&key));

        let cache_clone = cache.clone();
        let handle = tokio::spawn(async move {
            manager.start(cache_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(!cache.contains(&key));

        handle.abort();
    }

    #[tokio::test]
    async fn test_ttl_manager_shutdown() {
        let config = TtlConfig::new(
            Duration::from_secs(300),
            Duration::from_secs(600),
            Duration::from_millis(10),
        );

        let cache = Arc::new(PackCache::new(1024 * 1024, config.default_ttl));
        let manager = Arc::new(TtlManager::new(config));

        let cache_clone = cache.clone();
        let manager_clone = manager.clone();
        let handle = tokio::spawn(async move {
            manager_clone.start(cache_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        manager.shutdown();

        let result = tokio::time::timeout(Duration::from_millis(100), handle).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_ttl_config_debug() {
        let config = TtlConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TtlConfig"));
    }

    #[test]
    fn test_ttl_config_clone() {
        let config = TtlConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.default_ttl, config.default_ttl);
    }
}
