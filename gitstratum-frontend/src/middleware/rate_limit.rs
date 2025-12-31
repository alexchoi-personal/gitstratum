use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::error::{FrontendError, Result};

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub burst_size: u32,
    pub per_repo_limit: u32,
    pub per_user_limit: u32,
}

impl RateLimitConfig {
    pub fn new() -> Self {
        Self {
            requests_per_second: 100,
            burst_size: 200,
            per_repo_limit: 1000,
            per_user_limit: 100,
        }
    }

    pub fn with_rps(mut self, rps: u32) -> Self {
        self.requests_per_second = rps;
        self
    }

    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst_size = burst;
        self
    }

    pub fn with_per_repo(mut self, limit: u32) -> Self {
        self.per_repo_limit = limit;
        self
    }

    pub fn with_per_user(mut self, limit: u32) -> Self {
        self.per_user_limit = limit;
        self
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_update: Instant,
    rate: f64,
    capacity: f64,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_update: Instant::now(),
            rate,
            capacity,
        }
    }

    fn try_consume(&mut self, count: f64) -> bool {
        self.refill();
        if self.tokens >= count {
            self.tokens -= count;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);
        let new_tokens = elapsed.as_secs_f64() * self.rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity);
        self.last_update = now;
    }
}

pub struct RateLimitMiddleware {
    config: RateLimitConfig,
    user_buckets: Arc<RwLock<HashMap<String, TokenBucket>>>,
    repo_buckets: Arc<RwLock<HashMap<String, TokenBucket>>>,
    global_bucket: Arc<RwLock<TokenBucket>>,
}

impl RateLimitMiddleware {
    pub fn new(config: RateLimitConfig) -> Self {
        let global_bucket = TokenBucket::new(
            config.requests_per_second as f64,
            config.burst_size as f64,
        );

        Self {
            config,
            user_buckets: Arc::new(RwLock::new(HashMap::new())),
            repo_buckets: Arc::new(RwLock::new(HashMap::new())),
            global_bucket: Arc::new(RwLock::new(global_bucket)),
        }
    }

    pub async fn check_limit(&self, user_id: &str, repo_id: &str) -> Result<()> {
        {
            let mut global = self.global_bucket.write().await;
            if !global.try_consume(1.0) {
                return Err(FrontendError::InvalidProtocol(
                    "global rate limit exceeded".to_string(),
                ));
            }
        }

        {
            let mut buckets = self.user_buckets.write().await;
            let bucket = buckets.entry(user_id.to_string()).or_insert_with(|| {
                TokenBucket::new(
                    self.config.per_user_limit as f64,
                    self.config.per_user_limit as f64 * 2.0,
                )
            });
            if !bucket.try_consume(1.0) {
                return Err(FrontendError::InvalidProtocol(
                    "user rate limit exceeded".to_string(),
                ));
            }
        }

        {
            let mut buckets = self.repo_buckets.write().await;
            let bucket = buckets.entry(repo_id.to_string()).or_insert_with(|| {
                TokenBucket::new(
                    self.config.per_repo_limit as f64,
                    self.config.per_repo_limit as f64 * 2.0,
                )
            });
            if !bucket.try_consume(1.0) {
                return Err(FrontendError::InvalidProtocol(
                    "repository rate limit exceeded".to_string(),
                ));
            }
        }

        Ok(())
    }

    pub async fn cleanup_old_buckets(&self, max_age: Duration) {
        let cutoff = Instant::now() - max_age;

        {
            let mut buckets = self.user_buckets.write().await;
            buckets.retain(|_, bucket| bucket.last_update > cutoff);
        }

        {
            let mut buckets = self.repo_buckets.write().await;
            buckets.retain(|_, bucket| bucket.last_update > cutoff);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_config_new() {
        let config = RateLimitConfig::new();
        assert_eq!(config.requests_per_second, 100);
        assert_eq!(config.burst_size, 200);
    }

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert_eq!(config.requests_per_second, 100);
    }

    #[test]
    fn test_rate_limit_config_builders() {
        let config = RateLimitConfig::new()
            .with_rps(50)
            .with_burst(100)
            .with_per_repo(500)
            .with_per_user(50);

        assert_eq!(config.requests_per_second, 50);
        assert_eq!(config.burst_size, 100);
        assert_eq!(config.per_repo_limit, 500);
        assert_eq!(config.per_user_limit, 50);
    }

    #[test]
    fn test_token_bucket() {
        let mut bucket = TokenBucket::new(10.0, 20.0);

        assert!(bucket.try_consume(10.0));
        assert!(bucket.try_consume(10.0));
        assert!(!bucket.try_consume(1.0));
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_ok() {
        let config = RateLimitConfig::new();
        let middleware = RateLimitMiddleware::new(config);

        let result = middleware.check_limit("user-1", "repo-1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_exceeded() {
        let config = RateLimitConfig::new().with_per_user(1);
        let middleware = RateLimitMiddleware::new(config);

        middleware.check_limit("user-1", "repo-1").await.unwrap();
        middleware.check_limit("user-1", "repo-1").await.unwrap();

        let result = middleware.check_limit("user-1", "repo-1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rate_limit_cleanup() {
        let config = RateLimitConfig::new();
        let middleware = RateLimitMiddleware::new(config);

        middleware.check_limit("user-1", "repo-1").await.unwrap();

        middleware.cleanup_old_buckets(Duration::from_secs(0)).await;

        let buckets = middleware.user_buckets.read().await;
        assert!(buckets.is_empty());
    }
}
