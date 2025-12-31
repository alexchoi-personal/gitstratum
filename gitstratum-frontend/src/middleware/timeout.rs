use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;

use crate::error::{FrontendError, Result};

#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    pub connection_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub operation_timeout: Duration,
}

impl TimeoutConfig {
    pub fn new() -> Self {
        Self {
            connection_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(120),
            operation_timeout: Duration::from_secs(300),
        }
    }

    pub fn with_connection_timeout(mut self, duration: Duration) -> Self {
        self.connection_timeout = duration;
        self
    }

    pub fn with_read_timeout(mut self, duration: Duration) -> Self {
        self.read_timeout = duration;
        self
    }

    pub fn with_write_timeout(mut self, duration: Duration) -> Self {
        self.write_timeout = duration;
        self
    }

    pub fn with_operation_timeout(mut self, duration: Duration) -> Self {
        self.operation_timeout = duration;
        self
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TimeoutMiddleware {
    config: TimeoutConfig,
}

impl TimeoutMiddleware {
    pub fn new(config: TimeoutConfig) -> Self {
        Self { config }
    }

    pub async fn with_connection_timeout<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.with_timeout(future, self.config.connection_timeout, "connection").await
    }

    pub async fn with_read_timeout<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.with_timeout(future, self.config.read_timeout, "read").await
    }

    pub async fn with_write_timeout<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.with_timeout(future, self.config.write_timeout, "write").await
    }

    pub async fn with_operation_timeout<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.with_timeout(future, self.config.operation_timeout, "operation").await
    }

    async fn with_timeout<F, T>(&self, future: F, duration: Duration, kind: &str) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        match timeout(duration, future).await {
            Ok(result) => result,
            Err(_) => Err(FrontendError::InvalidProtocol(format!(
                "{} timeout after {:?}",
                kind, duration
            ))),
        }
    }

    pub fn config(&self) -> &TimeoutConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_config_new() {
        let config = TimeoutConfig::new();
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
        assert_eq!(config.write_timeout, Duration::from_secs(120));
        assert_eq!(config.operation_timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_timeout_config_default() {
        let config = TimeoutConfig::default();
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_timeout_config_builders() {
        let config = TimeoutConfig::new()
            .with_connection_timeout(Duration::from_secs(10))
            .with_read_timeout(Duration::from_secs(20))
            .with_write_timeout(Duration::from_secs(30))
            .with_operation_timeout(Duration::from_secs(40));

        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.read_timeout, Duration::from_secs(20));
        assert_eq!(config.write_timeout, Duration::from_secs(30));
        assert_eq!(config.operation_timeout, Duration::from_secs(40));
    }

    #[tokio::test]
    async fn test_timeout_middleware_success() {
        let config = TimeoutConfig::new();
        let middleware = TimeoutMiddleware::new(config);

        let result = middleware
            .with_connection_timeout(async { Ok(42) })
            .await
            .unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_timeout_middleware_timeout() {
        let config = TimeoutConfig::new().with_connection_timeout(Duration::from_millis(1));
        let middleware = TimeoutMiddleware::new(config);

        let result: Result<i32> = middleware
            .with_connection_timeout(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(42)
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_timeout_middleware_read() {
        let config = TimeoutConfig::new();
        let middleware = TimeoutMiddleware::new(config);

        let result = middleware.with_read_timeout(async { Ok(1) }).await.unwrap();
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_timeout_middleware_write() {
        let config = TimeoutConfig::new();
        let middleware = TimeoutMiddleware::new(config);

        let result = middleware.with_write_timeout(async { Ok(2) }).await.unwrap();
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn test_timeout_middleware_operation() {
        let config = TimeoutConfig::new();
        let middleware = TimeoutMiddleware::new(config);

        let result = middleware
            .with_operation_timeout(async { Ok(3) })
            .await
            .unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn test_timeout_middleware_config() {
        let config = TimeoutConfig::new();
        let middleware = TimeoutMiddleware::new(config);

        assert_eq!(
            middleware.config().connection_timeout,
            Duration::from_secs(30)
        );
    }
}
