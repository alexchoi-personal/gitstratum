use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum RateLimitError {
    GlobalLimitExceeded {
        operation: String,
        limit: String,
        retry_after: Duration,
    },
    PerClientLimitExceeded {
        operation: String,
        limit: String,
        retry_after: Duration,
    },
    TooManyWatchers {
        current: u32,
        max: u32,
    },
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RateLimitError::GlobalLimitExceeded {
                operation,
                limit,
                retry_after,
            } => {
                write!(
                    f,
                    "Global rate limit exceeded for {}: {} (retry after {:?})",
                    operation, limit, retry_after
                )
            }
            RateLimitError::PerClientLimitExceeded {
                operation,
                limit,
                retry_after,
            } => {
                write!(
                    f,
                    "Per-client rate limit exceeded for {}: {} (retry after {:?})",
                    operation, limit, retry_after
                )
            }
            RateLimitError::TooManyWatchers { current, max } => {
                write!(f, "Too many watchers: {} (max {})", current, max)
            }
        }
    }
}

impl std::error::Error for RateLimitError {}

pub struct TokenBucket {
    capacity: u32,
    tokens: AtomicU32,
    refill_rate: u32,
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_rate: u32) -> Self {
        Self {
            capacity,
            tokens: AtomicU32::new(capacity),
            refill_rate,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    fn refill(&self) {
        let mut last_refill = self.last_refill.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);
        let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u32;

        if tokens_to_add > 0 {
            let current = self.tokens.load(Ordering::Acquire);
            let new_tokens = current.saturating_add(tokens_to_add).min(self.capacity);
            self.tokens.store(new_tokens, Ordering::Release);
            *last_refill = now;
        }
    }

    pub fn try_acquire(&self) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            match self.tokens.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    pub fn time_until_available(&self) -> Duration {
        let current = self.tokens.load(Ordering::Acquire);
        if current > 0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(1.0 / self.refill_rate as f64)
    }
}

pub struct WatchGuard<'a> {
    counter: &'a AtomicU32,
}

impl<'a> Drop for WatchGuard<'a> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct GlobalRateLimiter {
    register_limiter: TokenBucket,
    heartbeat_limiter: TokenBucket,
    topology_limiter: TokenBucket,
    watch_connections: AtomicU32,
    max_watch_connections: u32,
}

impl GlobalRateLimiter {
    pub fn new() -> Self {
        Self {
            register_limiter: TokenBucket::new(200, 100),
            heartbeat_limiter: TokenBucket::new(20_000, 10_000),
            topology_limiter: TokenBucket::new(100_000, 50_000),
            watch_connections: AtomicU32::new(0),
            max_watch_connections: 1_000,
        }
    }

    pub fn try_register(&self) -> Result<(), RateLimitError> {
        if !self.register_limiter.try_acquire() {
            return Err(RateLimitError::GlobalLimitExceeded {
                operation: "RegisterNode".to_string(),
                limit: "100/sec".to_string(),
                retry_after: self.register_limiter.time_until_available(),
            });
        }
        Ok(())
    }

    pub fn try_heartbeat(&self) -> Result<(), RateLimitError> {
        if !self.heartbeat_limiter.try_acquire() {
            return Err(RateLimitError::GlobalLimitExceeded {
                operation: "Heartbeat".to_string(),
                limit: "10000/sec".to_string(),
                retry_after: self.heartbeat_limiter.time_until_available(),
            });
        }
        Ok(())
    }

    pub fn try_topology_read(&self) -> Result<(), RateLimitError> {
        if !self.topology_limiter.try_acquire() {
            return Err(RateLimitError::GlobalLimitExceeded {
                operation: "GetTopology".to_string(),
                limit: "50000/sec".to_string(),
                retry_after: self.topology_limiter.time_until_available(),
            });
        }
        Ok(())
    }

    pub fn try_watch(&self) -> Result<WatchGuard<'_>, RateLimitError> {
        loop {
            let current = self.watch_connections.load(Ordering::SeqCst);
            if current >= self.max_watch_connections {
                return Err(RateLimitError::TooManyWatchers {
                    current,
                    max: self.max_watch_connections,
                });
            }
            match self.watch_connections.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Ok(WatchGuard {
                        counter: &self.watch_connections,
                    });
                }
                Err(_) => continue,
            }
        }
    }

    pub fn watch_count(&self) -> u32 {
        self.watch_connections.load(Ordering::SeqCst)
    }

    pub fn decrement_watch(&self) {
        self.watch_connections.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn try_increment_watch(&self) -> Result<(), RateLimitError> {
        loop {
            let current = self.watch_connections.load(Ordering::SeqCst);
            if current >= self.max_watch_connections {
                return Err(RateLimitError::TooManyWatchers {
                    current,
                    max: self.max_watch_connections,
                });
            }
            match self.watch_connections.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue,
            }
        }
    }
}

impl Default for GlobalRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClientRateLimiter {
    register_limiter: TokenBucket,
    heartbeat_limiter: TokenBucket,
    topology_limiter: TokenBucket,
    watch_connections: AtomicU32,
    max_watch_connections: u32,
}

impl ClientRateLimiter {
    pub fn new() -> Self {
        Self {
            register_limiter: TokenBucket::new(10, 10),
            heartbeat_limiter: TokenBucket::new(100, 100),
            topology_limiter: TokenBucket::new(1_000, 1_000),
            watch_connections: AtomicU32::new(0),
            max_watch_connections: 10,
        }
    }

    pub fn try_register(&self) -> Result<(), RateLimitError> {
        if !self.register_limiter.try_acquire() {
            return Err(RateLimitError::PerClientLimitExceeded {
                operation: "RegisterNode".to_string(),
                limit: "10/min".to_string(),
                retry_after: self.register_limiter.time_until_available(),
            });
        }
        Ok(())
    }

    pub fn try_heartbeat(&self) -> Result<(), RateLimitError> {
        if !self.heartbeat_limiter.try_acquire() {
            return Err(RateLimitError::PerClientLimitExceeded {
                operation: "Heartbeat".to_string(),
                limit: "100/min".to_string(),
                retry_after: self.heartbeat_limiter.time_until_available(),
            });
        }
        Ok(())
    }

    pub fn try_topology_read(&self) -> Result<(), RateLimitError> {
        if !self.topology_limiter.try_acquire() {
            return Err(RateLimitError::PerClientLimitExceeded {
                operation: "GetTopology".to_string(),
                limit: "1000/min".to_string(),
                retry_after: self.topology_limiter.time_until_available(),
            });
        }
        Ok(())
    }

    pub fn try_watch(&self) -> Result<WatchGuard<'_>, RateLimitError> {
        loop {
            let current = self.watch_connections.load(Ordering::SeqCst);
            if current >= self.max_watch_connections {
                return Err(RateLimitError::TooManyWatchers {
                    current,
                    max: self.max_watch_connections,
                });
            }
            match self.watch_connections.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Ok(WatchGuard {
                        counter: &self.watch_connections,
                    });
                }
                Err(_) => continue,
            }
        }
    }

    pub fn watch_count(&self) -> u32 {
        self.watch_connections.load(Ordering::SeqCst)
    }
}

impl Default for ClientRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 10);
        for _ in 0..10 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn test_token_bucket_time_until_available_with_tokens() {
        let bucket = TokenBucket::new(10, 10);
        assert_eq!(bucket.time_until_available(), Duration::ZERO);
    }

    #[test]
    fn test_token_bucket_time_until_available_empty() {
        let bucket = TokenBucket::new(1, 10);
        bucket.try_acquire();
        let time = bucket.time_until_available();
        assert!(time > Duration::ZERO);
    }

    #[test]
    fn test_global_rate_limiter_register() {
        let limiter = GlobalRateLimiter::new();
        for _ in 0..200 {
            assert!(limiter.try_register().is_ok());
        }
        assert!(limiter.try_register().is_err());
    }

    #[test]
    fn test_global_rate_limiter_heartbeat() {
        let limiter = GlobalRateLimiter::new();
        for _ in 0..100 {
            assert!(limiter.try_heartbeat().is_ok());
        }
    }

    #[test]
    fn test_global_rate_limiter_topology_read() {
        let limiter = GlobalRateLimiter::new();
        for _ in 0..100 {
            assert!(limiter.try_topology_read().is_ok());
        }
    }

    #[test]
    fn test_global_rate_limiter_watch() {
        let limiter = GlobalRateLimiter::new();
        let mut guards = Vec::new();
        for _ in 0..1000 {
            guards.push(limiter.try_watch().unwrap());
        }
        assert!(limiter.try_watch().is_err());
        drop(guards.pop());
        assert!(limiter.try_watch().is_ok());
    }

    #[test]
    fn test_global_rate_limiter_default() {
        let limiter = GlobalRateLimiter::default();
        assert_eq!(limiter.watch_count(), 0);
    }

    #[test]
    fn test_global_rate_limiter_decrement_watch() {
        let limiter = GlobalRateLimiter::new();
        limiter.try_increment_watch().unwrap();
        assert_eq!(limiter.watch_count(), 1);
        limiter.decrement_watch();
        assert_eq!(limiter.watch_count(), 0);
    }

    #[test]
    fn test_global_rate_limiter_try_increment_watch() {
        let limiter = GlobalRateLimiter::new();
        for _ in 0..1000 {
            assert!(limiter.try_increment_watch().is_ok());
        }
        assert!(limiter.try_increment_watch().is_err());
    }

    #[test]
    fn test_watch_guard_decrement() {
        let limiter = GlobalRateLimiter::new();
        assert_eq!(limiter.watch_count(), 0);
        {
            let _guard = limiter.try_watch().unwrap();
            assert_eq!(limiter.watch_count(), 1);
        }
        assert_eq!(limiter.watch_count(), 0);
    }

    #[test]
    fn test_client_rate_limiter() {
        let limiter = ClientRateLimiter::new();
        for _ in 0..10 {
            assert!(limiter.try_register().is_ok());
        }
        assert!(limiter.try_register().is_err());
    }

    #[test]
    fn test_client_rate_limiter_heartbeat() {
        let limiter = ClientRateLimiter::new();
        for _ in 0..100 {
            assert!(limiter.try_heartbeat().is_ok());
        }
        assert!(limiter.try_heartbeat().is_err());
    }

    #[test]
    fn test_client_rate_limiter_topology_read() {
        let limiter = ClientRateLimiter::new();
        for _ in 0..1_000 {
            assert!(limiter.try_topology_read().is_ok());
        }
        assert!(limiter.try_topology_read().is_err());
    }

    #[test]
    fn test_client_rate_limiter_watch() {
        let limiter = ClientRateLimiter::new();
        let mut guards = Vec::new();
        for _ in 0..10 {
            guards.push(limiter.try_watch().unwrap());
        }
        assert!(limiter.try_watch().is_err());
        drop(guards.pop());
        assert!(limiter.try_watch().is_ok());
    }

    #[test]
    fn test_client_rate_limiter_watch_count() {
        let limiter = ClientRateLimiter::new();
        assert_eq!(limiter.watch_count(), 0);
        let _guard = limiter.try_watch().unwrap();
        assert_eq!(limiter.watch_count(), 1);
    }

    #[test]
    fn test_client_rate_limiter_default() {
        let limiter = ClientRateLimiter::default();
        assert_eq!(limiter.watch_count(), 0);
    }

    #[test]
    fn test_rate_limit_error_display_global() {
        let err = RateLimitError::GlobalLimitExceeded {
            operation: "RegisterNode".to_string(),
            limit: "100/sec".to_string(),
            retry_after: Duration::from_millis(100),
        };
        assert!(err.to_string().contains("RegisterNode"));
        assert!(err.to_string().contains("100/sec"));
        assert!(err.to_string().contains("Global"));
    }

    #[test]
    fn test_rate_limit_error_display_per_client() {
        let err = RateLimitError::PerClientLimitExceeded {
            operation: "Heartbeat".to_string(),
            limit: "100/min".to_string(),
            retry_after: Duration::from_millis(500),
        };
        assert!(err.to_string().contains("Heartbeat"));
        assert!(err.to_string().contains("Per-client"));
    }

    #[test]
    fn test_rate_limit_error_display_too_many_watchers() {
        let err = RateLimitError::TooManyWatchers {
            current: 1000,
            max: 1000,
        };
        assert!(err.to_string().contains("Too many watchers"));
        assert!(err.to_string().contains("1000"));
    }

    #[test]
    fn test_rate_limit_error_clone() {
        let err = RateLimitError::GlobalLimitExceeded {
            operation: "Test".to_string(),
            limit: "10/sec".to_string(),
            retry_after: Duration::from_secs(1),
        };
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }

    #[test]
    fn test_rate_limit_error_debug() {
        let err = RateLimitError::GlobalLimitExceeded {
            operation: "Test".to_string(),
            limit: "10/sec".to_string(),
            retry_after: Duration::from_secs(1),
        };
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("GlobalLimitExceeded"));
    }

    #[test]
    fn test_rate_limit_error_is_std_error() {
        let err = RateLimitError::GlobalLimitExceeded {
            operation: "Test".to_string(),
            limit: "10/sec".to_string(),
            retry_after: Duration::from_secs(1),
        };
        let _: &dyn std::error::Error = &err;
    }
}
