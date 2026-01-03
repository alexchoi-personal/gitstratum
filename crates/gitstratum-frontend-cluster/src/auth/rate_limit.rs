use dashmap::DashMap;
use std::time::{Duration, Instant};

pub struct AuthRateLimiter {
    failed_attempts: DashMap<String, (u32, Instant)>,
    max_attempts: u32,
    window: Duration,
    lockout: Duration,
}

impl AuthRateLimiter {
    pub fn new(max_attempts: u32, window_secs: u64, lockout_secs: u64) -> Self {
        Self {
            failed_attempts: DashMap::new(),
            max_attempts,
            window: Duration::from_secs(window_secs),
            lockout: Duration::from_secs(lockout_secs),
        }
    }

    pub fn check(&self, key: &str) -> bool {
        if let Some(entry) = self.failed_attempts.get(key) {
            let (count, window_start) = *entry;
            let elapsed = window_start.elapsed();

            if count >= self.max_attempts {
                if elapsed < self.lockout {
                    return false;
                }
            } else if elapsed >= self.window {
                drop(entry);
                self.failed_attempts.remove(key);
            }
        }
        true
    }

    pub fn record_failure(&self, key: &str) {
        self.failed_attempts
            .entry(key.to_string())
            .and_modify(|(count, start)| {
                if start.elapsed() >= self.window {
                    *count = 1;
                    *start = Instant::now();
                } else {
                    *count += 1;
                }
            })
            .or_insert((1, Instant::now()));
    }

    pub fn clear(&self, key: &str) {
        self.failed_attempts.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_rate_limiter() {
        let limiter = AuthRateLimiter::new(5, 300, 900);
        assert!(limiter.check("test"));
    }

    #[test]
    fn test_check_allows_initially() {
        let limiter = AuthRateLimiter::new(3, 60, 120);
        assert!(limiter.check("user@example.com"));
    }

    #[test]
    fn test_record_failure_and_check() {
        let limiter = AuthRateLimiter::new(3, 60, 120);

        limiter.record_failure("user@example.com");
        assert!(limiter.check("user@example.com"));

        limiter.record_failure("user@example.com");
        assert!(limiter.check("user@example.com"));

        limiter.record_failure("user@example.com");
        assert!(!limiter.check("user@example.com"));
    }

    #[test]
    fn test_clear_resets_failures() {
        let limiter = AuthRateLimiter::new(3, 60, 120);

        for _ in 0..3 {
            limiter.record_failure("user@example.com");
        }
        assert!(!limiter.check("user@example.com"));

        limiter.clear("user@example.com");
        assert!(limiter.check("user@example.com"));
    }

    #[test]
    fn test_different_keys_isolated() {
        let limiter = AuthRateLimiter::new(2, 60, 120);

        limiter.record_failure("user1@example.com");
        limiter.record_failure("user1@example.com");
        assert!(!limiter.check("user1@example.com"));

        assert!(limiter.check("user2@example.com"));
    }

    #[test]
    fn test_lockout_period() {
        let limiter = AuthRateLimiter::new(2, 1, 1);

        limiter.record_failure("user@example.com");
        limiter.record_failure("user@example.com");
        assert!(!limiter.check("user@example.com"));

        std::thread::sleep(Duration::from_millis(1100));
        assert!(limiter.check("user@example.com"));
    }

    #[test]
    fn test_window_reset() {
        let limiter = AuthRateLimiter::new(3, 1, 60);

        limiter.record_failure("user@example.com");
        limiter.record_failure("user@example.com");
        assert!(limiter.check("user@example.com"));

        std::thread::sleep(Duration::from_millis(1100));
        limiter.record_failure("user@example.com");
        assert!(limiter.check("user@example.com"));
    }
}
