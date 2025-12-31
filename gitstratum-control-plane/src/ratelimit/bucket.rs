use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TokenBucketConfig {
    pub capacity: u64,
    pub refill_rate: f64,
    pub refill_interval: Duration,
}

impl Default for TokenBucketConfig {
    fn default() -> Self {
        Self {
            capacity: 100,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        }
    }
}

pub struct TokenBucket {
    config: TokenBucketConfig,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(config: TokenBucketConfig) -> Self {
        Self {
            tokens: config.capacity as f64,
            config,
            last_refill: Instant::now(),
        }
    }

    pub fn try_consume(&mut self, tokens: u64) -> bool {
        self.refill();

        if self.tokens >= tokens as f64 {
            self.tokens -= tokens as f64;
            true
        } else {
            false
        }
    }

    pub fn consume(&mut self, tokens: u64) -> Duration {
        self.refill();

        if self.tokens >= tokens as f64 {
            self.tokens -= tokens as f64;
            Duration::ZERO
        } else {
            let deficit = tokens as f64 - self.tokens;
            let wait_intervals = (deficit / self.config.refill_rate).ceil();
            let wait_time = self.config.refill_interval.mul_f64(wait_intervals);

            self.tokens = 0.0;

            wait_time
        }
    }

    pub fn available_tokens(&self) -> u64 {
        self.tokens as u64
    }

    pub fn capacity(&self) -> u64 {
        self.config.capacity
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        let intervals = elapsed.as_secs_f64() / self.config.refill_interval.as_secs_f64();
        let new_tokens = intervals * self.config.refill_rate;

        self.tokens = (self.tokens + new_tokens).min(self.config.capacity as f64);
        self.last_refill = now;
    }

    pub fn config(&self) -> &TokenBucketConfig {
        &self.config
    }

    pub fn time_until_tokens(&self, tokens: u64) -> Duration {
        if self.tokens >= tokens as f64 {
            Duration::ZERO
        } else {
            let deficit = tokens as f64 - self.tokens;
            let intervals = deficit / self.config.refill_rate;
            self.config.refill_interval.mul_f64(intervals)
        }
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new(TokenBucketConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_config_and_initialization() {
        let default_config = TokenBucketConfig::default();
        assert_eq!(default_config.capacity, 100);
        assert_eq!(default_config.refill_rate, 10.0);
        assert_eq!(default_config.refill_interval, Duration::from_secs(1));

        let cloned = default_config.clone();
        assert_eq!(cloned.capacity, 100);

        let debug_str = format!("{:?}", default_config);
        assert!(debug_str.contains("TokenBucketConfig"));
        assert!(debug_str.contains("capacity"));
        assert!(debug_str.contains("refill_rate"));
        assert!(debug_str.contains("refill_interval"));

        let custom_config = TokenBucketConfig {
            capacity: 75,
            refill_rate: 7.5,
            refill_interval: Duration::from_millis(100),
        };
        let bucket = TokenBucket::new(custom_config);
        assert_eq!(bucket.capacity(), 75);
        assert_eq!(bucket.available_tokens(), 75);
        let retrieved_config = bucket.config();
        assert_eq!(retrieved_config.capacity, 75);
        assert_eq!(retrieved_config.refill_rate, 7.5);
        assert_eq!(retrieved_config.refill_interval, Duration::from_millis(100));

        let default_bucket = TokenBucket::default();
        assert_eq!(default_bucket.capacity(), 100);
        assert_eq!(default_bucket.available_tokens(), 100);

        let large_config = TokenBucketConfig {
            capacity: u64::MAX,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        };
        let large_bucket = TokenBucket::new(large_config);
        assert_eq!(large_bucket.capacity(), u64::MAX);
    }

    #[test]
    fn test_token_consumption_and_blocking() {
        let mut bucket = TokenBucket::default();
        assert!(bucket.try_consume(0));
        assert_eq!(bucket.available_tokens(), 100);
        let wait = bucket.consume(0);
        assert_eq!(wait, Duration::ZERO);
        assert_eq!(bucket.available_tokens(), 100);

        assert!(bucket.try_consume(10));
        assert_eq!(bucket.available_tokens(), 90);

        let wait = bucket.consume(10);
        assert_eq!(wait, Duration::ZERO);
        assert_eq!(bucket.available_tokens(), 80);

        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        assert!(bucket.try_consume(10));
        assert_eq!(bucket.available_tokens(), 0);

        let config = TokenBucketConfig {
            capacity: 50,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        let wait = bucket.consume(50);
        assert_eq!(wait, Duration::ZERO);
        assert_eq!(bucket.available_tokens(), 0);

        let config = TokenBucketConfig {
            capacity: 5,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        assert!(!bucket.try_consume(10));
        assert_eq!(bucket.available_tokens(), 5);

        let config = TokenBucketConfig {
            capacity: 5,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        let wait = bucket.consume(15);
        assert!(wait > Duration::ZERO);
        assert_eq!(wait, Duration::from_secs(1));
        assert_eq!(bucket.available_tokens(), 0);

        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 5.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        let wait = bucket.consume(20);
        assert!(wait >= Duration::from_secs(2));

        let mut bucket = TokenBucket::default();
        assert!(bucket.try_consume(30));
        assert!(bucket.try_consume(30));
        assert!(bucket.try_consume(30));
        assert!(!bucket.try_consume(30));
        assert_eq!(bucket.available_tokens(), 10);

        let config = TokenBucketConfig {
            capacity: 100,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        assert!(bucket.try_consume(100));
        assert!(!bucket.try_consume(1));
    }

    #[test]
    fn test_time_until_tokens_calculation() {
        let bucket = TokenBucket::default();
        assert_eq!(bucket.time_until_tokens(50), Duration::ZERO);
        assert_eq!(bucket.time_until_tokens(100), Duration::ZERO);

        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        bucket.try_consume(10);
        let wait = bucket.time_until_tokens(5);
        assert_eq!(wait, Duration::from_millis(500));

        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 1.0,
            refill_interval: Duration::from_secs(1),
        };
        let bucket = TokenBucket::new(config);
        let wait = bucket.time_until_tokens(20);
        assert_eq!(wait, Duration::from_secs(10));
    }

    #[test]
    fn test_refill_behavior_and_edge_cases() {
        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 1000.0,
            refill_interval: Duration::from_millis(1),
        };
        let mut bucket = TokenBucket::new(config);
        bucket.try_consume(10);
        std::thread::sleep(Duration::from_millis(10));
        assert!(bucket.try_consume(1));

        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 1000.0,
            refill_interval: Duration::from_millis(1),
        };
        let mut bucket = TokenBucket::new(config);
        std::thread::sleep(Duration::from_millis(100));
        bucket.try_consume(0);
        assert_eq!(bucket.available_tokens(), 10);

        let config = TokenBucketConfig {
            capacity: 100,
            refill_rate: 100.0,
            refill_interval: Duration::from_millis(100),
        };
        let mut bucket = TokenBucket::new(config);
        bucket.try_consume(50);
        std::thread::sleep(Duration::from_millis(50));
        assert!(bucket.try_consume(50));

        let config = TokenBucketConfig {
            capacity: 100,
            refill_rate: 0.5,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        bucket.try_consume(100);
        std::thread::sleep(Duration::from_millis(100));
        bucket.try_consume(0);
        assert!(bucket.available_tokens() < 1);

        let config = TokenBucketConfig {
            capacity: 100,
            refill_rate: 10.0,
            refill_interval: Duration::from_nanos(1),
        };
        let mut bucket = TokenBucket::new(config);
        bucket.try_consume(50);
        std::thread::sleep(Duration::from_micros(10));
        assert!(bucket.available_tokens() >= 50);

        let config = TokenBucketConfig {
            capacity: 0,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        assert_eq!(bucket.capacity(), 0);
        assert_eq!(bucket.available_tokens(), 0);
        assert!(!bucket.try_consume(1));
        assert!(bucket.try_consume(0));

        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 0.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);
        bucket.try_consume(5);
        std::thread::sleep(Duration::from_millis(10));
        bucket.try_consume(0);
        assert_eq!(bucket.available_tokens(), 5);
    }
}
