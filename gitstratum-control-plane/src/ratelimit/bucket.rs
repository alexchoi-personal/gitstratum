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
