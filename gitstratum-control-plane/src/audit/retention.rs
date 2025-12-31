use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetentionConfig {
    pub max_age: Duration,
    pub max_entries: usize,
    pub max_size_bytes: usize,
    pub compress_after: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(30 * 24 * 60 * 60),
            max_entries: 1_000_000,
            max_size_bytes: 1024 * 1024 * 1024,
            compress_after: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

pub struct RetentionPolicy {
    config: RetentionConfig,
    current_entries: usize,
    current_size_bytes: usize,
    oldest_entry_timestamp: Option<u64>,
}

impl RetentionPolicy {
    pub fn new(config: RetentionConfig) -> Self {
        Self {
            config,
            current_entries: 0,
            current_size_bytes: 0,
            oldest_entry_timestamp: None,
        }
    }

    pub fn should_evict(&self) -> bool {
        if self.current_entries > self.config.max_entries {
            return true;
        }

        if self.current_size_bytes > self.config.max_size_bytes {
            return true;
        }

        if let Some(oldest) = self.oldest_entry_timestamp {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let age_ms = now.saturating_sub(oldest);
            if age_ms > self.config.max_age.as_millis() as u64 {
                return true;
            }
        }

        false
    }

    pub fn should_compress(&self, entry_timestamp: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let age_ms = now.saturating_sub(entry_timestamp);
        age_ms > self.config.compress_after.as_millis() as u64
    }

    pub fn record_entry(&mut self, size_bytes: usize, timestamp: u64) {
        self.current_entries += 1;
        self.current_size_bytes += size_bytes;

        if self.oldest_entry_timestamp.is_none() || Some(timestamp) < self.oldest_entry_timestamp {
            self.oldest_entry_timestamp = Some(timestamp);
        }
    }

    pub fn record_eviction(&mut self, size_bytes: usize, new_oldest_timestamp: Option<u64>) {
        self.current_entries = self.current_entries.saturating_sub(1);
        self.current_size_bytes = self.current_size_bytes.saturating_sub(size_bytes);
        self.oldest_entry_timestamp = new_oldest_timestamp;
    }

    pub fn entries_over_limit(&self) -> usize {
        self.current_entries.saturating_sub(self.config.max_entries)
    }

    pub fn bytes_over_limit(&self) -> usize {
        self.current_size_bytes
            .saturating_sub(self.config.max_size_bytes)
    }

    pub fn current_entries(&self) -> usize {
        self.current_entries
    }

    pub fn current_size_bytes(&self) -> usize {
        self.current_size_bytes
    }

    pub fn config(&self) -> &RetentionConfig {
        &self.config
    }

    pub fn update_config(&mut self, config: RetentionConfig) {
        self.config = config;
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::new(RetentionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod retention_config_tests {
        use super::*;

        #[test]
        fn test_default_config() {
            let config = RetentionConfig::default();
            assert_eq!(config.max_age, Duration::from_secs(30 * 24 * 60 * 60));
            assert_eq!(config.max_entries, 1_000_000);
            assert_eq!(config.max_size_bytes, 1024 * 1024 * 1024);
            assert_eq!(config.compress_after, Duration::from_secs(7 * 24 * 60 * 60));
        }

        #[test]
        fn test_custom_config() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(60),
                max_entries: 100,
                max_size_bytes: 1024,
                compress_after: Duration::from_secs(30),
            };
            assert_eq!(config.max_age, Duration::from_secs(60));
            assert_eq!(config.max_entries, 100);
            assert_eq!(config.max_size_bytes, 1024);
            assert_eq!(config.compress_after, Duration::from_secs(30));
        }

        #[test]
        fn test_config_clone() {
            let config = RetentionConfig::default();
            let cloned = config.clone();
            assert_eq!(config.max_age, cloned.max_age);
            assert_eq!(config.max_entries, cloned.max_entries);
            assert_eq!(config.max_size_bytes, cloned.max_size_bytes);
            assert_eq!(config.compress_after, cloned.compress_after);
        }

        #[test]
        fn test_config_debug() {
            let config = RetentionConfig::default();
            let debug_str = format!("{:?}", config);
            assert!(debug_str.contains("RetentionConfig"));
            assert!(debug_str.contains("max_age"));
            assert!(debug_str.contains("max_entries"));
        }
    }

    mod retention_policy_tests {
        use super::*;

        #[test]
        fn test_new_policy() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 500,
                max_size_bytes: 2048,
                compress_after: Duration::from_secs(1800),
            };
            let policy = RetentionPolicy::new(config);
            assert_eq!(policy.current_entries(), 0);
            assert_eq!(policy.current_size_bytes(), 0);
            assert_eq!(policy.config().max_entries, 500);
            assert_eq!(policy.config().max_size_bytes, 2048);
        }

        #[test]
        fn test_default_policy() {
            let policy = RetentionPolicy::default();
            assert_eq!(policy.current_entries(), 0);
            assert_eq!(policy.current_size_bytes(), 0);
            assert_eq!(policy.config().max_entries, 1_000_000);
        }

        #[test]
        fn test_record_entry() {
            let mut policy = RetentionPolicy::default();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(100, timestamp);
            assert_eq!(policy.current_entries(), 1);
            assert_eq!(policy.current_size_bytes(), 100);

            policy.record_entry(200, timestamp + 1000);
            assert_eq!(policy.current_entries(), 2);
            assert_eq!(policy.current_size_bytes(), 300);
        }

        #[test]
        fn test_record_entry_updates_oldest_timestamp() {
            let mut policy = RetentionPolicy::default();

            policy.record_entry(100, 2000);
            policy.record_entry(100, 1000);
            policy.record_entry(100, 3000);

            assert_eq!(policy.current_entries(), 3);
        }

        #[test]
        fn test_record_eviction() {
            let mut policy = RetentionPolicy::default();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(100, timestamp);
            policy.record_entry(200, timestamp + 1000);
            assert_eq!(policy.current_entries(), 2);
            assert_eq!(policy.current_size_bytes(), 300);

            policy.record_eviction(100, Some(timestamp + 1000));
            assert_eq!(policy.current_entries(), 1);
            assert_eq!(policy.current_size_bytes(), 200);
        }

        #[test]
        fn test_record_eviction_saturating() {
            let mut policy = RetentionPolicy::default();

            policy.record_eviction(100, None);
            assert_eq!(policy.current_entries(), 0);
            assert_eq!(policy.current_size_bytes(), 0);
        }

        #[test]
        fn test_should_evict_under_limits() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 100,
                max_size_bytes: 10000,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(100, now);
            assert!(!policy.should_evict());
        }

        #[test]
        fn test_should_evict_entries_over_limit() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 2,
                max_size_bytes: 100000,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(10, now);
            policy.record_entry(10, now + 1);
            assert!(!policy.should_evict());

            policy.record_entry(10, now + 2);
            assert!(policy.should_evict());
        }

        #[test]
        fn test_should_evict_size_over_limit() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 1000,
                max_size_bytes: 100,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(50, now);
            assert!(!policy.should_evict());

            policy.record_entry(60, now + 1);
            assert!(policy.should_evict());
        }

        #[test]
        fn test_should_evict_age_over_limit() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(1),
                max_entries: 1000,
                max_size_bytes: 100000,
                compress_after: Duration::from_secs(1),
            };
            let mut policy = RetentionPolicy::new(config);

            let old_timestamp = 0u64;
            policy.record_entry(10, old_timestamp);
            assert!(policy.should_evict());
        }

        #[test]
        fn test_should_evict_no_oldest_timestamp() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(1),
                max_entries: 1000,
                max_size_bytes: 100000,
                compress_after: Duration::from_secs(1),
            };
            let policy = RetentionPolicy::new(config);
            assert!(!policy.should_evict());
        }

        #[test]
        fn test_should_compress_old_entry() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 1000,
                max_size_bytes: 100000,
                compress_after: Duration::from_secs(1),
            };
            let policy = RetentionPolicy::new(config);

            let old_timestamp = 0u64;
            assert!(policy.should_compress(old_timestamp));
        }

        #[test]
        fn test_should_compress_recent_entry() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 1000,
                max_size_bytes: 100000,
                compress_after: Duration::from_secs(3600),
            };
            let policy = RetentionPolicy::new(config);

            let recent_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            assert!(!policy.should_compress(recent_timestamp));
        }

        #[test]
        fn test_entries_over_limit() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 5,
                max_size_bytes: 100000,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            for i in 0..5 {
                policy.record_entry(10, now + i);
            }
            assert_eq!(policy.entries_over_limit(), 0);

            policy.record_entry(10, now + 5);
            policy.record_entry(10, now + 6);
            policy.record_entry(10, now + 7);
            assert_eq!(policy.entries_over_limit(), 3);
        }

        #[test]
        fn test_bytes_over_limit() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 1000,
                max_size_bytes: 100,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(50, now);
            assert_eq!(policy.bytes_over_limit(), 0);

            policy.record_entry(80, now + 1);
            assert_eq!(policy.bytes_over_limit(), 30);
        }

        #[test]
        fn test_update_config() {
            let mut policy = RetentionPolicy::default();
            assert_eq!(policy.config().max_entries, 1_000_000);

            let new_config = RetentionConfig {
                max_age: Duration::from_secs(60),
                max_entries: 50,
                max_size_bytes: 500,
                compress_after: Duration::from_secs(30),
            };
            policy.update_config(new_config);
            assert_eq!(policy.config().max_entries, 50);
            assert_eq!(policy.config().max_size_bytes, 500);
        }

        #[test]
        fn test_config_getter() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(120),
                max_entries: 200,
                max_size_bytes: 4096,
                compress_after: Duration::from_secs(60),
            };
            let policy = RetentionPolicy::new(config);
            let retrieved_config = policy.config();
            assert_eq!(retrieved_config.max_age, Duration::from_secs(120));
            assert_eq!(retrieved_config.max_entries, 200);
            assert_eq!(retrieved_config.max_size_bytes, 4096);
            assert_eq!(retrieved_config.compress_after, Duration::from_secs(60));
        }

        #[test]
        fn test_boundary_exactly_at_limit() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 3,
                max_size_bytes: 300,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(100, now);
            policy.record_entry(100, now + 1);
            policy.record_entry(100, now + 2);

            assert!(!policy.should_evict());
            assert_eq!(policy.entries_over_limit(), 0);
            assert_eq!(policy.bytes_over_limit(), 0);
        }

        #[test]
        fn test_multiple_evictions() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 100,
                max_size_bytes: 1000,
                compress_after: Duration::from_secs(1800),
            };
            let mut policy = RetentionPolicy::new(config);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            for i in 0..5 {
                policy.record_entry(100, now + i);
            }
            assert_eq!(policy.current_entries(), 5);
            assert_eq!(policy.current_size_bytes(), 500);

            policy.record_eviction(100, Some(now + 1));
            policy.record_eviction(100, Some(now + 2));
            policy.record_eviction(100, Some(now + 3));

            assert_eq!(policy.current_entries(), 2);
            assert_eq!(policy.current_size_bytes(), 200);
        }

        #[test]
        fn test_zero_size_entries() {
            let mut policy = RetentionPolicy::default();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            policy.record_entry(0, now);
            policy.record_entry(0, now + 1);

            assert_eq!(policy.current_entries(), 2);
            assert_eq!(policy.current_size_bytes(), 0);
        }

        #[test]
        fn test_large_values() {
            let config = RetentionConfig {
                max_age: Duration::from_secs(365 * 24 * 60 * 60),
                max_entries: usize::MAX / 2,
                max_size_bytes: usize::MAX / 2,
                compress_after: Duration::from_secs(30 * 24 * 60 * 60),
            };
            let policy = RetentionPolicy::new(config);
            assert_eq!(policy.config().max_entries, usize::MAX / 2);
            assert_eq!(policy.config().max_size_bytes, usize::MAX / 2);
        }
    }
}
