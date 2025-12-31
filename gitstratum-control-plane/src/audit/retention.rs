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

        if self.oldest_entry_timestamp.is_none()
            || Some(timestamp) < self.oldest_entry_timestamp
        {
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
