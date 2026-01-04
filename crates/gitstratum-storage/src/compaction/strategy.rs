use std::time::Duration;

use crate::config::CompactionConfig;

#[allow(dead_code)]
pub(crate) struct CompactionStrategy {
    config: CompactionConfig,
}

#[allow(dead_code)]
impl CompactionStrategy {
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    pub fn should_compact(&self, dead_bytes: u64, total_bytes: u64) -> bool {
        if total_bytes == 0 {
            return false;
        }
        let fragmentation = dead_bytes as f64 / total_bytes as f64;
        fragmentation >= self.config.fragmentation_threshold
    }

    pub fn check_interval(&self) -> Duration {
        self.config.check_interval
    }

    pub fn max_concurrent(&self) -> usize {
        self.config.max_concurrent
    }
}
