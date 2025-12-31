use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    pub default_ttl: Duration,
    pub min_ttl: Duration,
    pub max_ttl: Duration,
    pub hot_repo_multiplier: f64,
    pub cold_repo_divisor: f64,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(300),
            min_ttl: Duration::from_secs(60),
            max_ttl: Duration::from_secs(3600),
            hot_repo_multiplier: 2.0,
            cold_repo_divisor: 2.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoTemperature {
    Hot,
    Warm,
    Cold,
}

pub struct TtlCalculator {
    config: TtlConfig,
}

impl TtlCalculator {
    pub fn new(config: TtlConfig) -> Self {
        Self { config }
    }

    pub fn calculate(&self, temperature: RepoTemperature) -> Duration {
        let ttl = match temperature {
            RepoTemperature::Hot => {
                let secs = self.config.default_ttl.as_secs_f64() * self.config.hot_repo_multiplier;
                Duration::from_secs_f64(secs)
            }
            RepoTemperature::Warm => self.config.default_ttl,
            RepoTemperature::Cold => {
                let secs = self.config.default_ttl.as_secs_f64() / self.config.cold_repo_divisor;
                Duration::from_secs_f64(secs)
            }
        };

        ttl.clamp(self.config.min_ttl, self.config.max_ttl)
    }

    pub fn calculate_from_hit_rate(&self, hit_rate: f64) -> Duration {
        let temperature = if hit_rate >= 0.8 {
            RepoTemperature::Hot
        } else if hit_rate >= 0.3 {
            RepoTemperature::Warm
        } else {
            RepoTemperature::Cold
        };

        self.calculate(temperature)
    }

    pub fn calculate_from_access_count(
        &self,
        access_count: u64,
        time_window_secs: u64,
    ) -> Duration {
        let rate = access_count as f64 / time_window_secs as f64;

        let temperature = if rate >= 1.0 {
            RepoTemperature::Hot
        } else if rate >= 0.1 {
            RepoTemperature::Warm
        } else {
            RepoTemperature::Cold
        };

        self.calculate(temperature)
    }

    pub fn config(&self) -> &TtlConfig {
        &self.config
    }
}

impl Default for TtlCalculator {
    fn default() -> Self {
        Self::new(TtlConfig::default())
    }
}

#[derive(Debug, Clone)]
pub struct TtlEntry {
    pub created_at: i64,
    pub expires_at: i64,
    pub ttl_secs: i64,
}

impl TtlEntry {
    pub fn new(ttl: Duration) -> Self {
        let now = chrono::Utc::now().timestamp();
        let ttl_secs = ttl.as_secs() as i64;
        Self {
            created_at: now,
            expires_at: now + ttl_secs,
            ttl_secs,
        }
    }

    pub fn is_expired(&self) -> bool {
        chrono::Utc::now().timestamp() > self.expires_at
    }

    pub fn remaining_secs(&self) -> i64 {
        let now = chrono::Utc::now().timestamp();
        (self.expires_at - now).max(0)
    }

    pub fn age_secs(&self) -> i64 {
        let now = chrono::Utc::now().timestamp();
        now - self.created_at
    }

    pub fn extend(&mut self, additional: Duration) {
        self.expires_at += additional.as_secs() as i64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_config_default() {
        let config = TtlConfig::default();
        assert_eq!(config.default_ttl, Duration::from_secs(300));
        assert_eq!(config.min_ttl, Duration::from_secs(60));
        assert_eq!(config.max_ttl, Duration::from_secs(3600));
        assert_eq!(config.hot_repo_multiplier, 2.0);
        assert_eq!(config.cold_repo_divisor, 2.0);
    }

    #[test]
    fn test_ttl_calculator_hot() {
        let calculator = TtlCalculator::default();
        let ttl = calculator.calculate(RepoTemperature::Hot);
        assert_eq!(ttl, Duration::from_secs(600));
    }

    #[test]
    fn test_ttl_calculator_warm() {
        let calculator = TtlCalculator::default();
        let ttl = calculator.calculate(RepoTemperature::Warm);
        assert_eq!(ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_ttl_calculator_cold() {
        let calculator = TtlCalculator::default();
        let ttl = calculator.calculate(RepoTemperature::Cold);
        assert_eq!(ttl, Duration::from_secs(150));
    }

    #[test]
    fn test_ttl_calculator_clamping_max() {
        let config = TtlConfig {
            default_ttl: Duration::from_secs(2000),
            min_ttl: Duration::from_secs(60),
            max_ttl: Duration::from_secs(3600),
            hot_repo_multiplier: 2.0,
            cold_repo_divisor: 2.0,
        };
        let calculator = TtlCalculator::new(config);
        let ttl = calculator.calculate(RepoTemperature::Hot);
        assert_eq!(ttl, Duration::from_secs(3600));
    }

    #[test]
    fn test_ttl_calculator_clamping_min() {
        let config = TtlConfig {
            default_ttl: Duration::from_secs(100),
            min_ttl: Duration::from_secs(60),
            max_ttl: Duration::from_secs(3600),
            hot_repo_multiplier: 2.0,
            cold_repo_divisor: 4.0,
        };
        let calculator = TtlCalculator::new(config);
        let ttl = calculator.calculate(RepoTemperature::Cold);
        assert_eq!(ttl, Duration::from_secs(60));
    }

    #[test]
    fn test_ttl_calculator_from_hit_rate() {
        let calculator = TtlCalculator::default();

        let hot = calculator.calculate_from_hit_rate(0.9);
        let warm = calculator.calculate_from_hit_rate(0.5);
        let cold = calculator.calculate_from_hit_rate(0.1);

        assert!(hot > warm);
        assert!(warm > cold);
    }

    #[test]
    fn test_ttl_calculator_from_access_count() {
        let calculator = TtlCalculator::default();

        let hot = calculator.calculate_from_access_count(100, 60);
        let warm = calculator.calculate_from_access_count(10, 60);
        let cold = calculator.calculate_from_access_count(1, 60);

        assert!(hot > warm);
        assert!(warm > cold);
    }

    #[test]
    fn test_ttl_calculator_config() {
        let config = TtlConfig::default();
        let calculator = TtlCalculator::new(config.clone());

        assert_eq!(calculator.config().default_ttl, config.default_ttl);
    }

    #[test]
    fn test_ttl_entry_new() {
        let entry = TtlEntry::new(Duration::from_secs(300));
        assert!(!entry.is_expired());
        assert!(entry.remaining_secs() > 0);
        assert!(entry.age_secs() >= 0);
    }

    #[test]
    fn test_ttl_entry_extend() {
        let mut entry = TtlEntry::new(Duration::from_secs(100));
        let original_expires_at = entry.expires_at;

        entry.extend(Duration::from_secs(100));
        assert_eq!(entry.expires_at, original_expires_at + 100);
    }

    #[test]
    fn test_repo_temperature() {
        assert_eq!(RepoTemperature::Hot, RepoTemperature::Hot);
        assert_ne!(RepoTemperature::Hot, RepoTemperature::Cold);
    }
}
