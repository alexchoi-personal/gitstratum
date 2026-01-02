use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    pub interval: Duration,
    pub timeout: Duration,
    pub unhealthy_threshold: u32,
    pub healthy_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
        }
    }
}

pub struct HealthCheck {
    config: HealthCheckConfig,
    consecutive_failures: u32,
    consecutive_successes: u32,
    status: HealthStatus,
}

impl HealthCheck {
    pub fn new(config: HealthCheckConfig) -> Self {
        Self {
            config,
            consecutive_failures: 0,
            consecutive_successes: 0,
            status: HealthStatus::Unknown,
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.consecutive_successes += 1;

        if self.consecutive_successes >= self.config.healthy_threshold {
            self.status = HealthStatus::Healthy;
        }
    }

    pub fn record_failure(&mut self) {
        self.consecutive_successes = 0;
        self.consecutive_failures += 1;

        if self.consecutive_failures >= self.config.unhealthy_threshold {
            self.status = HealthStatus::Unhealthy;
        } else if self.consecutive_failures > 0 {
            self.status = HealthStatus::Degraded;
        }
    }

    pub fn status(&self) -> HealthStatus {
        self.status
    }

    pub fn config(&self) -> &HealthCheckConfig {
        &self.config
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub fn consecutive_successes(&self) -> u32 {
        self.consecutive_successes
    }
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new(HealthCheckConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_variants() {
        assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
        assert_eq!(HealthStatus::Degraded, HealthStatus::Degraded);
        assert_eq!(HealthStatus::Unhealthy, HealthStatus::Unhealthy);
        assert_eq!(HealthStatus::Unknown, HealthStatus::Unknown);

        assert_ne!(HealthStatus::Healthy, HealthStatus::Unhealthy);
        assert_ne!(HealthStatus::Degraded, HealthStatus::Unknown);
    }

    #[test]
    fn test_health_status_default() {
        let status = HealthStatus::default();
        assert_eq!(status, HealthStatus::Unknown);
    }

    #[test]
    fn test_health_status_clone() {
        let status = HealthStatus::Healthy;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_health_status_copy() {
        let status = HealthStatus::Degraded;
        let copied = status;
        assert_eq!(status, copied);
    }

    #[test]
    fn test_health_status_debug() {
        let status = HealthStatus::Healthy;
        let debug_str = format!("{:?}", status);
        assert_eq!(debug_str, "Healthy");

        let status = HealthStatus::Degraded;
        let debug_str = format!("{:?}", status);
        assert_eq!(debug_str, "Degraded");

        let status = HealthStatus::Unhealthy;
        let debug_str = format!("{:?}", status);
        assert_eq!(debug_str, "Unhealthy");

        let status = HealthStatus::Unknown;
        let debug_str = format!("{:?}", status);
        assert_eq!(debug_str, "Unknown");
    }

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.interval, Duration::from_secs(5));
        assert_eq!(config.timeout, Duration::from_secs(3));
        assert_eq!(config.unhealthy_threshold, 3);
        assert_eq!(config.healthy_threshold, 2);
    }

    #[test]
    fn test_health_check_config_custom() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            unhealthy_threshold: 5,
            healthy_threshold: 3,
        };
        assert_eq!(config.interval, Duration::from_secs(10));
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.unhealthy_threshold, 5);
        assert_eq!(config.healthy_threshold, 3);
    }

    #[test]
    fn test_health_check_config_clone() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            unhealthy_threshold: 5,
            healthy_threshold: 3,
        };
        let cloned = config.clone();
        assert_eq!(config.interval, cloned.interval);
        assert_eq!(config.timeout, cloned.timeout);
        assert_eq!(config.unhealthy_threshold, cloned.unhealthy_threshold);
        assert_eq!(config.healthy_threshold, cloned.healthy_threshold);
    }

    #[test]
    fn test_health_check_config_debug() {
        let config = HealthCheckConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("HealthCheckConfig"));
        assert!(debug_str.contains("interval"));
        assert!(debug_str.contains("timeout"));
    }

    #[test]
    fn test_health_check_new() {
        let config = HealthCheckConfig::default();
        let check = HealthCheck::new(config);

        assert_eq!(check.status(), HealthStatus::Unknown);
        assert_eq!(check.consecutive_failures(), 0);
        assert_eq!(check.consecutive_successes(), 0);
    }

    #[test]
    fn test_health_check_default() {
        let check = HealthCheck::default();

        assert_eq!(check.status(), HealthStatus::Unknown);
        assert_eq!(check.consecutive_failures(), 0);
        assert_eq!(check.consecutive_successes(), 0);
        assert_eq!(check.config().interval, Duration::from_secs(5));
    }

    #[test]
    fn test_record_success_increments_counter() {
        let mut check = HealthCheck::default();
        assert_eq!(check.consecutive_successes(), 0);

        check.record_success();
        assert_eq!(check.consecutive_successes(), 1);

        check.record_success();
        assert_eq!(check.consecutive_successes(), 2);
    }

    #[test]
    fn test_record_success_resets_failures() {
        let mut check = HealthCheck::default();
        check.record_failure();
        check.record_failure();
        assert_eq!(check.consecutive_failures(), 2);

        check.record_success();
        assert_eq!(check.consecutive_failures(), 0);
        assert_eq!(check.consecutive_successes(), 1);
    }

    #[test]
    fn test_record_success_transitions_to_healthy() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
        };
        let mut check = HealthCheck::new(config);

        check.record_success();
        assert_eq!(check.status(), HealthStatus::Unknown);

        check.record_success();
        assert_eq!(check.status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_record_failure_increments_counter() {
        let mut check = HealthCheck::default();
        assert_eq!(check.consecutive_failures(), 0);

        check.record_failure();
        assert_eq!(check.consecutive_failures(), 1);

        check.record_failure();
        assert_eq!(check.consecutive_failures(), 2);
    }

    #[test]
    fn test_record_failure_resets_successes() {
        let mut check = HealthCheck::default();
        check.record_success();
        check.record_success();
        assert_eq!(check.consecutive_successes(), 2);

        check.record_failure();
        assert_eq!(check.consecutive_successes(), 0);
        assert_eq!(check.consecutive_failures(), 1);
    }

    #[test]
    fn test_record_failure_transitions_to_degraded() {
        let mut check = HealthCheck::default();

        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Degraded);
    }

    #[test]
    fn test_record_failure_transitions_to_unhealthy() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
        };
        let mut check = HealthCheck::new(config);

        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Degraded);

        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Degraded);

        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_status_transitions_healthy_to_degraded() {
        let mut check = HealthCheck::default();

        check.record_success();
        check.record_success();
        assert_eq!(check.status(), HealthStatus::Healthy);

        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Degraded);
    }

    #[test]
    fn test_status_transitions_unhealthy_to_healthy() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            unhealthy_threshold: 2,
            healthy_threshold: 2,
        };
        let mut check = HealthCheck::new(config);

        check.record_failure();
        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Unhealthy);

        check.record_success();
        assert_eq!(check.status(), HealthStatus::Unhealthy);

        check.record_success();
        assert_eq!(check.status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_config_getter() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            unhealthy_threshold: 5,
            healthy_threshold: 3,
        };
        let check = HealthCheck::new(config.clone());

        assert_eq!(check.config().interval, config.interval);
        assert_eq!(check.config().timeout, config.timeout);
        assert_eq!(
            check.config().unhealthy_threshold,
            config.unhealthy_threshold
        );
        assert_eq!(check.config().healthy_threshold, config.healthy_threshold);
    }

    #[test]
    fn test_alternating_success_failure() {
        let mut check = HealthCheck::default();

        check.record_success();
        assert_eq!(check.consecutive_successes(), 1);
        assert_eq!(check.consecutive_failures(), 0);

        check.record_failure();
        assert_eq!(check.consecutive_successes(), 0);
        assert_eq!(check.consecutive_failures(), 1);

        check.record_success();
        assert_eq!(check.consecutive_successes(), 1);
        assert_eq!(check.consecutive_failures(), 0);
    }

    #[test]
    fn test_health_check_with_threshold_one() {
        let config = HealthCheckConfig {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            unhealthy_threshold: 1,
            healthy_threshold: 1,
        };
        let mut check = HealthCheck::new(config);

        check.record_success();
        assert_eq!(check.status(), HealthStatus::Healthy);

        check.record_failure();
        assert_eq!(check.status(), HealthStatus::Unhealthy);

        check.record_success();
        assert_eq!(check.status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_many_consecutive_successes() {
        let mut check = HealthCheck::default();

        for _ in 0..100 {
            check.record_success();
        }

        assert_eq!(check.consecutive_successes(), 100);
        assert_eq!(check.status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_many_consecutive_failures() {
        let mut check = HealthCheck::default();

        for _ in 0..100 {
            check.record_failure();
        }

        assert_eq!(check.consecutive_failures(), 100);
        assert_eq!(check.status(), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_status_eq_and_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(HealthStatus::Healthy);
        set.insert(HealthStatus::Degraded);
        set.insert(HealthStatus::Unhealthy);
        set.insert(HealthStatus::Unknown);

        assert_eq!(set.len(), 4);
        assert!(set.contains(&HealthStatus::Healthy));
        assert!(set.contains(&HealthStatus::Degraded));
        assert!(set.contains(&HealthStatus::Unhealthy));
        assert!(set.contains(&HealthStatus::Unknown));

        set.insert(HealthStatus::Healthy);
        assert_eq!(set.len(), 4);
    }
}
