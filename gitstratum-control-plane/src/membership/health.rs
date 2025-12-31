use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
