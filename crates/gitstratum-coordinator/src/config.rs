use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub suspect_timeout: Duration,
    pub down_timeout: Duration,
    pub joining_timeout: Duration,
    pub draining_timeout: Duration,
    pub detector_interval: Duration,

    pub leader_grace_period: Duration,

    pub flap_threshold: u32,
    pub flap_window: Duration,
    pub flap_multiplier: f32,
    pub stability_window: Duration,

    pub watch_buffer_size: usize,
    pub keepalive_interval: Duration,
    pub full_sync_threshold: u64,
    pub max_watch_subscribers: u32,

    pub heartbeat_batch_interval: Duration,

    pub max_registrations_per_min: u32,
    pub max_heartbeats_per_min: u32,
    pub max_topology_reads_per_min: u32,

    pub global_registrations_per_sec: u32,
    pub global_heartbeats_per_sec: u32,
    pub global_topology_reads_per_sec: u32,

    pub client_limiter_cleanup_interval: Duration,
    pub client_limiter_max_idle: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            suspect_timeout: Duration::from_secs(45),
            down_timeout: Duration::from_secs(45),
            joining_timeout: Duration::from_secs(60),
            draining_timeout: Duration::from_secs(600),
            detector_interval: Duration::from_secs(5),

            leader_grace_period: Duration::from_secs(90),

            flap_threshold: 3,
            flap_window: Duration::from_secs(600),
            flap_multiplier: 2.0,
            stability_window: Duration::from_secs(300),

            watch_buffer_size: 10_000,
            keepalive_interval: Duration::from_secs(30),
            full_sync_threshold: 100,
            max_watch_subscribers: 1_000,

            heartbeat_batch_interval: Duration::from_secs(1),

            max_registrations_per_min: 10,
            max_heartbeats_per_min: 100,
            max_topology_reads_per_min: 1_000,

            global_registrations_per_sec: 100,
            global_heartbeats_per_sec: 10_000,
            global_topology_reads_per_sec: 50_000,

            client_limiter_cleanup_interval: Duration::from_secs(60),
            client_limiter_max_idle: Duration::from_secs(600),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    pub verify_client_cert: bool,

    pub crl_url: Option<String>,
    pub crl_refresh_interval: Duration,
    pub crl_max_age: Duration,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            verify_client_cert: true,

            crl_url: None,
            crl_refresh_interval: Duration::from_secs(300),
            crl_max_age: Duration::from_secs(3600),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_config_default() {
        let config = CoordinatorConfig::default();
        assert_eq!(config.suspect_timeout, Duration::from_secs(45));
        assert_eq!(config.down_timeout, Duration::from_secs(45));
        assert_eq!(config.joining_timeout, Duration::from_secs(60));
        assert_eq!(config.draining_timeout, Duration::from_secs(600));
        assert_eq!(config.detector_interval, Duration::from_secs(5));
        assert_eq!(config.leader_grace_period, Duration::from_secs(90));
        assert_eq!(config.flap_threshold, 3);
        assert_eq!(config.flap_window, Duration::from_secs(600));
        assert!((config.flap_multiplier - 2.0).abs() < f32::EPSILON);
        assert_eq!(config.stability_window, Duration::from_secs(300));
        assert_eq!(config.watch_buffer_size, 10_000);
        assert_eq!(config.keepalive_interval, Duration::from_secs(30));
        assert_eq!(config.full_sync_threshold, 100);
        assert_eq!(config.max_watch_subscribers, 1_000);
        assert_eq!(config.heartbeat_batch_interval, Duration::from_secs(1));
        assert_eq!(config.max_registrations_per_min, 10);
        assert_eq!(config.max_heartbeats_per_min, 100);
        assert_eq!(config.max_topology_reads_per_min, 1_000);
        assert_eq!(config.global_registrations_per_sec, 100);
        assert_eq!(config.global_heartbeats_per_sec, 10_000);
        assert_eq!(config.global_topology_reads_per_sec, 50_000);
        assert_eq!(
            config.client_limiter_cleanup_interval,
            Duration::from_secs(60)
        );
        assert_eq!(config.client_limiter_max_idle, Duration::from_secs(600));
    }

    #[test]
    fn test_coordinator_config_clone() {
        let config = CoordinatorConfig::default();
        let cloned = config.clone();
        assert_eq!(config.suspect_timeout, cloned.suspect_timeout);
        assert_eq!(config.down_timeout, cloned.down_timeout);
    }

    #[test]
    fn test_coordinator_config_debug() {
        let config = CoordinatorConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("CoordinatorConfig"));
        assert!(debug_str.contains("suspect_timeout"));
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert!(config.cert_path.as_os_str().is_empty());
        assert!(config.key_path.as_os_str().is_empty());
        assert!(config.ca_path.as_os_str().is_empty());
        assert!(config.verify_client_cert);
        assert!(config.crl_url.is_none());
        assert_eq!(config.crl_refresh_interval, Duration::from_secs(300));
        assert_eq!(config.crl_max_age, Duration::from_secs(3600));
    }

    #[test]
    fn test_tls_config_clone() {
        let config = TlsConfig {
            enabled: false,
            cert_path: PathBuf::from("/etc/certs/server.crt"),
            ..TlsConfig::default()
        };
        let cloned = config.clone();
        assert!(!cloned.enabled);
        assert_eq!(cloned.cert_path, PathBuf::from("/etc/certs/server.crt"));
    }

    #[test]
    fn test_tls_config_debug() {
        let config = TlsConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TlsConfig"));
        assert!(debug_str.contains("enabled"));
    }
}
