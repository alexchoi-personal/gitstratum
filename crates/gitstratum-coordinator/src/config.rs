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
            enabled: true,
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
