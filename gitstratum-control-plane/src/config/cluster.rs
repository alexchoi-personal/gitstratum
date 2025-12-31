use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub bind_address: String,
    pub bind_port: u16,
    pub advertise_address: Option<String>,
    pub advertise_port: Option<u16>,
    pub peers: Vec<PeerConfig>,
    pub raft: RaftConfig,
    pub rate_limit: RateLimitConfig,
    pub data_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub node_id: u64,
    pub address: String,
    pub port: u16,
}

impl PeerConfig {
    pub fn new(node_id: u64, address: impl Into<String>, port: u16) -> Self {
        Self {
            node_id,
            address: address.into(),
            port,
        }
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    #[serde(with = "humantime_serde")]
    pub election_timeout_min: Duration,
    #[serde(with = "humantime_serde")]
    pub election_timeout_max: Duration,
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    pub snapshot_threshold: u64,
    pub max_entries_per_append: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            snapshot_threshold: 10000,
            max_entries_per_append: 64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub per_client: u64,
    pub per_repo: u64,
    pub global: u64,
    pub refill_rate: f64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            per_client: 100,
            per_repo: 1000,
            global: 10000,
            refill_rate: 10.0,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            bind_address: "0.0.0.0".to_string(),
            bind_port: 9100,
            advertise_address: None,
            advertise_port: None,
            peers: Vec::new(),
            raft: RaftConfig::default(),
            rate_limit: RateLimitConfig::default(),
            data_dir: "/var/lib/gitstratum/control-plane".to_string(),
        }
    }
}

impl ClusterConfig {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    pub fn with_bind(mut self, address: impl Into<String>, port: u16) -> Self {
        self.bind_address = address.into();
        self.bind_port = port;
        self
    }

    pub fn with_advertise(mut self, address: impl Into<String>, port: u16) -> Self {
        self.advertise_address = Some(address.into());
        self.advertise_port = Some(port);
        self
    }

    pub fn with_peer(mut self, peer: PeerConfig) -> Self {
        self.peers.push(peer);
        self
    }

    pub fn with_data_dir(mut self, data_dir: impl Into<String>) -> Self {
        self.data_dir = data_dir.into();
        self
    }

    pub fn advertise_endpoint(&self) -> String {
        let address = self
            .advertise_address
            .as_ref()
            .unwrap_or(&self.bind_address);
        let port = self.advertise_port.unwrap_or(self.bind_port);
        format!("{}:{}", address, port)
    }

    pub fn bind_endpoint(&self) -> String {
        format!("{}:{}", self.bind_address, self.bind_port)
    }
}

mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}ms", duration.as_millis()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        if let Some(ms_str) = s.strip_suffix("ms") {
            let ms: u64 = ms_str.parse().map_err(serde::de::Error::custom)?;
            return Ok(Duration::from_millis(ms));
        }

        if let Some(s_str) = s.strip_suffix('s') {
            let secs: u64 = s_str.parse().map_err(serde::de::Error::custom)?;
            return Ok(Duration::from_secs(secs));
        }

        let ms: u64 = s.parse().map_err(serde::de::Error::custom)?;
        Ok(Duration::from_millis(ms))
    }
}
