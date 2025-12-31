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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_configuration_workflow() {
        let default_config = ClusterConfig::default();
        assert_eq!(default_config.node_id, 1);
        assert_eq!(default_config.bind_address, "0.0.0.0");
        assert_eq!(default_config.bind_port, 9100);
        assert!(default_config.advertise_address.is_none());
        assert!(default_config.advertise_port.is_none());
        assert!(default_config.peers.is_empty());
        assert_eq!(default_config.data_dir, "/var/lib/gitstratum/control-plane");

        let config = ClusterConfig::new(42);
        assert_eq!(config.node_id, 42);
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.bind_port, 9100);

        let peer1 = PeerConfig::new(2, "node2.example.com", 9100);
        let peer2 = PeerConfig::new(3, String::from("node3.example.com"), 9100);
        assert_eq!(peer1.node_id, 2);
        assert_eq!(peer1.address, "node2.example.com");
        assert_eq!(peer1.port, 9100);
        assert_eq!(peer1.endpoint(), "node2.example.com:9100");
        assert_eq!(peer2.endpoint(), "node3.example.com:9100");

        let full_config = ClusterConfig::new(1)
            .with_bind(String::from("192.168.1.1"), 8080)
            .with_advertise(String::from("public.example.com"), 443)
            .with_peer(peer1)
            .with_peer(peer2)
            .with_data_dir(String::from("/custom/data/path"));

        assert_eq!(full_config.node_id, 1);
        assert_eq!(full_config.bind_address, "192.168.1.1");
        assert_eq!(full_config.bind_port, 8080);
        assert_eq!(
            full_config.advertise_address,
            Some("public.example.com".to_string())
        );
        assert_eq!(full_config.advertise_port, Some(443));
        assert_eq!(full_config.peers.len(), 2);
        assert_eq!(full_config.data_dir, "/custom/data/path");

        assert_eq!(full_config.bind_endpoint(), "192.168.1.1:8080");
        assert_eq!(full_config.advertise_endpoint(), "public.example.com:443");

        let no_advertise = ClusterConfig::new(1).with_bind("127.0.0.1", 9000);
        assert_eq!(no_advertise.advertise_endpoint(), "127.0.0.1:9000");

        let mut partial_addr = ClusterConfig::new(1).with_bind("0.0.0.0", 9100);
        partial_addr.advertise_address = Some("10.0.0.1".to_string());
        assert_eq!(partial_addr.advertise_endpoint(), "10.0.0.1:9100");

        let mut partial_port = ClusterConfig::new(1).with_bind("0.0.0.0", 9100);
        partial_port.advertise_port = Some(9200);
        assert_eq!(partial_port.advertise_endpoint(), "0.0.0.0:9200");

        let raft = RaftConfig::default();
        assert_eq!(raft.election_timeout_min, Duration::from_millis(150));
        assert_eq!(raft.election_timeout_max, Duration::from_millis(300));
        assert_eq!(raft.heartbeat_interval, Duration::from_millis(50));
        assert_eq!(raft.snapshot_threshold, 10000);
        assert_eq!(raft.max_entries_per_append, 64);

        let rate_limit = RateLimitConfig::default();
        assert_eq!(rate_limit.per_client, 100);
        assert_eq!(rate_limit.per_repo, 1000);
        assert_eq!(rate_limit.global, 10000);
        assert_eq!(rate_limit.refill_rate, 10.0);
    }

    #[test]
    fn test_serialization_workflow() {
        let peer = PeerConfig::new(42, "peer.example.com", 9500);
        let json = serde_json::to_string(&peer).unwrap();
        let deserialized: PeerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.node_id, 42);
        assert_eq!(deserialized.address, "peer.example.com");
        assert_eq!(deserialized.port, 9500);

        let rate_limit = RateLimitConfig::default();
        let json = serde_json::to_string(&rate_limit).unwrap();
        let deserialized: RateLimitConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.per_client, rate_limit.per_client);
        assert_eq!(deserialized.per_repo, rate_limit.per_repo);
        assert_eq!(deserialized.global, rate_limit.global);
        assert_eq!(deserialized.refill_rate, rate_limit.refill_rate);

        let raft = RaftConfig::default();
        let json = serde_json::to_string(&raft).unwrap();
        assert!(json.contains("\"150ms\""));
        assert!(json.contains("\"300ms\""));
        assert!(json.contains("\"50ms\""));
        let deserialized: RaftConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.election_timeout_min,
            Duration::from_millis(150)
        );
        assert_eq!(
            deserialized.election_timeout_max,
            Duration::from_millis(300)
        );
        assert_eq!(deserialized.heartbeat_interval, Duration::from_millis(50));

        let peer = PeerConfig::new(2, "node2", 9100);
        let config = ClusterConfig::new(1)
            .with_bind("127.0.0.1", 8080)
            .with_advertise("10.0.0.1", 9200)
            .with_peer(peer)
            .with_data_dir("/custom/path");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ClusterConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.node_id, config.node_id);
        assert_eq!(deserialized.bind_address, config.bind_address);
        assert_eq!(deserialized.bind_port, config.bind_port);
        assert_eq!(deserialized.advertise_address, config.advertise_address);
        assert_eq!(deserialized.advertise_port, config.advertise_port);
        assert_eq!(deserialized.peers.len(), config.peers.len());
        assert_eq!(deserialized.data_dir, config.data_dir);
    }

    #[test]
    fn test_humantime_parsing() {
        let seconds_json = r#"{
            "election_timeout_min": "1s",
            "election_timeout_max": "2s",
            "heartbeat_interval": "500ms",
            "snapshot_threshold": 5000,
            "max_entries_per_append": 32
        }"#;
        let raft: RaftConfig = serde_json::from_str(seconds_json).unwrap();
        assert_eq!(raft.election_timeout_min, Duration::from_secs(1));
        assert_eq!(raft.election_timeout_max, Duration::from_secs(2));
        assert_eq!(raft.heartbeat_interval, Duration::from_millis(500));

        let plain_json = r#"{
            "election_timeout_min": "100",
            "election_timeout_max": "200",
            "heartbeat_interval": "50",
            "snapshot_threshold": 5000,
            "max_entries_per_append": 32
        }"#;
        let raft: RaftConfig = serde_json::from_str(plain_json).unwrap();
        assert_eq!(raft.election_timeout_min, Duration::from_millis(100));
        assert_eq!(raft.election_timeout_max, Duration::from_millis(200));
        assert_eq!(raft.heartbeat_interval, Duration::from_millis(50));

        let invalid_ms = r#"{
            "election_timeout_min": "invalidms",
            "election_timeout_max": "200ms",
            "heartbeat_interval": "50ms",
            "snapshot_threshold": 5000,
            "max_entries_per_append": 32
        }"#;
        assert!(serde_json::from_str::<RaftConfig>(invalid_ms).is_err());

        let invalid_seconds = r#"{
            "election_timeout_min": "invalids",
            "election_timeout_max": "200ms",
            "heartbeat_interval": "50ms",
            "snapshot_threshold": 5000,
            "max_entries_per_append": 32
        }"#;
        assert!(serde_json::from_str::<RaftConfig>(invalid_seconds).is_err());

        let invalid_plain = r#"{
            "election_timeout_min": "notanumber",
            "election_timeout_max": "200ms",
            "heartbeat_interval": "50ms",
            "snapshot_threshold": 5000,
            "max_entries_per_append": 32
        }"#;
        assert!(serde_json::from_str::<RaftConfig>(invalid_plain).is_err());
    }

    #[test]
    fn test_derived_traits() {
        let peer = PeerConfig::new(1, "node1", 9100);
        let cloned_peer = peer.clone();
        assert_eq!(cloned_peer.node_id, peer.node_id);
        assert_eq!(cloned_peer.address, peer.address);
        assert_eq!(cloned_peer.port, peer.port);
        let debug_str = format!("{:?}", peer);
        assert!(debug_str.contains("PeerConfig"));
        assert!(debug_str.contains("node_id: 1"));

        let raft = RaftConfig::default();
        let cloned_raft = raft.clone();
        assert_eq!(cloned_raft.election_timeout_min, raft.election_timeout_min);
        assert_eq!(cloned_raft.heartbeat_interval, raft.heartbeat_interval);
        let debug_str = format!("{:?}", raft);
        assert!(debug_str.contains("RaftConfig"));

        let rate_limit = RateLimitConfig::default();
        let cloned_rate = rate_limit.clone();
        assert_eq!(cloned_rate.per_client, rate_limit.per_client);
        assert_eq!(cloned_rate.refill_rate, rate_limit.refill_rate);
        let debug_str = format!("{:?}", rate_limit);
        assert!(debug_str.contains("RateLimitConfig"));

        let config = ClusterConfig::new(1)
            .with_bind("127.0.0.1", 8080)
            .with_advertise("10.0.0.1", 9200)
            .with_peer(PeerConfig::new(2, "node2", 9100));
        let cloned_config = config.clone();
        assert_eq!(cloned_config.node_id, config.node_id);
        assert_eq!(cloned_config.bind_address, config.bind_address);
        assert_eq!(cloned_config.peers.len(), config.peers.len());
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ClusterConfig"));
        assert!(debug_str.contains("node_id: 1"));
    }
}
