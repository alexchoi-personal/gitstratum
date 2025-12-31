mod cluster;
mod feature_flags;

pub use cluster::{ClusterConfig, PeerConfig, RaftConfig, RateLimitConfig};
pub use feature_flags::{FeatureFlag, FeatureFlags};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_reexport() {
        let config = ClusterConfig::new(1);
        assert_eq!(config.node_id, 1);
    }

    #[test]
    fn test_peer_config_reexport() {
        let peer = PeerConfig::new(1, "localhost", 9100);
        assert_eq!(peer.node_id, 1);
    }

    #[test]
    fn test_raft_config_reexport() {
        let raft = RaftConfig::default();
        assert_eq!(raft.snapshot_threshold, 10000);
    }

    #[test]
    fn test_rate_limit_config_reexport() {
        let rate_limit = RateLimitConfig::default();
        assert_eq!(rate_limit.per_client, 100);
    }

    #[test]
    fn test_feature_flag_reexport() {
        let flag = FeatureFlag::new("test", true);
        assert!(flag.enabled);
    }

    #[test]
    fn test_feature_flags_reexport() {
        let flags = FeatureFlags::new();
        assert_eq!(flags.flag_count(), 0);
    }
}
