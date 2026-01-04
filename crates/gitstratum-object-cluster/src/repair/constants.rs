#![allow(dead_code)]

use std::time::Duration;

/// Default repair bandwidth limit: 100 MB/s
///
/// This limits the rate at which objects are transferred during repair operations
/// to prevent overwhelming network or disk I/O.
pub const DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC: u64 = 100 * 1024 * 1024;

/// Default pause threshold: pause repair when write rate exceeds 10K ops/sec
///
/// When the object store's write rate exceeds this threshold, repair operations
/// will be paused to prioritize client traffic.
pub const DEFAULT_PAUSE_THRESHOLD_WRITE_RATE: u64 = 10_000;

/// Default session timeout: 1 hour
///
/// Repair sessions that have not made progress within this duration will be
/// marked as failed and cleaned up.
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(3600);

/// Default checkpoint interval: every 1000 objects
///
/// During repair, progress is checkpointed after this many objects are processed.
/// This allows resuming from the last checkpoint if the repair is interrupted.
pub const DEFAULT_CHECKPOINT_INTERVAL: u64 = 1000;

/// Default anti-entropy scan interval: 1 hour
///
/// How often the anti-entropy repairer scans for inconsistencies between
/// replicas. More frequent scans detect issues faster but consume more resources.
pub const DEFAULT_ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(3600);

/// Default maximum concurrent repair sessions: 5
///
/// Limits the number of repair sessions that can run simultaneously to prevent
/// excessive resource consumption.
pub const DEFAULT_MAX_CONCURRENT_SESSIONS: usize = 5;

/// Default batch size for rebalance operations: 1000 objects
///
/// Objects are transferred in batches of this size during rebalance operations.
pub const DEFAULT_REBALANCE_BATCH_SIZE: usize = 1000;

/// Default repair queue size: 10000 items
///
/// Maximum number of items that can be queued for repair. Additional items
/// will be rejected until the queue has capacity.
pub const DEFAULT_REPAIR_QUEUE_SIZE: usize = 10000;

/// Default sample size for anti-entropy: 1000 objects
///
/// When sampling mode is enabled, this many objects are randomly selected
/// for comparison during anti-entropy scans.
pub const DEFAULT_ANTI_ENTROPY_SAMPLE_SIZE: usize = 1000;

/// Default Merkle tree depth: 4 levels
///
/// Depth of the Merkle tree used for comparing object sets between nodes.
/// Deeper trees provide finer granularity but require more computation.
pub const DEFAULT_MERKLE_TREE_DEPTH: u8 = 4;

/// Default peer timeout: 30 seconds
///
/// Timeout for RPC calls to peer nodes during repair operations.
pub const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum concurrent peers: 3
///
/// Maximum number of peer nodes to contact simultaneously during repair.
pub const DEFAULT_MAX_CONCURRENT_PEERS: usize = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bandwidth_constant() {
        assert_eq!(DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC, 100 * 1024 * 1024);
        assert_eq!(DEFAULT_REPAIR_BANDWIDTH_BYTES_PER_SEC, 104_857_600);
    }

    #[test]
    fn test_pause_threshold_constant() {
        assert_eq!(DEFAULT_PAUSE_THRESHOLD_WRITE_RATE, 10_000);
    }

    #[test]
    fn test_session_timeout_constant() {
        assert_eq!(DEFAULT_SESSION_TIMEOUT, Duration::from_secs(3600));
        assert_eq!(DEFAULT_SESSION_TIMEOUT.as_secs(), 3600);
    }

    #[test]
    fn test_checkpoint_interval_constant() {
        assert_eq!(DEFAULT_CHECKPOINT_INTERVAL, 1000);
    }

    #[test]
    fn test_anti_entropy_interval_constant() {
        assert_eq!(DEFAULT_ANTI_ENTROPY_INTERVAL, Duration::from_secs(3600));
        assert_eq!(DEFAULT_ANTI_ENTROPY_INTERVAL.as_secs(), 3600);
    }

    #[test]
    fn test_max_concurrent_sessions_constant() {
        assert_eq!(DEFAULT_MAX_CONCURRENT_SESSIONS, 5);
    }

    #[test]
    fn test_rebalance_batch_size_constant() {
        assert_eq!(DEFAULT_REBALANCE_BATCH_SIZE, 1000);
    }

    #[test]
    fn test_repair_queue_size_constant() {
        assert_eq!(DEFAULT_REPAIR_QUEUE_SIZE, 10000);
    }

    #[test]
    fn test_anti_entropy_sample_size_constant() {
        assert_eq!(DEFAULT_ANTI_ENTROPY_SAMPLE_SIZE, 1000);
    }

    #[test]
    fn test_merkle_tree_depth_constant() {
        assert_eq!(DEFAULT_MERKLE_TREE_DEPTH, 4);
    }

    #[test]
    fn test_peer_timeout_constant() {
        assert_eq!(DEFAULT_PEER_TIMEOUT, Duration::from_secs(30));
        assert_eq!(DEFAULT_PEER_TIMEOUT.as_secs(), 30);
    }

    #[test]
    fn test_max_concurrent_peers_constant() {
        assert_eq!(DEFAULT_MAX_CONCURRENT_PEERS, 3);
    }
}
