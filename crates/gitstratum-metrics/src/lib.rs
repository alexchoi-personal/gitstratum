pub mod collectors;
pub mod prometheus;

pub use prometheus::{init_metrics, MetricsHandle};

use metrics::{counter, gauge, histogram};

pub fn record_request(cluster: &str, operation: &str, duration_secs: f64, success: bool) {
    let labels = [
        ("cluster", cluster.to_string()),
        ("operation", operation.to_string()),
    ];

    counter!("gitstratum_requests_total", &labels).increment(1);
    histogram!("gitstratum_request_duration_seconds", &labels).record(duration_secs);

    if success {
        counter!("gitstratum_requests_success_total", &labels).increment(1);
    } else {
        counter!("gitstratum_requests_error_total", &labels).increment(1);
    }
}

pub fn record_cache_access(cluster: &str, cache_name: &str, hit: bool) {
    let labels = [
        ("cluster", cluster.to_string()),
        ("cache", cache_name.to_string()),
        ("result", if hit { "hit" } else { "miss" }.to_string()),
    ];
    counter!("gitstratum_cache_accesses_total", &labels).increment(1);
}

pub fn set_cluster_nodes(cluster: &str, state: &str, count: u64) {
    let labels = [
        ("cluster", cluster.to_string()),
        ("state", state.to_string()),
    ];
    gauge!("gitstratum_cluster_nodes", &labels).set(count as f64);
}

pub fn record_bytes_transferred(cluster: &str, direction: &str, bytes: u64) {
    let labels = [
        ("cluster", cluster.to_string()),
        ("direction", direction.to_string()),
    ];
    counter!("gitstratum_bytes_total", &labels).increment(bytes);
}

pub fn set_storage_bytes(cluster: &str, storage_type: &str, bytes: u64) {
    let labels = [
        ("cluster", cluster.to_string()),
        ("type", storage_type.to_string()),
    ];
    gauge!("gitstratum_storage_bytes", &labels).set(bytes as f64);
}

pub fn record_raft_operation(operation: &str, success: bool, duration_secs: f64) {
    let labels = [("operation", operation.to_string())];
    counter!("gitstratum_raft_operations_total", &labels).increment(1);
    histogram!("gitstratum_raft_operation_duration_seconds", &labels).record(duration_secs);

    if !success {
        counter!("gitstratum_raft_operations_failed_total", &labels).increment(1);
    }
}

pub fn set_raft_state(node_id: &str, is_leader: bool, term: u64) {
    let labels = [("node_id", node_id.to_string())];
    gauge!("gitstratum_raft_is_leader", &labels).set(if is_leader { 1.0 } else { 0.0 });
    gauge!("gitstratum_raft_term", &labels).set(term as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_request() {
        record_request("frontend", "clone", 0.5, true);
        record_request("frontend", "push", 1.2, false);
    }

    #[test]
    fn test_record_cache_access() {
        record_cache_access("frontend", "refs", true);
        record_cache_access("frontend", "refs", false);
    }

    #[test]
    fn test_set_cluster_nodes() {
        set_cluster_nodes("object", "active", 5);
        set_cluster_nodes("object", "draining", 1);
    }

    #[test]
    fn test_record_bytes_transferred() {
        record_bytes_transferred("object", "read", 1024);
        record_bytes_transferred("object", "write", 2048);
    }

    #[test]
    fn test_set_storage_bytes() {
        set_storage_bytes("object", "used", 1_000_000);
        set_storage_bytes("object", "available", 500_000);
    }

    #[test]
    fn test_record_raft_operation() {
        record_raft_operation("append_entries", true, 0.01);
        record_raft_operation("request_vote", false, 0.05);
    }

    #[test]
    fn test_set_raft_state() {
        set_raft_state("node-1", true, 5);
        set_raft_state("node-2", false, 5);
    }
}
