use metrics::{counter, gauge, histogram};

pub struct CoordinatorMetrics {
    node_id: String,
}

impl CoordinatorMetrics {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn set_topology_version(&self, version: u64) {
        gauge!("coordinator_topology_version").set(version as f64);
    }

    pub fn set_nodes_total(&self, node_type: &str, state: &str, count: u64) {
        let labels = [
            ("type", node_type.to_string()),
            ("state", state.to_string()),
        ];
        gauge!("coordinator_nodes_total", &labels).set(count as f64);
    }

    pub fn record_node_state_transition(&self, from: &str, to: &str) {
        let labels = [("from", from.to_string()), ("to", to.to_string())];
        counter!("coordinator_node_state_transitions_total", &labels).increment(1);
    }

    pub fn set_raft_term(&self, term: u64) {
        gauge!("coordinator_raft_term").set(term as f64);
    }

    pub fn set_raft_state(&self, state: &str) {
        let labels = [("state", state.to_string())];
        gauge!("coordinator_raft_state", &labels).set(1.0);
    }

    pub fn set_raft_committed_index(&self, index: u64) {
        gauge!("coordinator_raft_committed_index").set(index as f64);
    }

    pub fn set_raft_applied_index(&self, index: u64) {
        gauge!("coordinator_raft_applied_index").set(index as f64);
    }

    pub fn record_rpc_duration(&self, method: &str, status: &str, duration_secs: f64) {
        let labels = [
            ("method", method.to_string()),
            ("status", status.to_string()),
        ];
        histogram!("coordinator_rpc_duration_seconds", &labels).record(duration_secs);
    }

    pub fn record_rpc(&self, method: &str, status: &str) {
        let labels = [
            ("method", method.to_string()),
            ("status", status.to_string()),
        ];
        counter!("coordinator_rpc_total", &labels).increment(1);
    }

    pub fn set_watch_subscribers(&self, count: u64) {
        gauge!("coordinator_watch_subscribers").set(count as f64);
    }

    pub fn record_watch_update_sent(&self) {
        counter!("coordinator_watch_updates_sent_total").increment(1);
    }

    pub fn record_watch_gap_detected(&self) {
        counter!("coordinator_watch_gaps_detected_total").increment(1);
    }

    pub fn record_watch_full_sync_sent(&self) {
        counter!("coordinator_watch_full_syncs_sent_total").increment(1);
    }

    pub fn record_heartbeat_latency(&self, node_id: &str, latency_secs: f64) {
        let labels = [("node_id", node_id.to_string())];
        histogram!("coordinator_heartbeat_latency_seconds", &labels).record(latency_secs);
    }

    pub fn record_suspect_event(&self) {
        counter!("coordinator_suspect_events_total").increment(1);
    }

    pub fn record_flap_damping_activation(&self) {
        counter!("coordinator_flap_damping_activations_total").increment(1);
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl Default for CoordinatorMetrics {
    fn default() -> Self {
        Self::new("default")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_metrics_new() {
        let metrics = CoordinatorMetrics::new("node-1");
        assert_eq!(metrics.node_id(), "node-1");
    }

    #[test]
    fn test_coordinator_metrics_default() {
        let metrics = CoordinatorMetrics::default();
        assert_eq!(metrics.node_id(), "default");
    }

    #[test]
    fn test_topology_metrics() {
        let metrics = CoordinatorMetrics::new("node-1");
        metrics.set_topology_version(42);
        metrics.set_nodes_total("frontend", "active", 3);
        metrics.set_nodes_total("metadata", "suspect", 1);
        metrics.set_nodes_total("object", "down", 0);
        metrics.record_node_state_transition("active", "suspect");
        metrics.record_node_state_transition("suspect", "down");
    }

    #[test]
    fn test_raft_metrics() {
        let metrics = CoordinatorMetrics::new("node-1");
        metrics.set_raft_term(5);
        metrics.set_raft_state("leader");
        metrics.set_raft_state("follower");
        metrics.set_raft_state("candidate");
        metrics.set_raft_committed_index(100);
        metrics.set_raft_applied_index(99);
    }

    #[test]
    fn test_rpc_metrics() {
        let metrics = CoordinatorMetrics::new("node-1");
        metrics.record_rpc_duration("GetTopology", "ok", 0.005);
        metrics.record_rpc_duration("Heartbeat", "error", 0.001);
        metrics.record_rpc("GetTopology", "ok");
        metrics.record_rpc("Heartbeat", "error");
    }

    #[test]
    fn test_watch_metrics() {
        let metrics = CoordinatorMetrics::new("node-1");
        metrics.set_watch_subscribers(10);
        metrics.record_watch_update_sent();
        metrics.record_watch_gap_detected();
        metrics.record_watch_full_sync_sent();
    }

    #[test]
    fn test_failure_detection_metrics() {
        let metrics = CoordinatorMetrics::new("node-1");
        metrics.record_heartbeat_latency("peer-1", 0.015);
        metrics.record_heartbeat_latency("peer-2", 0.025);
        metrics.record_suspect_event();
        metrics.record_flap_damping_activation();
    }
}
