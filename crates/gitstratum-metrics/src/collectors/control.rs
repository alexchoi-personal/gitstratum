use metrics::{counter, gauge, histogram};

pub struct ControlPlaneCollector {
    node_id: String,
}

impl ControlPlaneCollector {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn set_is_leader(&self, is_leader: bool) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_control_is_leader", &labels).set(if is_leader { 1.0 } else { 0.0 });
    }

    pub fn set_raft_term(&self, term: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_control_raft_term", &labels).set(term as f64);
    }

    pub fn set_raft_commit_index(&self, index: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_control_raft_commit_index", &labels).set(index as f64);
    }

    pub fn record_auth_decision(&self, result: &str, duration_secs: f64) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("result", result.to_string()),
        ];
        counter!("gitstratum_control_auth_decisions_total", &labels).increment(1);
        histogram!("gitstratum_control_auth_duration_seconds", &labels).record(duration_secs);
    }

    pub fn record_rate_limit(&self, allowed: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            (
                "result",
                if allowed { "allowed" } else { "throttled" }.to_string(),
            ),
        ];
        counter!("gitstratum_control_rate_limit_decisions_total", &labels).increment(1);
    }

    pub fn record_lock_operation(&self, operation: &str, success: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("operation", operation.to_string()),
            ("success", success.to_string()),
        ];
        counter!("gitstratum_control_lock_operations_total", &labels).increment(1);
    }

    pub fn set_active_locks(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_control_active_locks", &labels).set(count as f64);
    }

    pub fn record_audit_event(&self) {
        let labels = [("node_id", self.node_id.clone())];
        counter!("gitstratum_control_audit_events_total", &labels).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_plane_collector() {
        let collector = ControlPlaneCollector::new("control-1");

        collector.set_is_leader(true);
        collector.set_raft_term(5);
        collector.set_raft_commit_index(100);
        collector.record_auth_decision("allowed", 0.001);
        collector.record_auth_decision("denied", 0.002);
        collector.record_rate_limit(true);
        collector.record_rate_limit(false);
        collector.record_lock_operation("acquire", true);
        collector.record_lock_operation("release", true);
        collector.set_active_locks(3);
        collector.record_audit_event();
    }
}
