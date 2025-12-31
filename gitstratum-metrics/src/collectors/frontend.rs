use metrics::{counter, gauge, histogram};

pub struct FrontendCollector {
    node_id: String,
}

impl FrontendCollector {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn record_git_operation(
        &self,
        operation: &str,
        repo: &str,
        duration_secs: f64,
        success: bool,
    ) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("operation", operation.to_string()),
            ("repo", repo.to_string()),
        ];

        counter!("gitstratum_frontend_git_operations_total", &labels).increment(1);
        histogram!(
            "gitstratum_frontend_git_operation_duration_seconds",
            &labels
        )
        .record(duration_secs);

        if !success {
            counter!("gitstratum_frontend_git_operations_failed_total", &labels).increment(1);
        }
    }

    pub fn record_refs_cache(&self, hit: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("result", if hit { "hit" } else { "miss" }.to_string()),
        ];
        counter!("gitstratum_frontend_refs_cache_total", &labels).increment(1);
    }

    pub fn record_negotiation_cache(&self, hit: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("result", if hit { "hit" } else { "miss" }.to_string()),
        ];
        counter!("gitstratum_frontend_negotiation_cache_total", &labels).increment(1);
    }

    pub fn set_active_connections(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_frontend_active_connections", &labels).set(count as f64);
    }

    pub fn record_pack_size(&self, bytes: u64) {
        let labels = [("node_id", self.node_id.clone())];
        histogram!("gitstratum_frontend_pack_size_bytes", &labels).record(bytes as f64);
    }

    pub fn record_objects_fetched(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        counter!("gitstratum_frontend_objects_fetched_total", &labels).increment(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frontend_collector() {
        let collector = FrontendCollector::new("frontend-1");

        collector.record_git_operation("clone", "org/repo", 0.5, true);
        collector.record_git_operation("push", "org/repo", 0.3, false);
        collector.record_refs_cache(true);
        collector.record_refs_cache(false);
        collector.record_negotiation_cache(true);
        collector.set_active_connections(10);
        collector.record_pack_size(1024);
        collector.record_objects_fetched(50);
    }
}
