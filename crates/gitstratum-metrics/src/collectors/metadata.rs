use metrics::{counter, gauge, histogram};

pub struct MetadataCollector {
    node_id: String,
}

impl MetadataCollector {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn record_ref_operation(&self, operation: &str, repo: &str, duration_secs: f64) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("operation", operation.to_string()),
            ("repo", repo.to_string()),
        ];
        counter!("gitstratum_metadata_ref_operations_total", &labels).increment(1);
        histogram!(
            "gitstratum_metadata_ref_operation_duration_seconds",
            &labels
        )
        .record(duration_secs);
    }

    pub fn record_commit_operation(&self, operation: &str, duration_secs: f64) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("operation", operation.to_string()),
        ];
        counter!("gitstratum_metadata_commit_operations_total", &labels).increment(1);
        histogram!(
            "gitstratum_metadata_commit_operation_duration_seconds",
            &labels
        )
        .record(duration_secs);
    }

    pub fn record_refs_cache(&self, hit: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("result", if hit { "hit" } else { "miss" }.to_string()),
        ];
        counter!("gitstratum_metadata_refs_cache_total", &labels).increment(1);
    }

    pub fn record_commit_cache(&self, hit: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("result", if hit { "hit" } else { "miss" }.to_string()),
        ];
        counter!("gitstratum_metadata_commit_cache_total", &labels).increment(1);
    }

    pub fn set_total_refs(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_metadata_total_refs", &labels).set(count as f64);
    }

    pub fn set_total_commits(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_metadata_total_commits", &labels).set(count as f64);
    }

    pub fn set_total_repos(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_metadata_total_repos", &labels).set(count as f64);
    }

    pub fn record_graph_walk(&self, commits_visited: u64, duration_secs: f64) {
        let labels = [("node_id", self.node_id.clone())];
        counter!("gitstratum_metadata_graph_walks_total", &labels).increment(1);
        histogram!("gitstratum_metadata_graph_walk_duration_seconds", &labels)
            .record(duration_secs);
        histogram!("gitstratum_metadata_graph_walk_commits", &labels)
            .record(commits_visited as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_collector() {
        let collector = MetadataCollector::new("metadata-1");

        collector.record_ref_operation("get", "org/repo", 0.001);
        collector.record_ref_operation("update", "org/repo", 0.005);
        collector.record_commit_operation("get", 0.002);
        collector.record_commit_operation("put", 0.003);
        collector.record_refs_cache(true);
        collector.record_refs_cache(false);
        collector.record_commit_cache(true);
        collector.set_total_refs(1000);
        collector.set_total_commits(50000);
        collector.set_total_repos(100);
        collector.record_graph_walk(50, 0.1);
    }
}
