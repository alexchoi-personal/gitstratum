use metrics::{counter, gauge, histogram};

pub struct ObjectCollector {
    node_id: String,
}

impl ObjectCollector {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn record_blob_operation(&self, operation: &str, bytes: u64, duration_secs: f64) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("operation", operation.to_string()),
        ];
        counter!("gitstratum_object_blob_operations_total", &labels).increment(1);
        histogram!("gitstratum_object_blob_operation_duration_seconds", &labels)
            .record(duration_secs);
        counter!("gitstratum_object_blob_bytes_total", &labels).increment(bytes);
    }

    pub fn record_pack_cache(&self, hit: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("result", if hit { "hit" } else { "miss" }.to_string()),
        ];
        counter!("gitstratum_object_pack_cache_total", &labels).increment(1);
    }

    pub fn record_delta_computation(
        &self,
        original_size: u64,
        delta_size: u64,
        duration_secs: f64,
    ) {
        let labels = [("node_id", self.node_id.clone())];
        counter!("gitstratum_object_deltas_computed_total", &labels).increment(1);
        histogram!(
            "gitstratum_object_delta_computation_duration_seconds",
            &labels
        )
        .record(duration_secs);

        if original_size > 0 {
            let ratio = delta_size as f64 / original_size as f64;
            histogram!("gitstratum_object_delta_ratio", &labels).record(ratio);
        }
    }

    pub fn set_total_blobs(&self, count: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_object_total_blobs", &labels).set(count as f64);
    }

    pub fn set_storage_bytes(&self, used: u64, available: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_object_storage_used_bytes", &labels).set(used as f64);
        gauge!("gitstratum_object_storage_available_bytes", &labels).set(available as f64);
    }

    pub fn set_pack_cache_size(&self, bytes: u64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_object_pack_cache_bytes", &labels).set(bytes as f64);
    }

    pub fn record_gc_run(&self, objects_collected: u64, bytes_freed: u64, duration_secs: f64) {
        let labels = [("node_id", self.node_id.clone())];
        counter!("gitstratum_object_gc_runs_total", &labels).increment(1);
        counter!("gitstratum_object_gc_objects_collected_total", &labels)
            .increment(objects_collected);
        counter!("gitstratum_object_gc_bytes_freed_total", &labels).increment(bytes_freed);
        histogram!("gitstratum_object_gc_duration_seconds", &labels).record(duration_secs);
    }

    pub fn record_replication(&self, target_node: &str, bytes: u64, success: bool) {
        let labels = [
            ("node_id", self.node_id.clone()),
            ("target", target_node.to_string()),
            ("success", success.to_string()),
        ];
        counter!("gitstratum_object_replications_total", &labels).increment(1);
        if success {
            counter!("gitstratum_object_replication_bytes_total", &labels).increment(bytes);
        }
    }

    pub fn set_compression_ratio(&self, ratio: f64) {
        let labels = [("node_id", self.node_id.clone())];
        gauge!("gitstratum_object_compression_ratio", &labels).set(ratio);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_collector() {
        let collector = ObjectCollector::new("object-1");

        collector.record_blob_operation("get", 1024, 0.001);
        collector.record_blob_operation("put", 2048, 0.005);
        collector.record_pack_cache(true);
        collector.record_pack_cache(false);
        collector.record_delta_computation(1000, 200, 0.01);
        collector.set_total_blobs(10000);
        collector.set_storage_bytes(1_000_000, 500_000);
        collector.set_pack_cache_size(100_000);
        collector.record_gc_run(100, 50_000, 5.0);
        collector.record_replication("object-2", 1024, true);
        collector.set_compression_ratio(0.3);
    }
}
