use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum FailureEvent {
    NodeUnreachable { node_id: String },
    NodeTimeout { node_id: String },
    NodeCrashed { node_id: String },
    NetworkPartition { affected_nodes: Vec<String> },
}

pub struct FailureDetector {
    heartbeat_interval: Duration,
    failure_threshold: Duration,
    last_heartbeats: std::collections::HashMap<String, Instant>,
}

impl FailureDetector {
    pub fn new(heartbeat_interval: Duration, failure_threshold: Duration) -> Self {
        Self {
            heartbeat_interval,
            failure_threshold,
            last_heartbeats: std::collections::HashMap::new(),
        }
    }

    pub fn record_heartbeat(&mut self, node_id: &str) {
        self.last_heartbeats
            .insert(node_id.to_string(), Instant::now());
    }

    pub fn check_node(&self, node_id: &str) -> Option<FailureEvent> {
        if let Some(&last_heartbeat) = self.last_heartbeats.get(node_id) {
            if last_heartbeat.elapsed() > self.failure_threshold {
                return Some(FailureEvent::NodeUnreachable {
                    node_id: node_id.to_string(),
                });
            }
        }
        None
    }

    pub fn detect_failures(&self) -> Vec<FailureEvent> {
        let now = Instant::now();
        self.last_heartbeats
            .iter()
            .filter_map(|(node_id, &last_heartbeat)| {
                if now.duration_since(last_heartbeat) > self.failure_threshold {
                    Some(FailureEvent::NodeUnreachable {
                        node_id: node_id.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn remove_node(&mut self, node_id: &str) {
        self.last_heartbeats.remove(node_id);
    }

    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub fn failure_threshold(&self) -> Duration {
        self.failure_threshold
    }

    pub fn node_count(&self) -> usize {
        self.last_heartbeats.len()
    }
}

impl Default for FailureDetector {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_failure_event_variants() {
        let node_unreachable = FailureEvent::NodeUnreachable {
            node_id: "node1".to_string(),
        };
        let node_timeout = FailureEvent::NodeTimeout {
            node_id: "node2".to_string(),
        };
        let node_crashed = FailureEvent::NodeCrashed {
            node_id: "node3".to_string(),
        };
        let network_partition = FailureEvent::NetworkPartition {
            affected_nodes: vec!["node4".to_string(), "node5".to_string()],
        };

        match node_unreachable {
            FailureEvent::NodeUnreachable { node_id } => assert_eq!(node_id, "node1"),
            _ => panic!("Expected NodeUnreachable"),
        }
        match node_timeout {
            FailureEvent::NodeTimeout { node_id } => assert_eq!(node_id, "node2"),
            _ => panic!("Expected NodeTimeout"),
        }
        match node_crashed {
            FailureEvent::NodeCrashed { node_id } => assert_eq!(node_id, "node3"),
            _ => panic!("Expected NodeCrashed"),
        }
        match network_partition {
            FailureEvent::NetworkPartition { affected_nodes } => {
                assert_eq!(affected_nodes.len(), 2);
                assert!(affected_nodes.contains(&"node4".to_string()));
                assert!(affected_nodes.contains(&"node5".to_string()));
            }
            _ => panic!("Expected NetworkPartition"),
        }
    }

    #[test]
    fn test_failure_event_clone() {
        let event = FailureEvent::NodeUnreachable {
            node_id: "node1".to_string(),
        };
        let cloned = event.clone();
        match cloned {
            FailureEvent::NodeUnreachable { node_id } => assert_eq!(node_id, "node1"),
            _ => panic!("Expected NodeUnreachable"),
        }
    }

    #[test]
    fn test_failure_event_debug() {
        let event = FailureEvent::NodeUnreachable {
            node_id: "node1".to_string(),
        };
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("NodeUnreachable"));
        assert!(debug_str.contains("node1"));
    }

    #[test]
    fn test_failure_detector_new() {
        let detector = FailureDetector::new(Duration::from_secs(2), Duration::from_secs(10));
        assert_eq!(detector.heartbeat_interval(), Duration::from_secs(2));
        assert_eq!(detector.failure_threshold(), Duration::from_secs(10));
        assert_eq!(detector.node_count(), 0);
    }

    #[test]
    fn test_failure_detector_default() {
        let detector = FailureDetector::default();
        assert_eq!(detector.heartbeat_interval(), Duration::from_secs(1));
        assert_eq!(detector.failure_threshold(), Duration::from_secs(5));
        assert_eq!(detector.node_count(), 0);
    }

    #[test]
    fn test_record_heartbeat() {
        let mut detector = FailureDetector::default();
        assert_eq!(detector.node_count(), 0);

        detector.record_heartbeat("node1");
        assert_eq!(detector.node_count(), 1);

        detector.record_heartbeat("node2");
        assert_eq!(detector.node_count(), 2);

        detector.record_heartbeat("node1");
        assert_eq!(detector.node_count(), 2);
    }

    #[test]
    fn test_check_node_healthy() {
        let mut detector = FailureDetector::default();
        detector.record_heartbeat("node1");

        let result = detector.check_node("node1");
        assert!(result.is_none());
    }

    #[test]
    fn test_check_node_not_registered() {
        let detector = FailureDetector::default();
        let result = detector.check_node("unknown");
        assert!(result.is_none());
    }

    #[test]
    fn test_check_node_failed() {
        let mut detector =
            FailureDetector::new(Duration::from_millis(10), Duration::from_millis(50));
        detector.record_heartbeat("node1");

        thread::sleep(Duration::from_millis(100));

        let result = detector.check_node("node1");
        assert!(result.is_some());
        match result.unwrap() {
            FailureEvent::NodeUnreachable { node_id } => assert_eq!(node_id, "node1"),
            _ => panic!("Expected NodeUnreachable"),
        }
    }

    #[test]
    fn test_detect_failures_empty() {
        let detector = FailureDetector::default();
        let failures = detector.detect_failures();
        assert!(failures.is_empty());
    }

    #[test]
    fn test_detect_failures_all_healthy() {
        let mut detector = FailureDetector::default();
        detector.record_heartbeat("node1");
        detector.record_heartbeat("node2");
        detector.record_heartbeat("node3");

        let failures = detector.detect_failures();
        assert!(failures.is_empty());
    }

    #[test]
    fn test_detect_failures_some_failed() {
        let mut detector =
            FailureDetector::new(Duration::from_millis(10), Duration::from_millis(50));

        detector.record_heartbeat("node1");
        thread::sleep(Duration::from_millis(100));

        detector.record_heartbeat("node2");

        let failures = detector.detect_failures();
        assert_eq!(failures.len(), 1);
        match &failures[0] {
            FailureEvent::NodeUnreachable { node_id } => assert_eq!(node_id, "node1"),
            _ => panic!("Expected NodeUnreachable"),
        }
    }

    #[test]
    fn test_detect_failures_all_failed() {
        let mut detector =
            FailureDetector::new(Duration::from_millis(10), Duration::from_millis(50));

        detector.record_heartbeat("node1");
        detector.record_heartbeat("node2");
        thread::sleep(Duration::from_millis(100));

        let failures = detector.detect_failures();
        assert_eq!(failures.len(), 2);
    }

    #[test]
    fn test_remove_node() {
        let mut detector = FailureDetector::default();
        detector.record_heartbeat("node1");
        detector.record_heartbeat("node2");
        assert_eq!(detector.node_count(), 2);

        detector.remove_node("node1");
        assert_eq!(detector.node_count(), 1);

        let result = detector.check_node("node1");
        assert!(result.is_none());
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let mut detector = FailureDetector::default();
        detector.record_heartbeat("node1");
        assert_eq!(detector.node_count(), 1);

        detector.remove_node("unknown");
        assert_eq!(detector.node_count(), 1);
    }

    #[test]
    fn test_heartbeat_refreshes_time() {
        let mut detector =
            FailureDetector::new(Duration::from_millis(10), Duration::from_millis(80));

        detector.record_heartbeat("node1");
        thread::sleep(Duration::from_millis(50));

        detector.record_heartbeat("node1");

        let result = detector.check_node("node1");
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_heartbeats_multiple_nodes() {
        let mut detector =
            FailureDetector::new(Duration::from_millis(10), Duration::from_millis(100));

        for i in 0..10 {
            detector.record_heartbeat(&format!("node{}", i));
        }

        assert_eq!(detector.node_count(), 10);

        let failures = detector.detect_failures();
        assert!(failures.is_empty());
    }

    #[test]
    fn test_failure_detector_getters() {
        let interval = Duration::from_secs(3);
        let threshold = Duration::from_secs(15);
        let detector = FailureDetector::new(interval, threshold);

        assert_eq!(detector.heartbeat_interval(), interval);
        assert_eq!(detector.failure_threshold(), threshold);
    }
}
