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
