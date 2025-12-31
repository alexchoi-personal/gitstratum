use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HashRingNotification {
    NodeAdded {
        node_id: String,
        ring_type: RingType,
    },
    NodeRemoved {
        node_id: String,
        ring_type: RingType,
    },
    NodeStateChanged {
        node_id: String,
        ring_type: RingType,
        old_state: gitstratum_hashring::NodeState,
        new_state: gitstratum_hashring::NodeState,
    },
    TopologyChanged {
        ring_type: RingType,
        version: u64,
    },
    RebalanceStarted {
        rebalance_id: String,
    },
    RebalanceCompleted {
        rebalance_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RingType {
    Object,
    Metadata,
}

pub type NotificationCallback = Box<dyn Fn(&HashRingNotification) + Send + Sync>;

pub struct HashRingNotifier {
    subscribers: HashMap<String, NotificationCallback>,
    notification_history: Vec<HashRingNotification>,
    max_history_size: usize,
}

impl HashRingNotifier {
    pub fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
            notification_history: Vec::new(),
            max_history_size: 1000,
        }
    }

    pub fn with_max_history(mut self, max_size: usize) -> Self {
        self.max_history_size = max_size;
        self
    }

    pub fn subscribe(&mut self, id: impl Into<String>, callback: NotificationCallback) {
        self.subscribers.insert(id.into(), callback);
    }

    pub fn unsubscribe(&mut self, id: &str) -> bool {
        self.subscribers.remove(id).is_some()
    }

    pub fn notify(&mut self, notification: HashRingNotification) {
        for callback in self.subscribers.values() {
            callback(&notification);
        }

        self.notification_history.push(notification);

        while self.notification_history.len() > self.max_history_size {
            self.notification_history.remove(0);
        }
    }

    pub fn notify_node_added(&mut self, node_id: impl Into<String>, ring_type: RingType) {
        self.notify(HashRingNotification::NodeAdded {
            node_id: node_id.into(),
            ring_type,
        });
    }

    pub fn notify_node_removed(&mut self, node_id: impl Into<String>, ring_type: RingType) {
        self.notify(HashRingNotification::NodeRemoved {
            node_id: node_id.into(),
            ring_type,
        });
    }

    pub fn notify_state_changed(
        &mut self,
        node_id: impl Into<String>,
        ring_type: RingType,
        old_state: gitstratum_hashring::NodeState,
        new_state: gitstratum_hashring::NodeState,
    ) {
        self.notify(HashRingNotification::NodeStateChanged {
            node_id: node_id.into(),
            ring_type,
            old_state,
            new_state,
        });
    }

    pub fn notify_topology_changed(&mut self, ring_type: RingType, version: u64) {
        self.notify(HashRingNotification::TopologyChanged { ring_type, version });
    }

    pub fn recent_notifications(&self, count: usize) -> &[HashRingNotification] {
        let start = self.notification_history.len().saturating_sub(count);
        &self.notification_history[start..]
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    pub fn history_size(&self) -> usize {
        self.notification_history.len()
    }
}

impl Default for HashRingNotifier {
    fn default() -> Self {
        Self::new()
    }
}
