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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_notification_types_and_serialization() {
        let object = RingType::Object;
        let metadata = RingType::Metadata;
        assert_eq!(object, RingType::Object);
        assert_eq!(metadata, RingType::Metadata);
        assert_ne!(object, metadata);
        let object_copy = object;
        assert_eq!(object, object_copy);
        assert_eq!(format!("{:?}", object), "Object");
        assert_eq!(format!("{:?}", metadata), "Metadata");

        let object_json = serde_json::to_string(&object).unwrap();
        let metadata_json = serde_json::to_string(&metadata).unwrap();
        let object_back: RingType = serde_json::from_str(&object_json).unwrap();
        let metadata_back: RingType = serde_json::from_str(&metadata_json).unwrap();
        assert_eq!(object, object_back);
        assert_eq!(metadata, metadata_back);

        let notifications = vec![
            HashRingNotification::NodeAdded {
                node_id: "node1".to_string(),
                ring_type: RingType::Object,
            },
            HashRingNotification::NodeRemoved {
                node_id: "node2".to_string(),
                ring_type: RingType::Metadata,
            },
            HashRingNotification::NodeStateChanged {
                node_id: "node3".to_string(),
                ring_type: RingType::Object,
                old_state: gitstratum_hashring::NodeState::Joining,
                new_state: gitstratum_hashring::NodeState::Active,
            },
            HashRingNotification::TopologyChanged {
                ring_type: RingType::Metadata,
                version: 42,
            },
            HashRingNotification::RebalanceStarted {
                rebalance_id: "rebalance-123".to_string(),
            },
            HashRingNotification::RebalanceCompleted {
                rebalance_id: "rebalance-456".to_string(),
            },
        ];

        for notification in &notifications {
            let json = serde_json::to_string(notification).unwrap();
            let back: HashRingNotification = serde_json::from_str(&json).unwrap();
            let _ = notification.clone();
            let debug_str = format!("{:?}", back);
            assert!(!debug_str.is_empty());
        }

        if let HashRingNotification::NodeAdded { node_id, ring_type } = &notifications[0] {
            assert_eq!(node_id, "node1");
            assert_eq!(*ring_type, RingType::Object);
        } else {
            panic!("Expected NodeAdded variant");
        }

        if let HashRingNotification::NodeRemoved { node_id, ring_type } = &notifications[1] {
            assert_eq!(node_id, "node2");
            assert_eq!(*ring_type, RingType::Metadata);
        } else {
            panic!("Expected NodeRemoved variant");
        }

        if let HashRingNotification::NodeStateChanged {
            node_id,
            ring_type,
            old_state,
            new_state,
        } = &notifications[2]
        {
            assert_eq!(node_id, "node3");
            assert_eq!(*ring_type, RingType::Object);
            assert_eq!(*old_state, gitstratum_hashring::NodeState::Joining);
            assert_eq!(*new_state, gitstratum_hashring::NodeState::Active);
        } else {
            panic!("Expected NodeStateChanged variant");
        }

        if let HashRingNotification::TopologyChanged { ring_type, version } = &notifications[3] {
            assert_eq!(*ring_type, RingType::Metadata);
            assert_eq!(*version, 42);
        } else {
            panic!("Expected TopologyChanged variant");
        }

        if let HashRingNotification::RebalanceStarted { rebalance_id } = &notifications[4] {
            assert_eq!(rebalance_id, "rebalance-123");
        } else {
            panic!("Expected RebalanceStarted variant");
        }

        if let HashRingNotification::RebalanceCompleted { rebalance_id } = &notifications[5] {
            assert_eq!(rebalance_id, "rebalance-456");
        } else {
            panic!("Expected RebalanceCompleted variant");
        }
    }

    #[test]
    fn test_subscriber_management() {
        let mut notifier = HashRingNotifier::new();
        assert_eq!(notifier.subscriber_count(), 0);
        assert_eq!(notifier.history_size(), 0);

        let notifier_default = HashRingNotifier::default();
        assert_eq!(notifier_default.subscriber_count(), 0);
        assert_eq!(notifier_default.history_size(), 0);

        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter1_clone = counter1.clone();
        let counter2 = Arc::new(AtomicUsize::new(0));
        let counter2_clone = counter2.clone();
        let counter3 = Arc::new(AtomicUsize::new(0));
        let counter3_clone = counter3.clone();

        notifier.subscribe(
            "sub1",
            Box::new(move |_| {
                counter1_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert_eq!(notifier.subscriber_count(), 1);

        notifier.subscribe(
            "sub2",
            Box::new(move |_| {
                counter2_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert_eq!(notifier.subscriber_count(), 2);

        notifier.subscribe(
            String::from("sub3"),
            Box::new(move |_| {
                counter3_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert_eq!(notifier.subscriber_count(), 3);

        notifier.notify_node_added("node1", RingType::Object);
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
        assert_eq!(counter3.load(Ordering::SeqCst), 1);

        let replacement_counter = Arc::new(AtomicUsize::new(0));
        let replacement_clone = replacement_counter.clone();
        notifier.subscribe(
            "sub1",
            Box::new(move |_| {
                replacement_clone.fetch_add(100, Ordering::SeqCst);
            }),
        );
        assert_eq!(notifier.subscriber_count(), 3);

        notifier.notify_node_added("node2", RingType::Object);
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(replacement_counter.load(Ordering::SeqCst), 100);

        assert!(notifier.unsubscribe("sub2"));
        assert_eq!(notifier.subscriber_count(), 2);
        assert!(!notifier.unsubscribe("nonexistent"));

        notifier.notify_node_added("node3", RingType::Object);
        assert_eq!(counter2.load(Ordering::SeqCst), 2);

        assert!(notifier.unsubscribe("sub1"));
        assert!(notifier.unsubscribe("sub3"));
        assert_eq!(notifier.subscriber_count(), 0);
    }

    #[test]
    fn test_notification_delivery_workflow() {
        let mut notifier = HashRingNotifier::new();
        let received = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();

        notifier.subscribe(
            "collector",
            Box::new(move |notification| {
                let desc = match notification {
                    HashRingNotification::NodeAdded { node_id, ring_type } => {
                        format!("added:{}:{:?}", node_id, ring_type)
                    }
                    HashRingNotification::NodeRemoved { node_id, ring_type } => {
                        format!("removed:{}:{:?}", node_id, ring_type)
                    }
                    HashRingNotification::NodeStateChanged {
                        node_id,
                        ring_type,
                        old_state,
                        new_state,
                    } => {
                        format!(
                            "state_changed:{}:{:?}:{:?}->{:?}",
                            node_id, ring_type, old_state, new_state
                        )
                    }
                    HashRingNotification::TopologyChanged { ring_type, version } => {
                        format!("topology:{:?}:v{}", ring_type, version)
                    }
                    HashRingNotification::RebalanceStarted { rebalance_id } => {
                        format!("rebalance_start:{}", rebalance_id)
                    }
                    HashRingNotification::RebalanceCompleted { rebalance_id } => {
                        format!("rebalance_complete:{}", rebalance_id)
                    }
                };
                received_clone.lock().unwrap().push(desc);
            }),
        );

        notifier.notify_node_added("obj-node-1", RingType::Object);
        notifier.notify_node_added(String::from("meta-node-1"), RingType::Metadata);

        notifier.notify_state_changed(
            "obj-node-1",
            RingType::Object,
            gitstratum_hashring::NodeState::Joining,
            gitstratum_hashring::NodeState::Active,
        );
        notifier.notify_state_changed(
            String::from("meta-node-1"),
            RingType::Metadata,
            gitstratum_hashring::NodeState::Active,
            gitstratum_hashring::NodeState::Draining,
        );

        notifier.notify_topology_changed(RingType::Object, 1);
        notifier.notify_topology_changed(RingType::Metadata, 2);

        notifier.notify(HashRingNotification::RebalanceStarted {
            rebalance_id: "rb-001".to_string(),
        });
        notifier.notify(HashRingNotification::RebalanceCompleted {
            rebalance_id: "rb-001".to_string(),
        });

        notifier.notify_node_removed("obj-node-1", RingType::Object);
        notifier.notify_node_removed(String::from("meta-node-1"), RingType::Metadata);

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 10);
        assert_eq!(events[0], "added:obj-node-1:Object");
        assert_eq!(events[1], "added:meta-node-1:Metadata");
        assert_eq!(events[2], "state_changed:obj-node-1:Object:Joining->Active");
        assert_eq!(
            events[3],
            "state_changed:meta-node-1:Metadata:Active->Draining"
        );
        assert_eq!(events[4], "topology:Object:v1");
        assert_eq!(events[5], "topology:Metadata:v2");
        assert_eq!(events[6], "rebalance_start:rb-001");
        assert_eq!(events[7], "rebalance_complete:rb-001");
        assert_eq!(events[8], "removed:obj-node-1:Object");
        assert_eq!(events[9], "removed:meta-node-1:Metadata");
        drop(events);

        assert_eq!(notifier.history_size(), 10);
        let history = notifier.recent_notifications(10);
        assert_eq!(history.len(), 10);

        if let HashRingNotification::NodeAdded { node_id, ring_type } = &history[0] {
            assert_eq!(node_id, "obj-node-1");
            assert_eq!(*ring_type, RingType::Object);
        } else {
            panic!("Expected NodeAdded");
        }

        if let HashRingNotification::NodeRemoved { node_id, ring_type } = &history[9] {
            assert_eq!(node_id, "meta-node-1");
            assert_eq!(*ring_type, RingType::Metadata);
        } else {
            panic!("Expected NodeRemoved");
        }
    }

    #[test]
    fn test_history_management() {
        let notifier_empty = HashRingNotifier::new();
        let recent_empty = notifier_empty.recent_notifications(10);
        assert_eq!(recent_empty.len(), 0);

        let mut notifier = HashRingNotifier::new().with_max_history(5);
        assert_eq!(notifier.history_size(), 0);

        for i in 0..3 {
            notifier.notify_node_added(format!("node{}", i), RingType::Object);
            assert_eq!(notifier.history_size(), i + 1);
        }

        notifier.notify_node_added("node3", RingType::Object);
        notifier.notify_node_added("node4", RingType::Object);
        assert_eq!(notifier.history_size(), 5);

        notifier.notify_node_added("node5", RingType::Object);
        notifier.notify_node_added("node6", RingType::Object);
        assert_eq!(notifier.history_size(), 5);

        let recent = notifier.recent_notifications(3);
        assert_eq!(recent.len(), 3);
        if let HashRingNotification::NodeAdded { node_id, .. } = &recent[0] {
            assert_eq!(node_id, "node4");
        } else {
            panic!("Expected NodeAdded");
        }
        if let HashRingNotification::NodeAdded { node_id, .. } = &recent[1] {
            assert_eq!(node_id, "node5");
        } else {
            panic!("Expected NodeAdded");
        }
        if let HashRingNotification::NodeAdded { node_id, .. } = &recent[2] {
            assert_eq!(node_id, "node6");
        } else {
            panic!("Expected NodeAdded");
        }

        let all = notifier.recent_notifications(100);
        assert_eq!(all.len(), 5);
        if let HashRingNotification::NodeAdded { node_id, .. } = &all[0] {
            assert_eq!(node_id, "node2");
        } else {
            panic!("Expected NodeAdded");
        }

        let mut zero_history = HashRingNotifier::new().with_max_history(0);
        zero_history.notify_node_added("will_not_be_stored", RingType::Object);
        assert_eq!(zero_history.history_size(), 0);

        let mut boundary_notifier = HashRingNotifier::new().with_max_history(3);
        boundary_notifier.notify_node_added("b1", RingType::Object);
        assert_eq!(boundary_notifier.history_size(), 1);
        boundary_notifier.notify_node_added("b2", RingType::Object);
        assert_eq!(boundary_notifier.history_size(), 2);
        boundary_notifier.notify_node_added("b3", RingType::Object);
        assert_eq!(boundary_notifier.history_size(), 3);
        boundary_notifier.notify_node_added("b4", RingType::Object);
        assert_eq!(boundary_notifier.history_size(), 3);

        let boundary_recent = boundary_notifier.recent_notifications(10);
        assert_eq!(boundary_recent.len(), 3);
        if let HashRingNotification::NodeAdded { node_id, .. } = &boundary_recent[0] {
            assert_eq!(node_id, "b2");
        } else {
            panic!("Expected NodeAdded");
        }

        let mut mixed = HashRingNotifier::new();
        mixed.notify_node_added("m1", RingType::Object);
        mixed.notify_node_removed("m1", RingType::Object);
        mixed.notify_topology_changed(RingType::Metadata, 1);
        assert_eq!(mixed.history_size(), 3);
    }
}
