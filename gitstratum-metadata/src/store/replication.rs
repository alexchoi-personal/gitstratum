use std::collections::HashSet;

use gitstratum_core::{Oid, RepoId};

use crate::error::Result;

pub type NodeId = String;

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub factor: usize,
    pub min_replicas: usize,
    pub sync_timeout_ms: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            factor: 3,
            min_replicas: 2,
            sync_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationStatus {
    Synced,
    Pending { missing_replicas: usize },
    Degraded { available_replicas: usize },
    Failed,
}

pub struct ReplicationManager {
    config: ReplicationConfig,
    local_node_id: NodeId,
    peer_nodes: HashSet<NodeId>,
}

impl ReplicationManager {
    pub fn new(local_node_id: NodeId, config: ReplicationConfig) -> Self {
        Self {
            config,
            local_node_id,
            peer_nodes: HashSet::new(),
        }
    }

    pub fn add_peer(&mut self, node_id: NodeId) {
        if node_id != self.local_node_id {
            self.peer_nodes.insert(node_id);
        }
    }

    pub fn remove_peer(&mut self, node_id: &str) {
        self.peer_nodes.remove(node_id);
    }

    pub fn get_peers(&self) -> &HashSet<NodeId> {
        &self.peer_nodes
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    pub fn select_replica_nodes(&self, _repo_id: &RepoId) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self.peer_nodes.iter().cloned().collect();
        nodes.truncate(self.config.factor.saturating_sub(1));
        nodes
    }

    pub fn check_replication_status(&self, available_replicas: usize) -> ReplicationStatus {
        if available_replicas >= self.config.factor {
            ReplicationStatus::Synced
        } else if available_replicas >= self.config.min_replicas {
            ReplicationStatus::Pending {
                missing_replicas: self.config.factor - available_replicas,
            }
        } else if available_replicas > 0 {
            ReplicationStatus::Degraded { available_replicas }
        } else {
            ReplicationStatus::Failed
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationEvent {
    pub repo_id: RepoId,
    pub oid: Oid,
    pub event_type: ReplicationEventType,
    pub source_node: NodeId,
    pub target_nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationEventType {
    RefUpdate,
    CommitSync,
    TreeSync,
    FullSync,
}

pub trait ReplicationSender: Send + Sync {
    fn send_replication_event(&self, event: ReplicationEvent) -> Result<()>;
}

pub trait ReplicationReceiver: Send + Sync {
    fn receive_replication_event(&self) -> Result<Option<ReplicationEvent>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_config_default() {
        let config = ReplicationConfig::default();
        assert_eq!(config.factor, 3);
        assert_eq!(config.min_replicas, 2);
        assert_eq!(config.sync_timeout_ms, 5000);
    }

    #[test]
    fn test_replication_manager_new() {
        let config = ReplicationConfig::default();
        let manager = ReplicationManager::new("node1".to_string(), config);
        assert_eq!(manager.local_node_id(), "node1");
        assert!(manager.get_peers().is_empty());
    }

    #[test]
    fn test_add_remove_peer() {
        let config = ReplicationConfig::default();
        let mut manager = ReplicationManager::new("node1".to_string(), config);

        manager.add_peer("node2".to_string());
        assert!(manager.get_peers().contains("node2"));

        manager.add_peer("node1".to_string());
        assert!(!manager.get_peers().contains("node1"));

        manager.remove_peer("node2");
        assert!(!manager.get_peers().contains("node2"));
    }

    #[test]
    fn test_select_replica_nodes() {
        let config = ReplicationConfig {
            factor: 3,
            min_replicas: 2,
            sync_timeout_ms: 5000,
        };
        let mut manager = ReplicationManager::new("node1".to_string(), config);

        manager.add_peer("node2".to_string());
        manager.add_peer("node3".to_string());
        manager.add_peer("node4".to_string());

        let repo_id = RepoId::new("test/repo").unwrap();
        let nodes = manager.select_replica_nodes(&repo_id);
        assert!(nodes.len() <= 2);
    }

    #[test]
    fn test_check_replication_status() {
        let config = ReplicationConfig {
            factor: 3,
            min_replicas: 2,
            sync_timeout_ms: 5000,
        };
        let manager = ReplicationManager::new("node1".to_string(), config);

        assert_eq!(
            manager.check_replication_status(3),
            ReplicationStatus::Synced
        );
        assert_eq!(
            manager.check_replication_status(4),
            ReplicationStatus::Synced
        );
        assert_eq!(
            manager.check_replication_status(2),
            ReplicationStatus::Pending { missing_replicas: 1 }
        );
        assert_eq!(
            manager.check_replication_status(1),
            ReplicationStatus::Degraded { available_replicas: 1 }
        );
        assert_eq!(
            manager.check_replication_status(0),
            ReplicationStatus::Failed
        );
    }

    #[test]
    fn test_replication_event() {
        let repo_id = RepoId::new("test/repo").unwrap();
        let oid = Oid::hash(b"test");
        let event = ReplicationEvent {
            repo_id: repo_id.clone(),
            oid,
            event_type: ReplicationEventType::RefUpdate,
            source_node: "node1".to_string(),
            target_nodes: vec!["node2".to_string(), "node3".to_string()],
        };

        assert_eq!(event.repo_id.as_str(), "test/repo");
        assert_eq!(event.source_node, "node1");
        assert_eq!(event.target_nodes.len(), 2);
        assert_eq!(event.event_type, ReplicationEventType::RefUpdate);
    }

    #[test]
    fn test_replication_event_types() {
        assert_eq!(ReplicationEventType::RefUpdate, ReplicationEventType::RefUpdate);
        assert_eq!(ReplicationEventType::CommitSync, ReplicationEventType::CommitSync);
        assert_eq!(ReplicationEventType::TreeSync, ReplicationEventType::TreeSync);
        assert_eq!(ReplicationEventType::FullSync, ReplicationEventType::FullSync);
        assert_ne!(ReplicationEventType::RefUpdate, ReplicationEventType::CommitSync);
    }

    #[test]
    fn test_replication_manager_config() {
        let config = ReplicationConfig {
            factor: 5,
            min_replicas: 3,
            sync_timeout_ms: 10000,
        };
        let manager = ReplicationManager::new("node1".to_string(), config);

        let retrieved_config = manager.config();
        assert_eq!(retrieved_config.factor, 5);
        assert_eq!(retrieved_config.min_replicas, 3);
        assert_eq!(retrieved_config.sync_timeout_ms, 10000);
    }
}
