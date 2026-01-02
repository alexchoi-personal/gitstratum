use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::topology::{HashRingConfig, NodeEntry};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterCommand {
    AddObjectNode(NodeEntry),
    AddMetadataNode(NodeEntry),
    RemoveNode { node_id: String },
    SetNodeState { node_id: String, state: i32 },
    UpdateHashRingConfig(HashRingConfig),
    BatchHeartbeat(HashMap<String, SerializableHeartbeatInfo>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterResponse {
    Success { version: u64 },
    AlreadyRegistered { version: u64 },
    NotFound,
    GenerationMismatch { expected: String, got: String },
    Error(String),
}

impl ClusterResponse {
    pub fn success(version: u64) -> Self {
        Self::Success { version }
    }

    pub fn already_registered(version: u64) -> Self {
        Self::AlreadyRegistered { version }
    }

    pub fn not_found() -> Self {
        Self::NotFound
    }

    pub fn generation_mismatch(expected: impl Into<String>, got: impl Into<String>) -> Self {
        Self::GenerationMismatch {
            expected: expected.into(),
            got: got.into(),
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    pub fn version(&self) -> Option<u64> {
        match self {
            Self::Success { version } | Self::AlreadyRegistered { version } => Some(*version),
            _ => None,
        }
    }

    pub fn to_result(&self) -> (bool, String) {
        match self {
            Self::Success { .. } => (true, String::new()),
            Self::AlreadyRegistered { .. } => (true, String::new()),
            Self::NotFound => (false, "Node not found".to_string()),
            Self::GenerationMismatch { expected, got } => (
                false,
                format!("Generation mismatch: expected {}, got {}", expected, got),
            ),
            Self::Error(e) => (false, e.clone()),
        }
    }

    pub fn is_already_registered(&self) -> bool {
        matches!(self, Self::AlreadyRegistered { .. })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableHeartbeatInfo {
    pub known_version: u64,
    pub reported_state: i32,
    pub generation_id: String,
    pub received_at_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionedCommand {
    pub version: u32,
    pub command: ClusterCommand,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_node_entry() -> NodeEntry {
        NodeEntry {
            id: "node-1".to_string(),
            address: "192.168.1.10".to_string(),
            port: 9000,
            state: 1,
            last_heartbeat_at: 1234567890,
            suspect_count: 0,
            generation_id: "uuid-1234".to_string(),
            registered_at: 1234567000,
        }
    }

    #[test]
    fn test_cluster_command_add_object_node() {
        let cmd = ClusterCommand::AddObjectNode(sample_node_entry());
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&json).unwrap();
        match deserialized {
            ClusterCommand::AddObjectNode(entry) => assert_eq!(entry.id, "node-1"),
            _ => panic!("Expected AddObjectNode"),
        }
    }

    #[test]
    fn test_cluster_command_add_metadata_node() {
        let cmd = ClusterCommand::AddMetadataNode(sample_node_entry());
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&json).unwrap();
        match deserialized {
            ClusterCommand::AddMetadataNode(entry) => assert_eq!(entry.id, "node-1"),
            _ => panic!("Expected AddMetadataNode"),
        }
    }

    #[test]
    fn test_cluster_command_remove_node() {
        let cmd = ClusterCommand::RemoveNode {
            node_id: "node-1".to_string(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&json).unwrap();
        match deserialized {
            ClusterCommand::RemoveNode { node_id } => assert_eq!(node_id, "node-1"),
            _ => panic!("Expected RemoveNode"),
        }
    }

    #[test]
    fn test_cluster_command_set_node_state() {
        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-1".to_string(),
            state: 2,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&json).unwrap();
        match deserialized {
            ClusterCommand::SetNodeState { node_id, state } => {
                assert_eq!(node_id, "node-1");
                assert_eq!(state, 2);
            }
            _ => panic!("Expected SetNodeState"),
        }
    }

    #[test]
    fn test_cluster_command_update_hash_ring_config() {
        let cmd = ClusterCommand::UpdateHashRingConfig(HashRingConfig::default());
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("UpdateHashRingConfig"));
    }

    #[test]
    fn test_cluster_command_batch_heartbeat() {
        let mut batch = HashMap::new();
        batch.insert(
            "node-1".to_string(),
            SerializableHeartbeatInfo {
                known_version: 10,
                reported_state: 1,
                generation_id: "uuid-1234".to_string(),
                received_at_ms: 1000,
            },
        );
        let cmd = ClusterCommand::BatchHeartbeat(batch);
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&json).unwrap();
        match deserialized {
            ClusterCommand::BatchHeartbeat(b) => {
                assert!(b.contains_key("node-1"));
                assert_eq!(b.get("node-1").unwrap().known_version, 10);
            }
            _ => panic!("Expected BatchHeartbeat"),
        }
    }

    #[test]
    fn test_cluster_response_success() {
        let resp = ClusterResponse::success(42);
        assert!(resp.is_success());
        assert_eq!(resp.version(), Some(42));
        let (success, error) = resp.to_result();
        assert!(success);
        assert!(error.is_empty());
    }

    #[test]
    fn test_cluster_response_already_registered() {
        let resp = ClusterResponse::already_registered(10);
        assert!(!resp.is_success());
        assert!(resp.is_already_registered());
        assert_eq!(resp.version(), Some(10));
        let (success, error) = resp.to_result();
        assert!(success);
        assert!(error.is_empty());
    }

    #[test]
    fn test_cluster_response_not_found() {
        let resp = ClusterResponse::not_found();
        assert!(!resp.is_success());
        assert!(!resp.is_already_registered());
        assert_eq!(resp.version(), None);
        let (success, error) = resp.to_result();
        assert!(!success);
        assert_eq!(error, "Node not found");
    }

    #[test]
    fn test_cluster_response_generation_mismatch() {
        let resp = ClusterResponse::generation_mismatch("expected-id", "got-id");
        assert!(!resp.is_success());
        assert_eq!(resp.version(), None);
        let (success, error) = resp.to_result();
        assert!(!success);
        assert!(error.contains("expected-id"));
        assert!(error.contains("got-id"));
    }

    #[test]
    fn test_cluster_response_error() {
        let resp = ClusterResponse::error("something went wrong");
        assert!(!resp.is_success());
        assert_eq!(resp.version(), None);
        let (success, error) = resp.to_result();
        assert!(!success);
        assert_eq!(error, "something went wrong");
    }

    #[test]
    fn test_cluster_response_clone() {
        let resp = ClusterResponse::success(10);
        let cloned = resp.clone();
        assert_eq!(resp.version(), cloned.version());
    }

    #[test]
    fn test_cluster_response_debug() {
        let resp = ClusterResponse::success(10);
        let debug_str = format!("{:?}", resp);
        assert!(debug_str.contains("Success"));
    }

    #[test]
    fn test_serializable_heartbeat_info() {
        let info = SerializableHeartbeatInfo {
            known_version: 100,
            reported_state: 1,
            generation_id: "uuid-5678".to_string(),
            received_at_ms: 5000,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: SerializableHeartbeatInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.known_version, 100);
        assert_eq!(deserialized.reported_state, 1);
        assert_eq!(deserialized.generation_id, "uuid-5678");
        assert_eq!(deserialized.received_at_ms, 5000);
    }

    #[test]
    fn test_versioned_command() {
        let cmd = VersionedCommand {
            version: 1,
            command: ClusterCommand::RemoveNode {
                node_id: "node-1".to_string(),
            },
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: VersionedCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.version, 1);
    }
}
