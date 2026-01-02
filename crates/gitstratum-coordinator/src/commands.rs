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
