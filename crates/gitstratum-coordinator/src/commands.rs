use serde::{Deserialize, Serialize};

use crate::topology::{HashRingConfig, NodeEntry};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterCommand {
    AddObjectNode(NodeEntry),
    AddMetadataNode(NodeEntry),
    RemoveNode { node_id: String },
    SetNodeState { node_id: String, state: i32 },
    UpdateHashRingConfig(HashRingConfig),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClusterResponse {
    pub success: bool,
    pub error: Option<String>,
    pub version: u64,
}

impl ClusterResponse {
    pub fn success(version: u64) -> Self {
        Self {
            success: true,
            error: None,
            version,
        }
    }

    pub fn error(msg: impl Into<String>, version: u64) -> Self {
        Self {
            success: false,
            error: Some(msg.into()),
            version,
        }
    }
}
