use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub port: u16,
    pub state: NodeState,
}

impl NodeInfo {
    pub fn new(id: impl Into<String>, address: impl Into<String>, port: u16) -> Self {
        Self {
            id: NodeId::new(id),
            address: address.into(),
            port,
            state: NodeState::Active,
        }
    }

    pub fn with_state(mut self, state: NodeState) -> Self {
        self.state = state;
        self
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Active,
    Joining,
    Draining,
    Down,
}

impl NodeState {
    pub fn is_active(&self) -> bool {
        matches!(self, NodeState::Active)
    }

    pub fn can_serve_reads(&self) -> bool {
        matches!(self, NodeState::Active | NodeState::Draining)
    }

    pub fn can_serve_writes(&self) -> bool {
        matches!(self, NodeState::Active)
    }
}
