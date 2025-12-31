use crate::raft::node::NodeId;

pub struct RaftNetwork {
    _node_id: NodeId,
}

impl RaftNetwork {
    pub fn new(node_id: NodeId) -> Self {
        Self { _node_id: node_id }
    }
}
