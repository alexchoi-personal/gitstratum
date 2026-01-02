use crate::raft::node::NodeId;

pub struct RaftNetwork {
    _node_id: NodeId,
}

impl RaftNetwork {
    pub fn new(node_id: NodeId) -> Self {
        Self { _node_id: node_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_network_new() {
        let network = RaftNetwork::new(1);
        assert!(true);
    }

    #[test]
    fn test_raft_network_various_node_ids() {
        for node_id in [0u64, 1, 100, u64::MAX] {
            let network = RaftNetwork::new(node_id);
            let _ = network;
        }
    }
}
