use gitstratum_hashring::{ConsistentHashRing, HashRingBuilder, NodeInfo, NodeState};

use crate::topology::ClusterTopology;

impl ClusterTopology {
    pub fn to_hash_ring(&self) -> Result<ConsistentHashRing, gitstratum_hashring::HashRingError> {
        let mut builder = HashRingBuilder::new()
            .virtual_nodes(self.hash_ring_config.virtual_nodes_per_physical)
            .replication_factor(self.hash_ring_config.replication_factor as usize);

        for node in self.object_nodes.values() {
            let node_info = NodeInfo::new(&node.id, &node.address, node.port as u16)
                .with_state(convert_state(node.state));
            builder = builder.add_node(node_info);
        }

        builder.build()
    }
}

fn convert_state(state: i32) -> NodeState {
    match state {
        1 => NodeState::Active,
        2 => NodeState::Joining,
        3 => NodeState::Draining,
        4 => NodeState::Down,
        _ => NodeState::Down,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{HashRingConfig, NodeEntry};

    #[test]
    fn test_empty_topology_to_hash_ring() {
        let topology = ClusterTopology::default();
        let ring = topology.to_hash_ring();
        assert!(ring.is_ok());
    }

    #[test]
    fn test_topology_with_nodes_to_hash_ring() {
        let mut topology = ClusterTopology::default();
        topology.object_nodes.insert(
            "node-1".to_string(),
            NodeEntry {
                id: "node-1".to_string(),
                address: "10.0.0.1".to_string(),
                port: 9000,
                state: 1,
            },
        );
        topology.object_nodes.insert(
            "node-2".to_string(),
            NodeEntry {
                id: "node-2".to_string(),
                address: "10.0.0.2".to_string(),
                port: 9000,
                state: 1,
            },
        );
        topology.hash_ring_config = HashRingConfig {
            virtual_nodes_per_physical: 16,
            replication_factor: 2,
        };

        let ring = topology.to_hash_ring().unwrap();
        assert_eq!(ring.get_nodes().len(), 2);
    }
}
