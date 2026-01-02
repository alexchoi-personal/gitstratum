use crate::commands::{ClusterCommand, ClusterResponse};
use crate::topology::ClusterTopology;

const TOPOLOGY_KEY: &str = "__cluster_topology__";

pub fn apply_command(topology: &mut ClusterTopology, cmd: &ClusterCommand) -> ClusterResponse {
    let result = match cmd {
        ClusterCommand::AddObjectNode(node) => {
            topology.object_nodes.insert(node.id.clone(), node.clone());
            Ok(())
        }
        ClusterCommand::AddMetadataNode(node) => {
            topology
                .metadata_nodes
                .insert(node.id.clone(), node.clone());
            Ok(())
        }
        ClusterCommand::RemoveNode { node_id } => {
            let removed_object = topology.object_nodes.remove(node_id);
            let removed_metadata = topology.metadata_nodes.remove(node_id);
            if removed_object.is_some() || removed_metadata.is_some() {
                Ok(())
            } else {
                Err(format!("Node not found: {}", node_id))
            }
        }
        ClusterCommand::SetNodeState { node_id, state } => {
            if let Some(node) = topology.object_nodes.get_mut(node_id) {
                node.state = *state;
                Ok(())
            } else if let Some(node) = topology.metadata_nodes.get_mut(node_id) {
                node.state = *state;
                Ok(())
            } else {
                Err(format!("Node not found: {}", node_id))
            }
        }
        ClusterCommand::UpdateHashRingConfig(config) => {
            topology.hash_ring_config = config.clone();
            Ok(())
        }
    };

    match result {
        Ok(()) => {
            topology.version += 1;
            ClusterResponse::success(topology.version)
        }
        Err(e) => ClusterResponse::error(e, topology.version),
    }
}

pub fn topology_key() -> &'static str {
    TOPOLOGY_KEY
}

pub fn serialize_topology(topology: &ClusterTopology) -> String {
    serde_json::to_string(topology).unwrap_or_default()
}

pub fn deserialize_topology(data: &str) -> ClusterTopology {
    serde_json::from_str(data).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::NodeEntry;

    #[test]
    fn test_add_object_node() {
        let mut topo = ClusterTopology::default();
        let node = NodeEntry {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: 9000,
            state: 1,
        };

        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(node.clone()));
        assert!(resp.success);
        assert_eq!(resp.version, 1);
        assert!(topo.object_nodes.contains_key("node-1"));
    }

    #[test]
    fn test_remove_node() {
        let mut topo = ClusterTopology::default();
        let node = NodeEntry {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: 9000,
            state: 1,
        };

        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        let resp = apply_command(
            &mut topo,
            &ClusterCommand::RemoveNode {
                node_id: "node-1".to_string(),
            },
        );
        assert!(resp.success);
        assert!(!topo.object_nodes.contains_key("node-1"));
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut topo = ClusterTopology::default();
        let node = NodeEntry {
            id: "node-1".to_string(),
            address: "10.0.0.1".to_string(),
            port: 9000,
            state: 1,
        };
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let serialized = serialize_topology(&topo);
        let restored = deserialize_topology(&serialized);

        assert_eq!(restored.version, 1);
        assert!(restored.object_nodes.contains_key("node-1"));
    }
}
