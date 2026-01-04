use crate::commands::{ClusterCommand, ClusterResponse, VersionedCommand};
use crate::topology::ClusterTopology;
use gitstratum_proto::NodeState;

const TOPOLOGY_KEY: &str = "__cluster_topology__";

const CURRENT_COMMAND_VERSION: u32 = 1;

const NODE_STATE_DOWN: i32 = NodeState::Down as i32;

impl VersionedCommand {
    pub fn new(command: ClusterCommand) -> Self {
        Self {
            version: CURRENT_COMMAND_VERSION,
            command,
        }
    }
}

pub fn validate_generation_id(
    topology: &ClusterTopology,
    node_id: &str,
    generation_id: &str,
) -> Result<(), String> {
    let node = topology
        .object_nodes
        .get(node_id)
        .or_else(|| topology.metadata_nodes.get(node_id));

    match node {
        None => Ok(()),
        Some(entry) => {
            if entry.generation_id == generation_id || entry.state == NODE_STATE_DOWN {
                Ok(())
            } else {
                Err(format!(
                    "Generation ID mismatch for node {}: expected {}, got {}",
                    node_id, entry.generation_id, generation_id
                ))
            }
        }
    }
}

pub fn apply_command(topology: &mut ClusterTopology, cmd: &ClusterCommand) -> ClusterResponse {
    let result: Result<(), String> = match cmd {
        ClusterCommand::AddObjectNode(node) => {
            if let Some(existing) = topology.object_nodes.get(&node.id) {
                if existing.generation_id != node.generation_id && existing.state != NODE_STATE_DOWN
                {
                    return ClusterResponse::generation_mismatch(
                        &existing.generation_id,
                        &node.generation_id,
                    );
                }
            }
            topology.object_nodes.insert(node.id.clone(), node.clone());
            Ok(())
        }
        ClusterCommand::AddMetadataNode(node) => {
            if let Some(existing) = topology.metadata_nodes.get(&node.id) {
                if existing.generation_id != node.generation_id && existing.state != NODE_STATE_DOWN
                {
                    return ClusterResponse::generation_mismatch(
                        &existing.generation_id,
                        &node.generation_id,
                    );
                }
            }
            topology
                .metadata_nodes
                .insert(node.id.clone(), node.clone());
            Ok(())
        }
        ClusterCommand::RemoveNode { node_id } => {
            let removed_object = topology.object_nodes.remove(node_id);
            let removed_metadata = topology.metadata_nodes.remove(node_id);
            let changed = removed_object.is_some() || removed_metadata.is_some();
            if changed {
                topology.version += 1;
            }
            return ClusterResponse::success(topology.version);
        }
        ClusterCommand::SetNodeState { node_id, state } => {
            let changed = if let Some(node) = topology.object_nodes.get_mut(node_id) {
                node.state = *state;
                true
            } else if let Some(node) = topology.metadata_nodes.get_mut(node_id) {
                node.state = *state;
                true
            } else {
                false
            };
            if changed {
                topology.version += 1;
            }
            return ClusterResponse::success(topology.version);
        }
        ClusterCommand::UpdateHashRingConfig(config) => {
            topology.hash_ring_config = config.clone();
            Ok(())
        }
        ClusterCommand::BatchHeartbeat(batch) => {
            let mut updated_count = 0;
            for (node_id, info) in batch {
                if let Some(node) = topology.object_nodes.get_mut(node_id) {
                    if info.received_at_ms > node.last_heartbeat_at {
                        node.last_heartbeat_at = info.received_at_ms;
                        updated_count += 1;
                    }
                } else if let Some(node) = topology.metadata_nodes.get_mut(node_id) {
                    if info.received_at_ms > node.last_heartbeat_at {
                        node.last_heartbeat_at = info.received_at_ms;
                        updated_count += 1;
                    }
                }
            }
            if updated_count > 0 {
                topology.version += 1;
            }
            return ClusterResponse::success(topology.version);
        }
    };

    match result {
        Ok(()) => {
            topology.version += 1;
            ClusterResponse::success(topology.version)
        }
        Err(e) => ClusterResponse::error(e),
    }
}

pub fn topology_key() -> &'static str {
    TOPOLOGY_KEY
}

pub fn serialize_command(cmd: &ClusterCommand) -> Result<String, serde_json::Error> {
    let versioned = VersionedCommand::new(cmd.clone());
    serde_json::to_string(&versioned)
}

pub fn deserialize_command(data: &str) -> Result<ClusterCommand, serde_json::Error> {
    match serde_json::from_str::<VersionedCommand>(data) {
        Ok(versioned) => Ok(versioned.command),
        Err(_) => serde_json::from_str::<ClusterCommand>(data),
    }
}

pub fn serialize_topology(topology: &ClusterTopology) -> Result<String, serde_json::Error> {
    serde_json::to_string(topology)
}

pub fn deserialize_topology(data: &str) -> Result<ClusterTopology, serde_json::Error> {
    serde_json::from_str(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::SerializableHeartbeatInfo;
    use crate::topology::NodeEntry;
    use std::collections::HashMap;

    fn make_node(id: &str) -> NodeEntry {
        NodeEntry {
            id: id.to_string(),
            address: "10.0.0.1".to_string(),
            port: 9000,
            state: 1,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: format!("gen-{}", id),
            registered_at: 0,
        }
    }

    #[test]
    fn test_add_object_node() {
        let mut topo = ClusterTopology::default();
        let node = make_node("node-1");

        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(node.clone()));
        assert!(resp.is_success());
        assert_eq!(resp.version(), Some(1));
        assert!(topo.object_nodes.contains_key("node-1"));
    }

    #[test]
    fn test_remove_node() {
        let mut topo = ClusterTopology::default();
        let node = make_node("node-1");

        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        let resp = apply_command(
            &mut topo,
            &ClusterCommand::RemoveNode {
                node_id: "node-1".to_string(),
            },
        );
        assert!(resp.is_success());
        assert!(!topo.object_nodes.contains_key("node-1"));
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut topo = ClusterTopology::default();
        let node = make_node("node-1");
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let serialized = serialize_topology(&topo).expect("serialization should succeed");
        let restored = deserialize_topology(&serialized).expect("deserialization should succeed");

        assert_eq!(restored.version, 1);
        assert!(restored.object_nodes.contains_key("node-1"));
    }

    #[test]
    fn test_versioned_command_serialization() {
        let cmd = ClusterCommand::RemoveNode {
            node_id: "test-node".to_string(),
        };
        let serialized = serialize_command(&cmd).expect("serialization should succeed");
        let deserialized =
            deserialize_command(&serialized).expect("deserialization should succeed");

        match deserialized {
            ClusterCommand::RemoveNode { node_id } => assert_eq!(node_id, "test-node"),
            _ => panic!("Expected RemoveNode command"),
        }
    }

    #[test]
    fn test_legacy_command_deserialization() {
        let legacy_json = r#"{"RemoveNode":{"node_id":"legacy-node"}}"#;
        let deserialized =
            deserialize_command(legacy_json).expect("legacy deserialization should succeed");

        match deserialized {
            ClusterCommand::RemoveNode { node_id } => assert_eq!(node_id, "legacy-node"),
            _ => panic!("Expected RemoveNode command"),
        }
    }

    #[test]
    fn test_batch_heartbeat() {
        let mut topo = ClusterTopology::default();
        let mut node1 = make_node("node-1");
        node1.address = "10.0.0.1".to_string();
        let mut node2 = make_node("node-2");
        node2.address = "10.0.0.2".to_string();

        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node1));
        apply_command(&mut topo, &ClusterCommand::AddMetadataNode(node2));

        let mut batch = HashMap::new();
        batch.insert(
            "node-1".to_string(),
            SerializableHeartbeatInfo {
                known_version: 1,
                reported_state: 1,
                generation_id: "gen-node-1".to_string(),
                received_at_ms: 1000,
            },
        );
        batch.insert(
            "node-2".to_string(),
            SerializableHeartbeatInfo {
                known_version: 1,
                reported_state: 1,
                generation_id: "gen-node-2".to_string(),
                received_at_ms: 2000,
            },
        );

        let resp = apply_command(&mut topo, &ClusterCommand::BatchHeartbeat(batch));
        assert!(resp.is_success());

        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().last_heartbeat_at,
            1000
        );
        assert_eq!(
            topo.metadata_nodes.get("node-2").unwrap().last_heartbeat_at,
            2000
        );
    }

    #[test]
    fn test_validate_generation_id_new_node() {
        let topo = ClusterTopology::default();
        let result = validate_generation_id(&topo, "new-node", "any-gen");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_generation_id_matching() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("node-1");
        node.generation_id = "gen-abc".to_string();
        topo.object_nodes.insert(node.id.clone(), node);

        let result = validate_generation_id(&topo, "node-1", "gen-abc");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_generation_id_mismatch_active() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("node-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;
        topo.object_nodes.insert(node.id.clone(), node);

        let result = validate_generation_id(&topo, "node-1", "gen-xyz");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_generation_id_mismatch_down() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("node-1");
        node.generation_id = "gen-abc".to_string();
        node.state = NODE_STATE_DOWN;
        topo.object_nodes.insert(node.id.clone(), node);

        let result = validate_generation_id(&topo, "node-1", "gen-xyz");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_generation_id_metadata_node() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("meta-1");
        node.generation_id = "gen-meta".to_string();
        topo.metadata_nodes.insert(node.id.clone(), node);

        let result = validate_generation_id(&topo, "meta-1", "gen-meta");
        assert!(result.is_ok());

        let result = validate_generation_id(&topo, "meta-1", "wrong-gen");
        assert!(result.is_err());
    }

    #[test]
    fn test_set_node_state() {
        let mut topo = ClusterTopology::default();
        let node = make_node("node-1");
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let resp = apply_command(
            &mut topo,
            &ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NODE_STATE_DOWN,
            },
        );
        assert!(resp.is_success());
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state,
            NODE_STATE_DOWN
        );
    }

    #[test]
    fn test_reregister_object_node_same_generation_succeeds() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("node-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;

        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(node.clone()));
        assert!(resp.is_success());

        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(node.clone()));
        assert!(resp.is_success());
        assert!(topo.object_nodes.contains_key("node-1"));
    }

    #[test]
    fn test_reregister_object_node_different_generation_while_active_fails() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("node-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;

        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(node.clone()));
        assert!(resp.is_success());

        let mut zombie_node = node.clone();
        zombie_node.generation_id = "gen-xyz".to_string();
        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(zombie_node));
        assert!(!resp.is_success());
        assert!(matches!(resp, ClusterResponse::GenerationMismatch { .. }));

        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().generation_id,
            "gen-abc"
        );
    }

    #[test]
    fn test_reregister_object_node_different_generation_while_down_succeeds() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("node-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;

        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node.clone()));
        apply_command(
            &mut topo,
            &ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NODE_STATE_DOWN,
            },
        );

        let mut new_node = node.clone();
        new_node.generation_id = "gen-xyz".to_string();
        let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(new_node));
        assert!(resp.is_success());
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().generation_id,
            "gen-xyz"
        );
    }

    #[test]
    fn test_reregister_metadata_node_same_generation_succeeds() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("meta-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;

        let resp = apply_command(&mut topo, &ClusterCommand::AddMetadataNode(node.clone()));
        assert!(resp.is_success());

        let resp = apply_command(&mut topo, &ClusterCommand::AddMetadataNode(node.clone()));
        assert!(resp.is_success());
        assert!(topo.metadata_nodes.contains_key("meta-1"));
    }

    #[test]
    fn test_reregister_metadata_node_different_generation_while_active_fails() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("meta-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;

        let resp = apply_command(&mut topo, &ClusterCommand::AddMetadataNode(node.clone()));
        assert!(resp.is_success());

        let mut zombie_node = node.clone();
        zombie_node.generation_id = "gen-xyz".to_string();
        let resp = apply_command(&mut topo, &ClusterCommand::AddMetadataNode(zombie_node));
        assert!(!resp.is_success());
        assert!(matches!(resp, ClusterResponse::GenerationMismatch { .. }));

        assert_eq!(
            topo.metadata_nodes.get("meta-1").unwrap().generation_id,
            "gen-abc"
        );
    }

    #[test]
    fn test_reregister_metadata_node_different_generation_while_down_succeeds() {
        let mut topo = ClusterTopology::default();
        let mut node = make_node("meta-1");
        node.generation_id = "gen-abc".to_string();
        node.state = 1;

        apply_command(&mut topo, &ClusterCommand::AddMetadataNode(node.clone()));
        apply_command(
            &mut topo,
            &ClusterCommand::SetNodeState {
                node_id: "meta-1".to_string(),
                state: NODE_STATE_DOWN,
            },
        );

        let mut new_node = node.clone();
        new_node.generation_id = "gen-xyz".to_string();
        let resp = apply_command(&mut topo, &ClusterCommand::AddMetadataNode(new_node));
        assert!(resp.is_success());
        assert_eq!(
            topo.metadata_nodes.get("meta-1").unwrap().generation_id,
            "gen-xyz"
        );
    }
}
