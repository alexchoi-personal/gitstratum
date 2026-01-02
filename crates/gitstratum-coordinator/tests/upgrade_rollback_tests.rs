use std::collections::HashMap;

use gitstratum_coordinator::{
    apply_command, deserialize_command, deserialize_topology, serialize_command,
    serialize_topology, ClusterCommand, ClusterTopology, HashRingConfig, NodeEntry,
    SerializableHeartbeatInfo,
};
use gitstratum_proto::NodeState;

fn create_test_node(id: &str, state: NodeState) -> NodeEntry {
    NodeEntry {
        id: id.to_string(),
        address: format!("192.168.1.{}", id.len()),
        port: 9000,
        state: state as i32,
        last_heartbeat_at: 0,
        suspect_count: 0,
        generation_id: format!("gen-{}", id),
        registered_at: 0,
    }
}

mod version_compatibility_tests {
    use super::*;

    #[test]
    fn test_legacy_unversioned_command_parsing() {
        let legacy_commands = [
            r#"{"AddObjectNode":{"id":"node-1","address":"192.168.1.1","port":9000,"state":1,"last_heartbeat_at":0,"suspect_count":0,"generation_id":"gen-1","registered_at":0}}"#,
            r#"{"RemoveNode":{"node_id":"node-1"}}"#,
            r#"{"SetNodeState":{"node_id":"node-1","state":2}}"#,
            r#"{"UpdateHashRingConfig":{"virtual_nodes_per_physical":100,"replication_factor":3}}"#,
        ];

        for legacy_json in &legacy_commands {
            let cmd = deserialize_command(legacy_json);
            assert!(
                cmd.is_ok(),
                "Failed to parse legacy command: {}",
                legacy_json
            );
        }
    }

    #[test]
    fn test_versioned_command_parsing() {
        let versioned_commands = [
            r#"{"version":1,"command":{"AddObjectNode":{"id":"node-1","address":"192.168.1.1","port":9000,"state":1,"last_heartbeat_at":0,"suspect_count":0,"generation_id":"gen-1","registered_at":0}}}"#,
            r#"{"version":1,"command":{"RemoveNode":{"node_id":"node-1"}}}"#,
            r#"{"version":1,"command":{"SetNodeState":{"node_id":"node-1","state":2}}}"#,
        ];

        for versioned_json in &versioned_commands {
            let cmd = deserialize_command(versioned_json);
            assert!(
                cmd.is_ok(),
                "Failed to parse versioned command: {}",
                versioned_json
            );
        }
    }

    #[test]
    fn test_future_version_command_parsing() {
        let future_versioned = r#"{"version":99,"command":{"RemoveNode":{"node_id":"node-1"}}}"#;

        let cmd = deserialize_command(future_versioned);
        assert!(
            cmd.is_ok(),
            "Should handle future version commands gracefully"
        );

        match cmd.unwrap() {
            ClusterCommand::RemoveNode { node_id } => {
                assert_eq!(node_id, "node-1");
            }
            _ => panic!("Expected RemoveNode command"),
        }
    }

    #[test]
    fn test_command_round_trip_all_types() {
        let commands = [
            ClusterCommand::AddObjectNode(create_test_node("node-1", NodeState::Active)),
            ClusterCommand::AddMetadataNode(create_test_node("meta-1", NodeState::Active)),
            ClusterCommand::RemoveNode {
                node_id: "node-1".to_string(),
            },
            ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Suspect as i32,
            },
            ClusterCommand::UpdateHashRingConfig(HashRingConfig {
                virtual_nodes_per_physical: 100,
                replication_factor: 3,
            }),
            ClusterCommand::BatchHeartbeat({
                let mut batch = HashMap::new();
                batch.insert(
                    "node-1".to_string(),
                    SerializableHeartbeatInfo {
                        known_version: 10,
                        reported_state: NodeState::Active as i32,
                        generation_id: "gen-1".to_string(),
                        received_at_ms: 1000,
                    },
                );
                batch
            }),
        ];

        for cmd in &commands {
            let serialized = serialize_command(cmd).expect("Serialization should succeed");
            let deserialized =
                deserialize_command(&serialized).expect("Deserialization should succeed");

            let reserialized =
                serialize_command(&deserialized).expect("Re-serialization should succeed");
            assert_eq!(
                serialized, reserialized,
                "Round-trip should produce identical output"
            );
        }
    }
}

mod topology_compatibility_tests {
    use super::*;

    #[test]
    fn test_legacy_topology_without_new_fields() {
        let legacy_topology = r#"{
            "version": 5,
            "object_nodes": {},
            "metadata_nodes": {},
            "hash_ring_config": {
                "virtual_nodes_per_physical": 100,
                "replication_factor": 3
            }
        }"#;

        let topo: ClusterTopology =
            serde_json::from_str(legacy_topology).expect("Should parse legacy topology");
        assert_eq!(topo.version, 5);
    }

    #[test]
    fn test_topology_round_trip_with_nodes() {
        let mut topo = ClusterTopology::default();
        for i in 0..10 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let serialized = serialize_topology(&topo).expect("Should serialize");
        let restored: ClusterTopology = deserialize_topology(&serialized).expect("Should restore");

        assert_eq!(topo.version, restored.version);
        assert_eq!(topo.object_nodes.len(), restored.object_nodes.len());

        for (id, node) in &topo.object_nodes {
            let restored_node = restored
                .object_nodes
                .get(id)
                .expect("Node should exist in restored topology");
            assert_eq!(node.address, restored_node.address);
            assert_eq!(node.state, restored_node.state);
            assert_eq!(node.generation_id, restored_node.generation_id);
        }
    }

    #[test]
    fn test_topology_with_extra_unknown_fields() {
        let topology_with_extra = r#"{
            "version": 5,
            "object_nodes": {},
            "metadata_nodes": {},
            "hash_ring_config": {
                "virtual_nodes_per_physical": 100,
                "replication_factor": 3,
                "future_field": "ignored"
            },
            "some_future_field": "should_be_ignored"
        }"#;

        let result: Result<ClusterTopology, _> = serde_json::from_str(topology_with_extra);
        assert!(
            result.is_ok(),
            "Should ignore unknown fields for forward compatibility"
        );
    }
}

mod rolling_upgrade_simulation_tests {
    use super::*;

    #[test]
    fn test_mixed_version_command_log() {
        let mixed_log = [
            r#"{"AddObjectNode":{"id":"node-1","address":"192.168.1.1","port":9000,"state":1,"last_heartbeat_at":0,"suspect_count":0,"generation_id":"gen-1","registered_at":0}}"#,
            r#"{"version":1,"command":{"AddObjectNode":{"id":"node-2","address":"192.168.1.2","port":9000,"state":1,"last_heartbeat_at":0,"suspect_count":0,"generation_id":"gen-2","registered_at":0}}}"#,
            r#"{"SetNodeState":{"node_id":"node-1","state":5}}"#,
            r#"{"version":1,"command":{"SetNodeState":{"node_id":"node-2","state":3}}}"#,
        ];

        let mut topo = ClusterTopology::default();

        for log_entry in &mixed_log {
            let cmd = deserialize_command(log_entry).expect("Should parse mixed log entry");
            apply_command(&mut topo, &cmd);
        }

        assert_eq!(topo.object_nodes.len(), 2);
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state(),
            NodeState::Suspect
        );
        assert_eq!(
            topo.object_nodes.get("node-2").unwrap().state(),
            NodeState::Draining
        );
    }

    #[test]
    fn test_rolling_upgrade_node_replacement() {
        let mut topo = ClusterTopology::default();

        for i in 0..5 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        assert_eq!(topo.object_nodes.len(), 5);
        let original_version = topo.version;

        for i in 0..5 {
            let node_id = format!("node-{}", i);

            apply_command(
                &mut topo,
                &ClusterCommand::SetNodeState {
                    node_id: node_id.clone(),
                    state: NodeState::Draining as i32,
                },
            );

            apply_command(
                &mut topo,
                &ClusterCommand::SetNodeState {
                    node_id: node_id.clone(),
                    state: NodeState::Down as i32,
                },
            );

            apply_command(
                &mut topo,
                &ClusterCommand::RemoveNode {
                    node_id: node_id.clone(),
                },
            );

            let new_gen_id = format!("gen-v2-{}", i);
            let mut new_node = create_test_node(&node_id, NodeState::Joining);
            new_node.generation_id = new_gen_id;
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(new_node));

            apply_command(
                &mut topo,
                &ClusterCommand::SetNodeState {
                    node_id: node_id.clone(),
                    state: NodeState::Active as i32,
                },
            );
        }

        assert_eq!(topo.object_nodes.len(), 5);
        assert!(topo.version > original_version);

        for i in 0..5 {
            let node = topo.object_nodes.get(&format!("node-{}", i)).unwrap();
            assert!(node.generation_id.starts_with("gen-v2-"));
            assert_eq!(node.state(), NodeState::Active);
        }
    }

    #[test]
    fn test_batch_heartbeat_during_upgrade() {
        let mut topo = ClusterTopology::default();

        for i in 0..10 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        apply_command(
            &mut topo,
            &ClusterCommand::SetNodeState {
                node_id: "node-0".to_string(),
                state: NodeState::Draining as i32,
            },
        );
        apply_command(
            &mut topo,
            &ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Draining as i32,
            },
        );

        let mut batch = HashMap::new();
        for i in 2..10 {
            batch.insert(
                format!("node-{}", i),
                SerializableHeartbeatInfo {
                    known_version: topo.version,
                    reported_state: NodeState::Active as i32,
                    generation_id: format!("gen-node-{}", i),
                    received_at_ms: 1000 + i as i64,
                },
            );
        }

        let resp = apply_command(&mut topo, &ClusterCommand::BatchHeartbeat(batch));
        assert!(resp.is_success());

        for i in 2..10 {
            let node = topo.object_nodes.get(&format!("node-{}", i)).unwrap();
            assert_eq!(node.last_heartbeat_at, 1000 + i as i64);
        }
    }
}

mod raft_log_forward_compatibility_tests {
    use super::*;

    #[test]
    fn test_replay_historical_log() {
        let historical_log = [
            serialize_command(&ClusterCommand::AddObjectNode(create_test_node(
                "node-1",
                NodeState::Joining,
            )))
            .unwrap(),
            serialize_command(&ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Active as i32,
            })
            .unwrap(),
            serialize_command(&ClusterCommand::AddMetadataNode(create_test_node(
                "meta-1",
                NodeState::Active,
            )))
            .unwrap(),
            serialize_command(&ClusterCommand::UpdateHashRingConfig(HashRingConfig {
                virtual_nodes_per_physical: 200,
                replication_factor: 5,
            }))
            .unwrap(),
            serialize_command(&ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Suspect as i32,
            })
            .unwrap(),
            serialize_command(&ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Active as i32,
            })
            .unwrap(),
        ];

        let mut topo = ClusterTopology::default();

        for (i, entry) in historical_log.iter().enumerate() {
            let cmd = deserialize_command(entry)
                .unwrap_or_else(|e| panic!("Failed to deserialize log entry {}: {}", i, e));
            apply_command(&mut topo, &cmd);
        }

        assert_eq!(topo.object_nodes.len(), 1);
        assert_eq!(topo.metadata_nodes.len(), 1);
        assert_eq!(topo.hash_ring_config.virtual_nodes_per_physical, 200);
        assert_eq!(topo.hash_ring_config.replication_factor, 5);
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state(),
            NodeState::Active
        );
    }

    #[test]
    fn test_snapshot_and_replay() {
        let mut original_topo = ClusterTopology::default();
        for i in 0..20 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut original_topo, &ClusterCommand::AddObjectNode(node));
        }

        for i in 0..5 {
            apply_command(
                &mut original_topo,
                &ClusterCommand::SetNodeState {
                    node_id: format!("node-{}", i),
                    state: NodeState::Suspect as i32,
                },
            );
        }

        let snapshot = serialize_topology(&original_topo).expect("Snapshot should serialize");

        let incremental_commands = [
            serialize_command(&ClusterCommand::SetNodeState {
                node_id: "node-0".to_string(),
                state: NodeState::Down as i32,
            })
            .unwrap(),
            serialize_command(&ClusterCommand::RemoveNode {
                node_id: "node-0".to_string(),
            })
            .unwrap(),
            serialize_command(&ClusterCommand::AddObjectNode(create_test_node(
                "node-new",
                NodeState::Joining,
            )))
            .unwrap(),
        ];

        let mut restored_topo: ClusterTopology =
            deserialize_topology(&snapshot).expect("Should restore from snapshot");

        for entry in &incremental_commands {
            let cmd = deserialize_command(entry).expect("Should deserialize command");
            apply_command(&mut restored_topo, &cmd);
        }

        assert_eq!(restored_topo.object_nodes.len(), 20);
        assert!(!restored_topo.object_nodes.contains_key("node-0"));
        assert!(restored_topo.object_nodes.contains_key("node-new"));
    }

    #[test]
    fn test_command_log_determinism() {
        let commands = [
            ClusterCommand::AddObjectNode(create_test_node("node-1", NodeState::Active)),
            ClusterCommand::AddObjectNode(create_test_node("node-2", NodeState::Active)),
            ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Suspect as i32,
            },
            ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: NodeState::Active as i32,
            },
            ClusterCommand::BatchHeartbeat({
                let mut batch = HashMap::new();
                batch.insert(
                    "node-1".to_string(),
                    SerializableHeartbeatInfo {
                        known_version: 4,
                        reported_state: NodeState::Active as i32,
                        generation_id: "gen-node-1".to_string(),
                        received_at_ms: 5000,
                    },
                );
                batch.insert(
                    "node-2".to_string(),
                    SerializableHeartbeatInfo {
                        known_version: 4,
                        reported_state: NodeState::Active as i32,
                        generation_id: "gen-node-2".to_string(),
                        received_at_ms: 5001,
                    },
                );
                batch
            }),
        ];

        let log: Vec<String> = commands
            .iter()
            .map(|c| serialize_command(c).unwrap())
            .collect();

        let mut topo1 = ClusterTopology::default();
        let mut topo2 = ClusterTopology::default();

        for entry in &log {
            let cmd = deserialize_command(entry).unwrap();
            apply_command(&mut topo1, &cmd);
        }

        for entry in &log {
            let cmd = deserialize_command(entry).unwrap();
            apply_command(&mut topo2, &cmd);
        }

        assert_eq!(topo1.version, topo2.version);
        assert_eq!(topo1.object_nodes.len(), topo2.object_nodes.len());

        for (id, node1) in &topo1.object_nodes {
            let node2 = topo2.object_nodes.get(id).unwrap();
            assert_eq!(node1.state, node2.state);
            assert_eq!(node1.last_heartbeat_at, node2.last_heartbeat_at);
        }
    }
}

mod rollback_scenario_tests {
    use super::*;

    #[test]
    fn test_rollback_to_older_topology() {
        let mut topo = ClusterTopology::default();
        for i in 0..10 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let checkpoint = serialize_topology(&topo).expect("Should serialize checkpoint");

        for i in 0..5 {
            apply_command(
                &mut topo,
                &ClusterCommand::RemoveNode {
                    node_id: format!("node-{}", i),
                },
            );
        }

        assert_eq!(topo.object_nodes.len(), 5);

        let rolled_back: ClusterTopology =
            deserialize_topology(&checkpoint).expect("Should restore checkpoint");
        assert_eq!(rolled_back.object_nodes.len(), 10);
    }

    #[test]
    fn test_partial_upgrade_rollback() {
        let mut topo = ClusterTopology::default();
        for i in 0..6 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let pre_upgrade_snapshot = serialize_topology(&topo).expect("Should snapshot");

        for i in 0..3 {
            let node_id = format!("node-{}", i);
            apply_command(
                &mut topo,
                &ClusterCommand::SetNodeState {
                    node_id: node_id.clone(),
                    state: NodeState::Draining as i32,
                },
            );
            apply_command(
                &mut topo,
                &ClusterCommand::SetNodeState {
                    node_id: node_id.clone(),
                    state: NodeState::Down as i32,
                },
            );
            apply_command(&mut topo, &ClusterCommand::RemoveNode { node_id });
        }

        assert_eq!(topo.object_nodes.len(), 3);

        let rolled_back: ClusterTopology =
            deserialize_topology(&pre_upgrade_snapshot).expect("Should restore");
        assert_eq!(rolled_back.object_nodes.len(), 6);

        for i in 0..6 {
            assert!(
                rolled_back
                    .object_nodes
                    .contains_key(&format!("node-{}", i)),
                "node-{} should exist after rollback",
                i
            );
        }
    }

    #[test]
    fn test_generation_id_after_rollback() {
        let mut topo = ClusterTopology::default();
        let node = create_test_node("node-1", NodeState::Active);
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let original_gen = topo
            .object_nodes
            .get("node-1")
            .unwrap()
            .generation_id
            .clone();

        let checkpoint = serialize_topology(&topo).expect("Checkpoint");

        apply_command(
            &mut topo,
            &ClusterCommand::RemoveNode {
                node_id: "node-1".to_string(),
            },
        );

        let mut new_node = create_test_node("node-1", NodeState::Active);
        new_node.generation_id = "gen-v2-node-1".to_string();
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(new_node));

        let rolled_back: ClusterTopology = deserialize_topology(&checkpoint).expect("Restore");
        let restored_gen = rolled_back
            .object_nodes
            .get("node-1")
            .unwrap()
            .generation_id
            .clone();

        assert_eq!(original_gen, restored_gen);
        assert_ne!(
            topo.object_nodes.get("node-1").unwrap().generation_id,
            restored_gen
        );
    }
}

mod stress_tests {
    use super::*;

    #[test]
    fn test_large_topology_serialization_consistency() {
        let mut topo = ClusterTopology::default();
        for i in 0..1000 {
            let node = create_test_node(&format!("node-{:04}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let serialized1 = serialize_topology(&topo).expect("First serialization");
        let restored1: ClusterTopology = deserialize_topology(&serialized1).expect("First restore");

        let serialized2 = serialize_topology(&restored1).expect("Second serialization");
        let restored2: ClusterTopology =
            deserialize_topology(&serialized2).expect("Second restore");

        assert_eq!(restored1.version, restored2.version);
        assert_eq!(restored1.object_nodes.len(), restored2.object_nodes.len());
    }

    #[test]
    fn test_many_small_batches_equivalent_to_one_large() {
        let mut topo_small = ClusterTopology::default();
        let mut topo_large = ClusterTopology::default();

        for i in 0..100 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(
                &mut topo_small,
                &ClusterCommand::AddObjectNode(node.clone()),
            );
            apply_command(&mut topo_large, &ClusterCommand::AddObjectNode(node));
        }

        for i in 0..100 {
            let small_batch: HashMap<_, _> = [(
                format!("node-{}", i),
                SerializableHeartbeatInfo {
                    known_version: topo_small.version,
                    reported_state: NodeState::Active as i32,
                    generation_id: format!("gen-node-{}", i),
                    received_at_ms: 1000 + i as i64,
                },
            )]
            .into_iter()
            .collect();
            apply_command(
                &mut topo_small,
                &ClusterCommand::BatchHeartbeat(small_batch),
            );
        }

        let large_batch: HashMap<_, _> = (0..100)
            .map(|i| {
                (
                    format!("node-{}", i),
                    SerializableHeartbeatInfo {
                        known_version: topo_large.version,
                        reported_state: NodeState::Active as i32,
                        generation_id: format!("gen-node-{}", i),
                        received_at_ms: 1000 + i as i64,
                    },
                )
            })
            .collect();
        apply_command(
            &mut topo_large,
            &ClusterCommand::BatchHeartbeat(large_batch),
        );

        for i in 0..100 {
            let node_id = format!("node-{}", i);
            let small_ts = topo_small
                .object_nodes
                .get(&node_id)
                .unwrap()
                .last_heartbeat_at;
            let large_ts = topo_large
                .object_nodes
                .get(&node_id)
                .unwrap()
                .last_heartbeat_at;
            assert_eq!(
                small_ts, large_ts,
                "Heartbeat timestamps should match for {}",
                node_id
            );
        }
    }
}
