use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

use gitstratum_coordinator::{
    apply_command, serialize_topology, ClusterCommand, ClusterTopology, CoordinatorConfig,
    GlobalRateLimiter, HashRingConfig, HeartbeatBatcher, HeartbeatInfo, NodeEntry,
    SerializableHeartbeatInfo,
};
use gitstratum_proto::NodeState;

fn create_test_node(id: &str, state: NodeState) -> NodeEntry {
    NodeEntry {
        id: id.to_string(),
        address: format!("192.168.1.{}", id.chars().last().unwrap_or('1') as u8),
        port: 9000,
        state: state as i32,
        last_heartbeat_at: 0,
        suspect_count: 0,
        generation_id: format!("gen-{}", id),
        registered_at: 0,
    }
}

mod cluster_topology_tests {
    use super::*;

    #[test]
    fn test_add_100_nodes_to_topology() {
        let mut topo = ClusterTopology::default();

        for i in 0..100 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            let cmd = if i % 2 == 0 {
                ClusterCommand::AddObjectNode(node)
            } else {
                ClusterCommand::AddMetadataNode(node)
            };
            let resp = apply_command(&mut topo, &cmd);
            assert!(resp.is_success());
        }

        assert_eq!(topo.object_nodes.len(), 50);
        assert_eq!(topo.metadata_nodes.len(), 50);
        assert_eq!(topo.version, 100);
    }

    #[test]
    fn test_node_state_transitions() {
        let mut topo = ClusterTopology::default();

        let node = create_test_node("node-1", NodeState::Joining);
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let transitions = [
            NodeState::Active,
            NodeState::Suspect,
            NodeState::Active,
            NodeState::Draining,
            NodeState::Down,
        ];

        for state in transitions {
            let cmd = ClusterCommand::SetNodeState {
                node_id: "node-1".to_string(),
                state: state as i32,
            };
            let resp = apply_command(&mut topo, &cmd);
            assert!(resp.is_success());

            let node = topo.object_nodes.get("node-1").unwrap();
            assert_eq!(node.state(), state);
        }
    }

    #[test]
    fn test_batch_heartbeat_updates_many_nodes() {
        let mut topo = ClusterTopology::default();

        for i in 0..50 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let mut batch = HashMap::new();
        for i in 0..50 {
            batch.insert(
                format!("node-{}", i),
                SerializableHeartbeatInfo {
                    known_version: topo.version,
                    reported_state: NodeState::Active as i32,
                    generation_id: format!("gen-node-{}", i),
                    received_at_ms: 1000,
                },
            );
        }

        let cmd = ClusterCommand::BatchHeartbeat(batch);
        let resp = apply_command(&mut topo, &cmd);
        assert!(resp.is_success());
    }

    #[test]
    fn test_topology_serialization_round_trip() {
        let mut topo = ClusterTopology::default();
        topo.hash_ring_config = HashRingConfig {
            virtual_nodes_per_physical: 32,
            replication_factor: 5,
        };

        for i in 0..20 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let serialized = serialize_topology(&topo).unwrap();
        let deserialized: ClusterTopology = serde_json::from_str(&serialized).unwrap();

        assert_eq!(topo.version, deserialized.version);
        assert_eq!(topo.object_nodes.len(), deserialized.object_nodes.len());
        assert_eq!(
            topo.hash_ring_config.replication_factor,
            deserialized.hash_ring_config.replication_factor
        );
    }
}

mod rate_limiter_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::thread;

    #[test]
    fn test_concurrent_heartbeat_rate_limiting() {
        let limiter = Arc::new(GlobalRateLimiter::new());
        let success_count = Arc::new(AtomicU32::new(0));
        let fail_count = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];

        for _ in 0..10 {
            let limiter = Arc::clone(&limiter);
            let success = Arc::clone(&success_count);
            let fail = Arc::clone(&fail_count);

            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    if limiter.try_heartbeat().is_ok() {
                        success.fetch_add(1, Ordering::Relaxed);
                    } else {
                        fail.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let total = success_count.load(Ordering::Relaxed) + fail_count.load(Ordering::Relaxed);
        assert_eq!(total, 1000);
        assert!(success_count.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_watch_connection_limit() {
        let limiter = Arc::new(GlobalRateLimiter::new());
        let connected = Arc::new(AtomicU32::new(0));
        let rejected = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];

        for _ in 0..20 {
            let limiter = Arc::clone(&limiter);
            let connected = Arc::clone(&connected);
            let rejected = Arc::clone(&rejected);

            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    if limiter.try_increment_watch().is_ok() {
                        connected.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_micros(10));
                        limiter.decrement_watch();
                    } else {
                        rejected.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(limiter.watch_count(), 0);
    }
}

mod heartbeat_batcher_tests {
    use super::*;

    #[test]
    fn test_batcher_handles_high_throughput() {
        let batcher = HeartbeatBatcher::new(Duration::from_millis(100));

        for i in 0..1000 {
            let info = HeartbeatInfo {
                known_version: i as u64,
                reported_state: NodeState::Active as i32,
                generation_id: format!("gen-{}", i),
                received_at: unix_timestamp_ms(),
            };
            batcher.record_heartbeat(format!("node-{}", i), info);
        }

        assert_eq!(batcher.pending_count(), 1000);

        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 1000);
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_batcher_overwrites_with_latest() {
        let batcher = HeartbeatBatcher::new(Duration::from_millis(100));

        for version in 0..100 {
            let info = HeartbeatInfo {
                known_version: version,
                reported_state: NodeState::Active as i32,
                generation_id: "gen-1".to_string(),
                received_at: unix_timestamp_ms(),
            };
            batcher.record_heartbeat("node-1".to_string(), info);
        }

        assert_eq!(batcher.pending_count(), 1);

        let batch = batcher.take_batch();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.get("node-1").unwrap().known_version, 99);
    }

    #[tokio::test]
    async fn test_batcher_flush_loop_with_many_nodes() {
        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(50)));
        let flush_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let total_flushed = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let batcher_clone = Arc::clone(&batcher);
        let flush_count_clone = Arc::clone(&flush_count);
        let total_flushed_clone = Arc::clone(&total_flushed);

        let flush_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            for _ in 0..5 {
                interval.tick().await;
                let batch = batcher_clone.take_batch();
                if !batch.is_empty() {
                    flush_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    total_flushed_clone
                        .fetch_add(batch.len() as u32, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        for i in 0..100 {
            let info = HeartbeatInfo {
                known_version: i as u64,
                reported_state: NodeState::Active as i32,
                generation_id: format!("gen-{}", i),
                received_at: unix_timestamp_ms(),
            };
            batcher.record_heartbeat(format!("node-{}", i), info);

            if i % 25 == 24 {
                tokio::time::sleep(Duration::from_millis(60)).await;
            }
        }

        flush_handle.await.unwrap();

        assert!(flush_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }
}

mod config_tests {
    use super::*;

    #[test]
    fn test_config_timing_relationships() {
        let config = CoordinatorConfig::default();

        assert!(
            config.leader_grace_period >= config.suspect_timeout,
            "Leader grace period should be >= suspect timeout"
        );

        assert!(
            config.down_timeout >= config.suspect_timeout,
            "Down timeout should be >= suspect timeout"
        );

        assert!(
            config.stability_window >= config.flap_window / 2,
            "Stability window should be at least half of flap window"
        );

        assert!(
            config.flap_threshold >= 2,
            "Flap threshold should be at least 2 to avoid false positives"
        );
    }

    #[test]
    fn test_config_rate_limits_are_sensible() {
        let config = CoordinatorConfig::default();

        assert!(
            config.global_heartbeats_per_sec >= config.max_heartbeats_per_min,
            "Global heartbeat limit should be >= per-client limit"
        );

        assert!(
            config.global_registrations_per_sec >= config.max_registrations_per_min,
            "Global registration limit should be >= per-client limit"
        );
    }
}

mod failure_detection_simulation {
    use super::*;

    #[test]
    fn test_simulated_failure_detection_sequence() {
        let mut topo = ClusterTopology::default();

        for i in 0..10 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-5".to_string(),
            state: NodeState::Suspect as i32,
        };
        apply_command(&mut topo, &cmd);
        assert_eq!(
            topo.object_nodes.get("node-5").unwrap().state(),
            NodeState::Suspect
        );

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-5".to_string(),
            state: NodeState::Active as i32,
        };
        apply_command(&mut topo, &cmd);
        assert_eq!(
            topo.object_nodes.get("node-5").unwrap().state(),
            NodeState::Active
        );

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-5".to_string(),
            state: NodeState::Suspect as i32,
        };
        apply_command(&mut topo, &cmd);

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-5".to_string(),
            state: NodeState::Down as i32,
        };
        apply_command(&mut topo, &cmd);
        assert_eq!(
            topo.object_nodes.get("node-5").unwrap().state(),
            NodeState::Down
        );
    }

    #[test]
    fn test_multiple_nodes_failing_simultaneously() {
        let mut topo = ClusterTopology::default();

        for i in 0..20 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Active);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        for i in 0..5 {
            let cmd = ClusterCommand::SetNodeState {
                node_id: format!("node-{}", i),
                state: NodeState::Suspect as i32,
            };
            apply_command(&mut topo, &cmd);
        }

        let suspect_count = topo
            .object_nodes
            .values()
            .filter(|n| n.state() == NodeState::Suspect)
            .count();
        assert_eq!(suspect_count, 5);

        let active_count = topo
            .object_nodes
            .values()
            .filter(|n| n.state() == NodeState::Active)
            .count();
        assert_eq!(active_count, 15);
    }
}

mod heartbeat_recovery_tests {
    use super::*;
    use gitstratum_coordinator::validate_generation_id;

    #[test]
    fn test_suspect_node_can_recover_to_active_via_set_node_state() {
        let mut topo = ClusterTopology::default();
        let node = create_test_node("node-1", NodeState::Active);
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-1".to_string(),
            state: NodeState::Suspect as i32,
        };
        apply_command(&mut topo, &cmd);
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state(),
            NodeState::Suspect,
            "Node should be in SUSPECT state"
        );

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-1".to_string(),
            state: NodeState::Active as i32,
        };
        let resp = apply_command(&mut topo, &cmd);
        assert!(resp.is_success(), "SetNodeState to ACTIVE should succeed");
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state(),
            NodeState::Active,
            "Node should recover to ACTIVE state after SetNodeState command"
        );
    }

    #[test]
    fn test_generation_id_mismatch_detected_before_heartbeat_processing() {
        let mut topo = ClusterTopology::default();
        let node = NodeEntry {
            id: "node-1".to_string(),
            address: "192.168.1.1".to_string(),
            port: 9000,
            state: NodeState::Active as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "correct-generation-id".to_string(),
            registered_at: 0,
        };
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let result = validate_generation_id(&topo, "node-1", "wrong-generation-id");
        assert!(
            result.is_err(),
            "Generation ID mismatch should return an error"
        );
        assert!(
            result.unwrap_err().contains("mismatch"),
            "Error should indicate generation ID mismatch"
        );
    }

    #[test]
    fn test_generation_id_match_allows_heartbeat_processing() {
        let mut topo = ClusterTopology::default();
        let node = NodeEntry {
            id: "node-1".to_string(),
            address: "192.168.1.1".to_string(),
            port: 9000,
            state: NodeState::Active as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "correct-generation-id".to_string(),
            registered_at: 0,
        };
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let result = validate_generation_id(&topo, "node-1", "correct-generation-id");
        assert!(
            result.is_ok(),
            "Matching generation ID should not return an error"
        );
    }

    #[test]
    fn test_unregistered_node_skips_validation() {
        let topo = ClusterTopology::default();

        let result = validate_generation_id(&topo, "unknown-node", "any-generation-id");
        assert!(
            result.is_ok(),
            "Unregistered node should skip validation (returns Ok)"
        );
    }

    #[test]
    fn test_down_node_skips_validation() {
        let mut topo = ClusterTopology::default();
        let node = NodeEntry {
            id: "node-1".to_string(),
            address: "192.168.1.1".to_string(),
            port: 9000,
            state: NodeState::Down as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "old-generation-id".to_string(),
            registered_at: 0,
        };
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

        let result = validate_generation_id(&topo, "node-1", "new-generation-id");
        assert!(
            result.is_ok(),
            "DOWN node should skip generation ID validation to allow re-registration"
        );
    }

    #[test]
    fn test_suspect_to_active_transition_increments_version() {
        let mut topo = ClusterTopology::default();
        let node = create_test_node("node-1", NodeState::Suspect);
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        let version_after_add = topo.version;

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-1".to_string(),
            state: NodeState::Active as i32,
        };
        apply_command(&mut topo, &cmd);

        assert_eq!(
            topo.version,
            version_after_add + 1,
            "Version should increment when state changes"
        );
    }

    #[test]
    fn test_joining_node_can_transition_to_active_via_set_node_state() {
        let mut topo = ClusterTopology::default();
        let node = create_test_node("node-1", NodeState::Joining);
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state(),
            NodeState::Joining,
            "Node should be in JOINING state"
        );

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-1".to_string(),
            state: NodeState::Active as i32,
        };
        let resp = apply_command(&mut topo, &cmd);
        assert!(resp.is_success(), "SetNodeState to ACTIVE should succeed");
        assert_eq!(
            topo.object_nodes.get("node-1").unwrap().state(),
            NodeState::Active,
            "Node should transition from JOINING to ACTIVE state"
        );
    }

    #[test]
    fn test_joining_to_active_transition_increments_version() {
        let mut topo = ClusterTopology::default();
        let node = create_test_node("node-1", NodeState::Joining);
        apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        let version_after_add = topo.version;

        let cmd = ClusterCommand::SetNodeState {
            node_id: "node-1".to_string(),
            state: NodeState::Active as i32,
        };
        apply_command(&mut topo, &cmd);

        assert_eq!(
            topo.version,
            version_after_add + 1,
            "Version should increment when JOINING transitions to ACTIVE"
        );
    }
}

mod concurrent_write_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_sequential_writes_preserve_version_monotonicity() {
        let mut topo = ClusterTopology::default();
        let mut last_version = 0u64;

        for i in 0..100 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Joining);
            let resp = apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
            assert!(resp.is_success());
            let new_version = resp.version().unwrap_or(0);
            assert!(
                new_version > last_version,
                "Version should monotonically increase: {} should be > {}",
                new_version,
                last_version
            );
            last_version = new_version;
        }

        assert_eq!(topo.version, 100);
    }

    #[test]
    fn test_concurrent_state_updates_with_mutex_simulation() {
        use std::sync::Mutex;
        use std::thread;

        let topo = Arc::new(Mutex::new(ClusterTopology::default()));
        let successful_writes = Arc::new(AtomicU64::new(0));

        for i in 0..10 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Joining);
            let mut locked = topo.lock().unwrap();
            apply_command(&mut locked, &ClusterCommand::AddObjectNode(node));
        }

        let mut handles = vec![];
        for i in 0..10 {
            let topo = Arc::clone(&topo);
            let success_count = Arc::clone(&successful_writes);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    let node_id = format!("node-{}", i);
                    let cmd = ClusterCommand::SetNodeState {
                        node_id,
                        state: NodeState::Active as i32,
                    };
                    let mut locked = topo.lock().unwrap();
                    let resp = apply_command(&mut locked, &cmd);
                    if resp.is_success() {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_topo = topo.lock().unwrap();
        assert!(
            final_topo.version >= 10,
            "Version should have increased from concurrent writes"
        );
        assert!(
            successful_writes.load(Ordering::Relaxed) >= 10,
            "At least some writes should succeed"
        );
    }

    #[test]
    fn test_interleaved_add_and_state_change_commands() {
        let mut topo = ClusterTopology::default();

        for i in 0..50 {
            let node = create_test_node(&format!("node-{}", i), NodeState::Joining);
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));

            let cmd = ClusterCommand::SetNodeState {
                node_id: format!("node-{}", i),
                state: NodeState::Active as i32,
            };
            let resp = apply_command(&mut topo, &cmd);
            assert!(resp.is_success());
            assert_eq!(
                topo.object_nodes
                    .get(&format!("node-{}", i))
                    .unwrap()
                    .state(),
                NodeState::Active
            );
        }

        assert_eq!(topo.version, 100);
        assert_eq!(topo.object_nodes.len(), 50);
    }
}
