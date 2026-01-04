use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

use gitstratum_coordinator::{
    apply_command, serialize_command, serialize_topology, ClusterCommand, ClusterTopology,
    GlobalRateLimiter, HeartbeatBatcher, HeartbeatInfo, NodeEntry, SerializableHeartbeatInfo,
};
use gitstratum_proto::NodeState;

fn create_test_node(id: &str) -> NodeEntry {
    NodeEntry {
        id: id.to_string(),
        address: "192.168.1.1".to_string(),
        port: 9000,
        state: NodeState::Active as i32,
        last_heartbeat_at: 0,
        suspect_count: 0,
        generation_id: format!("gen-{}", id),
        registered_at: 0,
    }
}

mod load_tests {
    use super::*;

    #[test]
    fn test_100_nodes_heartbeat_throughput() {
        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_secs(1)));
        let limiter = Arc::new(GlobalRateLimiter::new());

        let start = Instant::now();
        let mut successful_heartbeats = 0u64;

        for round in 0..10 {
            for node_id in 0..100 {
                if limiter.try_heartbeat().is_ok() {
                    let info = HeartbeatInfo {
                        known_version: round as u64,
                        reported_state: NodeState::Active as i32,
                        generation_id: format!("gen-node-{}", node_id),
                        received_at: unix_timestamp_ms(),
                    };
                    batcher.record_heartbeat(format!("node-{}", node_id), info);
                    successful_heartbeats += 1;
                }
            }
        }

        let elapsed = start.elapsed();
        let rate = successful_heartbeats as f64 / elapsed.as_secs_f64();

        println!(
            "100 nodes x 10 rounds: {} heartbeats in {:?} ({:.0} heartbeats/sec)",
            successful_heartbeats, elapsed, rate
        );

        assert!(
            successful_heartbeats >= 900,
            "Expected at least 90% success rate"
        );
    }

    #[test]
    fn test_500_nodes_topology_operations() {
        let mut topo = ClusterTopology::default();

        let start = Instant::now();

        for i in 0..500 {
            let node = create_test_node(&format!("node-{}", i));
            let cmd = ClusterCommand::AddObjectNode(node);
            let resp = apply_command(&mut topo, &cmd);
            assert!(resp.is_success());
        }

        let add_elapsed = start.elapsed();

        let start = Instant::now();
        for i in 0..100 {
            let cmd = ClusterCommand::SetNodeState {
                node_id: format!("node-{}", i),
                state: NodeState::Suspect as i32,
            };
            apply_command(&mut topo, &cmd);
        }
        let update_elapsed = start.elapsed();

        println!(
            "500 node adds: {:?} ({:.0} ops/sec)",
            add_elapsed,
            500.0 / add_elapsed.as_secs_f64()
        );
        println!(
            "100 state updates: {:?} ({:.0} ops/sec)",
            update_elapsed,
            100.0 / update_elapsed.as_secs_f64()
        );

        assert_eq!(topo.object_nodes.len(), 500);
    }

    #[test]
    fn test_batch_heartbeat_scalability() {
        let mut topo = ClusterTopology::default();

        for i in 0..200 {
            let node = create_test_node(&format!("node-{}", i));
            apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
        }

        let batch_sizes = [10, 50, 100, 200];

        for &batch_size in &batch_sizes {
            let mut batch = HashMap::new();
            for i in 0..batch_size {
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

            let start = Instant::now();
            for _ in 0..100 {
                let mut topo_clone = topo.clone();
                apply_command(&mut topo_clone, &cmd);
            }
            let elapsed = start.elapsed();

            println!(
                "Batch size {}: 100 iterations in {:?} ({:.0} batches/sec)",
                batch_size,
                elapsed,
                100.0 / elapsed.as_secs_f64()
            );
        }
    }

    #[test]
    fn test_topology_serialization_scalability() {
        let node_counts = [10, 50, 100, 500, 1000];

        for &count in &node_counts {
            let mut topo = ClusterTopology::default();

            for i in 0..count {
                let node = create_test_node(&format!("node-{}", i));
                apply_command(&mut topo, &ClusterCommand::AddObjectNode(node));
            }

            let start = Instant::now();
            let mut total_bytes = 0usize;

            for _ in 0..100 {
                let serialized = serialize_topology(&topo).unwrap();
                total_bytes += serialized.len();
            }

            let elapsed = start.elapsed();

            println!(
                "{} nodes: avg {} bytes, 100 serializations in {:?} ({:.0}/sec)",
                count,
                total_bytes / 100,
                elapsed,
                100.0 / elapsed.as_secs_f64()
            );
        }
    }

    #[test]
    fn test_concurrent_rate_limiter_performance() {
        use std::thread;

        let limiter = Arc::new(GlobalRateLimiter::new());
        let total_attempts = Arc::new(AtomicU64::new(0));
        let total_success = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut handles = vec![];

        for _ in 0..8 {
            let limiter = Arc::clone(&limiter);
            let attempts = Arc::clone(&total_attempts);
            let success = Arc::clone(&total_success);

            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    if limiter.try_heartbeat().is_ok() {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();
        let attempts = total_attempts.load(Ordering::Relaxed);
        let success = total_success.load(Ordering::Relaxed);

        println!(
            "8 threads x 1000 attempts: {}/{} succeeded in {:?} ({:.0} attempts/sec)",
            success,
            attempts,
            elapsed,
            attempts as f64 / elapsed.as_secs_f64()
        );

        assert!(success > 0);
    }

    #[test]
    fn test_command_serialization_throughput() {
        let node = create_test_node("node-1");
        let add_cmd = ClusterCommand::AddObjectNode(node);

        let mut batch = HashMap::new();
        for i in 0..100 {
            batch.insert(
                format!("node-{}", i),
                SerializableHeartbeatInfo {
                    known_version: i as u64,
                    reported_state: NodeState::Active as i32,
                    generation_id: format!("gen-{}", i),
                    received_at_ms: 1000,
                },
            );
        }
        let batch_cmd = ClusterCommand::BatchHeartbeat(batch);

        let start = Instant::now();
        for _ in 0..10000 {
            let _ = serialize_command(&add_cmd).unwrap();
        }
        let add_elapsed = start.elapsed();

        let start = Instant::now();
        for _ in 0..1000 {
            let _ = serialize_command(&batch_cmd).unwrap();
        }
        let batch_elapsed = start.elapsed();

        println!(
            "AddNode serialization: 10000 in {:?} ({:.0}/sec)",
            add_elapsed,
            10000.0 / add_elapsed.as_secs_f64()
        );
        println!(
            "BatchHeartbeat(100) serialization: 1000 in {:?} ({:.0}/sec)",
            batch_elapsed,
            1000.0 / batch_elapsed.as_secs_f64()
        );
    }

    #[tokio::test]
    async fn test_simulated_cluster_workload() {
        let batcher = Arc::new(HeartbeatBatcher::new(Duration::from_millis(100)));
        let operations = Arc::new(AtomicU64::new(0));

        let batcher_clone = Arc::clone(&batcher);
        let ops_clone = Arc::clone(&operations);

        let producer = tokio::spawn(async move {
            for round in 0..50 {
                for node_id in 0..100 {
                    let info = HeartbeatInfo {
                        known_version: round as u64,
                        reported_state: NodeState::Active as i32,
                        generation_id: format!("gen-node-{}", node_id),
                        received_at: unix_timestamp_ms(),
                    };
                    batcher_clone.record_heartbeat(format!("node-{}", node_id), info);
                    ops_clone.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        let batcher_clone = Arc::clone(&batcher);
        let consumer = tokio::spawn(async move {
            let mut total_batches = 0u32;
            let mut total_items = 0usize;

            for _ in 0..60 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let batch = batcher_clone.take_batch();
                if !batch.is_empty() {
                    total_batches += 1;
                    total_items += batch.len();
                }
            }

            (total_batches, total_items)
        });

        producer.await.unwrap();
        let (batches, items) = consumer.await.unwrap();

        let total_ops = operations.load(Ordering::Relaxed);
        println!(
            "Produced {} heartbeats, consumed {} items in {} batches",
            total_ops, items, batches
        );

        assert!(batches > 0);
    }
}
