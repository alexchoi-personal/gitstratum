use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use gitstratum_coordinator::{
    apply_command, deserialize_command, serialize_command, serialize_topology, ClusterCommand,
    ClusterTopology, GlobalRateLimiter, HashRingConfig, HeartbeatBatcher, HeartbeatInfo, NodeEntry,
    SerializableHeartbeatInfo,
};
use gitstratum_proto::NodeState;

fn create_sample_topology(num_nodes: usize) -> ClusterTopology {
    let mut topo = ClusterTopology::default();
    topo.hash_ring_config = HashRingConfig {
        virtual_nodes_per_physical: 16,
        replication_factor: 3,
    };

    for i in 0..num_nodes {
        let entry = NodeEntry {
            id: format!("node-{}", i),
            address: format!("192.168.1.{}", i % 256),
            port: 9000 + (i as u32 % 1000),
            state: NodeState::Active as i32,
            last_heartbeat_at: 1234567890,
            suspect_count: 0,
            generation_id: format!("uuid-{}", i),
            registered_at: 1234567000,
        };
        if i % 2 == 0 {
            topo.object_nodes.insert(entry.id.clone(), entry);
        } else {
            topo.metadata_nodes.insert(entry.id.clone(), entry);
        }
    }
    topo.version = num_nodes as u64;
    topo
}

fn bench_topology_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("topology_serialization");

    for size in [10, 100, 1000] {
        let topo = create_sample_topology(size);

        group.bench_function(format!("serialize_{}_nodes", size), |b| {
            b.iter(|| serialize_topology(black_box(&topo)).unwrap())
        });
    }

    group.finish();
}

fn bench_command_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_serialization");

    let entry = NodeEntry {
        id: "node-1".to_string(),
        address: "192.168.1.10".to_string(),
        port: 9000,
        state: NodeState::Active as i32,
        last_heartbeat_at: 1234567890,
        suspect_count: 0,
        generation_id: "uuid-1234".to_string(),
        registered_at: 1234567000,
    };

    let add_cmd = ClusterCommand::AddObjectNode(entry.clone());
    group.bench_function("serialize_add_node", |b| {
        b.iter(|| serialize_command(black_box(&add_cmd)).unwrap())
    });

    let serialized = serialize_command(&add_cmd).unwrap();
    group.bench_function("deserialize_add_node", |b| {
        b.iter(|| deserialize_command(black_box(&serialized)).unwrap())
    });

    let mut batch = HashMap::new();
    for i in 0..100 {
        batch.insert(
            format!("node-{}", i),
            SerializableHeartbeatInfo {
                known_version: i as u64,
                reported_state: NodeState::Active as i32,
                generation_id: format!("uuid-{}", i),
                received_at_ms: 1000,
            },
        );
    }
    let batch_cmd = ClusterCommand::BatchHeartbeat(batch);

    group.bench_function("serialize_batch_heartbeat_100", |b| {
        b.iter(|| serialize_command(black_box(&batch_cmd)).unwrap())
    });

    group.finish();
}

fn bench_apply_command(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_command");

    let entry = NodeEntry {
        id: "new-node".to_string(),
        address: "192.168.1.100".to_string(),
        port: 9000,
        state: NodeState::Joining as i32,
        last_heartbeat_at: 0,
        suspect_count: 0,
        generation_id: "uuid-new".to_string(),
        registered_at: 1234567000,
    };

    group.bench_function("add_object_node_empty", |b| {
        b.iter_batched(
            || {
                (
                    ClusterTopology::default(),
                    ClusterCommand::AddObjectNode(entry.clone()),
                )
            },
            |(mut topo, cmd)| apply_command(black_box(&mut topo), black_box(&cmd)),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("add_object_node_100_existing", |b| {
        b.iter_batched(
            || {
                let topo = create_sample_topology(100);
                let new_entry = NodeEntry {
                    id: "new-node".to_string(),
                    ..entry.clone()
                };
                (topo, ClusterCommand::AddObjectNode(new_entry))
            },
            |(mut topo, cmd)| apply_command(black_box(&mut topo), black_box(&cmd)),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("set_node_state", |b| {
        b.iter_batched(
            || {
                let topo = create_sample_topology(100);
                let cmd = ClusterCommand::SetNodeState {
                    node_id: "node-50".to_string(),
                    state: NodeState::Suspect as i32,
                };
                (topo, cmd)
            },
            |(mut topo, cmd)| apply_command(black_box(&mut topo), black_box(&cmd)),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("remove_node", |b| {
        b.iter_batched(
            || {
                let topo = create_sample_topology(100);
                let cmd = ClusterCommand::RemoveNode {
                    node_id: "node-50".to_string(),
                };
                (topo, cmd)
            },
            |(mut topo, cmd)| apply_command(black_box(&mut topo), black_box(&cmd)),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_rate_limiter(c: &mut Criterion) {
    let mut group = c.benchmark_group("rate_limiter");

    group.bench_function("try_heartbeat", |b| {
        let limiter = GlobalRateLimiter::new();
        b.iter(|| {
            let _ = black_box(limiter.try_heartbeat());
        })
    });

    group.bench_function("try_register", |b| {
        let limiter = GlobalRateLimiter::new();
        b.iter(|| {
            let _ = black_box(limiter.try_register());
        })
    });

    group.bench_function("try_topology_read", |b| {
        let limiter = GlobalRateLimiter::new();
        b.iter(|| {
            let _ = black_box(limiter.try_topology_read());
        })
    });

    group.bench_function("try_increment_watch", |b| {
        b.iter_batched(
            GlobalRateLimiter::new,
            |limiter| {
                let _ = black_box(limiter.try_increment_watch());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_heartbeat_batcher(c: &mut Criterion) {
    let mut group = c.benchmark_group("heartbeat_batcher");

    group.bench_function("record_heartbeat", |b| {
        let batcher = HeartbeatBatcher::new(std::time::Duration::from_secs(1));
        let info = HeartbeatInfo {
            known_version: 100,
            reported_state: NodeState::Active as i32,
            generation_id: "uuid-1234".to_string(),
            received_at: unix_timestamp_ms(),
        };
        b.iter(|| {
            batcher.record_heartbeat(black_box("node-1".to_string()), black_box(info.clone()));
        })
    });

    group.bench_function("take_batch_100_nodes", |b| {
        b.iter_batched(
            || {
                let batcher = HeartbeatBatcher::new(std::time::Duration::from_secs(1));
                for i in 0..100 {
                    let info = HeartbeatInfo {
                        known_version: i as u64,
                        reported_state: NodeState::Active as i32,
                        generation_id: format!("uuid-{}", i),
                        received_at: unix_timestamp_ms(),
                    };
                    batcher.record_heartbeat(format!("node-{}", i), info);
                }
                batcher
            },
            |batcher| black_box(batcher.take_batch()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_topology_serialization,
    bench_command_serialization,
    bench_apply_command,
    bench_rate_limiter,
    bench_heartbeat_batcher,
);
criterion_main!(benches);
