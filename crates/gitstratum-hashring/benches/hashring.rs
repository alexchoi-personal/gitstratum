use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use gitstratum_core::Oid;
use gitstratum_hashring::{ConsistentHashRing, NodeInfo};

fn create_node(id: usize) -> NodeInfo {
    NodeInfo::new(format!("node-{}", id), format!("10.0.0.{}", id % 256), 9002)
}

fn create_ring(node_count: usize) -> ConsistentHashRing {
    let ring = ConsistentHashRing::new(64, 3).unwrap();
    for i in 0..node_count {
        ring.add_node(create_node(i)).unwrap();
    }
    ring
}

fn bench_primary_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("primary_node");

    for size in [10, 100, 1000] {
        let ring = create_ring(size);
        let key = b"benchmark-key-12345";

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| ring.primary_node(black_box(key)))
        });
    }

    group.finish();
}

fn bench_primary_node_for_oid(c: &mut Criterion) {
    let mut group = c.benchmark_group("primary_node_for_oid");

    for size in [10, 100, 1000] {
        let ring = create_ring(size);
        let oid = Oid::hash(b"benchmark content for oid lookup");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| ring.primary_node_for_oid(black_box(&oid)))
        });
    }

    group.finish();
}

fn bench_nodes_for_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("nodes_for_key");

    for size in [10, 100, 1000] {
        let ring = create_ring(size);
        let key = b"benchmark-key-replication";

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| ring.nodes_for_key(black_box(key)))
        });
    }

    group.finish();
}

fn bench_nodes_for_oid(c: &mut Criterion) {
    let mut group = c.benchmark_group("nodes_for_oid");

    for size in [10, 100, 1000] {
        let ring = create_ring(size);
        let oid = Oid::hash(b"benchmark content for oid replication");

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| ring.nodes_for_oid(black_box(&oid)))
        });
    }

    group.finish();
}

fn bench_add_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_node");

    for size in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_with_setup(
                || create_ring(size),
                |ring| {
                    ring.add_node(black_box(create_node(size + 1))).unwrap();
                },
            )
        });
    }

    group.finish();
}

fn bench_remove_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove_node");

    for size in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let ring = create_ring(size);
                    let node_id = ring.get_nodes().first().unwrap().id.clone();
                    (ring, node_id)
                },
                |(ring, node_id)| {
                    ring.remove_node(black_box(&node_id)).unwrap();
                },
            )
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_primary_node,
    bench_primary_node_for_oid,
    bench_nodes_for_key,
    bench_nodes_for_oid,
    bench_add_node,
    bench_remove_node,
);

criterion_main!(benches);
