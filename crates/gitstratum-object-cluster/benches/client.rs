use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use gitstratum_core::{Blob as CoreBlob, Oid};
use gitstratum_hashring::{HashRingBuilder, NodeInfo};
use gitstratum_object_cluster::ObjectClusterClient;
use std::sync::Arc;

fn create_ring(
    node_count: usize,
    replication_factor: usize,
) -> Arc<gitstratum_hashring::ConsistentHashRing> {
    let mut builder = HashRingBuilder::new()
        .virtual_nodes(150)
        .replication_factor(replication_factor);

    for i in 1..=node_count {
        builder = builder.add_node(NodeInfo::new(
            format!("node-{}", i),
            "127.0.0.1",
            9000 + i as u16,
        ));
    }

    Arc::new(builder.build().unwrap())
}

fn core_oid_to_proto(oid: &Oid) -> gitstratum_proto::Oid {
    gitstratum_proto::Oid {
        bytes: oid.as_bytes().to_vec(),
    }
}

fn proto_oid_to_core(oid: &gitstratum_proto::Oid) -> Oid {
    Oid::from_slice(&oid.bytes).unwrap()
}

fn core_blob_to_proto(blob: &CoreBlob) -> gitstratum_proto::Blob {
    gitstratum_proto::Blob {
        oid: Some(core_oid_to_proto(&blob.oid)),
        data: blob.data.to_vec(),
        compressed: false,
    }
}

fn bench_hash_ring_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_ring_lookups");

    for node_count in [3, 10, 50, 100] {
        let ring = create_ring(node_count, 3);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("nodes_for_oid", node_count),
            &node_count,
            |b, _| {
                let oid = Oid::hash(b"benchmark-key");
                b.iter(|| std::hint::black_box(ring.nodes_for_oid(&oid).unwrap()));
            },
        );
    }

    group.finish();
}

fn bench_proto_conversions(c: &mut Criterion) {
    let mut group = c.benchmark_group("proto_conversions");

    group.bench_function("core_oid_to_proto", |b| {
        let oid = Oid::hash(b"benchmark-oid");
        b.iter(|| std::hint::black_box(core_oid_to_proto(&oid)));
    });

    group.bench_function("proto_oid_to_core", |b| {
        let oid = Oid::hash(b"benchmark-oid");
        let proto = core_oid_to_proto(&oid);
        b.iter(|| std::hint::black_box(proto_oid_to_core(&proto)));
    });

    for size in [64, 1024, 65536] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let blob = CoreBlob::new(data);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("core_blob_to_proto", size),
            &blob,
            |b, blob| {
                b.iter(|| std::hint::black_box(core_blob_to_proto(blob)));
            },
        );
    }

    group.finish();
}

fn bench_client_creation_and_node_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_operations");

    group.bench_function("client_new", |b| {
        let ring = create_ring(10, 3);
        b.iter(|| std::hint::black_box(ObjectClusterClient::new(Arc::clone(&ring))));
    });

    group.bench_function("add_node", |b| {
        let ring = create_ring(10, 3);
        let client = ObjectClusterClient::new(ring);
        let mut counter = 100u16;
        b.iter(|| {
            let node = NodeInfo::new(format!("bench-node-{}", counter), "127.0.0.1", counter);
            counter += 1;
            client.add_node(node).unwrap();
        });
    });

    group.bench_function("nodes", |b| {
        let ring = create_ring(50, 3);
        let client = ObjectClusterClient::new(ring);
        b.iter(|| std::hint::black_box(client.nodes()));
    });

    group.bench_function("node_count", |b| {
        let ring = create_ring(50, 3);
        let client = ObjectClusterClient::new(ring);
        b.iter(|| std::hint::black_box(client.node_count()));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_hash_ring_lookups,
    bench_proto_conversions,
    bench_client_creation_and_node_ops
);
criterion_main!(benches);
