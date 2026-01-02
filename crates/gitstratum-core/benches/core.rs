use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use gitstratum_core::{Blob, Oid, Tree, TreeEntry, TreeEntryMode};

fn generate_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

fn generate_hex(count: usize) -> String {
    "ab".repeat(count)
}

fn generate_tree_entries(count: usize) -> Vec<TreeEntry> {
    (0..count)
        .map(|i| {
            TreeEntry::new(
                TreeEntryMode::File,
                format!("file_{:05}.txt", i),
                Oid::hash(&[i as u8]),
            )
        })
        .collect()
}

fn bench_oid_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("oid_hash");

    for size in [64, 1024, 16384, 262144] {
        let data = generate_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| Oid::hash(black_box(data)));
        });
    }

    group.finish();
}

fn bench_oid_hash_object(c: &mut Criterion) {
    let mut group = c.benchmark_group("oid_hash_object");

    for size in [64, 1024, 16384, 262144] {
        let data = generate_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| Oid::hash_object(black_box("blob"), black_box(data)));
        });
    }

    group.finish();
}

fn bench_oid_from_hex(c: &mut Criterion) {
    let hex = generate_hex(32);

    c.bench_function("oid_from_hex", |b| {
        b.iter(|| Oid::from_hex(black_box(&hex)));
    });
}

fn bench_oid_to_hex(c: &mut Criterion) {
    let oid = Oid::hash(b"benchmark data");

    c.bench_function("oid_to_hex", |b| {
        b.iter(|| black_box(&oid).to_hex());
    });
}

fn bench_tree_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("tree_new");

    for count in [10, 100, 1000] {
        let entries = generate_tree_entries(count);
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &entries,
            |b, entries| {
                b.iter(|| Tree::new(black_box(entries.clone())));
            },
        );
    }

    group.finish();
}

fn bench_tree_find(c: &mut Criterion) {
    let mut group = c.benchmark_group("tree_find");

    for count in [10, 100, 1000] {
        let entries = generate_tree_entries(count);
        let tree = Tree::new(entries);
        let search_name = format!("file_{:05}.txt", count / 2);
        group.bench_with_input(BenchmarkId::from_parameter(count), &tree, |b, tree| {
            b.iter(|| tree.find(black_box(&search_name)));
        });
    }

    group.finish();
}

fn bench_blob_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_compress");

    for size in [1024, 16384, 262144] {
        let data = generate_data(size);
        let blob = Blob::new(data);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &blob, |b, blob| {
            b.iter(|| blob.compress());
        });
    }

    group.finish();
}

fn bench_blob_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_decompress");

    for size in [1024, 16384, 262144] {
        let data = generate_data(size);
        let blob = Blob::new(data);
        let compressed = blob.compress().unwrap();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &(blob.oid, compressed),
            |b, (oid, compressed)| {
                b.iter(|| Blob::decompress(black_box(*oid), black_box(compressed)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_oid_hash,
    bench_oid_hash_object,
    bench_oid_from_hex,
    bench_oid_to_hex,
    bench_tree_new,
    bench_tree_find,
    bench_blob_compress,
    bench_blob_decompress,
);

criterion_main!(benches);
