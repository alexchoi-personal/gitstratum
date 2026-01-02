use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use gitstratum_core::Oid;
use gitstratum_storage::{BucketStore, BucketStoreConfig};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn create_test_oid(seed: u8) -> Oid {
    let mut bytes = [seed; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    Oid::from_bytes(bytes)
}

fn test_config(dir: &std::path::Path) -> BucketStoreConfig {
    BucketStoreConfig {
        data_dir: dir.to_path_buf(),
        max_data_file_size: 1024 * 1024,
        bucket_count: 64,
        bucket_cache_size: 16,
        io_queue_depth: 4,
        io_queue_count: 1,
        compaction: gitstratum_storage::config::CompactionConfig::default(),
    }
}

fn bench_put(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("bucketstore_put");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_record", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                (tmp, store)
            },
            |(_tmp, store)| {
                let oid = create_test_oid(0x42);
                let value = Bytes::from(vec![0u8; 1024]);
                rt.block_on(store.put(oid, value)).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sequential_writes", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                (tmp, store, 0u8)
            },
            |(_tmp, store, mut seed)| {
                for _ in 0..10 {
                    let oid = create_test_oid(seed);
                    let value = Bytes::from(vec![seed; 512]);
                    rt.block_on(store.put(oid, value)).unwrap();
                    seed = seed.wrapping_add(1);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_get_cache_hit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("bucketstore_get");
    group.throughput(Throughput::Elements(1));

    group.bench_function("cache_hit", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                let oid = create_test_oid(0x42);
                let value = Bytes::from(vec![0u8; 1024]);
                rt.block_on(store.put(oid, value)).unwrap();
                rt.block_on(store.get(&oid)).unwrap();
                (tmp, store, oid)
            },
            |(_tmp, store, oid)| {
                let _ = rt.block_on(store.get(&oid)).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("cache_miss", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                let oid = create_test_oid(0x42);
                let value = Bytes::from(vec![0u8; 1024]);
                rt.block_on(store.put(oid, value)).unwrap();
                (tmp, store, oid)
            },
            |(_tmp, store, oid)| {
                store.bucket_cache().clear();
                let _ = rt.block_on(store.get(&oid)).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_contains(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("bucketstore_contains");
    group.throughput(Throughput::Elements(1));

    group.bench_function("existing_key", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                let oid = create_test_oid(0x42);
                let value = Bytes::from(vec![0u8; 512]);
                rt.block_on(store.put(oid, value)).unwrap();
                (tmp, store, oid)
            },
            |(_tmp, store, oid)| {
                let _ = store.contains(&oid);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("nonexistent_key", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                let oid = create_test_oid(0x42);
                (tmp, store, oid)
            },
            |(_tmp, store, oid)| {
                let _ = store.contains(&oid);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("bucketstore_delete");
    group.throughput(Throughput::Elements(1));

    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                let oid = create_test_oid(0x42);
                let value = Bytes::from(vec![0u8; 512]);
                rt.block_on(store.put(oid, value)).unwrap();
                (tmp, store, oid)
            },
            |(_tmp, store, oid)| {
                let _ = rt.block_on(store.delete(&oid));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("mark_deleted", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                let oid = create_test_oid(0x42);
                let value = Bytes::from(vec![0u8; 512]);
                rt.block_on(store.put(oid, value)).unwrap();
                (tmp, store, oid)
            },
            |(_tmp, store, oid)| {
                let _ = rt.block_on(store.mark_deleted(&oid));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_iterator(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("bucketstore_iterator");
    group.throughput(Throughput::Elements(100));

    group.bench_function("scan_100_entries", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().unwrap();
                let config = test_config(tmp.path());
                let store = rt.block_on(BucketStore::open(config)).unwrap();
                for i in 0..100u8 {
                    let oid = create_test_oid(i);
                    let value = Bytes::from(vec![i; 256]);
                    rt.block_on(store.put(oid, value)).unwrap();
                }
                (tmp, store)
            },
            |(_tmp, store)| {
                use tokio_stream::StreamExt;
                let mut count = 0;
                let mut iter = store.iter();
                rt.block_on(async {
                    while let Some(result) = iter.next().await {
                        let _ = result.unwrap();
                        count += 1;
                    }
                });
                assert_eq!(count, 100);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get_cache_hit,
    bench_contains,
    bench_delete,
    bench_iterator
);
criterion_main!(benches);
