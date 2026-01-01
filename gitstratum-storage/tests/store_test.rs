use bytes::Bytes;
use gitstratum_core::Oid;
use gitstratum_storage::{BucketStore, BucketStoreConfig};
use std::time::Duration;
use tempfile::TempDir;

fn create_test_oid(seed: u8) -> Oid {
    let mut bytes = [seed; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    Oid::from_bytes(bytes)
}

fn create_test_config(dir: &TempDir) -> BucketStoreConfig {
    BucketStoreConfig {
        data_dir: dir.path().to_path_buf(),
        bucket_count: 1024,
        bucket_cache_size: 64,
        max_data_file_size: 64 * 1024,
        io_queue_depth: 64,
        io_queue_count: 2,
        compaction: gitstratum_storage::config::CompactionConfig {
            fragmentation_threshold: 0.4,
            check_interval: Duration::from_secs(300),
            max_concurrent: 1,
        },
    }
}

#[tokio::test]
async fn test_bucket_store_full_lifecycle() {
    let dir = TempDir::new().unwrap();
    let config = create_test_config(&dir);
    let store = BucketStore::open(config).await.unwrap();

    assert!(store.is_empty());
    let stats = store.stats();
    assert_eq!(stats.entries, 0);

    let oid = create_test_oid(1);
    let value = Bytes::from("hello world");

    let retrieved = store.get(&oid).await.unwrap();
    assert_eq!(retrieved, None);
    assert!(!store.contains(&oid));

    store.put(oid, value.clone()).await.unwrap();
    assert!(store.contains(&oid));
    assert_eq!(store.len(), 1);

    let retrieved = store.get(&oid).await.unwrap();
    assert_eq!(retrieved, Some(value));

    let stats = store.stats();
    assert_eq!(stats.entries, 1);
    assert!(stats.total_bytes > 0);

    let value2 = Bytes::from("updated value");
    store.put(oid, value2.clone()).await.unwrap();
    let retrieved = store.get(&oid).await.unwrap();
    assert_eq!(retrieved, Some(value2));

    let deleted = store.delete(&oid).await.unwrap();
    assert!(deleted);
    assert!(!store.contains(&oid));
    let retrieved = store.get(&oid).await.unwrap();
    assert_eq!(retrieved, None);

    let deleted_again = store.delete(&oid).await.unwrap();
    assert!(!deleted_again);

    store.close().await.unwrap();
}

#[tokio::test]
async fn test_bucket_store_multiple_entries() {
    let dir = TempDir::new().unwrap();
    let config = create_test_config(&dir);
    let store = BucketStore::open(config).await.unwrap();

    for i in 0..100 {
        let oid = create_test_oid(i);
        let value = Bytes::from(format!("value_{}", i));
        store.put(oid, value).await.unwrap();
    }

    assert_eq!(store.len(), 100);

    for i in 0..100 {
        let oid = create_test_oid(i);
        let expected = Bytes::from(format!("value_{}", i));
        let retrieved = store.get(&oid).await.unwrap();
        assert_eq!(retrieved, Some(expected));
    }

    store.close().await.unwrap();
}

#[tokio::test]
async fn test_bucket_store_large_value_and_sync() {
    let dir = TempDir::new().unwrap();
    let config = create_test_config(&dir);
    let store = BucketStore::open(config).await.unwrap();

    let oid = create_test_oid(1);
    let value = Bytes::from(vec![0xAB; 100_000]);

    store.put(oid, value.clone()).await.unwrap();
    store.sync().await.unwrap();

    let retrieved = store.get(&oid).await.unwrap();
    assert_eq!(retrieved, Some(value));

    store.close().await.unwrap();
}
