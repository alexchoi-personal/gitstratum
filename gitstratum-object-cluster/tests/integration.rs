use gitstratum_core::{Blob, Oid};
use gitstratum_object_cluster::{ObjectStorage, ObjectStore, StorageStats};
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(not(feature = "bucketstore"))]
fn create_test_store() -> (Arc<ObjectStore>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(ObjectStore::new(temp_dir.path()).unwrap());
    (store, temp_dir)
}

#[cfg(feature = "bucketstore")]
async fn create_test_store_async() -> (Arc<ObjectStore>, TempDir) {
    use gitstratum_storage::BucketStoreConfig;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let config = BucketStoreConfig {
        data_dir: temp_dir.path().to_path_buf(),
        bucket_count: 64,
        bucket_cache_size: 16,
        max_data_file_size: 1024 * 1024,
        sync_writes: true,
        io_queue_depth: 4,
        io_queue_count: 1,
        compaction: gitstratum_storage::config::CompactionConfig {
            fragmentation_threshold: 0.4,
            check_interval: Duration::from_secs(300),
            max_concurrent: 1,
        },
    };
    let store = Arc::new(ObjectStore::new(config).await.unwrap());
    (store, temp_dir)
}

#[cfg(not(feature = "bucketstore"))]
async fn create_test_store_async() -> (Arc<ObjectStore>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(ObjectStore::new(temp_dir.path()).unwrap());
    (store, temp_dir)
}

#[tokio::test]
async fn test_blob_storage_lifecycle() {
    let (store, _dir) = create_test_store_async().await;

    let blob = Blob::new(b"test content".to_vec());
    let oid = blob.oid;

    assert!(!store.has(&oid));
    let get_result = store.get(&oid).await.unwrap();
    assert!(get_result.is_none());

    store.put(&blob).await.unwrap();
    assert!(store.has(&oid));

    let retrieved = store.get(&oid).await.unwrap().unwrap();
    assert_eq!(retrieved.data.as_ref(), b"test content");

    store.put(&blob).await.unwrap();
    let stats = store.stats();
    assert_eq!(stats.total_blobs, 1);

    store.delete(&oid).await.unwrap();
    assert!(!store.has(&oid));

    let deleted_again = store.delete(&oid).await.unwrap();
    assert!(!deleted_again);

    let nonexistent_oid = Oid::hash(b"nonexistent");
    let deleted = store.delete(&nonexistent_oid).await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn test_multiple_blobs_with_stats() {
    let (store, _dir) = create_test_store_async().await;

    let blobs: Vec<Blob> = (0..10)
        .map(|i| Blob::new(format!("blob content {}", i).into_bytes()))
        .collect();

    let initial_stats = store.stats();
    assert_eq!(initial_stats.total_blobs, 0);

    for blob in &blobs {
        store.put(blob).await.unwrap();
    }

    let stats = store.stats();
    assert_eq!(stats.total_blobs, 10);

    for blob in &blobs {
        assert!(store.has(&blob.oid));
        let retrieved = store.get(&blob.oid).await.unwrap().unwrap();
        assert_eq!(retrieved.data, blob.data);
    }

    store.delete(&blobs[0].oid).await.unwrap();
    let stats_after_delete = store.stats();
    assert_eq!(stats_after_delete.total_blobs, 9);
}

#[tokio::test]
async fn test_various_blob_sizes_and_compression() {
    let (store, _dir) = create_test_store_async().await;

    let empty_blob = Blob::new(Vec::new());
    store.put(&empty_blob).await.unwrap();
    let retrieved_empty = store.get(&empty_blob.oid).await.unwrap().unwrap();
    assert!(retrieved_empty.data.is_empty());

    let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();
    let binary_blob = Blob::new(binary_data.clone());
    store.put(&binary_blob).await.unwrap();
    let retrieved_binary = store.get(&binary_blob.oid).await.unwrap().unwrap();
    assert_eq!(retrieved_binary.data.as_ref(), binary_data.as_slice());

    let compressible_data = "a".repeat(1000).into_bytes();
    let compressible_blob = Blob::new(compressible_data);
    store.put(&compressible_blob).await.unwrap();

    // Check that compression is working (RocksDB only - BucketStore has 4KB block alignment overhead)
    #[cfg(not(feature = "bucketstore"))]
    {
        let stats = store.stats();
        assert!(stats.total_bytes < 1000 * 3);
    }

    let size = 10 * 1024 * 1024;
    let large_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let large_blob = Blob::new(large_data.clone());
    store.put(&large_blob).await.unwrap();
    let retrieved_large = store.get(&large_blob.oid).await.unwrap().unwrap();
    assert_eq!(retrieved_large.data.len(), size);
    assert_eq!(retrieved_large.data.as_ref(), large_data.as_slice());
}

#[tokio::test]
async fn test_grpc_service_full_workflow() {
    use gitstratum_object_cluster::ObjectServiceImpl;
    use gitstratum_proto::object_service_server::ObjectService;
    use gitstratum_proto::{
        DeleteBlobRequest, GetBlobRequest, GetStatsRequest, HasBlobRequest, PutBlobRequest,
    };
    use tonic::Request;

    let (store, _dir) = create_test_store_async().await;
    let server = ObjectServiceImpl::new(store);

    let blob = Blob::new(b"grpc test content".to_vec());
    let proto_oid = gitstratum_proto::Oid {
        bytes: blob.oid.as_bytes().to_vec(),
    };
    let proto_blob = gitstratum_proto::Blob {
        oid: Some(proto_oid.clone()),
        data: blob.data.to_vec(),
        compressed: false,
    };

    let has_request = Request::new(HasBlobRequest {
        oid: Some(proto_oid.clone()),
    });
    let has_response = server.has_blob(has_request).await.unwrap();
    assert!(!has_response.into_inner().exists);

    let nonexistent_oid = gitstratum_proto::Oid {
        bytes: Oid::hash(b"nonexistent").as_bytes().to_vec(),
    };
    let get_nonexistent = Request::new(GetBlobRequest {
        oid: Some(nonexistent_oid),
    });
    let get_response = server.get_blob(get_nonexistent).await.unwrap();
    let inner = get_response.into_inner();
    assert!(!inner.found);
    assert!(inner.blob.is_none());

    let put_request = Request::new(PutBlobRequest {
        blob: Some(proto_blob.clone()),
    });
    let put_response = server.put_blob(put_request).await.unwrap();
    assert!(put_response.into_inner().success);

    let has_request = Request::new(HasBlobRequest {
        oid: Some(proto_oid.clone()),
    });
    let has_response = server.has_blob(has_request).await.unwrap();
    assert!(has_response.into_inner().exists);

    let get_request = Request::new(GetBlobRequest {
        oid: Some(proto_oid.clone()),
    });
    let get_response = server.get_blob(get_request).await.unwrap();
    let inner = get_response.into_inner();
    assert!(inner.found);
    assert_eq!(inner.blob.unwrap().data, b"grpc test content");

    let stats_request = Request::new(GetStatsRequest {});
    let stats_response = server.get_stats(stats_request).await.unwrap();
    assert_eq!(stats_response.into_inner().total_blobs, 1);

    let delete_request = Request::new(DeleteBlobRequest {
        oid: Some(proto_oid.clone()),
    });
    let delete_response = server.delete_blob(delete_request).await.unwrap();
    assert!(delete_response.into_inner().success);

    let has_after_delete = Request::new(HasBlobRequest {
        oid: Some(proto_oid),
    });
    let has_response = server.has_blob(has_after_delete).await.unwrap();
    assert!(!has_response.into_inner().exists);
}

#[test]
fn test_oid_and_blob_construction() {
    let data = b"test data for oid";
    let oid = Oid::hash(data);
    let bytes = oid.as_bytes();
    let reconstructed = Oid::from_slice(bytes).unwrap();
    assert_eq!(oid, reconstructed);

    let custom_blob = Blob::with_oid(oid, data.to_vec());
    assert_eq!(custom_blob.oid, oid);
    assert_eq!(custom_blob.data.as_ref(), data);

    let auto_blob = Blob::new(data.to_vec());
    assert_eq!(auto_blob.data.as_ref(), data);
    let auto_bytes = auto_blob.oid.as_bytes();
    let auto_reconstructed = Oid::from_slice(auto_bytes).unwrap();
    assert_eq!(auto_blob.oid, auto_reconstructed);
}

#[test]
fn test_storage_stats_structure() {
    let stats = StorageStats::default();
    assert_eq!(stats.total_blobs, 0);
    assert_eq!(stats.total_bytes, 0);
    assert_eq!(stats.used_bytes, 0);
    assert_eq!(stats.available_bytes, 0);
    assert_eq!(stats.io_utilization, 0.0);
}
