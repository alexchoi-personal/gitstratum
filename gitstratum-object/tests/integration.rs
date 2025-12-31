use gitstratum_core::{Blob, Oid};
use gitstratum_object::{ObjectStore, StorageStats};
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_store() -> (Arc<ObjectStore>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(ObjectStore::new(temp_dir.path()).unwrap());
    (store, temp_dir)
}

#[test]
fn test_store_basic_operations() {
    let (store, _dir) = create_test_store();

    let blob = Blob::new(b"test content".to_vec());
    let oid = blob.oid;

    assert!(!store.has(&oid));
    store.put(&blob).unwrap();
    assert!(store.has(&oid));

    let retrieved = store.get(&oid).unwrap().unwrap();
    assert_eq!(retrieved.data.as_ref(), b"test content");

    store.delete(&oid).unwrap();
    assert!(!store.has(&oid));
}

#[test]
fn test_store_multiple_blobs() {
    let (store, _dir) = create_test_store();

    let blobs: Vec<Blob> = (0..10)
        .map(|i| Blob::new(format!("blob content {}", i).into_bytes()))
        .collect();

    for blob in &blobs {
        store.put(blob).unwrap();
    }

    let stats = store.stats();
    assert_eq!(stats.total_blobs, 10);

    for blob in &blobs {
        assert!(store.has(&blob.oid));
        let retrieved = store.get(&blob.oid).unwrap().unwrap();
        assert_eq!(retrieved.data, blob.data);
    }
}

#[test]
fn test_store_large_blob() {
    let (store, _dir) = create_test_store();

    let size = 10 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let blob = Blob::new(data.clone());

    store.put(&blob).unwrap();

    let retrieved = store.get(&blob.oid).unwrap().unwrap();
    assert_eq!(retrieved.data.len(), size);
    assert_eq!(retrieved.data.as_ref(), data.as_slice());
}

#[test]
fn test_store_compression() {
    let (store, _dir) = create_test_store();

    let data = "a".repeat(1000).into_bytes();
    let blob = Blob::new(data);

    store.put(&blob).unwrap();

    let stats = store.stats();
    assert!(stats.total_bytes < 1000);
}

#[test]
fn test_store_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    let blob = Blob::new(b"persistent data".to_vec());
    let oid = blob.oid;

    {
        let store = ObjectStore::new(&path).unwrap();
        store.put(&blob).unwrap();
    }

    {
        let store = ObjectStore::new(&path).unwrap();
        assert!(store.has(&oid));
        let stats = store.stats();
        assert_eq!(stats.total_blobs, 1);
    }
}

#[test]
fn test_store_iteration() {
    let (store, _dir) = create_test_store();

    let blobs: Vec<Blob> = (0..5)
        .map(|i| Blob::new(format!("blob {}", i).into_bytes()))
        .collect();

    for blob in &blobs {
        store.put(blob).unwrap();
    }

    let count = store.iter().count();
    assert_eq!(count, 5);

    let oids: std::collections::HashSet<Oid> = blobs.iter().map(|b| b.oid).collect();
    for result in store.iter() {
        let (oid, _) = result.unwrap();
        assert!(oids.contains(&oid));
    }
}

#[test]
fn test_store_binary_data() {
    let (store, _dir) = create_test_store();

    let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
    let blob = Blob::new(data.clone());

    store.put(&blob).unwrap();

    let retrieved = store.get(&blob.oid).unwrap().unwrap();
    assert_eq!(retrieved.data.as_ref(), data.as_slice());
}

#[test]
fn test_store_empty_blob() {
    let (store, _dir) = create_test_store();

    let blob = Blob::new(Vec::new());
    store.put(&blob).unwrap();

    let retrieved = store.get(&blob.oid).unwrap().unwrap();
    assert!(retrieved.data.is_empty());
}

#[test]
fn test_store_duplicate_put() {
    let (store, _dir) = create_test_store();

    let blob = Blob::new(b"duplicate".to_vec());

    store.put(&blob).unwrap();
    store.put(&blob).unwrap();

    let stats = store.stats();
    assert_eq!(stats.total_blobs, 1);
}

#[test]
fn test_store_delete_nonexistent() {
    let (store, _dir) = create_test_store();

    let oid = Oid::hash(b"nonexistent");
    let deleted = store.delete(&oid).unwrap();
    assert!(!deleted);
}

#[test]
fn test_store_get_nonexistent() {
    let (store, _dir) = create_test_store();

    let oid = Oid::hash(b"nonexistent");
    let result = store.get(&oid).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_store_stats_accuracy() {
    let (store, _dir) = create_test_store();

    let initial_stats = store.stats();
    assert_eq!(initial_stats.total_blobs, 0);

    let blob1 = Blob::new(b"first blob".to_vec());
    store.put(&blob1).unwrap();

    let stats_after_one = store.stats();
    assert_eq!(stats_after_one.total_blobs, 1);

    let blob2 = Blob::new(b"second blob".to_vec());
    store.put(&blob2).unwrap();

    let stats_after_two = store.stats();
    assert_eq!(stats_after_two.total_blobs, 2);

    store.delete(&blob1.oid).unwrap();

    let stats_after_delete = store.stats();
    assert_eq!(stats_after_delete.total_blobs, 1);
}

#[tokio::test]
async fn test_server_service() {
    use gitstratum_object::ObjectServiceImpl;
    use gitstratum_proto::object_service_server::ObjectService;
    use gitstratum_proto::{GetBlobRequest, GetStatsRequest, HasBlobRequest, PutBlobRequest};
    use tonic::Request;

    let (store, _dir) = create_test_store();
    let server = ObjectServiceImpl::new(store);

    let blob = Blob::new(b"server test".to_vec());
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
    assert_eq!(inner.blob.unwrap().data, b"server test");

    let stats_request = Request::new(GetStatsRequest {});
    let stats_response = server.get_stats(stats_request).await.unwrap();
    assert_eq!(stats_response.into_inner().total_blobs, 1);
}

#[tokio::test]
async fn test_server_delete() {
    use gitstratum_object::ObjectServiceImpl;
    use gitstratum_proto::object_service_server::ObjectService;
    use gitstratum_proto::{DeleteBlobRequest, HasBlobRequest, PutBlobRequest};
    use tonic::Request;

    let (store, _dir) = create_test_store();
    let server = ObjectServiceImpl::new(store);

    let blob = Blob::new(b"to delete".to_vec());
    let proto_oid = gitstratum_proto::Oid {
        bytes: blob.oid.as_bytes().to_vec(),
    };
    let proto_blob = gitstratum_proto::Blob {
        oid: Some(proto_oid.clone()),
        data: blob.data.to_vec(),
        compressed: false,
    };

    let put_request = Request::new(PutBlobRequest {
        blob: Some(proto_blob),
    });
    server.put_blob(put_request).await.unwrap();

    let delete_request = Request::new(DeleteBlobRequest {
        oid: Some(proto_oid.clone()),
    });
    let delete_response = server.delete_blob(delete_request).await.unwrap();
    assert!(delete_response.into_inner().success);

    let has_request = Request::new(HasBlobRequest {
        oid: Some(proto_oid),
    });
    let has_response = server.has_blob(has_request).await.unwrap();
    assert!(!has_response.into_inner().exists);
}

#[tokio::test]
async fn test_server_get_nonexistent() {
    use gitstratum_object::ObjectServiceImpl;
    use gitstratum_proto::object_service_server::ObjectService;
    use gitstratum_proto::GetBlobRequest;
    use tonic::Request;

    let (store, _dir) = create_test_store();
    let server = ObjectServiceImpl::new(store);

    let oid = Oid::hash(b"nonexistent");
    let proto_oid = gitstratum_proto::Oid {
        bytes: oid.as_bytes().to_vec(),
    };

    let get_request = Request::new(GetBlobRequest {
        oid: Some(proto_oid),
    });
    let get_response = server.get_blob(get_request).await.unwrap();
    let inner = get_response.into_inner();
    assert!(!inner.found);
    assert!(inner.blob.is_none());
}

#[test]
fn test_oid_from_slice() {
    let data = b"test data for oid";
    let oid = Oid::hash(data);
    let bytes = oid.as_bytes();

    let reconstructed = Oid::from_slice(bytes).unwrap();
    assert_eq!(oid, reconstructed);
}

#[test]
fn test_blob_with_oid() {
    let data = b"custom oid blob";
    let oid = Oid::hash(data);
    let blob = Blob::with_oid(oid, data.to_vec());

    assert_eq!(blob.oid, oid);
    assert_eq!(blob.data.as_ref(), data);
}

#[test]
fn test_storage_stats_default() {
    let stats = StorageStats::default();
    assert_eq!(stats.total_blobs, 0);
    assert_eq!(stats.total_bytes, 0);
    assert_eq!(stats.used_bytes, 0);
    assert_eq!(stats.available_bytes, 0);
    assert_eq!(stats.io_utilization, 0.0);
}
