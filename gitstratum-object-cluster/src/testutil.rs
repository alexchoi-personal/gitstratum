use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

use async_trait::async_trait;
use gitstratum_core::{Blob, Oid};

use crate::error::{ObjectStoreError, Result};
use crate::store::{ObjectStorage, StorageStats};

pub struct MockObjectStorage {
    objects: RwLock<HashMap<Oid, Blob>>,
    get_fail: AtomicBool,
    put_fail: AtomicBool,
    delete_fail: AtomicBool,
    get_count: AtomicU64,
    put_count: AtomicU64,
    delete_count: AtomicU64,
    has_count: AtomicU64,
}

impl MockObjectStorage {
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            get_fail: AtomicBool::new(false),
            put_fail: AtomicBool::new(false),
            delete_fail: AtomicBool::new(false),
            get_count: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            has_count: AtomicU64::new(0),
        }
    }

    pub fn with_objects(objects: Vec<Blob>) -> Self {
        let storage = Self::new();
        {
            let mut map = storage.objects.write().unwrap();
            for blob in objects {
                map.insert(blob.oid, blob);
            }
        }
        storage
    }

    pub fn set_get_fail(&self, fail: bool) {
        self.get_fail.store(fail, Ordering::SeqCst);
    }

    pub fn set_put_fail(&self, fail: bool) {
        self.put_fail.store(fail, Ordering::SeqCst);
    }

    pub fn set_delete_fail(&self, fail: bool) {
        self.delete_fail.store(fail, Ordering::SeqCst);
    }

    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    pub fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    pub fn delete_count(&self) -> u64 {
        self.delete_count.load(Ordering::Relaxed)
    }

    pub fn has_count(&self) -> u64 {
        self.has_count.load(Ordering::Relaxed)
    }

    pub fn insert(&self, blob: Blob) {
        self.objects.write().unwrap().insert(blob.oid, blob);
    }

    pub fn len(&self) -> usize {
        self.objects.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.read().unwrap().is_empty()
    }

    pub fn clear(&self) {
        self.objects.write().unwrap().clear();
    }

    pub fn reset_counts(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.put_count.store(0, Ordering::Relaxed);
        self.delete_count.store(0, Ordering::Relaxed);
        self.has_count.store(0, Ordering::Relaxed);
    }
}

impl Default for MockObjectStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ObjectStorage for MockObjectStorage {
    async fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        if self.get_fail.load(Ordering::SeqCst) {
            return Err(ObjectStoreError::Internal("mock get failure".to_string()));
        }
        Ok(self.objects.read().unwrap().get(oid).cloned())
    }

    async fn put(&self, blob: &Blob) -> Result<()> {
        self.put_count.fetch_add(1, Ordering::Relaxed);
        if self.put_fail.load(Ordering::SeqCst) {
            return Err(ObjectStoreError::Internal("mock put failure".to_string()));
        }
        self.objects.write().unwrap().insert(blob.oid, blob.clone());
        Ok(())
    }

    async fn delete(&self, oid: &Oid) -> Result<bool> {
        self.delete_count.fetch_add(1, Ordering::Relaxed);
        if self.delete_fail.load(Ordering::SeqCst) {
            return Err(ObjectStoreError::Internal(
                "mock delete failure".to_string(),
            ));
        }
        Ok(self.objects.write().unwrap().remove(oid).is_some())
    }

    fn has(&self, oid: &Oid) -> bool {
        self.has_count.fetch_add(1, Ordering::Relaxed);
        self.objects.read().unwrap().contains_key(oid)
    }

    fn stats(&self) -> StorageStats {
        let objects = self.objects.read().unwrap();
        let total_bytes: u64 = objects.values().map(|b| b.data.len() as u64).sum();
        StorageStats {
            total_blobs: objects.len() as u64,
            total_bytes,
            used_bytes: total_bytes,
            available_bytes: 0,
            io_utilization: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_object_storage_new() {
        let storage = MockObjectStorage::new();
        assert!(storage.is_empty());
        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn test_mock_object_storage_default() {
        let storage = MockObjectStorage::default();
        assert!(storage.is_empty());
    }

    #[test]
    fn test_mock_object_storage_with_objects() {
        let blob1 = Blob::new(b"data1".to_vec());
        let blob2 = Blob::new(b"data2".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob1.clone(), blob2.clone()]);

        assert_eq!(storage.len(), 2);
        assert!(storage.has(&blob1.oid));
        assert!(storage.has(&blob2.oid));
    }

    #[tokio::test]
    async fn test_mock_object_storage_get() {
        let blob = Blob::new(b"test data".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob.clone()]);

        let result = storage.get(&blob.oid).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, blob.data);
        assert_eq!(storage.get_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_object_storage_get_not_found() {
        let storage = MockObjectStorage::new();
        let oid = Oid::hash(b"nonexistent");

        let result = storage.get(&oid).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_mock_object_storage_get_fail() {
        let blob = Blob::new(b"test data".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob.clone()]);
        storage.set_get_fail(true);

        let result = storage.get(&blob.oid).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_object_storage_put() {
        let storage = MockObjectStorage::new();
        let blob = Blob::new(b"test data".to_vec());

        storage.put(&blob).await.unwrap();

        assert!(storage.has(&blob.oid));
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.put_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_object_storage_put_fail() {
        let storage = MockObjectStorage::new();
        let blob = Blob::new(b"test data".to_vec());
        storage.set_put_fail(true);

        let result = storage.put(&blob).await;
        assert!(result.is_err());
        assert!(storage.is_empty());
    }

    #[tokio::test]
    async fn test_mock_object_storage_delete() {
        let blob = Blob::new(b"test data".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob.clone()]);

        let result = storage.delete(&blob.oid).await.unwrap();
        assert!(result);
        assert!(!storage.has(&blob.oid));
        assert_eq!(storage.delete_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_object_storage_delete_not_found() {
        let storage = MockObjectStorage::new();
        let oid = Oid::hash(b"nonexistent");

        let result = storage.delete(&oid).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_mock_object_storage_delete_fail() {
        let blob = Blob::new(b"test data".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob.clone()]);
        storage.set_delete_fail(true);

        let result = storage.delete(&blob.oid).await;
        assert!(result.is_err());
        assert!(storage.has(&blob.oid));
    }

    #[test]
    fn test_mock_object_storage_has() {
        let blob = Blob::new(b"test data".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob.clone()]);

        assert!(storage.has(&blob.oid));
        assert!(!storage.has(&Oid::hash(b"nonexistent")));
        assert_eq!(storage.has_count(), 2);
    }

    #[test]
    fn test_mock_object_storage_stats() {
        let blob1 = Blob::new(b"data1".to_vec());
        let blob2 = Blob::new(b"data2222".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob1, blob2]);

        let stats = storage.stats();
        assert_eq!(stats.total_blobs, 2);
        assert_eq!(stats.total_bytes, 13);
        assert_eq!(stats.used_bytes, 13);
        assert_eq!(stats.available_bytes, 0);
        assert_eq!(stats.io_utilization, 0.0);
    }

    #[test]
    fn test_mock_object_storage_insert() {
        let storage = MockObjectStorage::new();
        let blob = Blob::new(b"test data".to_vec());

        storage.insert(blob.clone());

        assert!(storage.has(&blob.oid));
        assert_eq!(storage.len(), 1);
    }

    #[test]
    fn test_mock_object_storage_clear() {
        let blob1 = Blob::new(b"data1".to_vec());
        let blob2 = Blob::new(b"data2".to_vec());
        let storage = MockObjectStorage::with_objects(vec![blob1, blob2]);

        assert_eq!(storage.len(), 2);
        storage.clear();
        assert!(storage.is_empty());
    }

    #[tokio::test]
    async fn test_mock_object_storage_reset_counts() {
        let storage = MockObjectStorage::new();
        let blob = Blob::new(b"test data".to_vec());

        storage.put(&blob).await.unwrap();
        storage.get(&blob.oid).await.unwrap();
        storage.has(&blob.oid);
        storage.delete(&blob.oid).await.unwrap();

        assert_eq!(storage.put_count(), 1);
        assert_eq!(storage.get_count(), 1);
        assert_eq!(storage.has_count(), 1);
        assert_eq!(storage.delete_count(), 1);

        storage.reset_counts();

        assert_eq!(storage.put_count(), 0);
        assert_eq!(storage.get_count(), 0);
        assert_eq!(storage.has_count(), 0);
        assert_eq!(storage.delete_count(), 0);
    }
}
