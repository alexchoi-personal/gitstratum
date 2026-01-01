use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use gitstratum_core::{Blob, Oid};
use gitstratum_storage::{BucketStore, BucketStoreConfig};
use tokio_stream::Stream;

use super::compression::{CompressionConfig, Compressor};
use super::rocksdb::StorageStats;
use super::traits::ObjectStorage;
use crate::error::Result;

pub struct BucketObjectStore {
    store: BucketStore,
    compressor: Compressor,
}

impl BucketObjectStore {
    pub async fn new(config: BucketStoreConfig) -> Result<Self> {
        Self::with_compression(config, CompressionConfig::default()).await
    }

    pub async fn with_compression(
        config: BucketStoreConfig,
        compression_config: CompressionConfig,
    ) -> Result<Self> {
        let store = BucketStore::open(config).await?;
        Ok(Self {
            store,
            compressor: Compressor::new(compression_config),
        })
    }

    pub fn iter(&self) -> BucketObjectIterator {
        BucketObjectIterator {
            inner: self.store.iter(),
            compressor: self.compressor.clone(),
        }
    }

    pub fn iter_by_position(&self, from: u64, to: u64) -> BucketPositionObjectIterator {
        BucketPositionObjectIterator {
            inner: self.store.iter_by_position(from, to),
            compressor: self.compressor.clone(),
        }
    }

    pub fn iter_range(&self, from: u64, to: u64) -> BucketPositionObjectIterator {
        self.iter_by_position(from, to)
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.compressor.compress(data)
    }

    fn decompress(&self, oid: &Oid, compressed: &[u8]) -> Result<Blob> {
        let data = self.compressor.decompress(compressed)?;
        Ok(Blob::with_oid(*oid, Bytes::from(data)))
    }
}

#[async_trait]
impl ObjectStorage for BucketObjectStore {
    async fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
        match self.store.get(oid).await? {
            Some(compressed) => {
                let blob = self.decompress(oid, &compressed)?;
                Ok(Some(blob))
            }
            None => Ok(None),
        }
    }

    async fn put(&self, blob: &Blob) -> Result<()> {
        let compressed = self.compress(&blob.data)?;
        self.store.put(blob.oid, Bytes::from(compressed)).await?;
        Ok(())
    }

    async fn delete(&self, oid: &Oid) -> Result<bool> {
        Ok(self.store.delete(oid).await?)
    }

    fn has(&self, oid: &Oid) -> bool {
        self.store.contains(oid)
    }

    fn stats(&self) -> StorageStats {
        let bucket_stats = self.store.stats();
        StorageStats {
            total_blobs: bucket_stats.entries,
            total_bytes: bucket_stats.total_bytes,
            used_bytes: bucket_stats.total_bytes - bucket_stats.dead_bytes,
            available_bytes: 0,
            io_utilization: 0.0,
        }
    }
}

pub struct BucketObjectIterator {
    inner: gitstratum_storage::BucketStoreIterator,
    compressor: Compressor,
}

impl Stream for BucketObjectIterator {
    type Item = Result<(Oid, Blob)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok((oid, compressed)))) => {
                let result = self
                    .compressor
                    .decompress(&compressed)
                    .map(|data| (oid, Blob::with_oid(oid, Bytes::from(data))));
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct BucketPositionObjectIterator {
    inner: gitstratum_storage::BucketStorePositionIterator,
    compressor: Compressor,
}

impl Stream for BucketPositionObjectIterator {
    type Item = Result<(Oid, Blob)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok((oid, compressed)))) => {
                let result = self
                    .compressor
                    .decompress(&compressed)
                    .map(|data| (oid, Blob::with_oid(oid, Bytes::from(data))));
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    fn test_config(dir: &std::path::Path) -> BucketStoreConfig {
        BucketStoreConfig {
            data_dir: dir.to_path_buf(),
            max_data_file_size: 1024 * 1024,
            bucket_count: 64,
            sync_writes: true,
            bucket_cache_size: 16,
            io_queue_depth: 4,
            io_queue_count: 1,
            compaction: gitstratum_storage::config::CompactionConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_put_get() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob = Blob::new(b"hello world".to_vec());
        store.put(&blob).await.unwrap();

        let retrieved = store.get(&blob.oid).await.unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let oid = Oid::hash(b"nonexistent");
        assert!(store.get(&oid).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_has() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob = Blob::new(b"test data".to_vec());
        assert!(!store.has(&blob.oid));

        store.put(&blob).await.unwrap();
        assert!(store.has(&blob.oid));
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob = Blob::new(b"to be deleted".to_vec());
        store.put(&blob).await.unwrap();
        assert!(store.has(&blob.oid));

        let deleted = store.delete(&blob.oid).await.unwrap();
        assert!(deleted);
        assert!(!store.has(&blob.oid));
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let oid = Oid::hash(b"nonexistent");
        let deleted = store.delete(&oid).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_stats() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let stats = store.stats();
        assert_eq!(stats.total_blobs, 0);

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());

        store.put(&blob1).await.unwrap();
        store.put(&blob2).await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.total_blobs, 2);
        assert!(stats.total_bytes > 0);
    }

    #[tokio::test]
    async fn test_iter() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());
        let blob3 = Blob::new(b"blob 3".to_vec());

        store.put(&blob1).await.unwrap();
        store.put(&blob2).await.unwrap();
        store.put(&blob3).await.unwrap();

        let blobs: Vec<_> = store.iter().collect::<Vec<_>>().await;
        assert_eq!(blobs.len(), 3);
    }

    #[tokio::test]
    async fn test_iter_by_position() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());

        store.put(&blob1).await.unwrap();
        store.put(&blob2).await.unwrap();

        let blobs: Vec<_> = store
            .iter_by_position(0, u64::MAX)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(blobs.len(), 2);

        for result in blobs {
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_large_blob() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let blob = Blob::new(data.clone());

        store.put(&blob).await.unwrap();
        let retrieved = store.get(&blob.oid).await.unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn test_with_compression() {
        use super::super::compression::CompressionType;

        let tmp = TempDir::new().unwrap();
        let config = CompressionConfig {
            compression_type: CompressionType::Zlib,
            level: 9,
        };
        let store = BucketObjectStore::with_compression(test_config(tmp.path()), config)
            .await
            .unwrap();

        let blob = Blob::new(b"test with custom compression".to_vec());
        store.put(&blob).await.unwrap();

        let retrieved = store.get(&blob.oid).await.unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), b"test with custom compression");
    }

    #[tokio::test]
    async fn test_overwrite() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob = Blob::new(b"original".to_vec());
        store.put(&blob).await.unwrap();

        let stats_before = store.stats();
        store.put(&blob).await.unwrap();
        let stats_after = store.stats();

        assert_eq!(stats_before.total_blobs, stats_after.total_blobs);
    }

    #[tokio::test]
    async fn test_storage_stats_values() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let stats = store.stats();
        assert_eq!(stats.total_blobs, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.available_bytes, 0);
        assert_eq!(stats.io_utilization, 0.0);
    }

    #[tokio::test]
    async fn test_iter_empty_store() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blobs: Vec<_> = store.iter().collect::<Vec<_>>().await;
        assert!(blobs.is_empty());
    }

    #[tokio::test]
    async fn test_iter_by_position_empty_store() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blobs: Vec<_> = store
            .iter_by_position(0, u64::MAX)
            .collect::<Vec<_>>()
            .await;
        assert!(blobs.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_puts_same_oid() {
        let tmp = TempDir::new().unwrap();
        let store = BucketObjectStore::new(test_config(tmp.path()))
            .await
            .unwrap();

        let blob = Blob::new(b"data".to_vec());

        store.put(&blob).await.unwrap();
        store.put(&blob).await.unwrap();
        store.put(&blob).await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.total_blobs, 1);

        let retrieved = store.get(&blob.oid).await.unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), b"data");
    }
}
