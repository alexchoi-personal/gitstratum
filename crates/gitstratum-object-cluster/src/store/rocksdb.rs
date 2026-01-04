use async_trait::async_trait;
use bytes::Bytes;
use gitstratum_core::{Blob, Oid};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, instrument, trace};

use super::compression::{CompressionConfig, Compressor};
use super::traits::ObjectStorage;
use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    pub total_blobs: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub io_utilization: f32,
}

pub struct RocksDbStore {
    db: Arc<DB>,
    blob_count: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
    compressor: Compressor,
}

impl RocksDbStore {
    #[instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_compression(path, CompressionConfig::default())
    }

    const META_BLOB_COUNT_KEY: &'static [u8] = b"\x00__blob_count__";
    const META_TOTAL_BYTES_KEY: &'static [u8] = b"\x00__total_bytes__";

    #[instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn with_compression(
        path: impl AsRef<Path>,
        compression_config: CompressionConfig,
    ) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        opts.increase_parallelism(4);
        opts.set_max_background_jobs(4);
        opts.set_write_buffer_size(64 * 1024 * 1024);

        let db = DB::open(&opts, path)?;
        let db = Arc::new(db);

        let blob_count = db
            .get(Self::META_BLOB_COUNT_KEY)?
            .and_then(|v| {
                if v.len() == 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&v);
                    Some(u64::from_le_bytes(bytes))
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let total_bytes = db
            .get(Self::META_TOTAL_BYTES_KEY)?
            .and_then(|v| {
                if v.len() == 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&v);
                    Some(u64::from_le_bytes(bytes))
                } else {
                    None
                }
            })
            .unwrap_or(0);

        debug!(blob_count, total_bytes, "initialized object store");

        Ok(Self {
            db,
            blob_count: Arc::new(AtomicU64::new(blob_count)),
            total_bytes: Arc::new(AtomicU64::new(total_bytes)),
            compressor: Compressor::new(compression_config),
        })
    }

    #[instrument(skip(self), fields(oid = %oid))]
    fn get_sync(&self, oid: &Oid) -> Result<Option<Blob>> {
        trace!("getting blob");

        let key = oid.as_bytes();
        match self.db.get(key)? {
            Some(compressed) => {
                let blob = self.decompress(oid, &compressed)?;
                Ok(Some(blob))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self, blob), fields(oid = %blob.oid))]
    fn put_sync(&self, blob: &Blob) -> Result<()> {
        trace!("putting blob");

        let key = blob.oid.as_bytes();
        let compressed = self.compress(&blob.data)?;

        let existed = self.db.get(key)?.is_some();
        self.db.put(key, &compressed)?;

        if !existed {
            let new_count = self.blob_count.fetch_add(1, Ordering::Relaxed) + 1;
            let new_bytes = self
                .total_bytes
                .fetch_add(compressed.len() as u64, Ordering::Relaxed)
                + compressed.len() as u64;
            self.db
                .put(Self::META_BLOB_COUNT_KEY, &new_count.to_le_bytes())?;
            self.db
                .put(Self::META_TOTAL_BYTES_KEY, &new_bytes.to_le_bytes())?;
        }

        Ok(())
    }

    #[instrument(skip(self), fields(oid = %oid))]
    fn delete_sync(&self, oid: &Oid) -> Result<bool> {
        trace!("deleting blob");

        let key = oid.as_bytes();
        if let Some(existing) = self.db.get(key)? {
            self.db.delete(key)?;
            let new_count = self.blob_count.fetch_sub(1, Ordering::Relaxed) - 1;
            let new_bytes = self
                .total_bytes
                .fetch_sub(existing.len() as u64, Ordering::Relaxed)
                - existing.len() as u64;
            self.db
                .put(Self::META_BLOB_COUNT_KEY, &new_count.to_le_bytes())?;
            self.db
                .put(Self::META_TOTAL_BYTES_KEY, &new_bytes.to_le_bytes())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self), fields(oid = %oid))]
    fn has_sync(&self, oid: &Oid) -> bool {
        trace!("checking blob existence");

        let key = oid.as_bytes();
        self.db.get(key).map(|v| v.is_some()).unwrap_or(false)
    }

    fn stats_sync(&self) -> StorageStats {
        StorageStats {
            total_blobs: self.blob_count.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            used_bytes: self.total_bytes.load(Ordering::Relaxed),
            available_bytes: 0,
            io_utilization: 0.0,
        }
    }

    fn is_metadata_key(key: &[u8]) -> bool {
        key.first() == Some(&0x00)
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<(Oid, Blob)>> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|item| {
                let (key, value) = item.ok()?;
                if Self::is_metadata_key(&key) {
                    return None;
                }
                Some((key, value))
            })
            .map(|(key, value)| {
                let oid = Oid::from_slice(&key)
                    .map_err(|e| ObjectStoreError::InvalidOid(e.to_string()))?;
                let blob = self.decompress(&oid, &value)?;
                Ok((oid, blob))
            })
    }

    pub fn iter_range(
        &self,
        from_position: u64,
        to_position: u64,
    ) -> impl Iterator<Item = Result<(Oid, Blob)>> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(move |item| {
                let (key, value) = item.ok()?;
                if Self::is_metadata_key(&key) {
                    return None;
                }
                let oid = Oid::from_slice(&key).ok()?;
                let position = self.oid_position(&oid);
                if position >= from_position && position < to_position {
                    Some((oid, value))
                } else {
                    None
                }
            })
            .map(|(oid, value)| {
                let blob = self.decompress(&oid, &value)?;
                Ok((oid, blob))
            })
    }

    fn oid_position(&self, oid: &Oid) -> u64 {
        u64::from_le_bytes(oid.as_bytes()[..8].try_into().unwrap())
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
impl ObjectStorage for RocksDbStore {
    async fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
        let db = self.db.clone();
        let compressor = self.compressor.clone();
        let oid = *oid;
        tokio::task::spawn_blocking(move || {
            let key = oid.as_bytes();
            match db.get(key) {
                Ok(Some(compressed)) => {
                    let data = compressor.decompress(&compressed)?;
                    Ok(Some(Blob::with_oid(oid, Bytes::from(data))))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await
        .map_err(|e| ObjectStoreError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
    }

    async fn put(&self, blob: &Blob) -> Result<()> {
        let db = self.db.clone();
        let compressor = self.compressor.clone();
        let blob_count = self.blob_count.clone();
        let total_bytes = self.total_bytes.clone();
        let oid = blob.oid;
        let data = blob.data.clone();
        tokio::task::spawn_blocking(move || {
            let key = oid.as_bytes();
            let compressed = compressor.compress(&data)?;
            let existed = db.get(key)?.is_some();
            db.put(key, &compressed)?;
            if !existed {
                let new_count = blob_count.fetch_add(1, Ordering::Relaxed) + 1;
                let new_bytes = total_bytes.fetch_add(compressed.len() as u64, Ordering::Relaxed)
                    + compressed.len() as u64;
                db.put(RocksDbStore::META_BLOB_COUNT_KEY, &new_count.to_le_bytes())?;
                db.put(RocksDbStore::META_TOTAL_BYTES_KEY, &new_bytes.to_le_bytes())?;
            }
            Ok(())
        })
        .await
        .map_err(|e| ObjectStoreError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
    }

    async fn delete(&self, oid: &Oid) -> Result<bool> {
        let db = self.db.clone();
        let blob_count = self.blob_count.clone();
        let total_bytes = self.total_bytes.clone();
        let oid = *oid;
        tokio::task::spawn_blocking(move || {
            let key = oid.as_bytes();
            if let Some(existing) = db.get(key)? {
                db.delete(key)?;
                let new_count = blob_count.fetch_sub(1, Ordering::Relaxed) - 1;
                let new_bytes = total_bytes.fetch_sub(existing.len() as u64, Ordering::Relaxed)
                    - existing.len() as u64;
                db.put(RocksDbStore::META_BLOB_COUNT_KEY, &new_count.to_le_bytes())?;
                db.put(RocksDbStore::META_TOTAL_BYTES_KEY, &new_bytes.to_le_bytes())?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .await
        .map_err(|e| ObjectStoreError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
    }

    fn has(&self, oid: &Oid) -> bool {
        self.has_sync(oid)
    }

    fn stats(&self) -> StorageStats {
        self.stats_sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (RocksDbStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksDbStore::new(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[test]
    fn test_put_get() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"hello world".to_vec());
        store.put_sync(&blob).unwrap();

        let retrieved = store.get_sync(&blob.oid).unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), b"hello world");
    }

    #[test]
    fn test_get_nonexistent() {
        let (store, _dir) = create_test_store();

        let oid = Oid::hash(b"nonexistent");
        assert!(store.get_sync(&oid).unwrap().is_none());
    }

    #[test]
    fn test_has() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"test data".to_vec());
        assert!(!store.has_sync(&blob.oid));

        store.put_sync(&blob).unwrap();
        assert!(store.has_sync(&blob.oid));
    }

    #[test]
    fn test_delete() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"to be deleted".to_vec());
        store.put_sync(&blob).unwrap();
        assert!(store.has_sync(&blob.oid));

        let deleted = store.delete_sync(&blob.oid).unwrap();
        assert!(deleted);
        assert!(!store.has_sync(&blob.oid));
    }

    #[test]
    fn test_delete_nonexistent() {
        let (store, _dir) = create_test_store();

        let oid = Oid::hash(b"nonexistent");
        let deleted = store.delete_sync(&oid).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_stats() {
        let (store, _dir) = create_test_store();

        let stats = store.stats_sync();
        assert_eq!(stats.total_blobs, 0);

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());

        store.put_sync(&blob1).unwrap();
        store.put_sync(&blob2).unwrap();

        let stats = store.stats_sync();
        assert_eq!(stats.total_blobs, 2);
        assert!(stats.total_bytes > 0);
    }

    #[test]
    fn test_iter() {
        let (store, _dir) = create_test_store();

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());
        let blob3 = Blob::new(b"blob 3".to_vec());

        store.put_sync(&blob1).unwrap();
        store.put_sync(&blob2).unwrap();
        store.put_sync(&blob3).unwrap();

        let blobs: Vec<_> = store.iter().collect();
        assert_eq!(blobs.len(), 3);
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();

        let blob = Blob::new(b"persistent data".to_vec());
        let oid = blob.oid;

        {
            let store = RocksDbStore::new(temp_dir.path()).unwrap();
            store.put_sync(&blob).unwrap();
        }

        {
            let store = RocksDbStore::new(temp_dir.path()).unwrap();
            assert!(store.has_sync(&oid));
            let retrieved = store.get_sync(&oid).unwrap().unwrap();
            assert_eq!(retrieved.data.as_ref(), b"persistent data");
        }
    }

    #[test]
    fn test_large_blob() {
        let (store, _dir) = create_test_store();

        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let blob = Blob::new(data.clone());

        store.put_sync(&blob).unwrap();
        let retrieved = store.get_sync(&blob.oid).unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), data.as_slice());
    }

    #[test]
    fn test_overwrite() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"original".to_vec());
        store.put_sync(&blob).unwrap();

        let stats_before = store.stats_sync();
        store.put_sync(&blob).unwrap();
        let stats_after = store.stats_sync();

        assert_eq!(stats_before.total_blobs, stats_after.total_blobs);
    }

    #[test]
    fn test_iter_range() {
        let (store, _dir) = create_test_store();

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());
        let blob3 = Blob::new(b"blob 3".to_vec());

        store.put_sync(&blob1).unwrap();
        store.put_sync(&blob2).unwrap();
        store.put_sync(&blob3).unwrap();

        let blobs: Vec<_> = store.iter_range(0, u64::MAX).collect();
        assert_eq!(blobs.len(), 3);

        for result in blobs {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_iter_range_partial() {
        let (store, _dir) = create_test_store();

        let blob1 = Blob::new(b"data 1".to_vec());
        let blob2 = Blob::new(b"data 2".to_vec());

        store.put_sync(&blob1).unwrap();
        store.put_sync(&blob2).unwrap();

        let mid = u64::MAX / 2;
        let blobs_first_half: Vec<_> = store.iter_range(0, mid).collect();
        let blobs_second_half: Vec<_> = store.iter_range(mid, u64::MAX).collect();

        let total = blobs_first_half.len() + blobs_second_half.len();
        assert_eq!(total, 2);
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

    #[test]
    fn test_with_compression() {
        use super::super::compression::CompressionType;

        let temp_dir = TempDir::new().unwrap();
        let config = CompressionConfig {
            compression_type: CompressionType::Zlib,
            level: 9,
        };
        let store = RocksDbStore::with_compression(temp_dir.path(), config).unwrap();

        let blob = Blob::new(b"test with custom compression".to_vec());
        store.put_sync(&blob).unwrap();

        let retrieved = store.get_sync(&blob.oid).unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), b"test with custom compression");
    }
}
