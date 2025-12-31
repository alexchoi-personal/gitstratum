use bytes::Bytes;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use gitstratum_core::{Blob, Oid};
use rocksdb::{Options, DB};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, instrument, trace};

use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    pub total_blobs: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub io_utilization: f32,
}

pub struct ObjectStore {
    db: Arc<DB>,
    blob_count: AtomicU64,
    total_bytes: AtomicU64,
}

impl ObjectStore {
    #[instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        opts.increase_parallelism(4);
        opts.set_max_background_jobs(4);
        opts.set_write_buffer_size(64 * 1024 * 1024);

        let db = DB::open(&opts, path)?;
        let db = Arc::new(db);

        let mut blob_count = 0u64;
        let mut total_bytes = 0u64;

        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            if let Ok((_, value)) = item {
                blob_count += 1;
                total_bytes += value.len() as u64;
            }
        }

        debug!(blob_count, total_bytes, "initialized object store");

        Ok(Self {
            db,
            blob_count: AtomicU64::new(blob_count),
            total_bytes: AtomicU64::new(total_bytes),
        })
    }

    #[instrument(skip(self), fields(oid = %oid))]
    pub fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
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
    pub fn put(&self, blob: &Blob) -> Result<()> {
        trace!("putting blob");

        let key = blob.oid.as_bytes();
        let compressed = self.compress(&blob.data)?;

        let existed = self.db.get(key)?.is_some();
        self.db.put(key, &compressed)?;

        if !existed {
            self.blob_count.fetch_add(1, Ordering::Relaxed);
            self.total_bytes
                .fetch_add(compressed.len() as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    #[instrument(skip(self), fields(oid = %oid))]
    pub fn delete(&self, oid: &Oid) -> Result<bool> {
        trace!("deleting blob");

        let key = oid.as_bytes();
        if let Some(existing) = self.db.get(key)? {
            self.db.delete(key)?;
            self.blob_count.fetch_sub(1, Ordering::Relaxed);
            self.total_bytes
                .fetch_sub(existing.len() as u64, Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self), fields(oid = %oid))]
    pub fn has(&self, oid: &Oid) -> bool {
        trace!("checking blob existence");

        let key = oid.as_bytes();
        self.db.get(key).map(|v| v.is_some()).unwrap_or(false)
    }

    pub fn stats(&self) -> StorageStats {
        StorageStats {
            total_blobs: self.blob_count.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            used_bytes: self.total_bytes.load(Ordering::Relaxed),
            available_bytes: 0,
            io_utilization: 0.0,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<(Oid, Blob)>> + '_ {
        self.db.iterator(rocksdb::IteratorMode::Start).map(|item| {
            let (key, value) = item?;
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
        let bytes = oid.as_bytes();
        u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .map_err(|e| ObjectStoreError::Compression(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| ObjectStoreError::Compression(e.to_string()))
    }

    fn decompress(&self, oid: &Oid, compressed: &[u8]) -> Result<Blob> {
        let mut decoder = ZlibDecoder::new(compressed);
        let mut data = Vec::new();
        decoder
            .read_to_end(&mut data)
            .map_err(|e| ObjectStoreError::Decompression(e.to_string()))?;
        Ok(Blob::with_oid(*oid, Bytes::from(data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (ObjectStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = ObjectStore::new(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[test]
    fn test_put_get() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"hello world".to_vec());
        store.put(&blob).unwrap();

        let retrieved = store.get(&blob.oid).unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), b"hello world");
    }

    #[test]
    fn test_get_nonexistent() {
        let (store, _dir) = create_test_store();

        let oid = Oid::hash(b"nonexistent");
        assert!(store.get(&oid).unwrap().is_none());
    }

    #[test]
    fn test_has() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"test data".to_vec());
        assert!(!store.has(&blob.oid));

        store.put(&blob).unwrap();
        assert!(store.has(&blob.oid));
    }

    #[test]
    fn test_delete() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"to be deleted".to_vec());
        store.put(&blob).unwrap();
        assert!(store.has(&blob.oid));

        let deleted = store.delete(&blob.oid).unwrap();
        assert!(deleted);
        assert!(!store.has(&blob.oid));
    }

    #[test]
    fn test_delete_nonexistent() {
        let (store, _dir) = create_test_store();

        let oid = Oid::hash(b"nonexistent");
        let deleted = store.delete(&oid).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_stats() {
        let (store, _dir) = create_test_store();

        let stats = store.stats();
        assert_eq!(stats.total_blobs, 0);

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());

        store.put(&blob1).unwrap();
        store.put(&blob2).unwrap();

        let stats = store.stats();
        assert_eq!(stats.total_blobs, 2);
        assert!(stats.total_bytes > 0);
    }

    #[test]
    fn test_iter() {
        let (store, _dir) = create_test_store();

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());
        let blob3 = Blob::new(b"blob 3".to_vec());

        store.put(&blob1).unwrap();
        store.put(&blob2).unwrap();
        store.put(&blob3).unwrap();

        let blobs: Vec<_> = store.iter().collect();
        assert_eq!(blobs.len(), 3);
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();

        let blob = Blob::new(b"persistent data".to_vec());
        let oid = blob.oid;

        {
            let store = ObjectStore::new(temp_dir.path()).unwrap();
            store.put(&blob).unwrap();
        }

        {
            let store = ObjectStore::new(temp_dir.path()).unwrap();
            assert!(store.has(&oid));
            let retrieved = store.get(&oid).unwrap().unwrap();
            assert_eq!(retrieved.data.as_ref(), b"persistent data");
        }
    }

    #[test]
    fn test_large_blob() {
        let (store, _dir) = create_test_store();

        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let blob = Blob::new(data.clone());

        store.put(&blob).unwrap();
        let retrieved = store.get(&blob.oid).unwrap().unwrap();
        assert_eq!(retrieved.data.as_ref(), data.as_slice());
    }

    #[test]
    fn test_overwrite() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"original".to_vec());
        store.put(&blob).unwrap();

        let stats_before = store.stats();
        store.put(&blob).unwrap();
        let stats_after = store.stats();

        assert_eq!(stats_before.total_blobs, stats_after.total_blobs);
    }

    #[test]
    fn test_iter_range() {
        let (store, _dir) = create_test_store();

        let blob1 = Blob::new(b"blob 1".to_vec());
        let blob2 = Blob::new(b"blob 2".to_vec());
        let blob3 = Blob::new(b"blob 3".to_vec());

        store.put(&blob1).unwrap();
        store.put(&blob2).unwrap();
        store.put(&blob3).unwrap();

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

        store.put(&blob1).unwrap();
        store.put(&blob2).unwrap();

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
}
