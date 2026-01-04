use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use gitstratum_core::Oid;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_stream::Stream;

use crate::bucket::{BucketCache, BucketIndex, CompactEntry, DiskBucket, EntryFlags};
use crate::config::BucketStoreConfig;
use crate::error::{BucketStoreError, Result};
use crate::file::{BucketFile, BucketIo, DataFile, FileManager};
use crate::io::{AsyncMultiQueueIo, IoQueueConfig};
use crate::record::DataRecord;

pub struct BucketStoreInner {
    bucket_io: Arc<BucketIo>,
    active_file: RwLock<Arc<DataFile>>,
}

pub struct BucketStore {
    config: BucketStoreConfig,
    bucket_index: Arc<BucketIndex>,
    bucket_cache: Arc<BucketCache>,
    inner: Arc<BucketStoreInner>,
    bucket_file_for_iter: RwLock<BucketFile>,
    file_manager: Arc<FileManager>,
    io: Arc<AsyncMultiQueueIo>,
    reaper_handles: RwLock<Vec<JoinHandle<()>>>,
    closed: AtomicBool,
    shutdown_notify: Arc<Notify>,
    dead_bytes: AtomicU64,
    total_bytes: AtomicU64,
}

impl BucketStore {
    pub async fn open(config: BucketStoreConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let io_config = IoQueueConfig {
            num_queues: config.io_queue_count,
            queue_depth: config.io_queue_depth,
        };
        let io = Arc::new(AsyncMultiQueueIo::new(io_config)?);
        let reaper_handles = io.start_reapers();

        let file_manager = Arc::new(FileManager::new(&config.data_dir, io.clone())?);

        let bucket_file_path = file_manager.bucket_file_path();

        let bucket_io = if bucket_file_path.exists() {
            Arc::new(BucketIo::open(&bucket_file_path, io.clone())?)
        } else {
            Arc::new(BucketIo::create(
                &bucket_file_path,
                config.bucket_count,
                io.clone(),
            )?)
        };

        let bucket_file_for_iter = if bucket_file_path.exists() {
            BucketFile::open(&bucket_file_path)?
        } else {
            BucketFile::create(&bucket_file_path, config.bucket_count)?
        };

        let bucket_index = Arc::new(BucketIndex::new(config.bucket_count));
        let bucket_cache = Arc::new(BucketCache::new(config.bucket_cache_size));

        let active_file = Arc::new(file_manager.create_data_file()?);

        let inner = Arc::new(BucketStoreInner {
            bucket_io,
            active_file: RwLock::new(active_file),
        });

        Ok(Self {
            config,
            bucket_index,
            bucket_cache,
            inner,
            bucket_file_for_iter: RwLock::new(bucket_file_for_iter),
            file_manager,
            io,
            reaper_handles: RwLock::new(reaper_handles),
            closed: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            dead_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        })
    }

    pub async fn get(&self, oid: &Oid) -> Result<Option<Bytes>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BucketStoreError::StoreClosed);
        }

        let bucket_id = self.bucket_index.bucket_id(oid);

        if let Some(bucket) = self.bucket_cache.get(bucket_id) {
            if let Some(entry) = bucket.find_entry(oid) {
                if entry.is_deleted() {
                    return Ok(None);
                }
                return self.read_value(entry).await.map(Some);
            }
            return Ok(None);
        }

        let bucket = self.inner.bucket_io.read_bucket(bucket_id).await?;

        self.bucket_cache.put(bucket_id, bucket.clone());

        if let Some(entry) = bucket.find_entry(oid) {
            if entry.is_deleted() {
                return Ok(None);
            }
            return self.read_value(entry).await.map(Some);
        }

        Ok(None)
    }

    pub async fn put(&self, oid: Oid, value: Bytes) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BucketStoreError::StoreClosed);
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let record = DataRecord::new(oid, value.clone(), timestamp)?;
        let record_size = record.record_size();

        let needs_rotation = {
            let active = self.inner.active_file.read();
            active.size() + record_size as u64 > self.config.max_data_file_size
        };
        if needs_rotation {
            self.rotate_data_file().await?;
        }

        let (file_id, offset) = {
            let active = self.inner.active_file.read().clone();
            let offset = active.append(&record).await?;
            (active.file_id(), offset)
        };

        let entry = CompactEntry::new(
            &oid,
            file_id,
            offset,
            record_size as u32,
            EntryFlags::NONE.bits(),
        )?;

        let bucket_id = self.bucket_index.bucket_id(&oid);
        self.bucket_cache.invalidate(bucket_id);

        let mut bucket = self.inner.bucket_io.read_bucket(bucket_id).await?;

        let mut found = false;
        for i in 0..bucket.header.count as usize {
            if bucket.entries[i].matches(&oid) {
                self.dead_bytes
                    .fetch_add(bucket.entries[i].size() as u64, Ordering::Relaxed);
                bucket.entries[i] = entry;
                found = true;
                break;
            }
        }

        if !found {
            bucket.insert(entry)?;
            self.bucket_index.increment_entry_count();
        }

        self.inner
            .bucket_io
            .write_bucket(bucket_id, &bucket)
            .await?;

        self.total_bytes
            .fetch_add(record_size as u64, Ordering::Relaxed);

        Ok(())
    }

    pub async fn delete(&self, oid: &Oid) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BucketStoreError::StoreClosed);
        }

        let bucket_id = self.bucket_index.bucket_id(oid);
        self.bucket_cache.invalidate(bucket_id);

        let mut bucket = self.inner.bucket_io.read_bucket(bucket_id).await?;

        for i in 0..bucket.header.count as usize {
            if bucket.entries[i].matches(oid) {
                self.dead_bytes
                    .fetch_add(bucket.entries[i].size() as u64, Ordering::Relaxed);

                let last_idx = bucket.header.count as usize - 1;
                if i != last_idx {
                    bucket.entries[i] = bucket.entries[last_idx];
                }
                bucket.entries[last_idx] = CompactEntry::EMPTY;
                bucket.header.count -= 1;

                self.inner
                    .bucket_io
                    .write_bucket(bucket_id, &bucket)
                    .await?;
                self.bucket_index.decrement_entry_count();

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn mark_deleted(&self, oid: &Oid) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BucketStoreError::StoreClosed);
        }

        let bucket_id = self.bucket_index.bucket_id(oid);
        self.bucket_cache.invalidate(bucket_id);

        let mut bucket = self.inner.bucket_io.read_bucket(bucket_id).await?;

        for i in 0..bucket.header.count as usize {
            if bucket.entries[i].matches(oid) {
                if bucket.entries[i].is_deleted() {
                    return Ok(false);
                }

                bucket.entries[i].flags |= EntryFlags::DELETED.bits();

                self.dead_bytes
                    .fetch_add(bucket.entries[i].size() as u64, Ordering::Relaxed);

                self.inner
                    .bucket_io
                    .write_bucket(bucket_id, &bucket)
                    .await?;

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }

        let bucket_id = self.bucket_index.bucket_id(oid);

        if let Some(bucket) = self.bucket_cache.get(bucket_id) {
            if let Some(entry) = bucket.find_entry(oid) {
                return !entry.is_deleted();
            }
            return false;
        }

        if let Ok(bucket) = self.bucket_file_for_iter.read().read_bucket_at(bucket_id) {
            self.bucket_cache.put(bucket_id, bucket.clone());
            if let Some(entry) = bucket.find_entry(oid) {
                return !entry.is_deleted();
            }
        }

        false
    }

    pub fn len(&self) -> u64 {
        self.bucket_index.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stats(&self) -> BucketStoreStats {
        BucketStoreStats {
            entries: self.bucket_index.entry_count(),
            dead_bytes: self.dead_bytes.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            bucket_cache_hit_rate: self.bucket_cache.hit_rate(),
            data_files: self.file_manager.file_count(),
            memory_usage: self.bucket_index.memory_usage(),
        }
    }

    pub fn bucket_count(&self) -> u32 {
        self.config.bucket_count
    }

    pub fn bucket_index(&self) -> &Arc<BucketIndex> {
        &self.bucket_index
    }

    pub fn bucket_cache(&self) -> &Arc<BucketCache> {
        &self.bucket_cache
    }

    pub fn file_manager(&self) -> &Arc<FileManager> {
        &self.file_manager
    }

    pub async fn read_bucket(&self, bucket_id: u32) -> Result<DiskBucket> {
        self.inner.bucket_io.read_bucket(bucket_id).await
    }

    pub async fn write_bucket(&self, bucket_id: u32, bucket: &DiskBucket) -> Result<()> {
        self.bucket_cache.invalidate(bucket_id);
        self.inner.bucket_io.write_bucket(bucket_id, bucket).await
    }

    pub fn reset_dead_bytes(&self) {
        self.dead_bytes.store(0, Ordering::Relaxed);
    }

    pub fn subtract_total_bytes(&self, amount: u64) {
        self.total_bytes.fetch_sub(amount, Ordering::Relaxed);
    }

    pub async fn sync(&self) -> Result<()> {
        let active_file = self.inner.active_file.read().clone();
        active_file.sync().await?;
        self.inner.bucket_io.sync().await?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();

        self.sync().await?;

        self.io.shutdown();
        let handles = std::mem::take(&mut *self.reaper_handles.write());
        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }

    async fn read_value(&self, entry: &CompactEntry) -> Result<Bytes> {
        let file_id = entry.file_id;
        let offset = entry.offset();
        let size = entry.size() as usize;

        let data_file = self.file_manager.open_data_file(file_id)?;
        data_file.read_at(offset, size).await
    }

    async fn rotate_data_file(&self) -> Result<()> {
        let new_file = Arc::new(self.file_manager.create_data_file()?);
        let old_file = self.inner.active_file.read().clone();
        old_file.sync().await?;
        let mut active = self.inner.active_file.write();
        *active = new_file;
        Ok(())
    }

    pub fn iter(&self) -> Result<BucketStoreIterator> {
        Ok(BucketStoreIterator::new(
            self.config.bucket_count,
            self.bucket_file_for_iter.read().try_clone()?,
            self.file_manager.clone(),
        ))
    }

    pub fn iter_by_position(&self, from: u64, to: u64) -> Result<BucketStorePositionIterator> {
        Ok(BucketStorePositionIterator::new(
            self.config.bucket_count,
            self.bucket_file_for_iter.read().try_clone()?,
            self.file_manager.clone(),
            from,
            to,
        ))
    }
}

fn compute_position(oid: &Oid) -> u64 {
    u64::from_le_bytes(
        oid.as_bytes()[..8]
            .try_into()
            .expect("Oid is always 32 bytes, first 8 bytes always valid"),
    )
}

pub struct BucketStoreIterator {
    bucket_count: u32,
    bucket_file: BucketFile,
    file_manager: Arc<FileManager>,
    current_bucket: u32,
    current_entries: Vec<CompactEntry>,
    current_entry_idx: usize,
}

impl BucketStoreIterator {
    fn new(bucket_count: u32, bucket_file: BucketFile, file_manager: Arc<FileManager>) -> Self {
        Self {
            bucket_count,
            bucket_file,
            file_manager,
            current_bucket: 0,
            current_entries: Vec::new(),
            current_entry_idx: 0,
        }
    }

    fn load_next_bucket(&mut self) -> Option<()> {
        while self.current_bucket < self.bucket_count {
            let bucket_id = self.current_bucket;
            self.current_bucket += 1;

            if let Ok(bucket) = self.bucket_file.read_bucket(bucket_id) {
                let count = { bucket.header.count };
                let entries: Vec<_> = (0..count as usize)
                    .map(|i| bucket.entries[i])
                    .filter(|e| !e.is_empty() && !e.is_deleted())
                    .collect();

                if !entries.is_empty() {
                    self.current_entries = entries;
                    self.current_entry_idx = 0;
                    return Some(());
                }
            }
        }
        None
    }

    fn read_entry(&self, entry: &CompactEntry) -> Option<(Oid, Bytes)> {
        let file_id = entry.file_id;
        let offset = entry.offset();
        let size = entry.size() as usize;

        let data_file = self.file_manager.open_data_file(file_id).ok()?;
        data_file.read_record_at_blocking(offset, size).ok()
    }
}

impl Stream for BucketStoreIterator {
    type Item = Result<(Oid, Bytes)>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.current_entry_idx < self.current_entries.len() {
                let entry = self.current_entries[self.current_entry_idx];
                self.current_entry_idx += 1;

                if let Some((oid, value)) = self.read_entry(&entry) {
                    return Poll::Ready(Some(Ok((oid, value))));
                }
                continue;
            }

            if self.load_next_bucket().is_none() {
                return Poll::Ready(None);
            }
        }
    }
}

pub struct BucketStorePositionIterator {
    inner: BucketStoreIterator,
    from: u64,
    to: u64,
}

impl BucketStorePositionIterator {
    fn new(
        bucket_count: u32,
        bucket_file: BucketFile,
        file_manager: Arc<FileManager>,
        from: u64,
        to: u64,
    ) -> Self {
        Self {
            inner: BucketStoreIterator::new(bucket_count, bucket_file, file_manager),
            from,
            to,
        }
    }
}

impl Stream for BucketStorePositionIterator {
    type Item = Result<(Oid, Bytes)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok((oid, value)))) => {
                    let position = compute_position(&oid);
                    if position >= self.from && position < self.to {
                        return Poll::Ready(Some(Ok((oid, value))));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BucketStoreStats {
    pub entries: u64,
    pub dead_bytes: u64,
    pub total_bytes: u64,
    pub bucket_cache_hit_rate: f64,
    pub data_files: usize,
    pub memory_usage: usize,
}

impl BucketStoreStats {
    pub fn fragmentation(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.dead_bytes as f64 / self.total_bytes as f64
        }
    }
}

impl Drop for BucketStore {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        self.io.shutdown();
        for handle in self.reaper_handles.write().drain(..) {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
            compaction: crate::config::CompactionConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x01);
        let value = Bytes::from("hello world");

        store.put(oid, value.clone()).await.unwrap();
        let retrieved = store.get(&oid).await.unwrap();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), value);
    }

    #[tokio::test]
    async fn test_contains() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x02);
        assert!(!store.contains(&oid));

        store.put(oid, Bytes::from("data")).await.unwrap();
        assert!(store.contains(&oid));
    }

    #[tokio::test]
    async fn test_hard_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x03);
        store.put(oid, Bytes::from("to delete")).await.unwrap();
        assert!(store.contains(&oid));

        let deleted = store.delete(&oid).await.unwrap();
        assert!(deleted);
        assert!(!store.contains(&oid));

        let deleted_again = store.delete(&oid).await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_mark_deleted_soft_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x04);
        let value = Bytes::from("soft delete me");

        store.put(oid, value).await.unwrap();
        assert!(store.contains(&oid));

        let marked = store.mark_deleted(&oid).await.unwrap();
        assert!(marked);

        assert!(!store.contains(&oid));

        let retrieved = store.get(&oid).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_mark_deleted_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x05);
        let marked = store.mark_deleted(&oid).await.unwrap();
        assert!(!marked);
    }

    #[tokio::test]
    async fn test_mark_deleted_twice() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x06);
        store.put(oid, Bytes::from("data")).await.unwrap();

        let first = store.mark_deleted(&oid).await.unwrap();
        assert!(first);

        let second = store.mark_deleted(&oid).await.unwrap();
        assert!(!second);
    }

    #[tokio::test]
    async fn test_soft_delete_tracks_dead_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x07);
        let value = Bytes::from("track my bytes");
        store.put(oid, value).await.unwrap();

        let stats_before = store.stats();
        assert_eq!(stats_before.dead_bytes, 0);

        store.mark_deleted(&oid).await.unwrap();

        let stats_after = store.stats();
        assert!(stats_after.dead_bytes > 0);
    }

    #[tokio::test]
    async fn test_put_multiple_get_all() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        for i in 0..10 {
            let oid = create_test_oid(i);
            let value = Bytes::from(format!("value_{}", i));
            store.put(oid, value).await.unwrap();
        }

        assert_eq!(store.len(), 10);

        for i in 0..10 {
            let oid = create_test_oid(i);
            let retrieved = store.get(&oid).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap(), Bytes::from(format!("value_{}", i)));
        }
    }

    #[tokio::test]
    async fn test_overwrite_value() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid = create_test_oid(0x10);
        store.put(oid, Bytes::from("first")).await.unwrap();
        store.put(oid, Bytes::from("second")).await.unwrap();

        let retrieved = store.get(&oid).await.unwrap();
        assert_eq!(retrieved.unwrap(), Bytes::from("second"));

        let stats = store.stats();
        assert!(stats.dead_bytes > 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.dead_bytes, 0);
        assert_eq!(stats.fragmentation(), 0.0);

        store
            .put(create_test_oid(0x01), Bytes::from("a"))
            .await
            .unwrap();
        let stats = store.stats();
        assert_eq!(stats.entries, 1);
        assert!(stats.total_bytes > 0);
    }

    #[tokio::test]
    async fn test_fragmentation_calculation() {
        let stats = BucketStoreStats {
            entries: 100,
            dead_bytes: 400,
            total_bytes: 1000,
            bucket_cache_hit_rate: 0.5,
            data_files: 1,
            memory_usage: 1024,
        };
        assert!((stats.fragmentation() - 0.4).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_fragmentation_zero_total() {
        let stats = BucketStoreStats {
            entries: 0,
            dead_bytes: 0,
            total_bytes: 0,
            bucket_cache_hit_rate: 0.0,
            data_files: 0,
            memory_usage: 0,
        };
        assert_eq!(stats.fragmentation(), 0.0);
    }

    #[tokio::test]
    async fn test_is_empty() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        assert!(store.is_empty());

        store
            .put(create_test_oid(0x01), Bytes::from("x"))
            .await
            .unwrap();
        assert!(!store.is_empty());
    }

    #[tokio::test]
    async fn test_bucket_accessors() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        assert_eq!(store.bucket_count(), 64);
        assert!(store.bucket_index().bucket_count() == 64);
        assert!(store.file_manager().file_count() >= 1);
    }

    #[tokio::test]
    async fn test_read_write_bucket() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let bucket = store.read_bucket(0).await.unwrap();
        let count = { bucket.header.count };
        assert_eq!(count, 0);

        store
            .put(create_test_oid(0x00), Bytes::from("test"))
            .await
            .unwrap();

        let bucket = store.read_bucket(0).await.unwrap();
        store.write_bucket(0, &bucket).await.unwrap();
    }

    #[tokio::test]
    async fn test_reset_dead_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        store
            .put(create_test_oid(0x01), Bytes::from("a"))
            .await
            .unwrap();
        store.mark_deleted(&create_test_oid(0x01)).await.unwrap();

        assert!(store.stats().dead_bytes > 0);

        store.reset_dead_bytes();
        assert_eq!(store.stats().dead_bytes, 0);
    }

    #[tokio::test]
    async fn test_subtract_total_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        store
            .put(create_test_oid(0x01), Bytes::from("data"))
            .await
            .unwrap();
        let before = store.stats().total_bytes;

        store.subtract_total_bytes(10);
        let after = store.stats().total_bytes;

        assert_eq!(after, before - 10);
    }

    #[tokio::test]
    async fn test_sync() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        store
            .put(create_test_oid(0x01), Bytes::from("sync me"))
            .await
            .unwrap();
        store.sync().await.unwrap();
    }

    #[tokio::test]
    async fn test_mixed_soft_and_hard_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid1 = create_test_oid(0x01);
        let oid2 = create_test_oid(0x02);
        let oid3 = create_test_oid(0x03);

        store.put(oid1, Bytes::from("one")).await.unwrap();
        store.put(oid2, Bytes::from("two")).await.unwrap();
        store.put(oid3, Bytes::from("three")).await.unwrap();

        store.mark_deleted(&oid1).await.unwrap();
        store.delete(&oid2).await.unwrap();

        assert!(!store.contains(&oid1));
        assert!(!store.contains(&oid2));
        assert!(store.contains(&oid3));

        assert!(store.get(&oid1).await.unwrap().is_none());
        assert!(store.get(&oid2).await.unwrap().is_none());
        assert!(store.get(&oid3).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_iter() {
        use tokio_stream::StreamExt;

        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid1 = create_test_oid(0x01);
        let oid2 = create_test_oid(0x02);
        let oid3 = create_test_oid(0x03);

        store.put(oid1, Bytes::from("one")).await.unwrap();
        store.put(oid2, Bytes::from("two")).await.unwrap();
        store.put(oid3, Bytes::from("three")).await.unwrap();

        assert_eq!(store.len(), 3, "Store should have 3 entries");

        let items: Vec<_> = store.iter().unwrap().collect::<Vec<_>>().await;
        assert_eq!(items.len(), 3);
    }

    #[tokio::test]
    async fn test_iter_by_position() {
        use tokio_stream::StreamExt;

        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BucketStore::open(config).await.unwrap();

        let oid1 = create_test_oid(0x01);
        let oid2 = create_test_oid(0x02);

        store.put(oid1, Bytes::from("one")).await.unwrap();
        store.put(oid2, Bytes::from("two")).await.unwrap();

        let items: Vec<_> = store
            .iter_by_position(0, u64::MAX)
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(items.len(), 2);
    }
}
