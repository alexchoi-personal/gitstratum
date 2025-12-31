use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use gitstratum_core::Oid;
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::bucket::{BucketCache, BucketIndex, CompactEntry, DiskBucket, EntryFlags};
use crate::config::BitcaskConfig;
use crate::error::{BitcaskError, Result};
use crate::file::{BucketFile, DataFile, FileManager};
use crate::record::DataRecord;

pub struct BitcaskStore {
    config: BitcaskConfig,
    bucket_index: Arc<BucketIndex>,
    bucket_cache: Arc<BucketCache>,
    bucket_file: RwLock<BucketFile>,
    file_manager: Arc<FileManager>,
    active_file: RwLock<DataFile>,
    closed: AtomicBool,
    shutdown_notify: Arc<Notify>,
    dead_bytes: AtomicU64,
    total_bytes: AtomicU64,
}

impl BitcaskStore {
    pub async fn open(config: BitcaskConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let file_manager = Arc::new(FileManager::new(&config.data_dir)?);

        let bucket_file_path = file_manager.bucket_file_path();
        let bucket_file = if bucket_file_path.exists() {
            BucketFile::open(&bucket_file_path)?
        } else {
            BucketFile::create(&bucket_file_path, config.bucket_count)?
        };

        let bucket_index = Arc::new(BucketIndex::new(config.bucket_count));
        let bucket_cache = Arc::new(BucketCache::new(config.bucket_cache_size));

        let active_file = file_manager.create_data_file()?;

        Ok(Self {
            config,
            bucket_index,
            bucket_cache,
            bucket_file: RwLock::new(bucket_file),
            file_manager,
            active_file: RwLock::new(active_file),
            closed: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            dead_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        })
    }

    pub async fn get(&self, oid: &Oid) -> Result<Option<Bytes>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BitcaskError::StoreClosed);
        }

        let bucket_id = self.bucket_index.bucket_id(oid);

        // Check cache first
        if let Some(bucket) = self.bucket_cache.get(bucket_id) {
            if let Some(entry) = bucket.find_entry(oid) {
                if entry.flags & EntryFlags::DELETED.bits() != 0 {
                    return Ok(None);
                }
                return self.read_value(entry).await.map(Some);
            }
            return Ok(None);
        }

        // Read bucket from disk
        let bucket = {
            let mut bucket_file = self.bucket_file.write();
            bucket_file.read_bucket(bucket_id)?
        };

        // Cache the bucket
        self.bucket_cache.put(bucket_id, bucket.clone());

        // Find entry
        if let Some(entry) = bucket.find_entry(oid) {
            if entry.flags & EntryFlags::DELETED.bits() != 0 {
                return Ok(None);
            }
            return self.read_value(entry).await.map(Some);
        }

        Ok(None)
    }

    pub async fn put(&self, oid: Oid, value: Bytes) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BitcaskError::StoreClosed);
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let record = DataRecord::new(oid, value.clone(), timestamp)?;
        let record_size = record.record_size();

        // Check if we need to rotate data files
        let needs_rotation = {
            let active = self.active_file.read();
            active.size() + record_size as u64 > self.config.max_data_file_size
        };
        if needs_rotation {
            self.rotate_data_file().await?;
        }

        // Write to data file
        let (file_id, offset) = {
            let mut active = self.active_file.write();
            let offset = active.append(&record)?;
            if self.config.sync_writes {
                active.sync()?;
            }
            (active.file_id(), offset)
        };

        // Create bucket entry
        let entry = CompactEntry::new(
            &oid,
            file_id,
            offset,
            record_size as u32,
            EntryFlags::NONE.bits(),
        );

        // Update bucket
        let bucket_id = self.bucket_index.bucket_id(&oid);
        self.bucket_cache.invalidate(bucket_id);

        {
            let mut bucket_file = self.bucket_file.write();
            let mut bucket = bucket_file.read_bucket(bucket_id)?;

            // Check if entry already exists (update case)
            let mut found = false;
            for i in 0..bucket.header.count as usize {
                if bucket.entries[i].matches(&oid) {
                    // Track dead bytes from old entry
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

            bucket_file.write_bucket(bucket_id, &bucket)?;
            if self.config.sync_writes {
                bucket_file.sync()?;
            }
        }

        self.total_bytes
            .fetch_add(record_size as u64, Ordering::Relaxed);

        Ok(())
    }

    pub async fn delete(&self, oid: &Oid) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BitcaskError::StoreClosed);
        }

        let bucket_id = self.bucket_index.bucket_id(oid);
        self.bucket_cache.invalidate(bucket_id);

        let mut bucket_file = self.bucket_file.write();
        let mut bucket = bucket_file.read_bucket(bucket_id)?;

        for i in 0..bucket.header.count as usize {
            if bucket.entries[i].matches(oid) {
                // Mark as dead bytes
                self.dead_bytes
                    .fetch_add(bucket.entries[i].size() as u64, Ordering::Relaxed);

                // Remove by swapping with last entry
                let last_idx = bucket.header.count as usize - 1;
                if i != last_idx {
                    bucket.entries[i] = bucket.entries[last_idx];
                }
                bucket.entries[last_idx] = CompactEntry::EMPTY;
                bucket.header.count -= 1;

                bucket_file.write_bucket(bucket_id, &bucket)?;
                self.bucket_index.decrement_entry_count();

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn mark_deleted(&self, oid: &Oid) -> Result<bool> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(BitcaskError::StoreClosed);
        }

        let bucket_id = self.bucket_index.bucket_id(oid);
        self.bucket_cache.invalidate(bucket_id);

        let mut bucket_file = self.bucket_file.write();
        let mut bucket = bucket_file.read_bucket(bucket_id)?;

        for i in 0..bucket.header.count as usize {
            if bucket.entries[i].matches(oid) {
                let flags = bucket.entries[i].flags;
                if flags & EntryFlags::DELETED.bits() != 0 {
                    return Ok(false);
                }

                bucket.entries[i].flags = flags | EntryFlags::DELETED.bits();

                self.dead_bytes
                    .fetch_add(bucket.entries[i].size() as u64, Ordering::Relaxed);

                bucket_file.write_bucket(bucket_id, &bucket)?;

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

        // Check cache first
        if let Some(bucket) = self.bucket_cache.get(bucket_id) {
            if let Some(entry) = bucket.find_entry(oid) {
                return entry.flags & EntryFlags::DELETED.bits() == 0;
            }
            return false;
        }

        // Read from disk
        if let Ok(bucket) = self.bucket_file.write().read_bucket(bucket_id) {
            self.bucket_cache.put(bucket_id, bucket.clone());
            if let Some(entry) = bucket.find_entry(oid) {
                return entry.flags & EntryFlags::DELETED.bits() == 0;
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

    pub fn stats(&self) -> BitcaskStats {
        BitcaskStats {
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

    pub fn file_manager(&self) -> &Arc<FileManager> {
        &self.file_manager
    }

    pub fn read_bucket(&self, bucket_id: u32) -> Result<DiskBucket> {
        let mut bucket_file = self.bucket_file.write();
        bucket_file.read_bucket(bucket_id)
    }

    pub fn write_bucket(&self, bucket_id: u32, bucket: &DiskBucket) -> Result<()> {
        self.bucket_cache.invalidate(bucket_id);
        let mut bucket_file = self.bucket_file.write();
        bucket_file.write_bucket(bucket_id, bucket)?;
        Ok(())
    }

    pub fn reset_dead_bytes(&self) {
        self.dead_bytes.store(0, Ordering::Relaxed);
    }

    pub fn subtract_total_bytes(&self, amount: u64) {
        self.total_bytes.fetch_sub(amount, Ordering::Relaxed);
    }

    pub async fn sync(&self) -> Result<()> {
        self.active_file.read().sync()?;
        self.bucket_file.read().sync()?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();

        self.sync().await?;
        Ok(())
    }

    async fn read_value(&self, entry: &CompactEntry) -> Result<Bytes> {
        let file_id = entry.file_id;
        let offset = entry.offset();
        let size = entry.size() as usize;

        let mut data_file = self.file_manager.open_data_file(file_id)?;
        data_file.read_at(offset, size)
    }

    async fn rotate_data_file(&self) -> Result<()> {
        let new_file = self.file_manager.create_data_file()?;
        let mut active = self.active_file.write();
        active.sync()?;
        *active = new_file;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BitcaskStats {
    pub entries: u64,
    pub dead_bytes: u64,
    pub total_bytes: u64,
    pub bucket_cache_hit_rate: f64,
    pub data_files: usize,
    pub memory_usage: usize,
}

impl BitcaskStats {
    pub fn fragmentation(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.dead_bytes as f64 / self.total_bytes as f64
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

    fn test_config(dir: &std::path::Path) -> BitcaskConfig {
        BitcaskConfig {
            data_dir: dir.to_path_buf(),
            max_data_file_size: 1024 * 1024,
            bucket_count: 64,
            sync_writes: false,
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
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

        let oid = create_test_oid(0x02);
        assert!(!store.contains(&oid));

        store.put(oid, Bytes::from("data")).await.unwrap();
        assert!(store.contains(&oid));
    }

    #[tokio::test]
    async fn test_hard_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

        let oid = create_test_oid(0x05);
        let marked = store.mark_deleted(&oid).await.unwrap();
        assert!(!marked);
    }

    #[tokio::test]
    async fn test_mark_deleted_twice() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

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
        let store = BitcaskStore::open(config).await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.dead_bytes, 0);
        assert_eq!(stats.fragmentation(), 0.0);

        store.put(create_test_oid(0x01), Bytes::from("a")).await.unwrap();
        let stats = store.stats();
        assert_eq!(stats.entries, 1);
        assert!(stats.total_bytes > 0);
    }

    #[tokio::test]
    async fn test_fragmentation_calculation() {
        let stats = BitcaskStats {
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
        let stats = BitcaskStats {
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
        let store = BitcaskStore::open(config).await.unwrap();

        assert!(store.is_empty());

        store.put(create_test_oid(0x01), Bytes::from("x")).await.unwrap();
        assert!(!store.is_empty());
    }

    #[tokio::test]
    async fn test_bucket_accessors() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

        assert_eq!(store.bucket_count(), 64);
        assert!(store.bucket_index().bucket_count() == 64);
        assert!(store.file_manager().file_count() >= 1);
    }

    #[tokio::test]
    async fn test_read_write_bucket() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

        let bucket = store.read_bucket(0).unwrap();
        let count = { bucket.header.count };
        assert_eq!(count, 0);

        store.put(create_test_oid(0x00), Bytes::from("test")).await.unwrap();

        let bucket = store.read_bucket(0).unwrap();
        store.write_bucket(0, &bucket).unwrap();
    }

    #[tokio::test]
    async fn test_reset_dead_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

        store.put(create_test_oid(0x01), Bytes::from("a")).await.unwrap();
        store.mark_deleted(&create_test_oid(0x01)).await.unwrap();

        assert!(store.stats().dead_bytes > 0);

        store.reset_dead_bytes();
        assert_eq!(store.stats().dead_bytes, 0);
    }

    #[tokio::test]
    async fn test_subtract_total_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

        store.put(create_test_oid(0x01), Bytes::from("data")).await.unwrap();
        let before = store.stats().total_bytes;

        store.subtract_total_bytes(10);
        let after = store.stats().total_bytes;

        assert_eq!(after, before - 10);
    }

    #[tokio::test]
    async fn test_sync() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

        store.put(create_test_oid(0x01), Bytes::from("sync me")).await.unwrap();
        store.sync().await.unwrap();
    }

    #[tokio::test]
    async fn test_mixed_soft_and_hard_delete() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = BitcaskStore::open(config).await.unwrap();

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
}
