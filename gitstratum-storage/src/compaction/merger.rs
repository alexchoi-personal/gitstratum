use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::bucket::{CompactEntry, DiskBucket, EntryFlags};
use crate::error::Result;
use crate::record::DataRecord;
use crate::store::BucketStore;

pub struct Compactor {
    store: Arc<BucketStore>,
}

#[derive(Debug, Clone)]
struct LiveEntry {
    bucket_id: u32,
    entry_idx: usize,
    file_id: u16,
    offset: u64,
    size: u32,
    oid_suffix: [u8; 16],
    flags: u8,
}

impl Compactor {
    pub fn new(store: Arc<BucketStore>) -> Self {
        Self { store }
    }

    pub async fn compact(&self) -> Result<CompactionResult> {
        let stats = self.store.stats();
        let fragmentation = stats.fragmentation();

        if fragmentation < 0.1 {
            return Ok(CompactionResult {
                files_compacted: 0,
                bytes_reclaimed: 0,
                entries_moved: 0,
                entries_purged: 0,
            });
        }

        self.compact_data_files().await
    }

    pub async fn compact_data_files(&self) -> Result<CompactionResult> {
        let bucket_count = self.store.bucket_count();
        let file_manager = self.store.file_manager();

        let mut live_entries: Vec<LiveEntry> = Vec::new();
        let mut deleted_entries: Vec<(u32, usize)> = Vec::new();
        let mut source_file_ids: HashSet<u16> = HashSet::new();
        let mut bytes_to_reclaim: u64 = 0;

        for bucket_id in 0..bucket_count {
            let bucket = self.store.read_bucket(bucket_id)?;
            let count = { bucket.header.count } as usize;

            for entry_idx in 0..count {
                let entry = &bucket.entries[entry_idx];
                let flags = { entry.flags };
                let file_id = { entry.file_id };

                source_file_ids.insert(file_id);

                if flags & EntryFlags::DELETED.bits() != 0 {
                    deleted_entries.push((bucket_id, entry_idx));
                    bytes_to_reclaim += entry.size() as u64;
                } else {
                    live_entries.push(LiveEntry {
                        bucket_id,
                        entry_idx,
                        file_id,
                        offset: entry.offset(),
                        size: entry.size(),
                        oid_suffix: entry.oid_suffix,
                        flags,
                    });
                }
            }
        }

        if live_entries.is_empty() && deleted_entries.is_empty() {
            return Ok(CompactionResult {
                files_compacted: 0,
                bytes_reclaimed: 0,
                entries_moved: 0,
                entries_purged: 0,
            });
        }

        let mut target_file = file_manager.create_data_file()?;
        let target_file_id = target_file.file_id();

        let mut bucket_updates: HashMap<u32, Vec<(usize, CompactEntry)>> = HashMap::new();
        let mut entries_moved = 0u64;

        for live in &live_entries {
            let mut source_file = file_manager.open_data_file(live.file_id)?;
            let value = source_file.read_at(live.offset, live.size as usize)?;

            let mut oid_bytes = [0u8; 32];
            oid_bytes[16..32].copy_from_slice(&live.oid_suffix);
            let oid = gitstratum_core::Oid::from_bytes(oid_bytes);

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            let record = DataRecord::new(oid, value, timestamp)?;

            let new_offset = target_file.append(&record)?;

            let new_entry = CompactEntry::new(
                &oid,
                target_file_id,
                new_offset,
                record.record_size() as u32,
                live.flags,
            );

            bucket_updates
                .entry(live.bucket_id)
                .or_default()
                .push((live.entry_idx, new_entry));

            entries_moved += 1;
        }

        target_file.sync()?;

        for (bucket_id, updates) in &bucket_updates {
            let mut bucket = self.store.read_bucket(*bucket_id)?;

            for (entry_idx, new_entry) in updates {
                bucket.entries[*entry_idx] = *new_entry;
            }

            self.store.write_bucket(*bucket_id, &bucket)?;
        }

        let mut entries_purged = 0u64;
        let mut buckets_to_update: HashMap<u32, DiskBucket> = HashMap::new();

        for (bucket_id, entry_idx) in deleted_entries.iter().rev() {
            let bucket = buckets_to_update
                .entry(*bucket_id)
                .or_insert_with(|| self.store.read_bucket(*bucket_id).unwrap());

            let count = { bucket.header.count } as usize;
            if *entry_idx < count {
                let last_idx = count - 1;
                if *entry_idx != last_idx {
                    bucket.entries[*entry_idx] = bucket.entries[last_idx];
                }
                bucket.entries[last_idx] = CompactEntry::EMPTY;
                bucket.header.count -= 1;
                entries_purged += 1;
            }
        }

        for (bucket_id, bucket) in &buckets_to_update {
            self.store.write_bucket(*bucket_id, bucket)?;
            self.store.bucket_index().decrement_entry_count();
        }

        self.store.reset_dead_bytes();
        self.store.subtract_total_bytes(bytes_to_reclaim);

        let files_compacted = source_file_ids.len();

        Ok(CompactionResult {
            files_compacted,
            bytes_reclaimed: bytes_to_reclaim,
            entries_moved,
            entries_purged,
        })
    }

    pub fn should_compact(&self) -> bool {
        let stats = self.store.stats();
        stats.fragmentation() > 0.4
    }

    pub fn fragmentation(&self) -> f64 {
        self.store.stats().fragmentation()
    }
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub files_compacted: usize,
    pub bytes_reclaimed: u64,
    pub entries_moved: u64,
    pub entries_purged: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use gitstratum_core::Oid;
    use tempfile::TempDir;
    use crate::config::{BucketStoreConfig, CompactionConfig};

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
            sync_writes: false,
            bucket_cache_size: 16,
            io_queue_depth: 4,
            io_queue_count: 1,
            compaction: CompactionConfig::default(),
        }
    }

    #[test]
    fn test_compaction_result_default() {
        let result = CompactionResult {
            files_compacted: 0,
            bytes_reclaimed: 0,
            entries_moved: 0,
            entries_purged: 0,
        };
        assert_eq!(result.files_compacted, 0);
        assert_eq!(result.bytes_reclaimed, 0);
        assert_eq!(result.entries_moved, 0);
        assert_eq!(result.entries_purged, 0);
    }

    #[test]
    fn test_compaction_result_with_values() {
        let result = CompactionResult {
            files_compacted: 5,
            bytes_reclaimed: 1024 * 1024,
            entries_moved: 100,
            entries_purged: 50,
        };
        assert_eq!(result.files_compacted, 5);
        assert_eq!(result.bytes_reclaimed, 1024 * 1024);
        assert_eq!(result.entries_moved, 100);
        assert_eq!(result.entries_purged, 50);
    }

    #[test]
    fn test_compaction_result_clone() {
        let result = CompactionResult {
            files_compacted: 3,
            bytes_reclaimed: 512,
            entries_moved: 10,
            entries_purged: 5,
        };
        let cloned = result.clone();
        assert_eq!(cloned.files_compacted, 3);
        assert_eq!(cloned.bytes_reclaimed, 512);
    }

    #[test]
    fn test_live_entry_struct() {
        let entry = LiveEntry {
            bucket_id: 10,
            entry_idx: 5,
            file_id: 1,
            offset: 1024,
            size: 256,
            oid_suffix: [0u8; 16],
            flags: 0,
        };
        assert_eq!(entry.bucket_id, 10);
        assert_eq!(entry.entry_idx, 5);
        assert_eq!(entry.file_id, 1);
        assert_eq!(entry.offset, 1024);
        assert_eq!(entry.size, 256);
    }

    #[tokio::test]
    async fn test_compactor_new() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());
        let compactor = Compactor::new(store);
        assert!(!compactor.should_compact());
    }

    #[tokio::test]
    async fn test_compactor_should_compact_empty() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());
        let compactor = Compactor::new(store);
        assert!(!compactor.should_compact());
        assert_eq!(compactor.fragmentation(), 0.0);
    }

    #[tokio::test]
    async fn test_compact_empty_store() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());
        let compactor = Compactor::new(store);

        let result = compactor.compact().await.unwrap();
        assert_eq!(result.files_compacted, 0);
        assert_eq!(result.bytes_reclaimed, 0);
        assert_eq!(result.entries_moved, 0);
        assert_eq!(result.entries_purged, 0);
    }

    #[tokio::test]
    async fn test_compact_no_deleted_entries() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        for i in 0..5 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from(format!("value_{}", i))).await.unwrap();
        }

        let compactor = Compactor::new(store.clone());
        assert!(!compactor.should_compact());

        let result = compactor.compact().await.unwrap();
        assert_eq!(result.entries_purged, 0);
    }

    #[tokio::test]
    async fn test_compact_with_soft_deleted_entries() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        for i in 0..10 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from(format!("value_{}", i))).await.unwrap();
        }

        for i in 0..5 {
            let oid = create_test_oid(i);
            store.mark_deleted(&oid).await.unwrap();
        }

        let stats_before = store.stats();
        assert!(stats_before.dead_bytes > 0);

        let compactor = Compactor::new(store.clone());
        let result = compactor.compact_data_files().await.unwrap();

        assert!(result.entries_purged > 0);
        assert!(result.entries_moved > 0);

        for i in 5..10 {
            let oid = create_test_oid(i);
            assert!(store.contains(&oid));
            let value = store.get(&oid).await.unwrap();
            assert!(value.is_some());
        }

        for i in 0..5 {
            let oid = create_test_oid(i);
            assert!(!store.contains(&oid));
        }
    }

    #[tokio::test]
    async fn test_compact_preserves_data_integrity() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        let test_data: Vec<(Oid, Bytes)> = (0..20)
            .map(|i| {
                let oid = create_test_oid(i);
                let value = Bytes::from(format!("test_data_for_oid_{}", i));
                (oid, value)
            })
            .collect();

        for (oid, value) in &test_data {
            store.put(*oid, value.clone()).await.unwrap();
        }

        for i in (0..20).step_by(2) {
            let oid = create_test_oid(i);
            store.mark_deleted(&oid).await.unwrap();
        }

        let compactor = Compactor::new(store.clone());
        compactor.compact_data_files().await.unwrap();

        for i in (1..20).step_by(2) {
            let oid = create_test_oid(i);
            let value = store.get(&oid).await.unwrap();
            assert!(value.is_some(), "OID {} should exist", i);
            assert_eq!(
                value.unwrap(),
                Bytes::from(format!("test_data_for_oid_{}", i))
            );
        }
    }

    #[tokio::test]
    async fn test_fragmentation_threshold() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        for i in 0..100 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from(vec![0u8; 1000])).await.unwrap();
        }

        for i in 0..50 {
            let oid = create_test_oid(i);
            store.mark_deleted(&oid).await.unwrap();
        }

        let compactor = Compactor::new(store.clone());
        let frag = compactor.fragmentation();
        assert!(frag > 0.0);
    }

    #[tokio::test]
    async fn test_compact_updates_stats() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        for i in 0..10 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from("data")).await.unwrap();
        }

        for i in 0..5 {
            let oid = create_test_oid(i);
            store.mark_deleted(&oid).await.unwrap();
        }

        let dead_before = store.stats().dead_bytes;
        assert!(dead_before > 0);

        let compactor = Compactor::new(store.clone());
        compactor.compact_data_files().await.unwrap();

        let dead_after = store.stats().dead_bytes;
        assert_eq!(dead_after, 0);
    }

    #[tokio::test]
    async fn test_compact_all_deleted() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        for i in 0..5 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from("delete me")).await.unwrap();
        }

        for i in 0..5 {
            let oid = create_test_oid(i);
            store.mark_deleted(&oid).await.unwrap();
        }

        let compactor = Compactor::new(store.clone());
        let result = compactor.compact_data_files().await.unwrap();

        assert_eq!(result.entries_moved, 0);
        assert!(result.entries_purged > 0);
    }

    #[tokio::test]
    async fn test_multiple_compactions() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(tmp.path());
        let store = Arc::new(BucketStore::open(config).await.unwrap());

        for i in 0..10 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from("round1")).await.unwrap();
        }
        for i in 0..5 {
            store.mark_deleted(&create_test_oid(i)).await.unwrap();
        }

        let compactor = Compactor::new(store.clone());
        compactor.compact_data_files().await.unwrap();

        for i in 10..20 {
            let oid = create_test_oid(i);
            store.put(oid, Bytes::from("round2")).await.unwrap();
        }
        for i in 10..15 {
            store.mark_deleted(&create_test_oid(i)).await.unwrap();
        }

        compactor.compact_data_files().await.unwrap();

        for i in 5..10 {
            assert!(store.contains(&create_test_oid(i)));
        }
        for i in 15..20 {
            assert!(store.contains(&create_test_oid(i)));
        }
    }
}
