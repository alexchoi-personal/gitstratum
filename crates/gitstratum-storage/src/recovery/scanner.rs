use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::bucket::{BucketIndex, CompactEntry, MAX_ENTRIES};
use crate::error::Result;
use crate::file::BucketFile;
use crate::record::{DataRecord, BLOCK_SIZE};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct IntegrityError {
    pub(crate) bucket_id: u32,
    pub(crate) entry_index: usize,
    pub(crate) file_id: u16,
    pub(crate) offset: u64,
    pub(crate) size: u32,
    pub(crate) reason: String,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct IntegrityReport {
    pub(crate) total_entries: u64,
    pub(crate) valid_entries: u64,
    pub(crate) corrupted_entries: Vec<IntegrityError>,
}

#[allow(dead_code)]
impl IntegrityReport {
    pub(crate) fn is_healthy(&self) -> bool {
        self.corrupted_entries.is_empty()
    }
}

#[allow(dead_code)]
pub(crate) struct RecoveryScanner;

#[allow(dead_code)]
impl RecoveryScanner {
    pub fn recover_bucket_index(bucket_file_path: &Path, bucket_count: u32) -> Result<BucketIndex> {
        if bucket_file_path.exists() {
            let bucket_file = BucketFile::open(bucket_file_path)?;
            let index = BucketIndex::new(bucket_file.bucket_count());
            Ok(index)
        } else {
            Ok(BucketIndex::new(bucket_count))
        }
    }

    pub fn verify_integrity(
        bucket_file: &mut BucketFile,
        data_dir: &Path,
    ) -> Result<IntegrityReport> {
        let mut report = IntegrityReport::default();
        let bucket_count = bucket_file.bucket_count();

        for bucket_id in 0..bucket_count {
            let bucket = match bucket_file.read_bucket(bucket_id) {
                Ok(b) => b,
                Err(_) => continue,
            };

            let count = { bucket.header.count } as usize;
            if count > MAX_ENTRIES {
                continue;
            }

            for entry_idx in 0..count {
                let entry = &bucket.entries[entry_idx];

                if entry.is_empty() {
                    continue;
                }

                if entry.is_deleted() {
                    continue;
                }

                report.total_entries += 1;

                match Self::verify_entry(entry, bucket_id, entry_idx, data_dir) {
                    Ok(()) => {
                        report.valid_entries += 1;
                    }
                    Err(error) => {
                        report.corrupted_entries.push(error);
                    }
                }
            }
        }

        Ok(report)
    }

    fn verify_entry(
        entry: &CompactEntry,
        bucket_id: u32,
        entry_index: usize,
        data_dir: &Path,
    ) -> std::result::Result<(), IntegrityError> {
        let file_id = { entry.file_id };
        let offset = entry.offset();
        let size = entry.size();

        let data_file_path = data_dir.join(format!("{:05}.data", file_id));
        if !data_file_path.exists() {
            return Err(IntegrityError {
                bucket_id,
                entry_index,
                file_id,
                offset,
                size,
                reason: format!("data file {:05}.data does not exist", file_id),
            });
        }

        let file = match File::open(&data_file_path) {
            Ok(f) => f,
            Err(e) => {
                return Err(IntegrityError {
                    bucket_id,
                    entry_index,
                    file_id,
                    offset,
                    size,
                    reason: format!("failed to open data file: {}", e),
                });
            }
        };

        let metadata = match file.metadata() {
            Ok(m) => m,
            Err(e) => {
                return Err(IntegrityError {
                    bucket_id,
                    entry_index,
                    file_id,
                    offset,
                    size,
                    reason: format!("failed to get file metadata: {}", e),
                });
            }
        };

        let file_len = metadata.len();
        let end_offset = offset.saturating_add(size as u64);
        if end_offset > file_len {
            return Err(IntegrityError {
                bucket_id,
                entry_index,
                file_id,
                offset,
                size,
                reason: format!(
                    "offset+size ({}) exceeds file length ({})",
                    end_offset, file_len
                ),
            });
        }

        let aligned_size = (size as usize + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1);
        let mut buffer = vec![0u8; aligned_size];
        if let Err(e) = file.read_at(&mut buffer, offset) {
            return Err(IntegrityError {
                bucket_id,
                entry_index,
                file_id,
                offset,
                size,
                reason: format!("failed to read data at offset {}: {}", offset, e),
            });
        }

        if let Err(e) = DataRecord::from_bytes(&buffer) {
            return Err(IntegrityError {
                bucket_id,
                entry_index,
                file_id,
                offset,
                size,
                reason: format!("record verification failed: {}", e),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use gitstratum_core::Oid;
    use tempfile::TempDir;

    fn create_test_oid(seed: u8) -> Oid {
        let mut bytes = [seed; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        Oid::from_bytes(bytes)
    }

    #[test]
    fn test_integrity_report_default() {
        let report = IntegrityReport::default();
        assert_eq!(report.total_entries, 0);
        assert_eq!(report.valid_entries, 0);
        assert!(report.corrupted_entries.is_empty());
        assert!(report.is_healthy());
    }

    #[test]
    fn test_integrity_report_healthy() {
        let report = IntegrityReport {
            total_entries: 10,
            valid_entries: 10,
            corrupted_entries: vec![],
        };
        assert!(report.is_healthy());
    }

    #[test]
    fn test_integrity_report_unhealthy() {
        let report = IntegrityReport {
            total_entries: 10,
            valid_entries: 9,
            corrupted_entries: vec![IntegrityError {
                bucket_id: 0,
                entry_index: 0,
                file_id: 0,
                offset: 0,
                size: 100,
                reason: "test error".to_string(),
            }],
        };
        assert!(!report.is_healthy());
    }

    #[test]
    fn test_verify_empty_bucket_file() {
        let tmp = TempDir::new().unwrap();
        let bucket_file_path = tmp.path().join("buckets.idx");

        let mut bucket_file = BucketFile::create(&bucket_file_path, 16).unwrap();

        let report = RecoveryScanner::verify_integrity(&mut bucket_file, tmp.path()).unwrap();

        assert_eq!(report.total_entries, 0);
        assert_eq!(report.valid_entries, 0);
        assert!(report.is_healthy());
    }

    #[test]
    fn test_verify_entry_missing_data_file() {
        let tmp = TempDir::new().unwrap();
        let oid = create_test_oid(0x42);
        let entry = crate::bucket::CompactEntry::new(&oid, 99, 0, 4096, 0).unwrap();

        let result = RecoveryScanner::verify_entry(&entry, 0, 0, tmp.path());

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.reason.contains("does not exist"));
        assert_eq!(err.file_id, 99);
    }

    #[tokio::test]
    async fn test_verify_integrity_with_valid_entries() {
        use crate::config::BucketStoreConfig;
        use crate::BucketStore;

        let tmp = TempDir::new().unwrap();
        let config = BucketStoreConfig {
            data_dir: tmp.path().to_path_buf(),
            max_data_file_size: 1024 * 1024,
            bucket_count: 16,
            bucket_cache_size: 8,
            io_queue_depth: 4,
            io_queue_count: 1,
            compaction: crate::config::CompactionConfig::default(),
        };

        let store = BucketStore::open(config).await.unwrap();

        for i in 0..5u8 {
            let oid = create_test_oid(i);
            let value = Bytes::from(vec![i; 256]);
            store.put(oid, value).await.unwrap();
        }

        store.sync().await.unwrap();

        let bucket_file_path = tmp.path().join("buckets.idx");
        let mut bucket_file = BucketFile::open(&bucket_file_path).unwrap();

        let report = RecoveryScanner::verify_integrity(&mut bucket_file, tmp.path()).unwrap();

        assert_eq!(report.total_entries, 5);
        assert_eq!(report.valid_entries, 5);
        assert!(report.is_healthy());
    }

    #[test]
    fn test_recover_bucket_index_new() {
        let tmp = TempDir::new().unwrap();
        let bucket_file_path = tmp.path().join("buckets.idx");

        let index = RecoveryScanner::recover_bucket_index(&bucket_file_path, 32).unwrap();
        assert_eq!(index.bucket_count(), 32);
    }

    #[test]
    fn test_recover_bucket_index_existing() {
        let tmp = TempDir::new().unwrap();
        let bucket_file_path = tmp.path().join("buckets.idx");

        let _ = BucketFile::create(&bucket_file_path, 64).unwrap();

        let index = RecoveryScanner::recover_bucket_index(&bucket_file_path, 32).unwrap();
        assert_eq!(index.bucket_count(), 64);
    }
}
