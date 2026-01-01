use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BucketStoreConfig {
    pub data_dir: PathBuf,
    pub max_data_file_size: u64,
    pub bucket_count: u32,
    /// Sync interval. Zero = sync on every write. Non-zero = background sync.
    pub sync_interval: Duration,
    pub bucket_cache_size: usize,
    pub io_queue_depth: u32,
    pub io_queue_count: usize,
    pub compaction: CompactionConfig,
}

impl Default for BucketStoreConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            max_data_file_size: 1024 * 1024 * 1024, // 1GB
            bucket_count: 1 << 27,                  // 128M buckets
            sync_interval: Duration::from_secs(1), // Background sync every second
            bucket_cache_size: 16 * 1024,          // 16K buckets = 64MB
            io_queue_depth: 256,
            io_queue_count: 4,
            compaction: CompactionConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub fragmentation_threshold: f64,
    pub check_interval: Duration,
    pub max_concurrent: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            fragmentation_threshold: 0.4,
            check_interval: Duration::from_secs(300),
            max_concurrent: 2,
        }
    }
}
