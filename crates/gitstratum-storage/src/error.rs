use thiserror::Error;

pub type Result<T> = std::result::Result<T, BucketStoreError>;

#[derive(Error, Debug)]
pub enum BucketStoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("io_uring error: {0}")]
    IoUring(String),

    #[error("object not found: {0}")]
    NotFound(String),

    #[error("corrupted bucket at offset {offset}: {reason}")]
    CorruptedBucket { offset: u64, reason: String },

    #[error("corrupted record at file {file_id} offset {offset}: {reason}")]
    CorruptedRecord {
        file_id: u16,
        offset: u64,
        reason: String,
    },

    #[error("CRC mismatch: expected {expected:08x}, got {actual:08x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("invalid magic: expected {expected:08x}, got {actual:08x}")]
    InvalidMagic { expected: u32, actual: u32 },

    #[error("bucket overflow: bucket {bucket_id} is full")]
    BucketOverflow { bucket_id: u32 },

    #[error("invalid bucket id: {bucket_id} >= {bucket_count}")]
    InvalidBucketId { bucket_id: u32, bucket_count: u32 },

    #[error("file too large: {size} bytes exceeds maximum {max}")]
    FileTooLarge { size: u64, max: u64 },

    #[error("object too large: {size} bytes exceeds maximum 16MB")]
    ObjectTooLarge { size: usize },

    #[error("entry offset {offset} exceeds maximum {max}")]
    EntryOffsetTooLarge { offset: u64, max: u64 },

    #[error("entry size {size} exceeds maximum {max}")]
    EntrySizeTooLarge { size: u32, max: u32 },

    #[error("store is closed")]
    StoreClosed,

    #[error("io completion channel closed")]
    IoCompletion,

    #[error("recovery failed: {0}")]
    RecoveryFailed(String),

    #[error("compaction failed: {0}")]
    CompactionFailed(String),
}
