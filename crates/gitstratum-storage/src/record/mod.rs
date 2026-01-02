pub mod checksum;
pub mod format;
pub mod header;

pub use checksum::compute_crc32;
pub use format::{DataRecord, BLOCK_SIZE, RECORD_MAGIC};
pub use header::{RecordHeader, HEADER_SIZE, KEY_SIZE};
