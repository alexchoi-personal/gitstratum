#[cfg(feature = "bucketstore")]
pub mod bucket;
pub mod compression;
pub mod rocksdb;
pub mod tiered;
pub mod traits;

#[cfg(feature = "bucketstore")]
pub use bucket::{BucketObjectIterator, BucketObjectStore, BucketPositionObjectIterator};
pub use compression::{CompressionConfig, CompressionType, Compressor};
pub use rocksdb::{RocksDbStore, StorageStats};
pub use tiered::{StorageTier, TieredStorage, TieredStorageConfig};
pub use traits::ObjectStorage;

#[cfg(feature = "bucketstore")]
pub type ObjectStore = BucketObjectStore;

#[cfg(not(feature = "bucketstore"))]
pub type ObjectStore = RocksDbStore;
