#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg(test)]
pub(crate) mod testutil;

pub mod cache;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
pub mod delta;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod gc;
pub mod integrity;
pub mod pack_cache;
pub mod repair;
pub mod replication;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod store;
pub mod util;

pub use client::ObjectClusterClient;
pub use error::{ObjectStoreError, Result};
pub use pack_cache::{HotRepoTracker, PackCache, PackPrecomputer};
pub use repair::prelude::*;
pub use replication::{QuorumWriter, ReplicationRepairer};
pub use server::ObjectServiceImpl;
pub use store::{ObjectStorage, ObjectStore, StorageStats};

#[cfg(feature = "bucketstore")]
pub use store::{BucketObjectIterator, BucketObjectStore, BucketPositionObjectIterator};

#[cfg(feature = "bucketstore")]
pub use gitstratum_storage::BucketStoreConfig;
