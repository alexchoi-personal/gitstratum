#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod cache;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
pub mod delta;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod gc;
pub mod integrity;
pub mod replication;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod store;

pub use client::ObjectClusterClient;
pub use error::{ObjectStoreError, Result};
pub use server::ObjectServiceImpl;
pub use store::{ObjectStore, StorageStats};
