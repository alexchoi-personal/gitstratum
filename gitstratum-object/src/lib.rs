#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod store;

pub use client::ObjectClusterClient;
pub use error::{ObjectStoreError, Result};
pub use server::ObjectServiceImpl;
pub use store::{ObjectStore, StorageStats};
