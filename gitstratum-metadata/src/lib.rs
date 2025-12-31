#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod graph;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod store;

pub use client::MetadataClient;
pub use error::{MetadataStoreError, Result};
pub use graph::{
    collect_reachable_commits, find_merge_base, is_ancestor, walk_commits, walk_commits_async,
    CommitWalker,
};
pub use server::MetadataServiceImpl;
pub use store::MetadataStore;
