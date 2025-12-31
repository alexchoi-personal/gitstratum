#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod graph;
pub mod object_index;
pub mod pack_cache;
pub mod partition;
pub mod refs;
pub mod repo;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod store;

pub use client::MetadataClient;
pub use error::{MetadataStoreError, Result};
pub use graph::{
    collect_reachable_commits, find_merge_base, is_ancestor, walk_commits, walk_commits_async,
    CommitWalker, GraphCache,
};
pub use object_index::{ObjectLocation, ObjectLocationIndex, ObjectLocator, ObjectType};
pub use pack_cache::{
    CacheInvalidator, InvalidationEvent, InvalidationTracker, PackCacheEntry, PackCacheKey,
    PackCacheStorage, PrecomputeHandler, PrecomputePriority, PrecomputeQueue, PrecomputeRequest,
    RepoTemperature, TtlCalculator, TtlConfig,
};
pub use partition::{
    Partition, PartitionConfig, PartitionRouter, RebalanceAction, RebalanceConfig, RebalancePlan,
    RebalancePlanner,
};
pub use refs::{AtomicRefUpdater, HotRefsCache, RefCache, RefStorage, RefUpdate, RefUpdateResult};
pub use repo::{
    HookConfig, HookConfigStore, HookType, RepoConfig, RepoConfigStore, RepoHooks, RepoSettings,
    RepoStats, RepoStatsCollector, RepoVisibility,
};
pub use server::MetadataServiceImpl;
pub use store::MetadataStore;
