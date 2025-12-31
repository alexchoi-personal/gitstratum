#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
pub mod error;
pub mod locks;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod raft;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod state;

pub use client::{AcquireLockResult, ClusterStateResponse, ControlPlaneClient, ControlPlaneClientPool, RebalanceStatus};
pub use error::{ControlPlaneError, Result};
pub use locks::LockInfo;
pub use raft::{
    create_stores, ControlPlaneRaft, ControlPlaneStore, LogStore, NodeId,
    Request as RaftRequest, Response as RaftResponse, StateMachineStore, TypeConfig,
};
pub use server::{start_server, ControlPlaneServer};
pub use state::{ClusterState, ClusterStateSnapshot, ExtendedNodeInfo, NodeType, RefLockKey};
