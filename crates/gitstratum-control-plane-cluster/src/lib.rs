#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod audit;
pub mod auth;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod client;
pub mod config;
pub mod error;
pub mod hashring;
pub mod membership;
pub mod proto;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod raft;
pub mod ratelimit;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod server;
pub mod time;

pub use client::{
    AcquireLockResult, ClusterStateResponse, ControlPlaneClient, ControlPlaneClientPool,
    RebalanceStatus as ClientRebalanceStatus,
};
pub use error::{ControlPlaneError, Result};
pub use membership::{
    ClusterState, ClusterStateSnapshot, ExtendedNodeInfo, LockInfo, NodeType, RefLockKey,
};
pub use raft::{
    create_stores, ControlPlaneRaft, ControlPlaneStore, GitStratumStateMachine, LogStore, NodeId,
    Request as RaftRequest, Response as RaftResponse, StateMachineStore, TypeConfig,
};
pub use server::{start_server, ControlPlaneServer};
