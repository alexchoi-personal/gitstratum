#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod controllers;
pub mod crd;
pub mod error;
pub mod leader;
pub mod resources;

pub use controllers::{reconcile, Context, ContextData, ScalingDecision};
pub use crd::{
    ClusterCondition, ClusterPhase, ConditionType, ControlPlaneSpec, ControlPlaneStatus,
    FrontendAutoScalingSpec, FrontendSpec, FrontendStatus, GitRepository, GitRepositorySpec,
    GitRepositoryStatus, GitStratumCluster, GitStratumClusterSpec, GitStratumClusterStatus,
    GitUser, GitUserSpec, GitUserStatus, HashRingStatus, LabelSelector, LabelSelectorOperator,
    LabelSelectorRequirement, MetadataSpec, MetadataStatus, ObjectAutoScalingSpec,
    ObjectClusterSpec, ObjectClusterStatus, PackCachePolicy, PackCachePolicySpec, Permission,
    RateLimitSpec, ReplicationMode, RepoAccessSpec, RepoState, ResourceRequirements, ServicePort,
    ServiceSpec, ServiceType, SshKeySpec, TokenScope, TokenSpec, UserState, Visibility,
};
pub use error::{OperatorError, Result};
pub use leader::{LeaderElection, LeaderElectionConfig};
pub use resources::{
    build_control_plane_pdb, build_control_plane_service, build_control_plane_statefulset,
    build_frontend_deployment, build_frontend_hpa, build_frontend_pdb, build_frontend_service,
    build_metadata_pdb, build_metadata_service, build_metadata_statefulset, build_object_pdb,
    build_object_service, build_object_statefulset,
};
