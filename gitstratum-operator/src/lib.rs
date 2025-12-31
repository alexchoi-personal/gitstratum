#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod controllers;
pub mod crd;
pub mod error;
pub mod resources;

pub use controllers::{reconcile, Context, ContextData, ScalingDecision};
pub use crd::{
    ClusterCondition, ClusterPhase, ConditionType, ControlPlaneSpec, ControlPlaneStatus,
    FrontendAutoScalingSpec, FrontendSpec, FrontendStatus, GitStratumCluster,
    GitStratumClusterSpec, GitStratumClusterStatus, HashRingStatus, MetadataSpec, MetadataStatus,
    ObjectAutoScalingSpec, ObjectClusterSpec, ObjectClusterStatus, ReplicationMode,
    ResourceRequirements, ServicePort, ServiceSpec, ServiceType,
};
pub use error::{OperatorError, Result};
pub use resources::{
    build_control_plane_pdb, build_control_plane_service, build_control_plane_statefulset,
    build_frontend_deployment, build_frontend_hpa, build_frontend_pdb, build_frontend_service,
    build_metadata_pdb, build_metadata_service, build_metadata_statefulset, build_object_pdb,
    build_object_service, build_object_statefulset,
};
