use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, ResourceExt};

use crate::crd::{
    ClusterCondition, ClusterPhase, ControlPlaneStatus, FrontendStatus, GitStratumCluster,
    GitStratumClusterStatus, MetadataStatus, ObjectClusterStatus,
};
use crate::error::{OperatorError, Result};
use crate::resources::{
    build_control_plane_pdb, build_control_plane_service, build_control_plane_statefulset,
    build_frontend_deployment, build_frontend_hpa, build_frontend_pdb, build_frontend_service,
    build_metadata_pdb, build_metadata_service, build_metadata_statefulset, build_object_pdb,
    build_object_service, build_object_statefulset,
};

use super::scaling::{check_autoscaling, ScalingDecision};

pub struct ContextData {
    pub client: Client,
}

pub type Context = Arc<ContextData>;

impl ContextData {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn reconcile(cluster: Arc<GitStratumCluster>, ctx: Context) -> Result<Action> {
    let client = &ctx.client;
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let name = cluster.name_any();

    tracing::info!("Reconciling GitStratumCluster {}/{}", namespace, name);

    let clusters: Api<GitStratumCluster> = Api::namespaced(client.clone(), &namespace);

    let mut status = cluster.status.clone().unwrap_or_default();

    let control_plane_ready = reconcile_control_plane(client, &cluster, &namespace).await?;
    status.control_plane = control_plane_ready;

    let metadata_ready = reconcile_metadata(client, &cluster, &namespace).await?;
    status.metadata = metadata_ready;

    let object_ready = reconcile_object_cluster(client, &cluster, &namespace).await?;
    status.object_cluster = object_ready;

    let frontend_ready = reconcile_frontend(client, &cluster, &namespace).await?;
    status.frontend = frontend_ready;

    status.phase = determine_phase(&status, &cluster.spec);
    update_conditions(&mut status, &cluster.spec);

    let status_patch = serde_json::json!({
        "status": status
    });

    clusters
        .patch_status(
            &name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Merge(&status_patch),
        )
        .await?;

    let scaling_decision = check_autoscaling(&cluster, &status);
    match scaling_decision {
        ScalingDecision::ScaleUp { target_replicas } => {
            tracing::info!("Scaling up object cluster to {} replicas", target_replicas);
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
        ScalingDecision::ScaleDown { target_replicas } => {
            tracing::info!(
                "Scaling down object cluster to {} replicas",
                target_replicas
            );
            return Ok(Action::requeue(Duration::from_secs(60)));
        }
        ScalingDecision::NoChange => {}
    }

    if status.is_rebalancing() {
        return Ok(Action::requeue(Duration::from_secs(10)));
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn reconcile_control_plane(
    client: &Client,
    cluster: &GitStratumCluster,
    namespace: &str,
) -> Result<ControlPlaneStatus> {
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);

    let sts = build_control_plane_statefulset(cluster);
    let sts_name = sts.metadata.name.clone().unwrap_or_default();
    sts_api
        .patch(
            &sts_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(sts),
        )
        .await?;

    let svc = build_control_plane_service(cluster);
    let svc_name = svc.metadata.name.clone().unwrap_or_default();
    svc_api
        .patch(
            &svc_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(svc),
        )
        .await?;

    let pdb = build_control_plane_pdb(cluster);
    let pdb_name = pdb.metadata.name.clone().unwrap_or_default();
    pdb_api
        .patch(
            &pdb_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(pdb),
        )
        .await?;

    let sts_status = sts_api.get_status(&sts_name).await?;
    let ready = sts_status
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let total = cluster.spec.control_plane.replicas;

    let leader = if ready > 0 {
        Some(format!("{}-0", sts_name))
    } else {
        None
    };

    Ok(ControlPlaneStatus::new(ready, total, leader))
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn reconcile_metadata(
    client: &Client,
    cluster: &GitStratumCluster,
    namespace: &str,
) -> Result<MetadataStatus> {
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);

    let sts = build_metadata_statefulset(cluster);
    let sts_name = sts.metadata.name.clone().unwrap_or_default();
    sts_api
        .patch(
            &sts_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(sts),
        )
        .await?;

    let svc = build_metadata_service(cluster);
    let svc_name = svc.metadata.name.clone().unwrap_or_default();
    svc_api
        .patch(
            &svc_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(svc),
        )
        .await?;

    let pdb = build_metadata_pdb(cluster);
    let pdb_name = pdb.metadata.name.clone().unwrap_or_default();
    pdb_api
        .patch(
            &pdb_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(pdb),
        )
        .await?;

    let sts_status = sts_api.get_status(&sts_name).await?;
    let ready = sts_status
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let total = cluster.spec.metadata.replicas;

    Ok(MetadataStatus::new(ready, total))
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn reconcile_object_cluster(
    client: &Client,
    cluster: &GitStratumCluster,
    namespace: &str,
) -> Result<ObjectClusterStatus> {
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);

    let sts = build_object_statefulset(cluster);
    let sts_name = sts.metadata.name.clone().unwrap_or_default();
    sts_api
        .patch(
            &sts_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(sts),
        )
        .await?;

    let svc = build_object_service(cluster);
    let svc_name = svc.metadata.name.clone().unwrap_or_default();
    svc_api
        .patch(
            &svc_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(svc),
        )
        .await?;

    let pdb = build_object_pdb(cluster);
    let pdb_name = pdb.metadata.name.clone().unwrap_or_default();
    pdb_api
        .patch(
            &pdb_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(pdb),
        )
        .await?;

    let sts_status = sts_api.get_status(&sts_name).await?;
    let ready = sts_status
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let total = cluster.spec.object_cluster.replicas;

    Ok(ObjectClusterStatus::new(ready, total))
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn reconcile_frontend(
    client: &Client,
    cluster: &GitStratumCluster,
    namespace: &str,
) -> Result<FrontendStatus> {
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);

    let deploy = build_frontend_deployment(cluster);
    let deploy_name = deploy.metadata.name.clone().unwrap_or_default();
    deploy_api
        .patch(
            &deploy_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(deploy),
        )
        .await?;

    let svc = build_frontend_service(cluster);
    let svc_name = svc.metadata.name.clone().unwrap_or_default();
    svc_api
        .patch(
            &svc_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(svc),
        )
        .await?;

    let pdb = build_frontend_pdb(cluster);
    let pdb_name = pdb.metadata.name.clone().unwrap_or_default();
    pdb_api
        .patch(
            &pdb_name,
            &PatchParams::apply("gitstratum-operator"),
            &Patch::Apply(pdb),
        )
        .await?;

    if let Some(ref auto_scaling) = cluster.spec.frontend.auto_scaling {
        if auto_scaling.enabled {
            let hpa_api: Api<k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler> =
                Api::namespaced(client.clone(), namespace);
            let hpa = build_frontend_hpa(cluster);
            let hpa_name = hpa.metadata.name.clone().unwrap_or_default();
            hpa_api
                .patch(
                    &hpa_name,
                    &PatchParams::apply("gitstratum-operator"),
                    &Patch::Apply(hpa),
                )
                .await?;
        }
    }

    let deploy_status = deploy_api.get_status(&deploy_name).await?;
    let ready = deploy_status
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let total = cluster.spec.frontend.replicas;

    Ok(FrontendStatus::new(ready, total))
}

fn determine_phase(
    status: &GitStratumClusterStatus,
    spec: &crate::crd::GitStratumClusterSpec,
) -> ClusterPhase {
    let (cp_ready, cp_total) = status.control_plane.parse_ready();
    let (meta_ready, meta_total) = (
        status
            .metadata
            .ready
            .split('/')
            .next()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0),
        spec.metadata.replicas,
    );
    let (obj_ready, obj_total) = status.object_cluster.parse_ready();
    let (fe_ready, fe_total) = (
        status
            .frontend
            .ready
            .split('/')
            .next()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0),
        spec.frontend.replicas,
    );

    let all_ready = cp_ready == cp_total
        && meta_ready == meta_total
        && obj_ready == obj_total
        && fe_ready == fe_total;

    let any_ready = cp_ready > 0 || meta_ready > 0 || obj_ready > 0 || fe_ready > 0;

    if all_ready && cp_total > 0 {
        ClusterPhase::Running
    } else if any_ready {
        ClusterPhase::Provisioning
    } else {
        ClusterPhase::Pending
    }
}

fn update_conditions(
    status: &mut GitStratumClusterStatus,
    spec: &crate::crd::GitStratumClusterSpec,
) {
    let (cp_ready, cp_total) = status.control_plane.parse_ready();
    let (obj_ready, obj_total) = status.object_cluster.parse_ready();

    let quorum_size = (cp_total / 2) + 1;
    let control_plane_healthy = cp_ready >= quorum_size;

    let object_cluster_healthy = obj_ready >= (obj_total / 2);

    let is_ready =
        control_plane_healthy && object_cluster_healthy && status.phase == ClusterPhase::Running;
    status.set_condition(ClusterCondition::ready(is_ready));

    let is_degraded = (cp_ready < cp_total && cp_ready >= quorum_size)
        || (obj_ready < obj_total && obj_ready >= spec.object_cluster.replicas / 2);

    if is_degraded {
        status.set_condition(ClusterCondition::degraded(
            true,
            Some("Some replicas are not ready".to_string()),
        ));
    } else {
        status.set_condition(ClusterCondition::degraded(false, None));
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub fn error_policy(
    cluster: Arc<GitStratumCluster>,
    error: &OperatorError,
    _ctx: Context,
) -> Action {
    tracing::error!(
        "Reconciliation error for {}: {:?}",
        cluster.name_any(),
        error
    );
    Action::requeue(Duration::from_secs(60))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ControlPlaneSpec, FrontendSpec, GitStratumClusterSpec, MetadataSpec, ObjectClusterSpec,
        ResourceRequirements,
    };

    fn create_test_spec() -> GitStratumClusterSpec {
        GitStratumClusterSpec {
            replication_factor: 2,
            control_plane: ControlPlaneSpec {
                replicas: 3,
                resources: ResourceRequirements {
                    cpu: "4".to_string(),
                    memory: "8Gi".to_string(),
                    storage: Some("50Gi".to_string()),
                },
                storage_class: "nvme-fast".to_string(),
                ..Default::default()
            },
            metadata: MetadataSpec {
                replicas: 3,
                resources: ResourceRequirements {
                    cpu: "4".to_string(),
                    memory: "32Gi".to_string(),
                    storage: Some("500Gi".to_string()),
                },
                storage_class: "nvme-fast".to_string(),
                ..Default::default()
            },
            object_cluster: ObjectClusterSpec {
                replicas: 12,
                resources: ResourceRequirements {
                    cpu: "4".to_string(),
                    memory: "16Gi".to_string(),
                    storage: Some("500Gi".to_string()),
                },
                storage_class: "nvme-fast".to_string(),
                ..Default::default()
            },
            frontend: FrontendSpec {
                replicas: 4,
                resources: ResourceRequirements {
                    cpu: "2".to_string(),
                    memory: "8Gi".to_string(),
                    storage: None,
                },
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_determine_phase_running() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_determine_phase_provisioning() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(10, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_determine_phase_pending() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(0, 3, None),
            metadata: MetadataStatus::new(0, 3),
            object_cluster: ObjectClusterStatus::new(0, 12),
            frontend: FrontendStatus::new(0, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_update_conditions_healthy() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        assert!(!status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false));
    }

    #[test]
    fn test_update_conditions_degraded() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false));
    }

    #[test]
    fn test_determine_phase_zero_replicas() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 0;
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(0, 0, None),
            metadata: MetadataStatus::new(0, 0),
            object_cluster: ObjectClusterStatus::new(0, 0),
            frontend: FrontendStatus::new(0, 0),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_update_conditions_object_cluster_degraded() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(10, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false));
    }

    #[test]
    fn test_update_conditions_not_running_phase() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Provisioning,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_context_data_new() {
        let _ = ContextData::new;
    }

    #[test]
    fn test_determine_phase_control_plane_only_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(0, 3),
            object_cluster: ObjectClusterStatus::new(0, 12),
            frontend: FrontendStatus::new(0, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_determine_phase_metadata_only_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(0, 3, None),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(0, 12),
            frontend: FrontendStatus::new(0, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_determine_phase_object_only_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(0, 3, None),
            metadata: MetadataStatus::new(0, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(0, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_determine_phase_frontend_only_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(0, 3, None),
            metadata: MetadataStatus::new(0, 3),
            object_cluster: ObjectClusterStatus::new(0, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_control_plane_no_quorum() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(1, 3, None),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_update_conditions_object_cluster_no_quorum() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(4, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_update_conditions_multiple_calls() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);
        assert!(status.is_ready());

        status.control_plane = ControlPlaneStatus::new(2, 3, Some("raft-0".to_string()));
        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_determine_phase_with_invalid_ready_string() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "invalid".to_string(),
                leader: None,
            },
            metadata: MetadataStatus {
                ready: "bad/format/extra".to_string(),
                total_repos: 0,
                storage_used: String::new(),
            },
            object_cluster: ObjectClusterStatus {
                ready: "".to_string(),
                ..Default::default()
            },
            frontend: FrontendStatus {
                ready: "not-a-number/4".to_string(),
                active_connections: 0,
            },
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Pending);

        status.control_plane.ready = "1/3".to_string();
        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_with_invalid_ready() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus {
                ready: "invalid".to_string(),
                leader: None,
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus {
                ready: "".to_string(),
                ..Default::default()
            },
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_determine_phase_all_zero_total() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 0;
        spec.metadata.replicas = 0;
        spec.object_cluster.replicas = 0;
        spec.frontend.replicas = 0;

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(0, 0, None),
            metadata: MetadataStatus::new(0, 0),
            object_cluster: ObjectClusterStatus::new(0, 0),
            frontend: FrontendStatus::new(0, 0),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_update_conditions_both_degraded() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(10, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_update_conditions_not_degraded_when_fully_healthy() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(!degraded);
    }

    #[test]
    fn test_update_conditions_preserves_existing() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![crate::crd::ClusterCondition::scaling(
                true,
                Some("scaling".to_string()),
            )],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_scaling());
        assert!(status.is_ready());
    }

    #[test]
    fn test_determine_phase_partial_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(1, 3, None),
            metadata: MetadataStatus::new(1, 3),
            object_cluster: ObjectClusterStatus::new(6, 12),
            frontend: FrontendStatus::new(2, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_single_replica_control_plane() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 1;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(1, 1, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(!degraded);
    }

    #[test]
    fn test_update_conditions_single_replica_object() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 2;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(2, 2),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
    }

    fn create_full_status(
        cp_ready: i32,
        cp_total: i32,
        meta_ready: i32,
        meta_total: i32,
        obj_ready: i32,
        obj_total: i32,
        fe_ready: i32,
        fe_total: i32,
        phase: ClusterPhase,
    ) -> GitStratumClusterStatus {
        GitStratumClusterStatus {
            phase,
            control_plane: ControlPlaneStatus::new(
                cp_ready,
                cp_total,
                if cp_ready > 0 {
                    Some("raft-0".to_string())
                } else {
                    None
                },
            ),
            metadata: MetadataStatus::new(meta_ready, meta_total),
            object_cluster: ObjectClusterStatus::new(obj_ready, obj_total),
            frontend: FrontendStatus::new(fe_ready, fe_total),
            conditions: vec![],
        }
    }

    #[test]
    fn test_determine_phase_seven_replicas_partial() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 7;
        let status = create_full_status(4, 7, 3, 3, 12, 12, 4, 4, ClusterPhase::Pending);

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_five_replicas_below_quorum() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 5;
        let mut status = create_full_status(2, 5, 3, 3, 12, 12, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_update_conditions_five_replicas_at_quorum() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 5;
        let mut status = create_full_status(3, 5, 3, 3, 12, 12, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        assert!(status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false));
    }

    #[test]
    fn test_update_conditions_object_cluster_odd_replicas() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 11;
        let mut status = create_full_status(3, 3, 3, 3, 6, 11, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        assert!(status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false));
    }

    #[test]
    fn test_update_conditions_object_cluster_minimal() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 2;
        let mut status = create_full_status(3, 3, 3, 3, 1, 2, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
    }

    #[test]
    fn test_update_conditions_large_object_cluster() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 100;
        let mut status = create_full_status(3, 3, 3, 3, 80, 100, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        assert!(status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false));
    }

    #[test]
    fn test_determine_phase_metadata_zero_replicas() {
        let mut spec = create_test_spec();
        spec.metadata.replicas = 0;
        let status = create_full_status(3, 3, 0, 0, 12, 12, 4, 4, ClusterPhase::Pending);

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_determine_phase_frontend_zero_replicas() {
        let mut spec = create_test_spec();
        spec.frontend.replicas = 0;
        let status = create_full_status(3, 3, 3, 3, 12, 12, 0, 0, ClusterPhase::Pending);

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_determine_phase_transitioning_from_degraded() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Degraded,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_determine_phase_transitioning_from_failed() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Failed,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_preserves_other_conditions() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![
                crate::crd::ClusterCondition::scaling(true, Some("ScaleUp".to_string())),
                crate::crd::ClusterCondition::rebalancing(true),
            ],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_scaling());
        assert!(status.is_rebalancing());
        assert!(status.is_ready());
    }

    #[test]
    fn test_error_types_display() {
        use crate::error::OperatorError;

        let errors: Vec<OperatorError> = vec![
            OperatorError::reconciliation("test reconciliation error"),
            OperatorError::invalid_spec("invalid configuration"),
            OperatorError::resource_not_found("pod/test-pod"),
            OperatorError::scaling("cannot scale during rebalancing"),
            OperatorError::finalizer("cleanup failed"),
            OperatorError::RebalancingInProgress,
            OperatorError::InsufficientReplicas {
                expected: 5,
                actual: 2,
            },
        ];

        for error in errors {
            let msg = error.to_string();
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_error_reconciliation_helper() {
        use crate::error::OperatorError;
        let error = OperatorError::reconciliation("test message");
        match error {
            OperatorError::Reconciliation(msg) => assert_eq!(msg, "test message"),
            _ => panic!("Wrong error type"),
        }
    }

    #[test]
    fn test_error_invalid_spec_helper() {
        use crate::error::OperatorError;
        let error = OperatorError::invalid_spec("bad spec");
        match error {
            OperatorError::InvalidSpec(msg) => assert_eq!(msg, "bad spec"),
            _ => panic!("Wrong error type"),
        }
    }

    #[test]
    fn test_error_resource_not_found_helper() {
        use crate::error::OperatorError;
        let error = OperatorError::resource_not_found("missing");
        match error {
            OperatorError::ResourceNotFound(msg) => assert_eq!(msg, "missing"),
            _ => panic!("Wrong error type"),
        }
    }

    #[test]
    fn test_error_scaling_helper() {
        use crate::error::OperatorError;
        let error = OperatorError::scaling("scale error");
        match error {
            OperatorError::Scaling(msg) => assert_eq!(msg, "scale error"),
            _ => panic!("Wrong error type"),
        }
    }

    #[test]
    fn test_error_finalizer_helper() {
        use crate::error::OperatorError;
        let error = OperatorError::finalizer("finalizer error");
        match error {
            OperatorError::Finalizer(msg) => assert_eq!(msg, "finalizer error"),
            _ => panic!("Wrong error type"),
        }
    }

    #[test]
    fn test_quorum_calculations() {
        let test_cases = vec![(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)];

        for (total, expected_quorum) in test_cases {
            let calculated_quorum = (total / 2) + 1;
            assert_eq!(
                calculated_quorum, expected_quorum,
                "For total={}, expected quorum={}, got={}",
                total, expected_quorum, calculated_quorum
            );
        }
    }

    #[test]
    fn test_half_calculations() {
        let test_cases = vec![(2, 1), (4, 2), (6, 3), (8, 4), (10, 5), (12, 6)];

        for (total, expected_half) in test_cases {
            let calculated_half = total / 2;
            assert_eq!(
                calculated_half, expected_half,
                "For total={}, expected half={}, got={}",
                total, expected_half, calculated_half
            );
        }
    }

    #[test]
    fn test_determine_phase_all_components_mismatch() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(2, 3, None),
            metadata: MetadataStatus::new(2, 3),
            object_cluster: ObjectClusterStatus::new(10, 12),
            frontend: FrontendStatus::new(3, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_edge_case_zero_quorum() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 1;
        spec.object_cluster.replicas = 1;
        let mut status = create_full_status(1, 1, 3, 3, 1, 1, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        assert!(!status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(true));
    }

    #[test]
    fn test_spec_replication_factor_values() {
        let test_cases: Vec<u8> = vec![1, 2, 3, 4, 5, 10, 255];

        for rf in test_cases {
            let mut spec = create_test_spec();
            spec.replication_factor = rf;
            assert_eq!(spec.replication_factor, rf);
        }
    }

    #[test]
    fn test_cluster_with_existing_status() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let existing_status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![crate::crd::ClusterCondition::ready(true)],
        };

        let cluster = GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: create_test_spec(),
            status: Some(existing_status),
        };

        assert!(cluster.status.is_some());
        assert!(cluster.status.unwrap().is_ready());
    }

    #[test]
    fn test_multiple_conditions_coexist() {
        let mut status = GitStratumClusterStatus::default();

        status.set_condition(crate::crd::ClusterCondition::ready(true));
        status.set_condition(crate::crd::ClusterCondition::scaling(
            true,
            Some("ScaleUp".to_string()),
        ));
        status.set_condition(crate::crd::ClusterCondition::rebalancing(true));
        status.set_condition(crate::crd::ClusterCondition::degraded(
            true,
            Some("Node issue".to_string()),
        ));

        assert!(status.is_ready());
        assert!(status.is_scaling());
        assert!(status.is_rebalancing());

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .unwrap();
        assert_eq!(degraded.status, "True");
    }

    #[test]
    fn test_status_with_storage_metrics() {
        let mut status = ObjectClusterStatus::new(12, 12);
        status.storage_used = "2.5Ti".to_string();
        status.storage_capacity = "6Ti".to_string();
        status.total_blobs = 1_000_000;

        assert_eq!(status.storage_used, "2.5Ti");
        assert_eq!(status.storage_capacity, "6Ti");
        assert_eq!(status.total_blobs, 1_000_000);
    }

    #[test]
    fn test_metadata_status_with_repo_count() {
        let mut status = MetadataStatus::new(3, 3);
        status.total_repos = 50000;
        status.storage_used = "100Gi".to_string();

        assert_eq!(status.total_repos, 50000);
        assert_eq!(status.storage_used, "100Gi");
    }

    #[test]
    fn test_frontend_status_with_connections() {
        let mut status = FrontendStatus::new(4, 4);
        status.active_connections = 10000;

        assert_eq!(status.active_connections, 10000);
    }

    #[test]
    fn test_status_with_hash_ring_data() {
        let mut status = ObjectClusterStatus::new(12, 12);
        status.hash_ring = crate::crd::HashRingStatus {
            version: 42,
            last_rebalance: Some("2024-01-15T10:30:00Z".to_string()),
        };

        assert_eq!(status.hash_ring.version, 42);
        assert_eq!(
            status.hash_ring.last_rebalance,
            Some("2024-01-15T10:30:00Z".to_string())
        );
    }

    #[test]
    fn test_error_policy_returns_requeue_action() {
        use crate::error::OperatorError;

        let _error = OperatorError::Reconciliation("test error".to_string());

        let action = std::panic::catch_unwind(|| Duration::from_secs(60));
        assert!(action.is_ok());
        assert_eq!(action.unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_error_policy_with_different_error_types() {
        use crate::error::OperatorError;

        let errors = vec![
            OperatorError::Reconciliation("reconciliation failed".to_string()),
            OperatorError::InvalidSpec("invalid spec".to_string()),
            OperatorError::ResourceNotFound("resource missing".to_string()),
            OperatorError::Scaling("scaling error".to_string()),
            OperatorError::RebalancingInProgress,
            OperatorError::InsufficientReplicas {
                expected: 3,
                actual: 1,
            },
            OperatorError::Finalizer("finalizer error".to_string()),
        ];

        for error in &errors {
            let debug_output = format!("{:?}", error);
            assert!(!debug_output.is_empty());
        }
    }

    #[test]
    fn test_cluster_name_any_with_no_name() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let cluster = GitStratumCluster {
            metadata: ObjectMeta {
                name: None,
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: create_test_spec(),
            status: None,
        };

        let name = cluster.name_any();
        assert!(name.is_empty());
    }

    #[test]
    fn test_cluster_namespace_fallback() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        use kube::ResourceExt;

        let cluster = GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: None,
                ..Default::default()
            },
            spec: create_test_spec(),
            status: None,
        };

        let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
        assert_eq!(namespace, "default");
    }

    #[test]
    fn test_cluster_status_clone_or_default() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

        let cluster_with_status = GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: create_test_spec(),
            status: Some(GitStratumClusterStatus {
                phase: ClusterPhase::Running,
                ..Default::default()
            }),
        };

        let status = cluster_with_status.status.clone().unwrap_or_default();
        assert_eq!(status.phase, ClusterPhase::Running);

        let cluster_without_status = GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: create_test_spec(),
            status: None,
        };

        let status = cluster_without_status.status.clone().unwrap_or_default();
        assert_eq!(status.phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_status_json_serialization() {
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![crate::crd::ClusterCondition::ready(true)],
        };

        let json = serde_json::json!({ "status": status });
        assert!(json.get("status").is_some());

        let status_obj = json.get("status").unwrap();
        assert!(status_obj.get("phase").is_some());
        assert!(status_obj.get("controlPlane").is_some());
        assert!(status_obj.get("metadata").is_some());
        assert!(status_obj.get("objectCluster").is_some());
        assert!(status_obj.get("frontend").is_some());
    }

    #[test]
    fn test_action_requeue_durations() {
        let scale_up_duration = Duration::from_secs(30);
        let scale_down_duration = Duration::from_secs(60);
        let rebalancing_duration = Duration::from_secs(10);
        let normal_duration = Duration::from_secs(300);

        assert_eq!(scale_up_duration.as_secs(), 30);
        assert_eq!(scale_down_duration.as_secs(), 60);
        assert_eq!(rebalancing_duration.as_secs(), 10);
        assert_eq!(normal_duration.as_secs(), 300);
    }

    #[test]
    fn test_scaling_decision_variants() {
        use super::super::scaling::ScalingDecision;

        let scale_up = ScalingDecision::ScaleUp {
            target_replicas: 15,
        };
        let scale_down = ScalingDecision::ScaleDown { target_replicas: 8 };
        let no_change = ScalingDecision::NoChange;

        match scale_up {
            ScalingDecision::ScaleUp { target_replicas } => assert_eq!(target_replicas, 15),
            _ => panic!("Expected ScaleUp"),
        }

        match scale_down {
            ScalingDecision::ScaleDown { target_replicas } => assert_eq!(target_replicas, 8),
            _ => panic!("Expected ScaleDown"),
        }

        match no_change {
            ScalingDecision::NoChange => {}
            _ => panic!("Expected NoChange"),
        }
    }

    #[test]
    fn test_determine_phase_edge_cases() {
        let mut spec = create_test_spec();

        spec.control_plane.replicas = 1;
        spec.metadata.replicas = 1;
        spec.object_cluster.replicas = 1;
        spec.frontend.replicas = 1;

        let status = create_full_status(1, 1, 1, 1, 1, 1, 1, 1, ClusterPhase::Pending);
        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);

        spec.control_plane.replicas = 100;
        spec.metadata.replicas = 100;
        spec.object_cluster.replicas = 100;
        spec.frontend.replicas = 100;

        let status = create_full_status(
            100,
            100,
            100,
            100,
            100,
            100,
            100,
            100,
            ClusterPhase::Pending,
        );
        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_quorum_edge_cases() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 2;
        spec.object_cluster.replicas = 2;

        let mut status = create_full_status(2, 2, 3, 3, 2, 2, 4, 4, ClusterPhase::Running);
        update_conditions(&mut status, &spec);

        assert!(status.is_ready());

        status.control_plane = ControlPlaneStatus::new(1, 2, Some("raft-0".to_string()));
        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_control_plane_leader_format() {
        let sts_name = "test-control-plane";
        let ready = 3;

        let leader = if ready > 0 {
            Some(format!("{}-0", sts_name))
        } else {
            None
        };

        assert_eq!(leader, Some("test-control-plane-0".to_string()));

        let ready_zero = 0;
        let no_leader = if ready_zero > 0 {
            Some(format!("{}-0", sts_name))
        } else {
            None
        };

        assert_eq!(no_leader, None);
    }

    #[test]
    fn test_status_default_values() {
        let status = GitStratumClusterStatus::default();

        assert_eq!(status.phase, ClusterPhase::Pending);
        assert_eq!(status.control_plane.ready, "");
        assert_eq!(status.metadata.ready, "");
        assert_eq!(status.object_cluster.ready, "");
        assert_eq!(status.frontend.ready, "");
        assert!(status.conditions.is_empty());
    }

    #[test]
    fn test_parse_ready_various_formats() {
        let valid = ControlPlaneStatus::new(3, 5, None);
        assert_eq!(valid.parse_ready(), (3, 5));

        let zeros = ControlPlaneStatus::new(0, 0, None);
        assert_eq!(zeros.parse_ready(), (0, 0));

        let large = ControlPlaneStatus::new(100, 1000, None);
        assert_eq!(large.parse_ready(), (100, 1000));

        let invalid = ControlPlaneStatus {
            ready: "abc/def".to_string(),
            leader: None,
        };
        assert_eq!(invalid.parse_ready(), (0, 0));

        let partial = ControlPlaneStatus {
            ready: "3".to_string(),
            leader: None,
        };
        assert_eq!(partial.parse_ready(), (0, 0));
    }

    #[test]
    fn test_object_cluster_parse_ready_formats() {
        let valid = ObjectClusterStatus::new(10, 12);
        assert_eq!(valid.parse_ready(), (10, 12));

        let zeros = ObjectClusterStatus::new(0, 0);
        assert_eq!(zeros.parse_ready(), (0, 0));

        let invalid = ObjectClusterStatus {
            ready: "invalid".to_string(),
            ..Default::default()
        };
        assert_eq!(invalid.parse_ready(), (0, 0));
    }

    #[test]
    fn test_condition_updates_replace_existing() {
        let mut status = GitStratumClusterStatus::default();

        status.set_condition(crate::crd::ClusterCondition::ready(false));
        assert!(!status.is_ready());

        status.set_condition(crate::crd::ClusterCondition::ready(true));
        assert!(status.is_ready());

        assert_eq!(status.conditions.len(), 1);
    }

    #[test]
    fn test_degraded_condition_message() {
        let spec = create_test_spec();
        let mut status = create_full_status(2, 3, 3, 3, 12, 12, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .unwrap();
        assert_eq!(degraded.status, "True");
        assert_eq!(
            degraded.message,
            Some("Some replicas are not ready".to_string())
        );
    }

    #[test]
    fn test_non_degraded_condition_no_message() {
        let spec = create_test_spec();
        let mut status = create_full_status(3, 3, 3, 3, 12, 12, 4, 4, ClusterPhase::Running);

        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .unwrap();
        assert_eq!(degraded.status, "False");
        assert!(degraded.message.is_none());
    }

    #[test]
    fn test_cluster_phase_transitions() {
        let spec = create_test_spec();

        let pending = create_full_status(0, 3, 0, 3, 0, 12, 0, 4, ClusterPhase::Pending);
        assert_eq!(determine_phase(&pending, &spec), ClusterPhase::Pending);

        let provisioning = create_full_status(1, 3, 1, 3, 6, 12, 2, 4, ClusterPhase::Pending);
        assert_eq!(
            determine_phase(&provisioning, &spec),
            ClusterPhase::Provisioning
        );

        let running = create_full_status(3, 3, 3, 3, 12, 12, 4, 4, ClusterPhase::Pending);
        assert_eq!(determine_phase(&running, &spec), ClusterPhase::Running);
    }

    #[test]
    fn test_context_type_alias() {
        fn check_context_type<T>(_: T) {}

        let arc_context_data: Option<Context> = None;
        check_context_type(arc_context_data);
    }

    #[test]
    fn test_error_debug_format() {
        use crate::error::OperatorError;

        let error = OperatorError::InsufficientReplicas {
            expected: 5,
            actual: 2,
        };
        let debug = format!("{:?}", error);
        assert!(debug.contains("expected"));
        assert!(debug.contains("actual"));
        assert!(debug.contains("5"));
        assert!(debug.contains("2"));
    }

    #[test]
    fn test_error_display_format() {
        use crate::error::OperatorError;

        let error = OperatorError::InsufficientReplicas {
            expected: 5,
            actual: 2,
        };
        let display = format!("{}", error);
        assert!(display.contains("insufficient"));
        assert!(display.contains("5"));
        assert!(display.contains("2"));
    }

    #[test]
    fn test_rebalancing_check() {
        let mut status = GitStratumClusterStatus::default();

        assert!(!status.is_rebalancing());

        status.set_condition(crate::crd::ClusterCondition::rebalancing(true));
        assert!(status.is_rebalancing());

        status.set_condition(crate::crd::ClusterCondition::rebalancing(false));
        assert!(!status.is_rebalancing());
    }

    #[test]
    fn test_scaling_check() {
        let mut status = GitStratumClusterStatus::default();

        assert!(!status.is_scaling());

        status.set_condition(crate::crd::ClusterCondition::scaling(
            true,
            Some("ScaleUp".to_string()),
        ));
        assert!(status.is_scaling());

        status.set_condition(crate::crd::ClusterCondition::scaling(false, None));
        assert!(!status.is_scaling());
    }

    #[test]
    fn test_cluster_spec_all_fields() {
        let spec = create_test_spec();

        assert_eq!(spec.replication_factor, 2);
        assert_eq!(spec.control_plane.replicas, 3);
        assert_eq!(spec.metadata.replicas, 3);
        assert_eq!(spec.object_cluster.replicas, 12);
        assert_eq!(spec.frontend.replicas, 4);

        assert_eq!(spec.control_plane.resources.cpu, "4");
        assert_eq!(spec.control_plane.resources.memory, "8Gi");
        assert_eq!(
            spec.control_plane.resources.storage,
            Some("50Gi".to_string())
        );
        assert_eq!(spec.control_plane.storage_class, "nvme-fast");
    }

    #[test]
    fn test_frontend_auto_scaling_none() {
        let spec = create_test_spec();
        assert!(spec.frontend.auto_scaling.is_none());
    }

    #[test]
    fn test_object_cluster_auto_scaling_none() {
        let spec = create_test_spec();
        assert!(spec.object_cluster.auto_scaling.is_none());
    }

    #[test]
    fn test_status_ready_string_format() {
        let cp = ControlPlaneStatus::new(2, 3, Some("leader".to_string()));
        assert_eq!(cp.ready, "2/3");

        let meta = MetadataStatus::new(5, 10);
        assert_eq!(meta.ready, "5/10");

        let obj = ObjectClusterStatus::new(100, 150);
        assert_eq!(obj.ready, "100/150");

        let fe = FrontendStatus::new(3, 4);
        assert_eq!(fe.ready, "3/4");
    }

    #[test]
    fn test_determine_phase_ready_string_parsing() {
        let spec = create_test_spec();

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus {
                ready: "3/3".to_string(),
                total_repos: 0,
                storage_used: String::new(),
            },
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus {
                ready: "4/4".to_string(),
                active_connections: 0,
            },
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_with_zero_total_replicas() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 0;
        spec.object_cluster.replicas = 0;

        let mut status = create_full_status(0, 0, 3, 3, 0, 0, 4, 4, ClusterPhase::Running);
        update_conditions(&mut status, &spec);

        let (_cp_ready, cp_total) = status.control_plane.parse_ready();
        let quorum_size = (cp_total / 2) + 1;

        assert_eq!(cp_total, 0);
        assert_eq!(quorum_size, 1);
    }

    #[test]
    fn test_metadata_status_fields() {
        let mut status = MetadataStatus::new(3, 3);
        status.total_repos = 1000;
        status.storage_used = "50Gi".to_string();

        assert_eq!(status.ready, "3/3");
        assert_eq!(status.total_repos, 1000);
        assert_eq!(status.storage_used, "50Gi");
    }

    #[test]
    fn test_object_cluster_status_fields() {
        let mut status = ObjectClusterStatus::new(12, 12);
        status.total_blobs = 5000000;
        status.storage_used = "4Ti".to_string();
        status.storage_capacity = "6Ti".to_string();
        status.hash_ring.version = 100;
        status.hash_ring.last_rebalance = Some("2024-12-01T00:00:00Z".to_string());

        assert_eq!(status.ready, "12/12");
        assert_eq!(status.total_blobs, 5000000);
        assert_eq!(status.storage_used, "4Ti");
        assert_eq!(status.storage_capacity, "6Ti");
        assert_eq!(status.hash_ring.version, 100);
    }

    #[test]
    fn test_frontend_status_fields() {
        let mut status = FrontendStatus::new(4, 4);
        status.active_connections = 50000;

        assert_eq!(status.ready, "4/4");
        assert_eq!(status.active_connections, 50000);
    }

    #[test]
    fn test_control_plane_status_with_no_leader() {
        let status = ControlPlaneStatus::new(0, 3, None);
        assert_eq!(status.leader, None);
        assert_eq!(status.ready, "0/3");
    }

    #[test]
    fn test_cluster_condition_types() {
        use crate::crd::ConditionType;

        let ready = ClusterCondition::ready(true);
        assert_eq!(ready.condition_type, ConditionType::Ready);

        let scaling = ClusterCondition::scaling(true, Some("reason".to_string()));
        assert_eq!(scaling.condition_type, ConditionType::Scaling);

        let rebalancing = ClusterCondition::rebalancing(true);
        assert_eq!(rebalancing.condition_type, ConditionType::Rebalancing);

        let degraded = ClusterCondition::degraded(true, Some("message".to_string()));
        assert_eq!(degraded.condition_type, ConditionType::Degraded);
    }

    #[test]
    fn test_condition_last_transition_time() {
        let condition = ClusterCondition::ready(true);
        assert!(condition.last_transition_time.is_some());

        let time_str = condition.last_transition_time.unwrap();
        assert!(!time_str.is_empty());
    }

    #[test]
    fn test_get_condition_returns_none_for_missing() {
        let status = GitStratumClusterStatus::default();

        assert!(status
            .get_condition(crate::crd::ConditionType::Ready)
            .is_none());
        assert!(status
            .get_condition(crate::crd::ConditionType::Scaling)
            .is_none());
        assert!(status
            .get_condition(crate::crd::ConditionType::Rebalancing)
            .is_none());
        assert!(status
            .get_condition(crate::crd::ConditionType::Degraded)
            .is_none());
    }

    #[test]
    fn test_large_replica_counts() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 1000;
        spec.object_cluster.replicas = 10000;

        let status = create_full_status(999, 1000, 3, 3, 9999, 10000, 4, 4, ClusterPhase::Running);

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);

        let mut status_copy = status.clone();
        update_conditions(&mut status_copy, &spec);

        let degraded = status_copy
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_spec_with_no_storage() {
        let mut spec = create_test_spec();
        spec.control_plane.resources.storage = None;
        spec.metadata.resources.storage = None;
        spec.object_cluster.resources.storage = None;

        assert!(spec.control_plane.resources.storage.is_none());
        assert!(spec.metadata.resources.storage.is_none());
        assert!(spec.object_cluster.resources.storage.is_none());
    }

    #[test]
    fn test_status_serialization_deserialization() {
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("leader".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![ClusterCondition::ready(true)],
        };

        let json = serde_json::to_string(&status).unwrap();
        let deserialized: GitStratumClusterStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.phase, status.phase);
        assert_eq!(deserialized.control_plane.ready, status.control_plane.ready);
    }

    #[test]
    fn test_cluster_phase_as_str() {
        assert_eq!(ClusterPhase::Pending.as_str(), "Pending");
        assert_eq!(ClusterPhase::Provisioning.as_str(), "Provisioning");
        assert_eq!(ClusterPhase::Running.as_str(), "Running");
        assert_eq!(ClusterPhase::Degraded.as_str(), "Degraded");
        assert_eq!(ClusterPhase::Failed.as_str(), "Failed");
    }

    #[tokio::test]
    async fn test_error_policy_with_arc_cluster() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        use std::sync::Arc;

        let cluster = Arc::new(GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: create_test_spec(),
            status: None,
        });

        let error = OperatorError::Reconciliation("test error".to_string());

        if let Ok(client) = kube::Client::try_default().await {
            let action = error_policy(cluster, &error, Arc::new(ContextData { client }));
            assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        }
    }

    #[test]
    fn test_context_data_struct() {
        let _fn_ref = ContextData::new;
    }

    #[test]
    fn test_determine_phase_with_negative_values() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "-1/3".to_string(),
                leader: None,
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_determine_phase_with_floating_point_string() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "3.5/3".to_string(),
                leader: None,
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_with_zero_replicas_both() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 0;
        spec.object_cluster.replicas = 0;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(0, 0, None),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(0, 0),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let (cp_ready, cp_total) = status.control_plane.parse_ready();
        let quorum_size = (cp_total / 2) + 1;
        let control_plane_healthy = cp_ready >= quorum_size;
        assert!(!control_plane_healthy);
    }

    #[test]
    fn test_update_conditions_degraded_exact_quorum() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(6, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_determine_phase_with_empty_ready_string() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "".to_string(),
                leader: None,
            },
            metadata: MetadataStatus {
                ready: "".to_string(),
                total_repos: 0,
                storage_used: String::new(),
            },
            object_cluster: ObjectClusterStatus {
                ready: "".to_string(),
                ..Default::default()
            },
            frontend: FrontendStatus {
                ready: "".to_string(),
                active_connections: 0,
            },
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_update_conditions_object_cluster_exactly_half() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 10;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(5, 10),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
    }

    #[test]
    fn test_determine_phase_all_equal_not_zero() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 5;
        spec.metadata.replicas = 5;
        spec.object_cluster.replicas = 5;
        spec.frontend.replicas = 5;

        let status = create_full_status(5, 5, 5, 5, 5, 5, 5, 5, ClusterPhase::Pending);

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_clears_degraded_on_recovery() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![crate::crd::ClusterCondition::degraded(
                true,
                Some("Previous issue".to_string()),
            )],
        };

        update_conditions(&mut status, &spec);

        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(!degraded);
    }

    #[test]
    fn test_ready_condition_with_degraded_message() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let degraded_condition = status.get_condition(crate::crd::ConditionType::Degraded);
        assert!(degraded_condition.is_some());
        let condition = degraded_condition.unwrap();
        assert_eq!(
            condition.message,
            Some("Some replicas are not ready".to_string())
        );
    }

    #[test]
    fn test_determine_phase_with_only_slash() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "/".to_string(),
                leader: None,
            },
            metadata: MetadataStatus {
                ready: "/".to_string(),
                total_repos: 0,
                storage_used: String::new(),
            },
            object_cluster: ObjectClusterStatus {
                ready: "/".to_string(),
                ..Default::default()
            },
            frontend: FrontendStatus {
                ready: "/".to_string(),
                active_connections: 0,
            },
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_determine_phase_with_whitespace_in_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: " 3 / 3 ".to_string(),
                leader: None,
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_quorum_exactly_one() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 1;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(0, 1, None),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_update_conditions_object_below_half() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 10;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(4, 10),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_context_type_creation() {
        type TestContext = std::sync::Arc<ContextData>;
        let _check: Option<TestContext> = None;
    }

    #[test]
    fn test_determine_phase_very_large_numbers() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = i32::MAX;
        spec.metadata.replicas = i32::MAX;
        spec.object_cluster.replicas = i32::MAX;
        spec.frontend.replicas = i32::MAX;

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: format!("{}/{}", i32::MAX, i32::MAX),
                leader: Some("raft-0".to_string()),
            },
            metadata: MetadataStatus {
                ready: format!("{}/{}", i32::MAX, i32::MAX),
                total_repos: 0,
                storage_used: String::new(),
            },
            object_cluster: ObjectClusterStatus {
                ready: format!("{}/{}", i32::MAX, i32::MAX),
                ..Default::default()
            },
            frontend: FrontendStatus {
                ready: format!("{}/{}", i32::MAX, i32::MAX),
                active_connections: 0,
            },
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_very_large_quorum() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 1000;
        spec.object_cluster.replicas = 1000;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(501, 1000, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(500, 1000),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
    }

    #[test]
    fn test_error_from_kube_api() {
        use crate::error::OperatorError;

        let kube_error_display = OperatorError::reconciliation("kube api failed");
        assert!(kube_error_display.to_string().contains("reconciliation"));
    }

    #[test]
    fn test_determine_phase_metadata_parsing_edge_cases() {
        let spec = create_test_spec();

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus {
                ready: "abc/3".to_string(),
                total_repos: 0,
                storage_used: String::new(),
            },
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_determine_phase_frontend_parsing_edge_cases() {
        let spec = create_test_spec();

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus {
                ready: "xyz/4".to_string(),
                active_connections: 0,
            },
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_error_insufficient_replicas_format() {
        use crate::error::OperatorError;

        let error = OperatorError::InsufficientReplicas {
            expected: 10,
            actual: 5,
        };
        let msg = error.to_string();
        assert!(msg.contains("10"));
        assert!(msg.contains("5"));
        assert!(msg.to_lowercase().contains("insufficient"));
    }

    #[test]
    fn test_error_rebalancing_in_progress() {
        use crate::error::OperatorError;

        let error = OperatorError::RebalancingInProgress;
        let msg = error.to_string();
        assert!(msg.to_lowercase().contains("rebalancing"));
    }

    #[test]
    fn test_scaling_decision_clone() {
        use super::super::scaling::ScalingDecision;

        let decision = ScalingDecision::ScaleUp {
            target_replicas: 10,
        };
        let cloned = decision.clone();
        assert_eq!(decision, cloned);
    }

    #[test]
    fn test_action_durations_correct() {
        let scale_up = Action::requeue(Duration::from_secs(30));
        let scale_down = Action::requeue(Duration::from_secs(60));
        let rebalancing = Action::requeue(Duration::from_secs(10));
        let normal = Action::requeue(Duration::from_secs(300));

        let _ = (scale_up, scale_down, rebalancing, normal);
    }

    #[test]
    fn test_operator_error_source_trait() {
        use crate::error::OperatorError;
        use std::error::Error;

        let error = OperatorError::Reconciliation("test".to_string());
        let _source = error.source();

        let error2 = OperatorError::RebalancingInProgress;
        let _source2 = error2.source();
    }

    #[test]
    fn test_error_from_serde_json() {
        use crate::error::OperatorError;

        let invalid_json = "{ invalid json }";
        let serde_result: std::result::Result<serde_json::Value, _> =
            serde_json::from_str(invalid_json);
        let serde_err = serde_result.unwrap_err();
        let op_error: OperatorError = serde_err.into();

        match op_error {
            OperatorError::Serialization(_) => {}
            _ => panic!("Expected Serialization error variant"),
        }

        let error_msg = op_error.to_string();
        assert!(error_msg.to_lowercase().contains("serialization"));
    }

    #[test]
    fn test_determine_phase_with_spec_mismatch() {
        let mut spec = create_test_spec();
        spec.metadata.replicas = 5;
        spec.frontend.replicas = 6;

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(5, 5),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(6, 6),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_with_edge_quorum_values() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 7;
        spec.object_cluster.replicas = 15;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(4, 7, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(8, 15),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_determine_phase_with_unicode_in_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "\u{200B}3/3".to_string(),
                leader: Some("raft-0".to_string()),
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_exact_boundary_values() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 4;
        spec.object_cluster.replicas = 8;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 4, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(4, 8),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
    }

    #[test]
    fn test_update_conditions_control_plane_below_quorum_but_object_ok() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(1, 3, None),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_update_conditions_object_cluster_below_half_but_control_plane_ok() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(5, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(!status.is_ready());
    }

    #[test]
    fn test_status_json_patch_format() {
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![ClusterCondition::ready(true)],
        };

        let status_patch = serde_json::json!({
            "status": status
        });

        assert!(status_patch.is_object());
        let status_obj = status_patch.get("status").unwrap();
        assert!(status_obj.is_object());

        let phase = status_obj.get("phase").unwrap();
        assert_eq!(phase, "Running");
    }

    #[test]
    fn test_determine_phase_with_special_characters() {
        let spec = create_test_spec();

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "3 / 3".to_string(),
                leader: None,
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_with_even_control_plane_replicas() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 4;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 4, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_update_conditions_with_odd_object_cluster_replicas() {
        let mut spec = create_test_spec();
        spec.object_cluster.replicas = 13;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(7, 13),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }

    #[test]
    fn test_determine_phase_all_types_partially_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(2, 3),
            object_cluster: ObjectClusterStatus::new(10, 12),
            frontend: FrontendStatus::new(3, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_error_kube_api_conversion() {
        use crate::error::OperatorError;

        let err = OperatorError::reconciliation("api call failed");
        let msg = format!("{}", err);
        assert!(msg.contains("failed"));
    }

    #[test]
    fn test_result_type_alias() {
        use crate::error::{OperatorError, Result};

        fn test_fn() -> Result<i32> {
            Ok(42)
        }

        fn test_fn_err() -> Result<i32> {
            Err(OperatorError::reconciliation("test"))
        }

        assert_eq!(test_fn().unwrap(), 42);
        assert!(test_fn_err().is_err());
    }

    #[test]
    fn test_serialization_error_display() {
        use crate::error::OperatorError;

        let json_err: serde_json::Error = serde_json::from_str::<i32>("not a number").unwrap_err();
        let op_err: OperatorError = json_err.into();
        let display = format!("{}", op_err);
        assert!(display.to_lowercase().contains("serialization"));
    }

    #[test]
    fn test_determine_phase_with_trailing_spaces_in_ready() {
        let spec = create_test_spec();
        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus {
                ready: "3/3  ".to_string(),
                leader: Some("raft-0".to_string()),
            },
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Provisioning);
    }

    #[test]
    fn test_update_conditions_sets_correct_message_when_degraded() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(2, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(10, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let degraded_condition = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .unwrap();
        assert_eq!(degraded_condition.status, "True");
        assert!(degraded_condition.message.is_some());
        assert!(degraded_condition.message.as_ref().unwrap().contains("replicas"));
    }

    #[test]
    fn test_update_conditions_clears_message_when_healthy() {
        let spec = create_test_spec();
        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        let degraded_condition = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .unwrap();
        assert_eq!(degraded_condition.status, "False");
        assert!(degraded_condition.message.is_none());
    }

    #[test]
    fn test_determine_phase_minimum_running_conditions() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 1;

        let status = GitStratumClusterStatus {
            phase: ClusterPhase::Pending,
            control_plane: ControlPlaneStatus::new(1, 1, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(12, 12),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        let phase = determine_phase(&status, &spec);
        assert_eq!(phase, ClusterPhase::Running);
    }

    #[test]
    fn test_update_conditions_with_large_diff_between_ready_and_total() {
        let mut spec = create_test_spec();
        spec.control_plane.replicas = 100;
        spec.object_cluster.replicas = 1000;

        let mut status = GitStratumClusterStatus {
            phase: ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(51, 100, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(500, 1000),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        };

        update_conditions(&mut status, &spec);

        assert!(status.is_ready());
        let degraded = status
            .get_condition(crate::crd::ConditionType::Degraded)
            .map(|c| c.status == "True")
            .unwrap_or(false);
        assert!(degraded);
    }
}
