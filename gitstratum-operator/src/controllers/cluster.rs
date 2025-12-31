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
            tracing::info!(
                "Scaling up object cluster to {} replicas",
                target_replicas
            );
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
    } else if cp_total == 0 {
        ClusterPhase::Pending
    } else {
        ClusterPhase::Pending
    }
}

fn update_conditions(status: &mut GitStratumClusterStatus, spec: &crate::crd::GitStratumClusterSpec) {
    let (cp_ready, cp_total) = status.control_plane.parse_ready();
    let (obj_ready, obj_total) = status.object_cluster.parse_ready();

    let quorum_size = (cp_total / 2) + 1;
    let control_plane_healthy = cp_ready >= quorum_size;

    let object_cluster_healthy = obj_ready >= (obj_total / 2);

    let is_ready = control_plane_healthy && object_cluster_healthy && status.phase == ClusterPhase::Running;
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
            },
            metadata: MetadataSpec {
                replicas: 3,
                resources: ResourceRequirements {
                    cpu: "4".to_string(),
                    memory: "32Gi".to_string(),
                    storage: Some("500Gi".to_string()),
                },
                storage_class: "nvme-fast".to_string(),
                replication_mode: Default::default(),
            },
            object_cluster: ObjectClusterSpec {
                replicas: 12,
                resources: ResourceRequirements {
                    cpu: "4".to_string(),
                    memory: "16Gi".to_string(),
                    storage: Some("500Gi".to_string()),
                },
                storage_class: "nvme-fast".to_string(),
                auto_scaling: None,
            },
            frontend: FrontendSpec {
                replicas: 4,
                resources: ResourceRequirements {
                    cpu: "2".to_string(),
                    memory: "8Gi".to_string(),
                    storage: None,
                },
                auto_scaling: None,
                service: Default::default(),
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
}
