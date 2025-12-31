use gitstratum_operator::{
    build_control_plane_pdb, build_control_plane_service, build_control_plane_statefulset,
    build_frontend_deployment, build_frontend_hpa, build_frontend_pdb, build_frontend_service,
    build_metadata_pdb, build_metadata_service, build_metadata_statefulset, build_object_pdb,
    build_object_service, build_object_statefulset, ClusterCondition, ClusterPhase,
    ConditionType, ControlPlaneSpec, ControlPlaneStatus, FrontendAutoScalingSpec, FrontendSpec,
    FrontendStatus, GitStratumCluster, GitStratumClusterSpec, GitStratumClusterStatus,
    MetadataSpec, MetadataStatus, ObjectAutoScalingSpec, ObjectClusterSpec, ObjectClusterStatus,
    OperatorError, ReplicationMode, ResourceRequirements, ScalingDecision, ServicePort,
    ServiceSpec, ServiceType,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

fn create_full_test_cluster() -> GitStratumCluster {
    GitStratumCluster {
        metadata: ObjectMeta {
            name: Some("production".to_string()),
            namespace: Some("gitstratum".to_string()),
            uid: Some("uid-12345-abcde".to_string()),
            ..Default::default()
        },
        spec: GitStratumClusterSpec {
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
                replication_mode: ReplicationMode::Sync,
            },
            object_cluster: ObjectClusterSpec {
                replicas: 12,
                resources: ResourceRequirements {
                    cpu: "4".to_string(),
                    memory: "16Gi".to_string(),
                    storage: Some("500Gi".to_string()),
                },
                storage_class: "nvme-fast".to_string(),
                auto_scaling: Some(ObjectAutoScalingSpec {
                    enabled: true,
                    min_replicas: 6,
                    max_replicas: 100,
                    target_disk_utilization: 80,
                }),
            },
            frontend: FrontendSpec {
                replicas: 4,
                resources: ResourceRequirements {
                    cpu: "2".to_string(),
                    memory: "8Gi".to_string(),
                    storage: None,
                },
                auto_scaling: Some(FrontendAutoScalingSpec {
                    enabled: true,
                    min_replicas: 2,
                    max_replicas: 50,
                    target_cpu_utilization: 70,
                    target_connections_per_pod: 500,
                }),
                service: ServiceSpec {
                    service_type: ServiceType::LoadBalancer,
                    ports: vec![
                        ServicePort {
                            name: "https".to_string(),
                            port: 443,
                            target_port: None,
                        },
                        ServicePort {
                            name: "ssh".to_string(),
                            port: 22,
                            target_port: None,
                        },
                    ],
                },
            },
        },
        status: None,
    }
}

#[test]
fn test_full_cluster_resource_generation() {
    let cluster = create_full_test_cluster();

    let cp_sts = build_control_plane_statefulset(&cluster);
    assert!(cp_sts.metadata.name.is_some());
    assert!(cp_sts.spec.is_some());

    let cp_svc = build_control_plane_service(&cluster);
    assert!(cp_svc.metadata.name.is_some());

    let cp_pdb = build_control_plane_pdb(&cluster);
    assert!(cp_pdb.metadata.name.is_some());

    let meta_sts = build_metadata_statefulset(&cluster);
    assert!(meta_sts.metadata.name.is_some());

    let meta_svc = build_metadata_service(&cluster);
    assert!(meta_svc.metadata.name.is_some());

    let meta_pdb = build_metadata_pdb(&cluster);
    assert!(meta_pdb.metadata.name.is_some());

    let obj_sts = build_object_statefulset(&cluster);
    assert!(obj_sts.metadata.name.is_some());

    let obj_svc = build_object_service(&cluster);
    assert!(obj_svc.metadata.name.is_some());

    let obj_pdb = build_object_pdb(&cluster);
    assert!(obj_pdb.metadata.name.is_some());

    let fe_deploy = build_frontend_deployment(&cluster);
    assert!(fe_deploy.metadata.name.is_some());

    let fe_svc = build_frontend_service(&cluster);
    assert!(fe_svc.metadata.name.is_some());

    let fe_pdb = build_frontend_pdb(&cluster);
    assert!(fe_pdb.metadata.name.is_some());

    let fe_hpa = build_frontend_hpa(&cluster);
    assert!(fe_hpa.metadata.name.is_some());
}

#[test]
fn test_cluster_status_lifecycle() {
    let mut status = GitStratumClusterStatus::default();
    assert_eq!(status.phase, ClusterPhase::Pending);
    assert!(!status.is_ready());

    status.control_plane = ControlPlaneStatus::new(1, 3, None);
    status.phase = ClusterPhase::Provisioning;
    assert!(!status.is_ready());

    status.control_plane = ControlPlaneStatus::new(3, 3, Some("raft-0".to_string()));
    status.metadata = MetadataStatus::new(3, 3);
    status.object_cluster = ObjectClusterStatus::new(12, 12);
    status.frontend = FrontendStatus::new(4, 4);
    status.phase = ClusterPhase::Running;

    status.set_condition(ClusterCondition::ready(true));
    assert!(status.is_ready());

    status.set_condition(ClusterCondition::scaling(true, Some("ScaleUp".to_string())));
    assert!(status.is_scaling());

    status.set_condition(ClusterCondition::rebalancing(true));
    assert!(status.is_rebalancing());

    status.set_condition(ClusterCondition::rebalancing(false));
    assert!(!status.is_rebalancing());
}

#[test]
fn test_operator_error_types() {
    let kube_err = OperatorError::reconciliation("test error");
    assert!(kube_err.to_string().contains("test error"));

    let spec_err = OperatorError::invalid_spec("invalid replicas");
    assert!(spec_err.to_string().contains("invalid replicas"));

    let not_found = OperatorError::resource_not_found("StatefulSet/test");
    assert!(not_found.to_string().contains("StatefulSet/test"));

    let scaling_err = OperatorError::scaling("cannot scale during rebalance");
    assert!(scaling_err.to_string().contains("cannot scale"));

    let finalizer_err = OperatorError::finalizer("cleanup failed");
    assert!(finalizer_err.to_string().contains("cleanup failed"));

    let rebalancing = OperatorError::RebalancingInProgress;
    assert!(rebalancing.to_string().contains("rebalancing"));

    let insufficient = OperatorError::InsufficientReplicas {
        expected: 3,
        actual: 1,
    };
    assert!(insufficient.to_string().contains("3"));
    assert!(insufficient.to_string().contains("1"));
}

#[test]
fn test_scaling_decisions() {
    let cluster = create_full_test_cluster();
    let mut status = GitStratumClusterStatus::default();
    status.object_cluster = ObjectClusterStatus::new(12, 12);
    status.object_cluster.storage_used = "3Ti".to_string();
    status.object_cluster.storage_capacity = "6Ti".to_string();

    let decision =
        gitstratum_operator::controllers::scaling::check_autoscaling(&cluster, &status);
    assert_eq!(decision, ScalingDecision::NoChange);

    status.object_cluster.storage_used = "4.5Ti".to_string();
    status.object_cluster.storage_capacity = "5Ti".to_string();
    let decision =
        gitstratum_operator::controllers::scaling::check_autoscaling(&cluster, &status);
    assert!(matches!(decision, ScalingDecision::ScaleUp { .. }));
}

#[test]
fn test_condition_management() {
    let mut status = GitStratumClusterStatus::default();

    status.set_condition(ClusterCondition::ready(false));
    assert!(!status.is_ready());

    let cond = status.get_condition(ConditionType::Ready);
    assert!(cond.is_some());
    assert_eq!(cond.unwrap().status, "False");

    status.set_condition(ClusterCondition::ready(true));
    assert!(status.is_ready());

    assert_eq!(status.conditions.len(), 1);

    status.set_condition(ClusterCondition::degraded(true, Some("test".to_string())));
    assert_eq!(status.conditions.len(), 2);
}

#[test]
fn test_replication_mode_default() {
    let mode = ReplicationMode::default();
    assert_eq!(mode, ReplicationMode::Sync);
}

#[test]
fn test_service_type_default() {
    let svc_type = ServiceType::default();
    assert_eq!(svc_type, ServiceType::ClusterIP);
}

#[test]
fn test_cluster_phase_display() {
    assert_eq!(ClusterPhase::Pending.as_str(), "Pending");
    assert_eq!(ClusterPhase::Provisioning.as_str(), "Provisioning");
    assert_eq!(ClusterPhase::Running.as_str(), "Running");
    assert_eq!(ClusterPhase::Degraded.as_str(), "Degraded");
    assert_eq!(ClusterPhase::Failed.as_str(), "Failed");
}

#[test]
fn test_status_parsing() {
    let cp_status = ControlPlaneStatus::new(2, 3, Some("raft-1".to_string()));
    let (ready, total) = cp_status.parse_ready();
    assert_eq!(ready, 2);
    assert_eq!(total, 3);

    let obj_status = ObjectClusterStatus::new(10, 12);
    let (ready, total) = obj_status.parse_ready();
    assert_eq!(ready, 10);
    assert_eq!(total, 12);
}

#[test]
fn test_all_resource_names_follow_convention() {
    let cluster = create_full_test_cluster();
    let name = "production";

    let cp_sts = build_control_plane_statefulset(&cluster);
    assert_eq!(
        cp_sts.metadata.name.as_ref().unwrap(),
        &format!("{}-control-plane", name)
    );

    let meta_sts = build_metadata_statefulset(&cluster);
    assert_eq!(
        meta_sts.metadata.name.as_ref().unwrap(),
        &format!("{}-metadata", name)
    );

    let obj_sts = build_object_statefulset(&cluster);
    assert_eq!(
        obj_sts.metadata.name.as_ref().unwrap(),
        &format!("{}-object", name)
    );

    let fe_deploy = build_frontend_deployment(&cluster);
    assert_eq!(
        fe_deploy.metadata.name.as_ref().unwrap(),
        &format!("{}-frontend", name)
    );
}

#[test]
fn test_pdb_min_available_calculation() {
    let cluster = create_full_test_cluster();

    let cp_pdb = build_control_plane_pdb(&cluster);
    let cp_spec = cp_pdb.spec.as_ref().unwrap();
    assert_eq!(
        cp_spec.min_available,
        Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
    );

    let meta_pdb = build_metadata_pdb(&cluster);
    let meta_spec = meta_pdb.spec.as_ref().unwrap();
    assert_eq!(
        meta_spec.min_available,
        Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
    );
}

#[test]
fn test_object_cluster_parallel_pod_management() {
    let cluster = create_full_test_cluster();
    let obj_sts = build_object_statefulset(&cluster);
    let spec = obj_sts.spec.as_ref().unwrap();

    assert_eq!(spec.pod_management_policy, Some("Parallel".to_string()));
}

#[test]
fn test_hpa_configuration() {
    let cluster = create_full_test_cluster();
    let hpa = build_frontend_hpa(&cluster);
    let spec = hpa.spec.as_ref().unwrap();

    assert_eq!(spec.min_replicas, Some(2));
    assert_eq!(spec.max_replicas, 50);

    let behavior = spec.behavior.as_ref().unwrap();
    let scale_up = behavior.scale_up.as_ref().unwrap();
    assert_eq!(scale_up.stabilization_window_seconds, Some(30));

    let scale_down = behavior.scale_down.as_ref().unwrap();
    assert_eq!(scale_down.stabilization_window_seconds, Some(300));
}

#[test]
fn test_scaling_helper_functions() {
    use gitstratum_operator::controllers::scaling::{
        calculate_scale_down_target, calculate_scale_up_target, can_safely_scale_down,
        estimate_rebalance_time_seconds,
    };

    let target = calculate_scale_up_target(10, 85, 80, 100);
    assert!(target > 10);

    let target = calculate_scale_down_target(12, 6);
    assert_eq!(target, 11);

    assert!(can_safely_scale_down(12, 6, 2, false));
    assert!(!can_safely_scale_down(12, 6, 2, true));
    assert!(!can_safely_scale_down(6, 6, 2, false));

    let time = estimate_rebalance_time_seconds(500.0, 10.0);
    assert!(time > 0.0);
}
