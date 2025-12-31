use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment, StatefulSet,
    StatefulSetSpec, StatefulSetUpdateStrategy,
};
use k8s_openapi::api::autoscaling::v2::{
    CrossVersionObjectReference, HorizontalPodAutoscaler, HorizontalPodAutoscalerBehavior,
    HorizontalPodAutoscalerSpec, HPAScalingPolicy, HPAScalingRules, MetricSpec, MetricTarget,
    ResourceMetricSource,
};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe,
    ResourceRequirements as K8sResourceRequirements, Service, ServicePort, ServiceSpec as K8sServiceSpec,
    VolumeMount,
};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;

use crate::crd::{GitStratumCluster, ServiceType};

const CONTROL_PLANE_PORT: i32 = 9000;
const METADATA_PORT: i32 = 9001;
const OBJECT_PORT: i32 = 9002;
const METRICS_PORT: i32 = 9090;

fn cluster_name(cluster: &GitStratumCluster) -> String {
    cluster.name_any()
}

fn labels(cluster: &GitStratumCluster, component: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), "gitstratum".to_string());
    labels.insert(
        "app.kubernetes.io/instance".to_string(),
        cluster_name(cluster),
    );
    labels.insert("app.kubernetes.io/component".to_string(), component.to_string());
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "gitstratum-operator".to_string(),
    );
    labels
}

fn owner_reference(cluster: &GitStratumCluster) -> Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference> {
    vec![k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: "gitstratum.io/v1".to_string(),
        kind: "GitStratumCluster".to_string(),
        name: cluster_name(cluster),
        uid: cluster.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }]
}

pub fn build_control_plane_statefulset(cluster: &GitStratumCluster) -> StatefulSet {
    let name = format!("{}-control-plane", cluster_name(cluster));
    let labels = labels(cluster, "control-plane");
    let spec = &cluster.spec.control_plane;

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            service_name: format!("{}-headless", name),
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "control-plane".to_string(),
                        image: Some("gitstratum/control-plane:latest".to_string()),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: CONTROL_PLANE_PORT,
                                name: Some("grpc".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: METRICS_PORT,
                                name: Some("metrics".to_string()),
                                ..Default::default()
                            },
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name: "POD_NAME".to_string(),
                                value_from: Some(EnvVarSource {
                                    field_ref: Some(ObjectFieldSelector {
                                        field_path: "metadata.name".to_string(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "STORAGE_PATH".to_string(),
                                value: Some("/data/raft".to_string()),
                                ..Default::default()
                            },
                        ]),
                        resources: Some(build_resource_requirements(&spec.resources)),
                        volume_mounts: Some(vec![VolumeMount {
                            name: "data".to_string(),
                            mount_path: "/data".to_string(),
                            ..Default::default()
                        }]),
                        readiness_probe: Some(Probe {
                            grpc: Some(k8s_openapi::api::core::v1::GRPCAction {
                                port: CONTROL_PLANE_PORT,
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(5),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        liveness_probe: Some(Probe {
                            grpc: Some(k8s_openapi::api::core::v1::GRPCAction {
                                port: CONTROL_PLANE_PORT,
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(15),
                            period_seconds: Some(20),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    storage_class_name: Some(spec.storage_class.clone()),
                    resources: Some(K8sResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert(
                                "storage".to_string(),
                                Quantity(spec.resources.storage.clone().unwrap_or_else(|| "50Gi".to_string())),
                            );
                            map
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_control_plane_service(cluster: &GitStratumCluster) -> Service {
    let name = format!("{}-control-plane", cluster_name(cluster));
    let labels = labels(cluster, "control-plane");

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            selector: Some(labels),
            ports: Some(vec![
                ServicePort {
                    name: Some("grpc".to_string()),
                    port: CONTROL_PLANE_PORT,
                    target_port: Some(IntOrString::Int(CONTROL_PLANE_PORT)),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: METRICS_PORT,
                    target_port: Some(IntOrString::Int(METRICS_PORT)),
                    ..Default::default()
                },
            ]),
            cluster_ip: Some("None".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_control_plane_pdb(cluster: &GitStratumCluster) -> PodDisruptionBudget {
    let name = format!("{}-control-plane-pdb", cluster_name(cluster));
    let labels = labels(cluster, "control-plane");

    let min_available = (cluster.spec.control_plane.replicas / 2) + 1;

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            selector: Some(LabelSelector {
                match_labels: Some(labels),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_metadata_statefulset(cluster: &GitStratumCluster) -> StatefulSet {
    let name = format!("{}-metadata", cluster_name(cluster));
    let labels = labels(cluster, "metadata");
    let spec = &cluster.spec.metadata;

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            service_name: format!("{}-headless", name),
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "metadata".to_string(),
                        image: Some("gitstratum/metadata:latest".to_string()),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: METADATA_PORT,
                                name: Some("grpc".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: METRICS_PORT,
                                name: Some("metrics".to_string()),
                                ..Default::default()
                            },
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name: "POD_NAME".to_string(),
                                value_from: Some(EnvVarSource {
                                    field_ref: Some(ObjectFieldSelector {
                                        field_path: "metadata.name".to_string(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "STORAGE_PATH".to_string(),
                                value: Some("/data/metadata".to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "CONTROL_PLANE_ADDR".to_string(),
                                value: Some(format!(
                                    "{}-control-plane:{}",
                                    cluster_name(cluster),
                                    CONTROL_PLANE_PORT
                                )),
                                ..Default::default()
                            },
                        ]),
                        resources: Some(build_resource_requirements(&spec.resources)),
                        volume_mounts: Some(vec![VolumeMount {
                            name: "data".to_string(),
                            mount_path: "/data".to_string(),
                            ..Default::default()
                        }]),
                        readiness_probe: Some(Probe {
                            grpc: Some(k8s_openapi::api::core::v1::GRPCAction {
                                port: METADATA_PORT,
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(5),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        liveness_probe: Some(Probe {
                            grpc: Some(k8s_openapi::api::core::v1::GRPCAction {
                                port: METADATA_PORT,
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(15),
                            period_seconds: Some(20),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    storage_class_name: Some(spec.storage_class.clone()),
                    resources: Some(K8sResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert(
                                "storage".to_string(),
                                Quantity(spec.resources.storage.clone().unwrap_or_else(|| "500Gi".to_string())),
                            );
                            map
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_metadata_service(cluster: &GitStratumCluster) -> Service {
    let name = format!("{}-metadata", cluster_name(cluster));
    let labels = labels(cluster, "metadata");

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            selector: Some(labels),
            ports: Some(vec![
                ServicePort {
                    name: Some("grpc".to_string()),
                    port: METADATA_PORT,
                    target_port: Some(IntOrString::Int(METADATA_PORT)),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: METRICS_PORT,
                    target_port: Some(IntOrString::Int(METRICS_PORT)),
                    ..Default::default()
                },
            ]),
            cluster_ip: Some("None".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_metadata_pdb(cluster: &GitStratumCluster) -> PodDisruptionBudget {
    let name = format!("{}-metadata-pdb", cluster_name(cluster));
    let labels = labels(cluster, "metadata");

    let min_available = (cluster.spec.metadata.replicas / 2) + 1;

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            selector: Some(LabelSelector {
                match_labels: Some(labels),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_object_statefulset(cluster: &GitStratumCluster) -> StatefulSet {
    let name = format!("{}-object", cluster_name(cluster));
    let labels = labels(cluster, "object");
    let spec = &cluster.spec.object_cluster;

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            service_name: format!("{}-headless", name),
            replicas: Some(spec.replicas),
            pod_management_policy: Some("Parallel".to_string()),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "object".to_string(),
                        image: Some("gitstratum/object:latest".to_string()),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: OBJECT_PORT,
                                name: Some("grpc".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: METRICS_PORT,
                                name: Some("metrics".to_string()),
                                ..Default::default()
                            },
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name: "POD_NAME".to_string(),
                                value_from: Some(EnvVarSource {
                                    field_ref: Some(ObjectFieldSelector {
                                        field_path: "metadata.name".to_string(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "STORAGE_PATH".to_string(),
                                value: Some("/data/blobs".to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "CONTROL_PLANE_ADDR".to_string(),
                                value: Some(format!(
                                    "{}-control-plane:{}",
                                    cluster_name(cluster),
                                    CONTROL_PLANE_PORT
                                )),
                                ..Default::default()
                            },
                        ]),
                        resources: Some(build_resource_requirements(&spec.resources)),
                        volume_mounts: Some(vec![VolumeMount {
                            name: "data".to_string(),
                            mount_path: "/data".to_string(),
                            ..Default::default()
                        }]),
                        readiness_probe: Some(Probe {
                            grpc: Some(k8s_openapi::api::core::v1::GRPCAction {
                                port: OBJECT_PORT,
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(5),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        liveness_probe: Some(Probe {
                            grpc: Some(k8s_openapi::api::core::v1::GRPCAction {
                                port: OBJECT_PORT,
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(15),
                            period_seconds: Some(20),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    storage_class_name: Some(spec.storage_class.clone()),
                    resources: Some(K8sResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert(
                                "storage".to_string(),
                                Quantity(spec.resources.storage.clone().unwrap_or_else(|| "500Gi".to_string())),
                            );
                            map
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_object_service(cluster: &GitStratumCluster) -> Service {
    let name = format!("{}-object", cluster_name(cluster));
    let labels = labels(cluster, "object");

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            selector: Some(labels),
            ports: Some(vec![
                ServicePort {
                    name: Some("grpc".to_string()),
                    port: OBJECT_PORT,
                    target_port: Some(IntOrString::Int(OBJECT_PORT)),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: METRICS_PORT,
                    target_port: Some(IntOrString::Int(METRICS_PORT)),
                    ..Default::default()
                },
            ]),
            cluster_ip: Some("None".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_object_pdb(cluster: &GitStratumCluster) -> PodDisruptionBudget {
    let name = format!("{}-object-pdb", cluster_name(cluster));
    let labels = labels(cluster, "object");

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            max_unavailable: Some(IntOrString::Int(1)),
            selector: Some(LabelSelector {
                match_labels: Some(labels),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_frontend_deployment(cluster: &GitStratumCluster) -> Deployment {
    let name = format!("{}-frontend", cluster_name(cluster));
    let labels = labels(cluster, "frontend");
    let spec = &cluster.spec.frontend;

    Deployment {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "frontend".to_string(),
                        image: Some("gitstratum/frontend:latest".to_string()),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: 443,
                                name: Some("https".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: 22,
                                name: Some("ssh".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: METRICS_PORT,
                                name: Some("metrics".to_string()),
                                ..Default::default()
                            },
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name: "METADATA_ADDR".to_string(),
                                value: Some(format!(
                                    "{}-metadata:{}",
                                    cluster_name(cluster),
                                    METADATA_PORT
                                )),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "CONTROL_PLANE_ADDR".to_string(),
                                value: Some(format!(
                                    "{}-control-plane:{}",
                                    cluster_name(cluster),
                                    CONTROL_PLANE_PORT
                                )),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "OBJECT_SERVICE_ADDR".to_string(),
                                value: Some(format!(
                                    "{}-object:{}",
                                    cluster_name(cluster),
                                    OBJECT_PORT
                                )),
                                ..Default::default()
                            },
                        ]),
                        resources: Some(build_resource_requirements(&spec.resources)),
                        readiness_probe: Some(Probe {
                            tcp_socket: Some(k8s_openapi::api::core::v1::TCPSocketAction {
                                port: IntOrString::Int(443),
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(5),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        liveness_probe: Some(Probe {
                            tcp_socket: Some(k8s_openapi::api::core::v1::TCPSocketAction {
                                port: IntOrString::Int(443),
                                ..Default::default()
                            }),
                            initial_delay_seconds: Some(15),
                            period_seconds: Some(20),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_string()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_surge: Some(IntOrString::String("25%".to_string())),
                    max_unavailable: Some(IntOrString::String("25%".to_string())),
                }),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_frontend_service(cluster: &GitStratumCluster) -> Service {
    let name = format!("{}-frontend", cluster_name(cluster));
    let labels = labels(cluster, "frontend");
    let spec = &cluster.spec.frontend.service;

    let service_type = match spec.service_type {
        ServiceType::ClusterIP => "ClusterIP",
        ServiceType::LoadBalancer => "LoadBalancer",
        ServiceType::NodePort => "NodePort",
    };

    let ports = if spec.ports.is_empty() {
        vec![
            ServicePort {
                name: Some("https".to_string()),
                port: 443,
                target_port: Some(IntOrString::Int(443)),
                ..Default::default()
            },
            ServicePort {
                name: Some("ssh".to_string()),
                port: 22,
                target_port: Some(IntOrString::Int(22)),
                ..Default::default()
            },
        ]
    } else {
        spec.ports
            .iter()
            .map(|p| ServicePort {
                name: Some(p.name.clone()),
                port: p.port,
                target_port: Some(IntOrString::Int(p.target_port.unwrap_or(p.port))),
                ..Default::default()
            })
            .collect()
    };

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            type_: Some(service_type.to_string()),
            selector: Some(labels),
            ports: Some(ports),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_frontend_pdb(cluster: &GitStratumCluster) -> PodDisruptionBudget {
    let name = format!("{}-frontend-pdb", cluster_name(cluster));
    let labels = labels(cluster, "frontend");

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::String("50%".to_string())),
            selector: Some(LabelSelector {
                match_labels: Some(labels),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_frontend_hpa(cluster: &GitStratumCluster) -> HorizontalPodAutoscaler {
    let name = format!("{}-frontend-hpa", cluster_name(cluster));
    let labels = labels(cluster, "frontend");
    let auto_scaling = cluster
        .spec
        .frontend
        .auto_scaling
        .as_ref()
        .expect("auto_scaling must be set");

    HorizontalPodAutoscaler {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cluster.namespace(),
            labels: Some(labels),
            owner_references: Some(owner_reference(cluster)),
            ..Default::default()
        },
        spec: Some(HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                api_version: Some("apps/v1".to_string()),
                kind: "Deployment".to_string(),
                name: format!("{}-frontend", cluster_name(cluster)),
            },
            min_replicas: Some(auto_scaling.min_replicas),
            max_replicas: auto_scaling.max_replicas,
            metrics: Some(vec![MetricSpec {
                type_: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        type_: "Utilization".to_string(),
                        average_utilization: Some(auto_scaling.target_cpu_utilization),
                        ..Default::default()
                    },
                }),
                ..Default::default()
            }]),
            behavior: Some(HorizontalPodAutoscalerBehavior {
                scale_up: Some(HPAScalingRules {
                    stabilization_window_seconds: Some(30),
                    policies: Some(vec![HPAScalingPolicy {
                        type_: "Pods".to_string(),
                        value: 4,
                        period_seconds: 60,
                    }]),
                    ..Default::default()
                }),
                scale_down: Some(HPAScalingRules {
                    stabilization_window_seconds: Some(300),
                    policies: Some(vec![HPAScalingPolicy {
                        type_: "Pods".to_string(),
                        value: 2,
                        period_seconds: 60,
                    }]),
                    ..Default::default()
                }),
            }),
        }),
        ..Default::default()
    }
}

fn build_resource_requirements(
    spec: &crate::crd::ResourceRequirements,
) -> K8sResourceRequirements {
    let mut requests = BTreeMap::new();
    let mut limits = BTreeMap::new();

    requests.insert("cpu".to_string(), Quantity(spec.cpu.clone()));
    requests.insert("memory".to_string(), Quantity(spec.memory.clone()));
    limits.insert("cpu".to_string(), Quantity(spec.cpu.clone()));
    limits.insert("memory".to_string(), Quantity(spec.memory.clone()));

    K8sResourceRequirements {
        requests: Some(requests),
        limits: Some(limits),
        claims: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ControlPlaneSpec, FrontendAutoScalingSpec, FrontendSpec, GitStratumClusterSpec,
        MetadataSpec, ObjectClusterSpec, ResourceRequirements, ServicePort as CrdServicePort,
        ServiceSpec, ServiceType,
    };

    fn create_test_cluster() -> GitStratumCluster {
        GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("gitstratum".to_string()),
                uid: Some("test-uid-12345".to_string()),
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
                            CrdServicePort {
                                name: "https".to_string(),
                                port: 443,
                                target_port: None,
                            },
                            CrdServicePort {
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
    fn test_build_control_plane_statefulset() {
        let cluster = create_test_cluster();
        let sts = build_control_plane_statefulset(&cluster);

        assert_eq!(
            sts.metadata.name,
            Some("test-cluster-control-plane".to_string())
        );
        assert_eq!(sts.metadata.namespace, Some("gitstratum".to_string()));

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(3));
        assert_eq!(
            spec.service_name,
            "test-cluster-control-plane-headless".to_string()
        );

        let labels = sts.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("app.kubernetes.io/component"), Some(&"control-plane".to_string()));
    }

    #[test]
    fn test_build_control_plane_service() {
        let cluster = create_test_cluster();
        let svc = build_control_plane_service(&cluster);

        assert_eq!(
            svc.metadata.name,
            Some("test-cluster-control-plane".to_string())
        );

        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.cluster_ip, Some("None".to_string()));
    }

    #[test]
    fn test_build_control_plane_pdb() {
        let cluster = create_test_cluster();
        let pdb = build_control_plane_pdb(&cluster);

        assert_eq!(
            pdb.metadata.name,
            Some("test-cluster-control-plane-pdb".to_string())
        );

        let spec = pdb.spec.as_ref().unwrap();
        assert_eq!(spec.min_available, Some(IntOrString::Int(2)));
    }

    #[test]
    fn test_build_metadata_statefulset() {
        let cluster = create_test_cluster();
        let sts = build_metadata_statefulset(&cluster);

        assert_eq!(
            sts.metadata.name,
            Some("test-cluster-metadata".to_string())
        );

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(3));
    }

    #[test]
    fn test_build_metadata_service() {
        let cluster = create_test_cluster();
        let svc = build_metadata_service(&cluster);

        assert_eq!(svc.metadata.name, Some("test-cluster-metadata".to_string()));
    }

    #[test]
    fn test_build_metadata_pdb() {
        let cluster = create_test_cluster();
        let pdb = build_metadata_pdb(&cluster);

        assert_eq!(
            pdb.metadata.name,
            Some("test-cluster-metadata-pdb".to_string())
        );
    }

    #[test]
    fn test_build_object_statefulset() {
        let cluster = create_test_cluster();
        let sts = build_object_statefulset(&cluster);

        assert_eq!(sts.metadata.name, Some("test-cluster-object".to_string()));

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(12));
        assert_eq!(spec.pod_management_policy, Some("Parallel".to_string()));
    }

    #[test]
    fn test_build_object_service() {
        let cluster = create_test_cluster();
        let svc = build_object_service(&cluster);

        assert_eq!(svc.metadata.name, Some("test-cluster-object".to_string()));
    }

    #[test]
    fn test_build_object_pdb() {
        let cluster = create_test_cluster();
        let pdb = build_object_pdb(&cluster);

        assert_eq!(
            pdb.metadata.name,
            Some("test-cluster-object-pdb".to_string())
        );

        let spec = pdb.spec.as_ref().unwrap();
        assert_eq!(spec.max_unavailable, Some(IntOrString::Int(1)));
    }

    #[test]
    fn test_build_frontend_deployment() {
        let cluster = create_test_cluster();
        let deploy = build_frontend_deployment(&cluster);

        assert_eq!(
            deploy.metadata.name,
            Some("test-cluster-frontend".to_string())
        );

        let spec = deploy.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(4));
    }

    #[test]
    fn test_build_frontend_service() {
        let cluster = create_test_cluster();
        let svc = build_frontend_service(&cluster);

        assert_eq!(
            svc.metadata.name,
            Some("test-cluster-frontend".to_string())
        );

        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_, Some("LoadBalancer".to_string()));
    }

    #[test]
    fn test_build_frontend_service_default_ports() {
        let mut cluster = create_test_cluster();
        cluster.spec.frontend.service.ports = vec![];

        let svc = build_frontend_service(&cluster);
        let spec = svc.spec.as_ref().unwrap();
        let ports = spec.ports.as_ref().unwrap();

        assert_eq!(ports.len(), 2);
        assert_eq!(ports[0].port, 443);
        assert_eq!(ports[1].port, 22);
    }

    #[test]
    fn test_build_frontend_pdb() {
        let cluster = create_test_cluster();
        let pdb = build_frontend_pdb(&cluster);

        assert_eq!(
            pdb.metadata.name,
            Some("test-cluster-frontend-pdb".to_string())
        );

        let spec = pdb.spec.as_ref().unwrap();
        assert_eq!(
            spec.min_available,
            Some(IntOrString::String("50%".to_string()))
        );
    }

    #[test]
    fn test_build_frontend_hpa() {
        let cluster = create_test_cluster();
        let hpa = build_frontend_hpa(&cluster);

        assert_eq!(
            hpa.metadata.name,
            Some("test-cluster-frontend-hpa".to_string())
        );

        let spec = hpa.spec.as_ref().unwrap();
        assert_eq!(spec.min_replicas, Some(2));
        assert_eq!(spec.max_replicas, 50);
    }

    #[test]
    fn test_labels() {
        let cluster = create_test_cluster();
        let labels = labels(&cluster, "control-plane");

        assert_eq!(labels.get("app.kubernetes.io/name"), Some(&"gitstratum".to_string()));
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"test-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"control-plane".to_string())
        );
    }

    #[test]
    fn test_owner_reference() {
        let cluster = create_test_cluster();
        let refs = owner_reference(&cluster);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].kind, "GitStratumCluster");
        assert_eq!(refs[0].name, "test-cluster");
        assert!(refs[0].controller.unwrap());
    }

    #[test]
    fn test_service_type_cluster_ip() {
        let mut cluster = create_test_cluster();
        cluster.spec.frontend.service.service_type = ServiceType::ClusterIP;

        let svc = build_frontend_service(&cluster);
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_, Some("ClusterIP".to_string()));
    }

    #[test]
    fn test_service_type_nodeport() {
        let mut cluster = create_test_cluster();
        cluster.spec.frontend.service.service_type = ServiceType::NodePort;

        let svc = build_frontend_service(&cluster);
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_, Some("NodePort".to_string()));
    }

    #[test]
    fn test_frontend_service_custom_target_port() {
        let mut cluster = create_test_cluster();
        cluster.spec.frontend.service.ports = vec![
            CrdServicePort {
                name: "custom".to_string(),
                port: 8080,
                target_port: Some(8081),
            },
        ];

        let svc = build_frontend_service(&cluster);
        let spec = svc.spec.as_ref().unwrap();
        let ports = spec.ports.as_ref().unwrap();

        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].port, 8080);
        assert_eq!(ports[0].target_port, Some(IntOrString::Int(8081)));
    }

    #[test]
    fn test_control_plane_without_storage() {
        let mut cluster = create_test_cluster();
        cluster.spec.control_plane.resources.storage = None;

        let sts = build_control_plane_statefulset(&cluster);
        let spec = sts.spec.as_ref().unwrap();
        let pvcs = spec.volume_claim_templates.as_ref().unwrap();
        let pvc_spec = pvcs[0].spec.as_ref().unwrap();
        let requests = pvc_spec.resources.as_ref().unwrap().requests.as_ref().unwrap();
        let storage = requests.get("storage").unwrap();

        assert_eq!(storage.0, "50Gi");
    }

    #[test]
    fn test_metadata_without_storage() {
        let mut cluster = create_test_cluster();
        cluster.spec.metadata.resources.storage = None;

        let sts = build_metadata_statefulset(&cluster);
        let spec = sts.spec.as_ref().unwrap();
        let pvcs = spec.volume_claim_templates.as_ref().unwrap();
        let pvc_spec = pvcs[0].spec.as_ref().unwrap();
        let requests = pvc_spec.resources.as_ref().unwrap().requests.as_ref().unwrap();
        let storage = requests.get("storage").unwrap();

        assert_eq!(storage.0, "500Gi");
    }

    #[test]
    fn test_object_without_storage() {
        let mut cluster = create_test_cluster();
        cluster.spec.object_cluster.resources.storage = None;

        let sts = build_object_statefulset(&cluster);
        let spec = sts.spec.as_ref().unwrap();
        let pvcs = spec.volume_claim_templates.as_ref().unwrap();
        let pvc_spec = pvcs[0].spec.as_ref().unwrap();
        let requests = pvc_spec.resources.as_ref().unwrap().requests.as_ref().unwrap();
        let storage = requests.get("storage").unwrap();

        assert_eq!(storage.0, "500Gi");
    }

    #[test]
    fn test_resource_requirements() {
        let resources = ResourceRequirements {
            cpu: "2".to_string(),
            memory: "4Gi".to_string(),
            storage: Some("100Gi".to_string()),
        };

        let k8s_resources = build_resource_requirements(&resources);
        let requests = k8s_resources.requests.as_ref().unwrap();
        let limits = k8s_resources.limits.as_ref().unwrap();

        assert_eq!(requests.get("cpu").unwrap().0, "2");
        assert_eq!(requests.get("memory").unwrap().0, "4Gi");
        assert_eq!(limits.get("cpu").unwrap().0, "2");
        assert_eq!(limits.get("memory").unwrap().0, "4Gi");
    }
}
