use crate::crd::{GitStratumCluster, GitStratumClusterStatus};

#[derive(Debug, Clone, PartialEq)]
pub enum ScalingDecision {
    NoChange,
    ScaleUp { target_replicas: i32 },
    ScaleDown { target_replicas: i32 },
}

#[derive(Debug, Clone, Default)]
pub struct ClusterMetrics {
    pub max_disk_utilization: i32,
    pub avg_disk_utilization: i32,
    pub avg_io_utilization: i32,
    pub total_storage_bytes: i64,
    pub used_storage_bytes: i64,
}

impl ClusterMetrics {
    pub fn disk_utilization_percent(&self) -> i32 {
        if self.total_storage_bytes == 0 {
            return 0;
        }
        ((self.used_storage_bytes * 100) / self.total_storage_bytes) as i32
    }
}

pub fn check_autoscaling(
    cluster: &GitStratumCluster,
    status: &GitStratumClusterStatus,
) -> ScalingDecision {
    let auto_scaling = match &cluster.spec.object_cluster.auto_scaling {
        Some(config) if config.enabled => config,
        _ => return ScalingDecision::NoChange,
    };

    if status.is_rebalancing() {
        return ScalingDecision::NoChange;
    }

    let (current_ready, current_total) = status.object_cluster.parse_ready();

    if current_ready < current_total {
        return ScalingDecision::NoChange;
    }

    let metrics = estimate_metrics_from_status(status);

    if metrics.max_disk_utilization > auto_scaling.target_disk_utilization {
        let target = std::cmp::min(current_total + 2, auto_scaling.max_replicas);
        if target > current_total {
            return ScalingDecision::ScaleUp {
                target_replicas: target,
            };
        }
    }

    if metrics.max_disk_utilization < 30
        && metrics.avg_io_utilization < 20
        && current_total > auto_scaling.min_replicas
    {
        // Since current_total > min_replicas, target = max(current_total - 1, min_replicas)
        // will always be < current_total, so we can return directly
        let target = std::cmp::max(current_total - 1, auto_scaling.min_replicas);
        return ScalingDecision::ScaleDown {
            target_replicas: target,
        };
    }

    ScalingDecision::NoChange
}

fn estimate_metrics_from_status(status: &GitStratumClusterStatus) -> ClusterMetrics {
    let mut metrics = ClusterMetrics::default();

    if !status.object_cluster.storage_used.is_empty()
        && !status.object_cluster.storage_capacity.is_empty()
    {
        metrics.used_storage_bytes = parse_storage_size(&status.object_cluster.storage_used);
        metrics.total_storage_bytes = parse_storage_size(&status.object_cluster.storage_capacity);

        let utilization = metrics.disk_utilization_percent();
        metrics.max_disk_utilization = utilization;
        metrics.avg_disk_utilization = utilization;
    }

    metrics
}

fn parse_storage_size(size_str: &str) -> i64 {
    let size_str = size_str.trim();
    if size_str.is_empty() {
        return 0;
    }

    let (num_str, multiplier) = if let Some(s) = size_str.strip_suffix("Ti") {
        (s, 1024i64 * 1024 * 1024 * 1024)
    } else if let Some(s) = size_str.strip_suffix("Gi") {
        (s, 1024i64 * 1024 * 1024)
    } else if let Some(s) = size_str.strip_suffix("Mi") {
        (s, 1024i64 * 1024)
    } else if let Some(s) = size_str.strip_suffix("Ki") {
        (s, 1024i64)
    } else if let Some(s) = size_str.strip_suffix('T') {
        (s, 1000i64 * 1000 * 1000 * 1000)
    } else if let Some(s) = size_str.strip_suffix('G') {
        (s, 1000i64 * 1000 * 1000)
    } else if let Some(s) = size_str.strip_suffix('M') {
        (s, 1000i64 * 1000)
    } else if let Some(s) = size_str.strip_suffix('K') {
        (s, 1000i64)
    } else {
        (size_str, 1i64)
    };

    num_str
        .parse::<f64>()
        .map(|n| (n * multiplier as f64) as i64)
        .unwrap_or(0)
}

pub fn calculate_scale_up_target(
    current_replicas: i32,
    current_utilization: i32,
    target_utilization: i32,
    max_replicas: i32,
) -> i32 {
    if target_utilization == 0 {
        return current_replicas;
    }

    let desired = (current_replicas as f64 * current_utilization as f64 / target_utilization as f64)
        .ceil() as i32;

    let step = std::cmp::min(2, desired - current_replicas);
    let target = current_replicas + std::cmp::max(1, step);

    std::cmp::min(target, max_replicas)
}

pub fn calculate_scale_down_target(current_replicas: i32, min_replicas: i32) -> i32 {
    std::cmp::max(current_replicas - 1, min_replicas)
}

pub fn can_safely_scale_down(
    current_replicas: i32,
    min_replicas: i32,
    replication_factor: u8,
    is_rebalancing: bool,
) -> bool {
    if is_rebalancing {
        return false;
    }

    if current_replicas <= min_replicas {
        return false;
    }

    current_replicas > replication_factor as i32
}

pub fn estimate_rebalance_time_seconds(data_per_node_gb: f64, network_bandwidth_gbps: f64) -> f64 {
    let data_to_move_gb = data_per_node_gb / 2.0;
    let bandwidth_gbs = network_bandwidth_gbps / 8.0;

    if bandwidth_gbs <= 0.0 {
        return f64::INFINITY;
    }

    data_to_move_gb / bandwidth_gbs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ClusterCondition, ControlPlaneSpec, ControlPlaneStatus, FrontendSpec, FrontendStatus,
        GitStratumClusterSpec, MetadataSpec, MetadataStatus, ObjectAutoScalingSpec,
        ObjectClusterSpec, ObjectClusterStatus, ResourceRequirements,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn create_test_cluster(auto_scaling: Option<ObjectAutoScalingSpec>) -> GitStratumCluster {
        GitStratumCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("gitstratum".to_string()),
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
                    auto_scaling,
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
            },
            status: None,
        }
    }

    fn create_test_status(ready: i32, total: i32) -> GitStratumClusterStatus {
        GitStratumClusterStatus {
            phase: crate::crd::ClusterPhase::Running,
            control_plane: ControlPlaneStatus::new(3, 3, Some("raft-0".to_string())),
            metadata: MetadataStatus::new(3, 3),
            object_cluster: ObjectClusterStatus::new(ready, total),
            frontend: FrontendStatus::new(4, 4),
            conditions: vec![],
        }
    }

    #[test]
    fn test_no_autoscaling_config() {
        let cluster = create_test_cluster(None);
        let status = create_test_status(12, 12);

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_autoscaling_disabled() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: false,
            min_replicas: 6,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let status = create_test_status(12, 12);

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_no_scaling_when_not_ready() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 6,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let status = create_test_status(10, 12);

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scale_up_high_disk() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 6,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let mut status = create_test_status(12, 12);
        status.object_cluster.storage_used = "4.8Ti".to_string();
        status.object_cluster.storage_capacity = "5Ti".to_string();

        let decision = check_autoscaling(&cluster, &status);
        assert!(matches!(decision, ScalingDecision::ScaleUp { .. }));
    }

    #[test]
    fn test_scale_down_low_disk() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 6,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let mut status = create_test_status(12, 12);
        status.object_cluster.storage_used = "1Ti".to_string();
        status.object_cluster.storage_capacity = "6Ti".to_string();

        let decision = check_autoscaling(&cluster, &status);
        assert!(matches!(decision, ScalingDecision::ScaleDown { .. }));
    }

    #[test]
    fn test_no_scale_down_at_min() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 12,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let mut status = create_test_status(12, 12);
        status.object_cluster.storage_used = "1Ti".to_string();
        status.object_cluster.storage_capacity = "6Ti".to_string();

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_parse_storage_size() {
        assert_eq!(parse_storage_size("1Ti"), 1024i64 * 1024 * 1024 * 1024);
        assert_eq!(parse_storage_size("500Gi"), 500 * 1024i64 * 1024 * 1024);
        assert_eq!(parse_storage_size("100Mi"), 100 * 1024i64 * 1024);
        assert_eq!(parse_storage_size("1000Ki"), 1000 * 1024i64);
        assert_eq!(parse_storage_size("1T"), 1000i64 * 1000 * 1000 * 1000);
        assert_eq!(parse_storage_size("500G"), 500 * 1000i64 * 1000 * 1000);
        assert_eq!(parse_storage_size("100M"), 100 * 1000i64 * 1000);
        assert_eq!(parse_storage_size("1000K"), 1000 * 1000i64);
        assert_eq!(parse_storage_size("1024"), 1024);
        assert_eq!(parse_storage_size(""), 0);
        assert_eq!(parse_storage_size("invalid"), 0);
        assert_eq!(
            parse_storage_size("4.8Ti"),
            (4.8 * 1024.0 * 1024.0 * 1024.0 * 1024.0) as i64
        );
    }

    #[test]
    fn test_calculate_scale_up_target() {
        let target = calculate_scale_up_target(12, 90, 80, 100);
        assert!(target > 12);
        assert!(target <= 100);

        let target_at_max = calculate_scale_up_target(99, 90, 80, 100);
        assert_eq!(target_at_max, 100);
    }

    #[test]
    fn test_calculate_scale_up_target_zero_target() {
        let target = calculate_scale_up_target(12, 90, 0, 100);
        assert_eq!(target, 12);
    }

    #[test]
    fn test_calculate_scale_down_target() {
        let target = calculate_scale_down_target(12, 6);
        assert_eq!(target, 11);

        let target_at_min = calculate_scale_down_target(6, 6);
        assert_eq!(target_at_min, 6);
    }

    #[test]
    fn test_can_safely_scale_down() {
        assert!(can_safely_scale_down(12, 6, 2, false));
        assert!(!can_safely_scale_down(12, 6, 2, true));
        assert!(!can_safely_scale_down(6, 6, 2, false));
        assert!(!can_safely_scale_down(2, 2, 2, false));
    }

    #[test]
    fn test_estimate_rebalance_time() {
        let time = estimate_rebalance_time_seconds(500.0, 10.0);
        assert!(time > 0.0);
        assert!(time < 300.0);

        let time_zero = estimate_rebalance_time_seconds(500.0, 0.0);
        assert!(time_zero.is_infinite());
    }

    #[test]
    fn test_cluster_metrics() {
        let metrics = ClusterMetrics {
            max_disk_utilization: 85,
            avg_disk_utilization: 75,
            avg_io_utilization: 50,
            total_storage_bytes: 1000,
            used_storage_bytes: 800,
        };
        assert_eq!(metrics.disk_utilization_percent(), 80);

        let empty_metrics = ClusterMetrics::default();
        assert_eq!(empty_metrics.disk_utilization_percent(), 0);
    }

    #[test]
    fn test_no_scale_up_at_max() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 6,
            max_replicas: 12,
            target_disk_utilization: 80,
        }));
        let mut status = create_test_status(12, 12);
        status.object_cluster.storage_used = "4.8Ti".to_string();
        status.object_cluster.storage_capacity = "5Ti".to_string();

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_no_scaling_when_rebalancing() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 6,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let mut status = create_test_status(12, 12);
        status.object_cluster.storage_used = "4.8Ti".to_string();
        status.object_cluster.storage_capacity = "5Ti".to_string();
        status.set_condition(ClusterCondition::rebalancing(true));

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scaling_decision_equality() {
        let scale_up1 = ScalingDecision::ScaleUp {
            target_replicas: 10,
        };
        let scale_up2 = ScalingDecision::ScaleUp {
            target_replicas: 10,
        };
        let scale_up3 = ScalingDecision::ScaleUp {
            target_replicas: 11,
        };
        let scale_down = ScalingDecision::ScaleDown { target_replicas: 5 };

        assert_eq!(scale_up1, scale_up2);
        assert_ne!(scale_up1, scale_up3);
        assert_ne!(scale_up1, scale_down);
        assert_eq!(ScalingDecision::NoChange, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scaling_decision_debug() {
        let decision = ScalingDecision::ScaleUp {
            target_replicas: 10,
        };
        let debug_str = format!("{:?}", decision);
        assert!(debug_str.contains("ScaleUp"));
        assert!(debug_str.contains("10"));
    }

    #[test]
    fn test_cluster_metrics_debug() {
        let metrics = ClusterMetrics {
            max_disk_utilization: 85,
            avg_disk_utilization: 75,
            avg_io_utilization: 50,
            total_storage_bytes: 1000,
            used_storage_bytes: 800,
        };
        let debug_str = format!("{:?}", metrics);
        assert!(debug_str.contains("85"));
    }

    #[test]
    fn test_scale_down_at_min_plus_one() {
        let cluster = create_test_cluster(Some(ObjectAutoScalingSpec {
            enabled: true,
            min_replicas: 11,
            max_replicas: 100,
            target_disk_utilization: 80,
        }));
        let mut status = create_test_status(12, 12);
        status.object_cluster.storage_used = "1Ti".to_string();
        status.object_cluster.storage_capacity = "6Ti".to_string();

        let decision = check_autoscaling(&cluster, &status);
        assert_eq!(
            decision,
            ScalingDecision::ScaleDown {
                target_replicas: 11
            }
        );
    }
}
