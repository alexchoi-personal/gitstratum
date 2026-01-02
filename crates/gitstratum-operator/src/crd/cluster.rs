use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gitstratum.io",
    version = "v1",
    kind = "GitStratumCluster",
    namespaced,
    status = "GitStratumClusterStatus",
    shortname = "gsc",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Control Plane","type":"string","jsonPath":".status.controlPlane.ready"}"#,
    printcolumn = r#"{"name":"Metadata","type":"string","jsonPath":".status.metadata.ready"}"#,
    printcolumn = r#"{"name":"Objects","type":"string","jsonPath":".status.objectCluster.ready"}"#,
    printcolumn = r#"{"name":"Frontends","type":"string","jsonPath":".status.frontend.ready"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GitStratumClusterSpec {
    pub replication_factor: u8,
    pub control_plane: ControlPlaneSpec,
    pub metadata: MetadataSpec,
    pub object_cluster: ObjectClusterSpec,
    pub frontend: FrontendSpec,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneSpec {
    pub replicas: i32,
    pub resources: ResourceRequirements,
    pub storage_class: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetadataSpec {
    pub replicas: i32,
    pub resources: ResourceRequirements,
    pub storage_class: String,
    #[serde(default)]
    pub replication_mode: ReplicationMode,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationMode {
    #[default]
    Sync,
    Async,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectClusterSpec {
    pub replicas: i32,
    pub resources: ResourceRequirements,
    pub storage_class: String,
    #[serde(default)]
    pub auto_scaling: Option<ObjectAutoScalingSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectAutoScalingSpec {
    pub enabled: bool,
    pub min_replicas: i32,
    pub max_replicas: i32,
    pub target_disk_utilization: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FrontendSpec {
    pub replicas: i32,
    pub resources: ResourceRequirements,
    #[serde(default)]
    pub auto_scaling: Option<FrontendAutoScalingSpec>,
    #[serde(default)]
    pub service: ServiceSpec,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FrontendAutoScalingSpec {
    pub enabled: bool,
    pub min_replicas: i32,
    pub max_replicas: i32,
    pub target_cpu_utilization: i32,
    pub target_connections_per_pod: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    #[serde(default)]
    pub service_type: ServiceType,
    #[serde(default)]
    pub ports: Vec<ServicePort>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum ServiceType {
    #[default]
    ClusterIP,
    LoadBalancer,
    NodePort,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServicePort {
    pub name: String,
    pub port: i32,
    #[serde(default)]
    pub target_port: Option<i32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    pub cpu: String,
    pub memory: String,
    #[serde(default)]
    pub storage: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitStratumClusterStatus {
    #[serde(default)]
    pub phase: ClusterPhase,
    #[serde(default)]
    pub control_plane: ControlPlaneStatus,
    #[serde(default)]
    pub metadata: MetadataStatus,
    #[serde(default)]
    pub object_cluster: ObjectClusterStatus,
    #[serde(default)]
    pub frontend: FrontendStatus,
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum ClusterPhase {
    #[default]
    Pending,
    Provisioning,
    Running,
    Degraded,
    Failed,
}

impl ClusterPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClusterPhase::Pending => "Pending",
            ClusterPhase::Provisioning => "Provisioning",
            ClusterPhase::Running => "Running",
            ClusterPhase::Degraded => "Degraded",
            ClusterPhase::Failed => "Failed",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneStatus {
    pub ready: String,
    #[serde(default)]
    pub leader: Option<String>,
}

impl ControlPlaneStatus {
    pub fn new(ready: i32, total: i32, leader: Option<String>) -> Self {
        Self {
            ready: format!("{}/{}", ready, total),
            leader,
        }
    }

    pub fn parse_ready(&self) -> (i32, i32) {
        let parts: Vec<&str> = self.ready.split('/').collect();
        if parts.len() == 2 {
            let ready = parts[0].parse().unwrap_or(0);
            let total = parts[1].parse().unwrap_or(0);
            (ready, total)
        } else {
            (0, 0)
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetadataStatus {
    pub ready: String,
    #[serde(default)]
    pub total_repos: i64,
    #[serde(default)]
    pub storage_used: String,
}

impl MetadataStatus {
    pub fn new(ready: i32, total: i32) -> Self {
        Self {
            ready: format!("{}/{}", ready, total),
            total_repos: 0,
            storage_used: String::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectClusterStatus {
    pub ready: String,
    #[serde(default)]
    pub total_blobs: i64,
    #[serde(default)]
    pub storage_used: String,
    #[serde(default)]
    pub storage_capacity: String,
    #[serde(default)]
    pub hash_ring: HashRingStatus,
}

impl ObjectClusterStatus {
    pub fn new(ready: i32, total: i32) -> Self {
        Self {
            ready: format!("{}/{}", ready, total),
            total_blobs: 0,
            storage_used: String::new(),
            storage_capacity: String::new(),
            hash_ring: HashRingStatus::default(),
        }
    }

    pub fn parse_ready(&self) -> (i32, i32) {
        let parts: Vec<&str> = self.ready.split('/').collect();
        if parts.len() == 2 {
            let ready = parts[0].parse().unwrap_or(0);
            let total = parts[1].parse().unwrap_or(0);
            (ready, total)
        } else {
            (0, 0)
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HashRingStatus {
    pub version: i64,
    #[serde(default)]
    pub last_rebalance: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FrontendStatus {
    pub ready: String,
    #[serde(default)]
    pub active_connections: i64,
}

impl FrontendStatus {
    pub fn new(ready: i32, total: i32) -> Self {
        Self {
            ready: format!("{}/{}", ready, total),
            active_connections: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    #[serde(rename = "type")]
    pub condition_type: ConditionType,
    pub status: String,
    #[serde(default)]
    pub last_transition_time: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum ConditionType {
    Ready,
    Scaling,
    Rebalancing,
    Degraded,
}

impl ClusterCondition {
    pub fn ready(status: bool) -> Self {
        Self {
            condition_type: ConditionType::Ready,
            status: if status { "True" } else { "False" }.to_string(),
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason: None,
            message: None,
        }
    }

    pub fn scaling(active: bool, reason: Option<String>) -> Self {
        Self {
            condition_type: ConditionType::Scaling,
            status: if active { "True" } else { "False" }.to_string(),
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason,
            message: None,
        }
    }

    pub fn rebalancing(active: bool) -> Self {
        Self {
            condition_type: ConditionType::Rebalancing,
            status: if active { "True" } else { "False" }.to_string(),
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason: None,
            message: None,
        }
    }

    pub fn degraded(active: bool, message: Option<String>) -> Self {
        Self {
            condition_type: ConditionType::Degraded,
            status: if active { "True" } else { "False" }.to_string(),
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason: None,
            message,
        }
    }
}

impl GitStratumClusterStatus {
    pub fn set_condition(&mut self, condition: ClusterCondition) {
        if let Some(pos) = self
            .conditions
            .iter()
            .position(|c| c.condition_type == condition.condition_type)
        {
            self.conditions[pos] = condition;
        } else {
            self.conditions.push(condition);
        }
    }

    pub fn get_condition(&self, condition_type: ConditionType) -> Option<&ClusterCondition> {
        self.conditions
            .iter()
            .find(|c| c.condition_type == condition_type)
    }

    pub fn is_ready(&self) -> bool {
        self.get_condition(ConditionType::Ready)
            .map(|c| c.status == "True")
            .unwrap_or(false)
    }

    pub fn is_scaling(&self) -> bool {
        self.get_condition(ConditionType::Scaling)
            .map(|c| c.status == "True")
            .unwrap_or(false)
    }

    pub fn is_rebalancing(&self) -> bool {
        self.get_condition(ConditionType::Rebalancing)
            .map(|c| c.status == "True")
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_phase() {
        assert_eq!(ClusterPhase::Pending.as_str(), "Pending");
        assert_eq!(ClusterPhase::Provisioning.as_str(), "Provisioning");
        assert_eq!(ClusterPhase::Running.as_str(), "Running");
        assert_eq!(ClusterPhase::Degraded.as_str(), "Degraded");
        assert_eq!(ClusterPhase::Failed.as_str(), "Failed");
    }

    #[test]
    fn test_control_plane_status() {
        let status = ControlPlaneStatus::new(2, 3, Some("raft-0".to_string()));
        assert_eq!(status.ready, "2/3");
        assert_eq!(status.leader, Some("raft-0".to_string()));

        let (ready, total) = status.parse_ready();
        assert_eq!(ready, 2);
        assert_eq!(total, 3);
    }

    #[test]
    fn test_object_cluster_status() {
        let status = ObjectClusterStatus::new(10, 12);
        assert_eq!(status.ready, "10/12");

        let (ready, total) = status.parse_ready();
        assert_eq!(ready, 10);
        assert_eq!(total, 12);
    }

    #[test]
    fn test_cluster_conditions() {
        let mut status = GitStratumClusterStatus::default();

        status.set_condition(ClusterCondition::ready(true));
        assert!(status.is_ready());

        status.set_condition(ClusterCondition::scaling(true, Some("ScaleUp".to_string())));
        assert!(status.is_scaling());

        status.set_condition(ClusterCondition::rebalancing(false));
        assert!(!status.is_rebalancing());

        status.set_condition(ClusterCondition::ready(false));
        assert!(!status.is_ready());
    }

    #[test]
    fn test_replication_mode() {
        assert_eq!(ReplicationMode::default(), ReplicationMode::Sync);
    }

    #[test]
    fn test_service_type() {
        assert_eq!(ServiceType::default(), ServiceType::ClusterIP);
    }

    #[test]
    fn test_metadata_status() {
        let status = MetadataStatus::new(3, 3);
        assert_eq!(status.ready, "3/3");
    }

    #[test]
    fn test_frontend_status() {
        let status = FrontendStatus::new(4, 4);
        assert_eq!(status.ready, "4/4");
    }

    #[test]
    fn test_condition_degraded() {
        let condition = ClusterCondition::degraded(true, Some("Node failure".to_string()));
        assert_eq!(condition.condition_type, ConditionType::Degraded);
        assert_eq!(condition.status, "True");
        assert_eq!(condition.message, Some("Node failure".to_string()));
    }

    #[test]
    fn test_parse_ready_invalid() {
        let status = ControlPlaneStatus {
            ready: "invalid".to_string(),
            leader: None,
        };
        let (ready, total) = status.parse_ready();
        assert_eq!(ready, 0);
        assert_eq!(total, 0);
    }

    #[test]
    fn test_object_parse_ready_invalid() {
        let status = ObjectClusterStatus {
            ready: "bad".to_string(),
            ..Default::default()
        };
        let (ready, total) = status.parse_ready();
        assert_eq!(ready, 0);
        assert_eq!(total, 0);
    }

    #[test]
    fn test_get_condition_missing() {
        let status = GitStratumClusterStatus::default();
        assert!(status.get_condition(ConditionType::Ready).is_none());
        assert!(!status.is_ready());
        assert!(!status.is_scaling());
        assert!(!status.is_rebalancing());
    }

    #[test]
    fn test_cluster_phase_default() {
        let phase = ClusterPhase::default();
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_cluster_condition_with_details() {
        let condition = ClusterCondition::degraded(true, Some("Test message".to_string()));
        assert_eq!(condition.message, Some("Test message".to_string()));
        assert!(condition.last_transition_time.is_some());

        let condition_no_msg = ClusterCondition::degraded(false, None);
        assert!(condition_no_msg.message.is_none());
    }

    #[test]
    fn test_hash_ring_status_default() {
        let status = HashRingStatus::default();
        assert_eq!(status.version, 0);
        assert!(status.last_rebalance.is_none());
    }
}
