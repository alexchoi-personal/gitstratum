use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use parking_lot::RwLock;
use siphasher::sip::SipHasher24;

use gitstratum_core::RepoId;

pub type NodeId = String;
pub type PartitionId = u32;

#[derive(Debug, Clone)]
pub struct PartitionConfig {
    pub partition_count: u32,
    pub replication_factor: u32,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            partition_count: 256,
            replication_factor: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: PartitionId,
    pub primary_node: Option<NodeId>,
    pub replica_nodes: Vec<NodeId>,
}

impl Partition {
    pub fn new(id: PartitionId) -> Self {
        Self {
            id,
            primary_node: None,
            replica_nodes: Vec::new(),
        }
    }

    pub fn all_nodes(&self) -> Vec<NodeId> {
        let mut nodes = Vec::new();
        if let Some(ref primary) = self.primary_node {
            nodes.push(primary.clone());
        }
        nodes.extend(self.replica_nodes.clone());
        nodes
    }

    pub fn has_node(&self, node_id: &str) -> bool {
        self.primary_node.as_deref() == Some(node_id)
            || self.replica_nodes.iter().any(|n| n == node_id)
    }
}

pub struct PartitionRouter {
    config: PartitionConfig,
    partitions: RwLock<HashMap<PartitionId, Partition>>,
    node_partitions: RwLock<HashMap<NodeId, Vec<PartitionId>>>,
}

impl PartitionRouter {
    pub fn new(config: PartitionConfig) -> Self {
        let mut partitions = HashMap::new();
        for i in 0..config.partition_count {
            partitions.insert(i, Partition::new(i));
        }

        Self {
            config,
            partitions: RwLock::new(partitions),
            node_partitions: RwLock::new(HashMap::new()),
        }
    }

    pub fn hash_repo(&self, repo_id: &RepoId) -> PartitionId {
        let mut hasher = SipHasher24::new();
        repo_id.as_str().hash(&mut hasher);
        let hash = hasher.finish();
        (hash % self.config.partition_count as u64) as PartitionId
    }

    pub fn route(&self, repo_id: &RepoId) -> Option<Partition> {
        let partition_id = self.hash_repo(repo_id);
        self.partitions.read().get(&partition_id).cloned()
    }

    pub fn route_to_primary(&self, repo_id: &RepoId) -> Option<NodeId> {
        self.route(repo_id).and_then(|p| p.primary_node)
    }

    pub fn route_to_replicas(&self, repo_id: &RepoId) -> Vec<NodeId> {
        self.route(repo_id)
            .map(|p| p.all_nodes())
            .unwrap_or_default()
    }

    pub fn assign_primary(&self, partition_id: PartitionId, node_id: NodeId) {
        let mut partitions = self.partitions.write();
        if let Some(partition) = partitions.get_mut(&partition_id) {
            if let Some(ref old_primary) = partition.primary_node {
                let mut node_partitions = self.node_partitions.write();
                if let Some(pids) = node_partitions.get_mut(old_primary) {
                    pids.retain(|&pid| pid != partition_id);
                }
            }

            partition.primary_node = Some(node_id.clone());

            let mut node_partitions = self.node_partitions.write();
            node_partitions
                .entry(node_id)
                .or_default()
                .push(partition_id);
        }
    }

    pub fn add_replica(&self, partition_id: PartitionId, node_id: NodeId) {
        let mut partitions = self.partitions.write();
        if let Some(partition) = partitions.get_mut(&partition_id) {
            if !partition.replica_nodes.contains(&node_id)
                && partition.primary_node.as_ref() != Some(&node_id)
            {
                partition.replica_nodes.push(node_id.clone());

                let mut node_partitions = self.node_partitions.write();
                node_partitions
                    .entry(node_id)
                    .or_default()
                    .push(partition_id);
            }
        }
    }

    pub fn remove_node(&self, node_id: &str) {
        let partition_ids: Vec<PartitionId> = self
            .node_partitions
            .write()
            .remove(node_id)
            .unwrap_or_default();

        let mut partitions = self.partitions.write();
        for pid in partition_ids {
            if let Some(partition) = partitions.get_mut(&pid) {
                if partition.primary_node.as_deref() == Some(node_id) {
                    partition.primary_node = partition.replica_nodes.pop();
                } else {
                    partition.replica_nodes.retain(|n| n != node_id);
                }
            }
        }
    }

    pub fn get_partitions_for_node(&self, node_id: &str) -> Vec<PartitionId> {
        self.node_partitions
            .read()
            .get(node_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_partition(&self, partition_id: PartitionId) -> Option<Partition> {
        self.partitions.read().get(&partition_id).cloned()
    }

    pub fn partition_count(&self) -> u32 {
        self.config.partition_count
    }

    pub fn config(&self) -> &PartitionConfig {
        &self.config
    }
}

impl Default for PartitionRouter {
    fn default() -> Self {
        Self::new(PartitionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_config_default() {
        let config = PartitionConfig::default();
        assert_eq!(config.partition_count, 256);
        assert_eq!(config.replication_factor, 3);
    }

    #[test]
    fn test_partition_new() {
        let partition = Partition::new(0);
        assert_eq!(partition.id, 0);
        assert!(partition.primary_node.is_none());
        assert!(partition.replica_nodes.is_empty());
    }

    #[test]
    fn test_partition_all_nodes() {
        let mut partition = Partition::new(0);
        partition.primary_node = Some("node1".to_string());
        partition.replica_nodes = vec!["node2".to_string(), "node3".to_string()];

        let all = partition.all_nodes();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&"node1".to_string()));
    }

    #[test]
    fn test_partition_has_node() {
        let mut partition = Partition::new(0);
        partition.primary_node = Some("node1".to_string());
        partition.replica_nodes = vec!["node2".to_string()];

        assert!(partition.has_node("node1"));
        assert!(partition.has_node("node2"));
        assert!(!partition.has_node("node3"));
    }

    #[test]
    fn test_partition_router_hash() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 16,
            replication_factor: 3,
        });

        let repo1 = RepoId::new("org/repo1").unwrap();
        let repo2 = RepoId::new("org/repo2").unwrap();

        let pid1 = router.hash_repo(&repo1);
        let pid2 = router.hash_repo(&repo2);

        assert!(pid1 < 16);
        assert!(pid2 < 16);

        assert_eq!(pid1, router.hash_repo(&repo1));
    }

    #[test]
    fn test_partition_router_route() {
        let router = PartitionRouter::default();
        let repo_id = RepoId::new("org/repo").unwrap();

        let partition = router.route(&repo_id).unwrap();
        assert!(partition.primary_node.is_none());
    }

    #[test]
    fn test_partition_router_assign_primary() {
        let router = PartitionRouter::default();
        let repo_id = RepoId::new("org/repo").unwrap();
        let partition_id = router.hash_repo(&repo_id);

        router.assign_primary(partition_id, "node1".to_string());

        let primary = router.route_to_primary(&repo_id).unwrap();
        assert_eq!(primary, "node1");
    }

    #[test]
    fn test_partition_router_add_replica() {
        let router = PartitionRouter::default();
        let repo_id = RepoId::new("org/repo").unwrap();
        let partition_id = router.hash_repo(&repo_id);

        router.assign_primary(partition_id, "node1".to_string());
        router.add_replica(partition_id, "node2".to_string());
        router.add_replica(partition_id, "node3".to_string());

        let replicas = router.route_to_replicas(&repo_id);
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn test_partition_router_add_replica_no_duplicates() {
        let router = PartitionRouter::default();
        let partition_id = 0;

        router.assign_primary(partition_id, "node1".to_string());
        router.add_replica(partition_id, "node2".to_string());
        router.add_replica(partition_id, "node2".to_string());
        router.add_replica(partition_id, "node1".to_string());

        let partition = router.get_partition(partition_id).unwrap();
        assert_eq!(partition.replica_nodes.len(), 1);
    }

    #[test]
    fn test_partition_router_remove_node() {
        let router = PartitionRouter::default();
        let partition_id = 0;

        router.assign_primary(partition_id, "node1".to_string());
        router.add_replica(partition_id, "node2".to_string());

        router.remove_node("node1");

        let partition = router.get_partition(partition_id).unwrap();
        assert_eq!(partition.primary_node, Some("node2".to_string()));
        assert!(partition.replica_nodes.is_empty());
    }

    #[test]
    fn test_partition_router_get_partitions_for_node() {
        let router = PartitionRouter::default();

        router.assign_primary(0, "node1".to_string());
        router.assign_primary(1, "node1".to_string());
        router.add_replica(2, "node1".to_string());

        let partitions = router.get_partitions_for_node("node1");
        assert_eq!(partitions.len(), 3);
    }

    #[test]
    fn test_partition_router_config() {
        let config = PartitionConfig {
            partition_count: 128,
            replication_factor: 5,
        };
        let router = PartitionRouter::new(config);

        assert_eq!(router.partition_count(), 128);
        assert_eq!(router.config().replication_factor, 5);
    }

    #[test]
    fn test_partition_router_default() {
        let router = PartitionRouter::default();
        assert_eq!(router.partition_count(), 256);
        assert_eq!(router.config().replication_factor, 3);
    }

    #[test]
    fn test_partition_all_nodes_no_primary() {
        let partition = Partition::new(0);
        let nodes = partition.all_nodes();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_partition_all_nodes_only_replicas() {
        let mut partition = Partition::new(0);
        partition.replica_nodes = vec!["replica1".to_string(), "replica2".to_string()];
        let nodes = partition.all_nodes();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&"replica1".to_string()));
        assert!(nodes.contains(&"replica2".to_string()));
    }

    #[test]
    fn test_route_to_primary_no_primary() {
        let router = PartitionRouter::default();
        let repo_id = RepoId::new("org/repo").unwrap();
        let primary = router.route_to_primary(&repo_id);
        assert!(primary.is_none());
    }

    #[test]
    fn test_route_to_replicas_no_nodes() {
        let router = PartitionRouter::default();
        let repo_id = RepoId::new("org/repo").unwrap();
        let replicas = router.route_to_replicas(&repo_id);
        assert!(replicas.is_empty());
    }

    #[test]
    fn test_get_partitions_for_unknown_node() {
        let router = PartitionRouter::default();
        let partitions = router.get_partitions_for_node("unknown_node");
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_get_partition_invalid_id() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 4,
            replication_factor: 2,
        });
        let partition = router.get_partition(999);
        assert!(partition.is_none());
    }

    #[test]
    fn test_assign_primary_replaces_old_primary() {
        let router = PartitionRouter::default();
        let partition_id = 0;

        router.assign_primary(partition_id, "node1".to_string());
        let partitions = router.get_partitions_for_node("node1");
        assert!(partitions.contains(&partition_id));

        router.assign_primary(partition_id, "node2".to_string());

        let partition = router.get_partition(partition_id).unwrap();
        assert_eq!(partition.primary_node, Some("node2".to_string()));

        let node1_partitions = router.get_partitions_for_node("node1");
        assert!(!node1_partitions.contains(&partition_id));

        let node2_partitions = router.get_partitions_for_node("node2");
        assert!(node2_partitions.contains(&partition_id));
    }

    #[test]
    fn test_assign_primary_invalid_partition() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 4,
            replication_factor: 2,
        });
        router.assign_primary(999, "node1".to_string());
        let partitions = router.get_partitions_for_node("node1");
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_add_replica_invalid_partition() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 4,
            replication_factor: 2,
        });
        router.add_replica(999, "node1".to_string());
        let partitions = router.get_partitions_for_node("node1");
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_remove_node_not_in_partitions() {
        let router = PartitionRouter::default();
        router.assign_primary(0, "node1".to_string());
        router.remove_node("unknown_node");
        let partition = router.get_partition(0).unwrap();
        assert_eq!(partition.primary_node, Some("node1".to_string()));
    }

    #[test]
    fn test_remove_node_only_replica() {
        let router = PartitionRouter::default();
        router.assign_primary(0, "node1".to_string());
        router.add_replica(0, "node2".to_string());
        router.add_replica(0, "node3".to_string());

        router.remove_node("node2");

        let partition = router.get_partition(0).unwrap();
        assert_eq!(partition.primary_node, Some("node1".to_string()));
        assert_eq!(partition.replica_nodes, vec!["node3".to_string()]);
    }

    #[test]
    fn test_remove_primary_promotes_replica() {
        let router = PartitionRouter::default();
        router.assign_primary(0, "node1".to_string());
        router.add_replica(0, "node2".to_string());
        router.add_replica(0, "node3".to_string());

        router.remove_node("node1");

        let partition = router.get_partition(0).unwrap();
        assert!(partition.primary_node.is_some());
        let new_primary = partition.primary_node.unwrap();
        assert!(new_primary == "node2" || new_primary == "node3");
    }

    #[test]
    fn test_hash_repo_distribution() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 16,
            replication_factor: 3,
        });

        let mut partition_counts = vec![0u32; 16];
        for i in 0..1000 {
            let repo_id = RepoId::new(format!("org/repo{}", i)).unwrap();
            let partition_id = router.hash_repo(&repo_id);
            assert!(partition_id < 16);
            partition_counts[partition_id as usize] += 1;
        }

        for count in &partition_counts {
            assert!(*count > 0, "Every partition should have at least one repo");
        }
    }

    #[test]
    fn test_hash_repo_consistency() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 256,
            replication_factor: 3,
        });

        let repo1 = RepoId::new("org/same-repo").unwrap();
        let repo2 = RepoId::new("org/same-repo").unwrap();

        assert_eq!(router.hash_repo(&repo1), router.hash_repo(&repo2));
    }

    #[test]
    fn test_partition_has_node_no_nodes() {
        let partition = Partition::new(0);
        assert!(!partition.has_node("any"));
    }

    #[test]
    fn test_multiple_partitions_same_node() {
        let router = PartitionRouter::default();

        router.assign_primary(0, "node1".to_string());
        router.assign_primary(1, "node1".to_string());
        router.assign_primary(2, "node1".to_string());
        router.add_replica(3, "node1".to_string());

        let partitions = router.get_partitions_for_node("node1");
        assert_eq!(partitions.len(), 4);
        assert!(partitions.contains(&0));
        assert!(partitions.contains(&1));
        assert!(partitions.contains(&2));
        assert!(partitions.contains(&3));
    }
}
