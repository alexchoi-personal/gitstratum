use std::collections::{HashMap, HashSet};

use crate::partition::router::{NodeId, PartitionId, PartitionRouter};

#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    pub max_partitions_per_node: u32,
    pub min_replicas: u32,
    pub max_moves_per_round: u32,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            max_partitions_per_node: 100,
            min_replicas: 2,
            max_moves_per_round: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RebalanceAction {
    AssignPrimary {
        partition_id: PartitionId,
        node_id: NodeId,
    },
    AddReplica {
        partition_id: PartitionId,
        node_id: NodeId,
    },
    RemoveReplica {
        partition_id: PartitionId,
        node_id: NodeId,
    },
    MovePrimary {
        partition_id: PartitionId,
        from_node: NodeId,
        to_node: NodeId,
    },
}

#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub actions: Vec<RebalanceAction>,
    pub estimated_data_transfer_bytes: u64,
}

impl RebalancePlan {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
            estimated_data_transfer_bytes: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn action_count(&self) -> usize {
        self.actions.len()
    }
}

impl Default for RebalancePlan {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RebalancePlanner {
    config: RebalanceConfig,
}

impl RebalancePlanner {
    pub fn new(config: RebalanceConfig) -> Self {
        Self { config }
    }

    pub fn plan(&self, router: &PartitionRouter, available_nodes: &[NodeId]) -> RebalancePlan {
        let mut plan = RebalancePlan::new();

        if available_nodes.is_empty() {
            return plan;
        }

        let mut node_loads: HashMap<NodeId, u32> = available_nodes
            .iter()
            .map(|n| (n.clone(), 0))
            .collect();

        for i in 0..router.partition_count() {
            if let Some(partition) = router.get_partition(i) {
                for node in partition.all_nodes() {
                    if let Some(load) = node_loads.get_mut(&node) {
                        *load += 1;
                    }
                }
            }
        }

        for i in 0..router.partition_count() {
            if let Some(partition) = router.get_partition(i) {
                if partition.primary_node.is_none() {
                    if let Some(node) = self.find_least_loaded(&node_loads) {
                        plan.actions.push(RebalanceAction::AssignPrimary {
                            partition_id: i,
                            node_id: node.clone(),
                        });
                        *node_loads.get_mut(&node).unwrap() += 1;
                    }
                }
            }
        }

        for i in 0..router.partition_count() {
            if let Some(partition) = router.get_partition(i) {
                let current_replicas = partition.all_nodes().len() as u32;
                let needed = router.config().replication_factor.saturating_sub(current_replicas);

                for _ in 0..needed {
                    let existing: HashSet<_> = partition.all_nodes().into_iter().collect();
                    if let Some(node) = self.find_least_loaded_excluding(&node_loads, &existing) {
                        plan.actions.push(RebalanceAction::AddReplica {
                            partition_id: i,
                            node_id: node.clone(),
                        });
                        *node_loads.get_mut(&node).unwrap() += 1;
                    }
                }

                if plan.actions.len() >= self.config.max_moves_per_round as usize {
                    break;
                }
            }
        }

        plan
    }

    fn find_least_loaded(&self, loads: &HashMap<NodeId, u32>) -> Option<NodeId> {
        loads
            .iter()
            .filter(|(_, &load)| load < self.config.max_partitions_per_node)
            .min_by_key(|(_, &load)| load)
            .map(|(node, _)| node.clone())
    }

    fn find_least_loaded_excluding(
        &self,
        loads: &HashMap<NodeId, u32>,
        exclude: &HashSet<NodeId>,
    ) -> Option<NodeId> {
        loads
            .iter()
            .filter(|(node, &load)| {
                load < self.config.max_partitions_per_node && !exclude.contains(*node)
            })
            .min_by_key(|(_, &load)| load)
            .map(|(node, _)| node.clone())
    }

    pub fn apply(&self, router: &PartitionRouter, plan: &RebalancePlan) {
        for action in &plan.actions {
            match action {
                RebalanceAction::AssignPrimary { partition_id, node_id } => {
                    router.assign_primary(*partition_id, node_id.clone());
                }
                RebalanceAction::AddReplica { partition_id, node_id } => {
                    router.add_replica(*partition_id, node_id.clone());
                }
                RebalanceAction::RemoveReplica { partition_id, node_id } => {
                    if let Some(mut partition) = router.get_partition(*partition_id) {
                        partition.replica_nodes.retain(|n| n != node_id);
                    }
                }
                RebalanceAction::MovePrimary { partition_id, to_node, .. } => {
                    router.assign_primary(*partition_id, to_node.clone());
                }
            }
        }
    }

    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }
}

impl Default for RebalancePlanner {
    fn default() -> Self {
        Self::new(RebalanceConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::router::PartitionConfig;

    #[test]
    fn test_rebalance_config_default() {
        let config = RebalanceConfig::default();
        assert_eq!(config.max_partitions_per_node, 100);
        assert_eq!(config.min_replicas, 2);
        assert_eq!(config.max_moves_per_round, 10);
    }

    #[test]
    fn test_rebalance_plan_new() {
        let plan = RebalancePlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.action_count(), 0);
    }

    #[test]
    fn test_rebalance_planner_assign_primaries() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 4,
            replication_factor: 2,
        });

        let planner = RebalancePlanner::new(RebalanceConfig {
            max_partitions_per_node: 10,
            min_replicas: 1,
            max_moves_per_round: 20,
        });

        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let plan = planner.plan(&router, &nodes);

        assert!(!plan.is_empty());

        let assign_count = plan.actions.iter().filter(|a| matches!(a, RebalanceAction::AssignPrimary { .. })).count();
        assert_eq!(assign_count, 4);
    }

    #[test]
    fn test_rebalance_planner_add_replicas() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 2,
            replication_factor: 3,
        });

        router.assign_primary(0, "node1".to_string());
        router.assign_primary(1, "node2".to_string());

        let planner = RebalancePlanner::new(RebalanceConfig {
            max_partitions_per_node: 10,
            min_replicas: 2,
            max_moves_per_round: 20,
        });

        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];
        let plan = planner.plan(&router, &nodes);

        let replica_count = plan.actions.iter().filter(|a| matches!(a, RebalanceAction::AddReplica { .. })).count();
        assert!(replica_count > 0);
    }

    #[test]
    fn test_rebalance_planner_empty_nodes() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 4,
            replication_factor: 2,
        });

        let planner = RebalancePlanner::default();
        let plan = planner.plan(&router, &[]);

        assert!(plan.is_empty());
    }

    #[test]
    fn test_rebalance_planner_max_moves() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 100,
            replication_factor: 3,
        });

        let planner = RebalancePlanner::new(RebalanceConfig {
            max_partitions_per_node: 100,
            min_replicas: 2,
            max_moves_per_round: 5,
        });

        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let plan = planner.plan(&router, &nodes);

        assert!(plan.action_count() <= 5);
    }

    #[test]
    fn test_rebalance_planner_apply() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 2,
            replication_factor: 2,
        });

        let planner = RebalancePlanner::new(RebalanceConfig {
            max_partitions_per_node: 10,
            min_replicas: 1,
            max_moves_per_round: 10,
        });

        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let plan = planner.plan(&router, &nodes);

        planner.apply(&router, &plan);

        assert!(router.get_partition(0).unwrap().primary_node.is_some());
        assert!(router.get_partition(1).unwrap().primary_node.is_some());
    }

    #[test]
    fn test_rebalance_planner_load_balancing() {
        let router = PartitionRouter::new(PartitionConfig {
            partition_count: 4,
            replication_factor: 1,
        });

        let planner = RebalancePlanner::new(RebalanceConfig {
            max_partitions_per_node: 10,
            min_replicas: 1,
            max_moves_per_round: 10,
        });

        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let plan = planner.plan(&router, &nodes);
        planner.apply(&router, &plan);

        let node1_count = router.get_partitions_for_node("node1").len();
        let node2_count = router.get_partitions_for_node("node2").len();

        assert_eq!(node1_count + node2_count, 4);
        assert!((node1_count as i32 - node2_count as i32).abs() <= 1);
    }

    #[test]
    fn test_rebalance_planner_config() {
        let config = RebalanceConfig {
            max_partitions_per_node: 50,
            min_replicas: 3,
            max_moves_per_round: 5,
        };
        let planner = RebalancePlanner::new(config);

        assert_eq!(planner.config().max_partitions_per_node, 50);
        assert_eq!(planner.config().min_replicas, 3);
        assert_eq!(planner.config().max_moves_per_round, 5);
    }
}
