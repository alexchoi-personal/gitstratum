use crate::error::Result;
use crate::node::NodeInfo;
use crate::ring::ConsistentHashRing;

pub struct HashRingBuilder {
    virtual_nodes_per_physical: u32,
    replication_factor: usize,
    nodes: Vec<NodeInfo>,
}

impl HashRingBuilder {
    pub fn new() -> Self {
        Self {
            virtual_nodes_per_physical: 16,
            replication_factor: 2,
            nodes: Vec::new(),
        }
    }

    pub fn virtual_nodes(mut self, count: u32) -> Self {
        self.virtual_nodes_per_physical = count;
        self
    }

    pub fn replication_factor(mut self, rf: usize) -> Self {
        self.replication_factor = rf;
        self
    }

    pub fn add_node(mut self, node: NodeInfo) -> Self {
        self.nodes.push(node);
        self
    }

    pub fn build(self) -> Result<ConsistentHashRing> {
        ConsistentHashRing::with_nodes(
            self.nodes,
            self.virtual_nodes_per_physical,
            self.replication_factor,
        )
    }
}

impl Default for HashRingBuilder {
    fn default() -> Self {
        Self::new()
    }
}
