#![allow(dead_code)]

use std::collections::BTreeMap;

use gitstratum_core::Oid;
use sha2::{Digest, Sha256};

const CHILDREN_COUNT: usize = 16;
const DEFAULT_DEPTH: u8 = 4;

#[derive(Debug, Clone)]
pub struct MerkleNode {
    hash: [u8; 32],
    position_start: u64,
    position_end: u64,
    object_count: u64,
    children: Option<Box<[MerkleNode; CHILDREN_COUNT]>>,
}

impl MerkleNode {
    pub fn new(position_start: u64, position_end: u64) -> Self {
        Self {
            hash: [0u8; 32],
            position_start,
            position_end,
            object_count: 0,
            children: None,
        }
    }

    pub fn with_hash(position_start: u64, position_end: u64, hash: [u8; 32]) -> Self {
        Self {
            hash,
            position_start,
            position_end,
            object_count: 0,
            children: None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.children.is_none()
    }

    pub fn hash(&self) -> &[u8; 32] {
        &self.hash
    }

    pub fn position_start(&self) -> u64 {
        self.position_start
    }

    pub fn position_end(&self) -> u64 {
        self.position_end
    }

    pub fn object_count(&self) -> u64 {
        self.object_count
    }

    pub fn children(&self) -> Option<&[MerkleNode; CHILDREN_COUNT]> {
        self.children.as_deref()
    }

    pub fn compute_hash(&mut self, oids: &[Oid]) {
        if oids.is_empty() {
            self.hash = [0u8; 32];
            self.object_count = 0;
            return;
        }

        let mut hasher = Sha256::new();
        for oid in oids {
            hasher.update(oid.as_bytes());
        }
        let result = hasher.finalize();
        self.hash.copy_from_slice(&result);
        self.object_count = oids.len() as u64;
    }

    pub fn compute_hash_from_children(&mut self) {
        if let Some(children) = &self.children {
            let mut hasher = Sha256::new();
            let mut total_count = 0u64;
            for child in children.iter() {
                hasher.update(child.hash);
                total_count += child.object_count;
            }
            let result = hasher.finalize();
            self.hash.copy_from_slice(&result);
            self.object_count = total_count;
        }
    }

    fn child_range(&self, index: usize) -> (u64, u64) {
        let range_size = self.position_end.saturating_sub(self.position_start);
        let child_size = range_size / CHILDREN_COUNT as u64;
        let start = self
            .position_start
            .saturating_add(index as u64 * child_size);
        let end = if index == CHILDREN_COUNT - 1 {
            self.position_end
        } else {
            start.saturating_add(child_size)
        };
        (start, end)
    }

    fn build_recursive(
        &mut self,
        positions: &BTreeMap<u64, Vec<Oid>>,
        current_depth: u8,
        max_depth: u8,
    ) {
        let oids_in_range: Vec<Oid> = positions
            .range(self.position_start..self.position_end)
            .flat_map(|(_, oids)| oids.iter().cloned())
            .collect();

        if current_depth >= max_depth || oids_in_range.is_empty() {
            self.compute_hash(&oids_in_range);
            return;
        }

        let children: [MerkleNode; CHILDREN_COUNT] = std::array::from_fn(|i| {
            let (start, end) = self.child_range(i);
            let mut child = MerkleNode::new(start, end);
            child.build_recursive(positions, current_depth + 1, max_depth);
            child
        });

        self.children = Some(Box::new(children));
        self.compute_hash_from_children();
    }
}

impl PartialEq for MerkleNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.position_start == other.position_start
            && self.position_end == other.position_end
    }
}

impl Eq for MerkleNode {}

#[derive(Debug, Clone)]
pub struct ObjectMerkleTree {
    root: MerkleNode,
    depth: u8,
    ring_version: u64,
}

impl ObjectMerkleTree {
    pub fn new(ring_version: u64) -> Self {
        Self::with_depth(ring_version, DEFAULT_DEPTH)
    }

    pub fn with_depth(ring_version: u64, depth: u8) -> Self {
        Self {
            root: MerkleNode::new(0, u64::MAX),
            depth,
            ring_version,
        }
    }

    pub fn build_from_positions<I>(ring_version: u64, iter: I) -> Self
    where
        I: IntoIterator<Item = (u64, Oid)>,
    {
        Self::build_from_positions_with_depth(ring_version, DEFAULT_DEPTH, iter)
    }

    pub fn build_from_positions_with_depth<I>(ring_version: u64, depth: u8, iter: I) -> Self
    where
        I: IntoIterator<Item = (u64, Oid)>,
    {
        let mut positions: BTreeMap<u64, Vec<Oid>> = BTreeMap::new();
        for (position, oid) in iter {
            positions.entry(position).or_default().push(oid);
        }

        let mut root = MerkleNode::new(0, u64::MAX);
        root.build_recursive(&positions, 0, depth);

        Self {
            root,
            depth,
            ring_version,
        }
    }

    pub fn root(&self) -> &MerkleNode {
        &self.root
    }

    pub fn depth(&self) -> u8 {
        self.depth
    }

    pub fn ring_version(&self) -> u64 {
        self.ring_version
    }

    pub fn root_hash(&self) -> &[u8; 32] {
        self.root.hash()
    }

    pub fn object_count(&self) -> u64 {
        self.root.object_count()
    }

    pub fn compare(&self, other: &Self) -> Vec<MerkleDiff> {
        let mut diffs = Vec::new();
        Self::compare_nodes(&self.root, &other.root, &mut diffs);
        diffs
    }

    fn compare_nodes(local: &MerkleNode, remote: &MerkleNode, diffs: &mut Vec<MerkleDiff>) {
        if local.hash == remote.hash {
            return;
        }

        match (&local.children, &remote.children) {
            (Some(local_children), Some(remote_children)) => {
                for (l, r) in local_children.iter().zip(remote_children.iter()) {
                    Self::compare_nodes(l, r, diffs);
                }
            }
            _ => {
                diffs.push(MerkleDiff {
                    position_start: local.position_start,
                    position_end: local.position_end,
                    local_hash: local.hash,
                    remote_hash: remote.hash,
                    local_count: local.object_count,
                    remote_count: remote.object_count,
                });
            }
        }
    }

    pub fn diff<I>(&self, other: &Self, local_iter: I) -> Vec<Oid>
    where
        I: IntoIterator<Item = (u64, Oid)>,
    {
        let diffs = self.compare(other);
        if diffs.is_empty() {
            return Vec::new();
        }

        let mut local_positions: BTreeMap<u64, Vec<Oid>> = BTreeMap::new();
        for (position, oid) in local_iter {
            local_positions.entry(position).or_default().push(oid);
        }

        let mut missing = Vec::new();
        for diff in diffs {
            if diff.local_count > diff.remote_count {
                let oids: Vec<Oid> = local_positions
                    .range(diff.position_start..diff.position_end)
                    .flat_map(|(_, oids)| oids.iter().cloned())
                    .collect();
                missing.extend(oids);
            }
        }

        missing
    }

    pub fn find_range_differences(&self, other: &Self) -> Vec<(u64, u64)> {
        self.compare(other)
            .into_iter()
            .map(|d| (d.position_start, d.position_end))
            .collect()
    }
}

impl PartialEq for ObjectMerkleTree {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.depth == other.depth
    }
}

impl Eq for ObjectMerkleTree {}

#[derive(Debug, Clone)]
pub struct MerkleDiff {
    pub position_start: u64,
    pub position_end: u64,
    pub local_hash: [u8; 32],
    pub remote_hash: [u8; 32],
    pub local_count: u64,
    pub remote_count: u64,
}

impl MerkleDiff {
    pub fn range_size(&self) -> u64 {
        self.position_end.saturating_sub(self.position_start)
    }

    pub fn count_difference(&self) -> i64 {
        self.local_count as i64 - self.remote_count as i64
    }
}

pub struct MerkleTreeBuilder {
    positions: BTreeMap<u64, Vec<Oid>>,
    ring_version: u64,
    depth: u8,
}

impl MerkleTreeBuilder {
    pub fn new(ring_version: u64) -> Self {
        Self::with_depth(ring_version, DEFAULT_DEPTH)
    }

    pub fn with_depth(ring_version: u64, depth: u8) -> Self {
        Self {
            positions: BTreeMap::new(),
            ring_version,
            depth,
        }
    }

    pub fn add(&mut self, position: u64, oid: Oid) {
        self.positions.entry(position).or_default().push(oid);
    }

    pub fn add_batch<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (u64, Oid)>,
    {
        for (position, oid) in iter {
            self.add(position, oid);
        }
    }

    pub fn len(&self) -> usize {
        self.positions.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub fn clear(&mut self) {
        self.positions.clear();
    }

    pub fn build(self) -> ObjectMerkleTree {
        let mut root = MerkleNode::new(0, u64::MAX);
        root.build_recursive(&self.positions, 0, self.depth);

        ObjectMerkleTree {
            root,
            depth: self.depth,
            ring_version: self.ring_version,
        }
    }

    pub fn build_range(self, start: u64, end: u64) -> ObjectMerkleTree {
        let filtered: BTreeMap<u64, Vec<Oid>> = self
            .positions
            .range(start..end)
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        let mut root = MerkleNode::new(start, end);
        root.build_recursive(&filtered, 0, self.depth);

        ObjectMerkleTree {
            root,
            depth: self.depth,
            ring_version: self.ring_version,
        }
    }
}

impl Default for MerkleTreeBuilder {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(data: &[u8]) -> Oid {
        Oid::hash(data)
    }

    #[test]
    fn test_merkle_node_new() {
        let node = MerkleNode::new(0, 100);
        assert_eq!(node.position_start(), 0);
        assert_eq!(node.position_end(), 100);
        assert_eq!(node.object_count(), 0);
        assert!(node.is_leaf());
        assert_eq!(node.hash(), &[0u8; 32]);
    }

    #[test]
    fn test_merkle_node_with_hash() {
        let hash = [1u8; 32];
        let node = MerkleNode::with_hash(10, 20, hash);
        assert_eq!(node.position_start(), 10);
        assert_eq!(node.position_end(), 20);
        assert_eq!(node.hash(), &hash);
    }

    #[test]
    fn test_merkle_node_compute_hash_empty() {
        let mut node = MerkleNode::new(0, 100);
        node.compute_hash(&[]);
        assert_eq!(node.hash(), &[0u8; 32]);
        assert_eq!(node.object_count(), 0);
    }

    #[test]
    fn test_merkle_node_compute_hash_single() {
        let mut node = MerkleNode::new(0, 100);
        let oid = make_oid(b"test");
        node.compute_hash(&[oid]);
        assert_ne!(node.hash(), &[0u8; 32]);
        assert_eq!(node.object_count(), 1);
    }

    #[test]
    fn test_merkle_node_compute_hash_multiple() {
        let mut node = MerkleNode::new(0, 100);
        let oids = vec![make_oid(b"a"), make_oid(b"b"), make_oid(b"c")];
        node.compute_hash(&oids);
        assert_ne!(node.hash(), &[0u8; 32]);
        assert_eq!(node.object_count(), 3);
    }

    #[test]
    fn test_merkle_node_compute_hash_deterministic() {
        let oids = vec![make_oid(b"a"), make_oid(b"b")];

        let mut node1 = MerkleNode::new(0, 100);
        node1.compute_hash(&oids);

        let mut node2 = MerkleNode::new(0, 100);
        node2.compute_hash(&oids);

        assert_eq!(node1.hash(), node2.hash());
    }

    #[test]
    fn test_merkle_node_compute_hash_order_matters() {
        let oid_a = make_oid(b"a");
        let oid_b = make_oid(b"b");

        let mut node1 = MerkleNode::new(0, 100);
        node1.compute_hash(&[oid_a, oid_b]);

        let mut node2 = MerkleNode::new(0, 100);
        node2.compute_hash(&[oid_b, oid_a]);

        assert_ne!(node1.hash(), node2.hash());
    }

    #[test]
    fn test_merkle_node_is_leaf() {
        let node = MerkleNode::new(0, 100);
        assert!(node.is_leaf());
        assert!(node.children().is_none());
    }

    #[test]
    fn test_merkle_node_equality() {
        let mut node1 = MerkleNode::new(0, 100);
        let mut node2 = MerkleNode::new(0, 100);

        let oid = make_oid(b"test");
        node1.compute_hash(&[oid]);
        node2.compute_hash(&[oid]);

        assert_eq!(node1, node2);
    }

    #[test]
    fn test_merkle_node_inequality_different_hash() {
        let mut node1 = MerkleNode::new(0, 100);
        let mut node2 = MerkleNode::new(0, 100);

        node1.compute_hash(&[make_oid(b"a")]);
        node2.compute_hash(&[make_oid(b"b")]);

        assert_ne!(node1, node2);
    }

    #[test]
    fn test_merkle_node_inequality_different_range() {
        let node1 = MerkleNode::new(0, 100);
        let node2 = MerkleNode::new(0, 200);

        assert_ne!(node1, node2);
    }

    #[test]
    fn test_object_merkle_tree_new() {
        let tree = ObjectMerkleTree::new(1);
        assert_eq!(tree.ring_version(), 1);
        assert_eq!(tree.depth(), DEFAULT_DEPTH);
        assert_eq!(tree.object_count(), 0);
    }

    #[test]
    fn test_object_merkle_tree_with_depth() {
        let tree = ObjectMerkleTree::with_depth(2, 6);
        assert_eq!(tree.ring_version(), 2);
        assert_eq!(tree.depth(), 6);
    }

    #[test]
    fn test_object_merkle_tree_build_empty() {
        let tree = ObjectMerkleTree::build_from_positions(1, std::iter::empty());
        assert_eq!(tree.object_count(), 0);
        assert_eq!(tree.root_hash(), &[0u8; 32]);
    }

    #[test]
    fn test_object_merkle_tree_build_single() {
        let oid = make_oid(b"test");
        let tree = ObjectMerkleTree::build_from_positions(1, [(1000, oid)]);
        assert_eq!(tree.object_count(), 1);
        assert_ne!(tree.root_hash(), &[0u8; 32]);
    }

    #[test]
    fn test_object_merkle_tree_build_multiple() {
        let items = vec![
            (1000u64, make_oid(b"a")),
            (2000u64, make_oid(b"b")),
            (3000u64, make_oid(b"c")),
        ];
        let tree = ObjectMerkleTree::build_from_positions(1, items);
        assert_eq!(tree.object_count(), 3);
    }

    #[test]
    fn test_object_merkle_tree_build_with_depth() {
        let items = vec![(1000u64, make_oid(b"a")), (2000u64, make_oid(b"b"))];
        let tree = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items);
        assert_eq!(tree.depth(), 2);
        assert_eq!(tree.object_count(), 2);
    }

    #[test]
    fn test_object_merkle_tree_compare_identical() {
        let items: Vec<(u64, Oid)> = vec![
            (1000u64, make_oid(b"a")),
            (2000u64, make_oid(b"b")),
            (3000u64, make_oid(b"c")),
        ];
        let tree1 = ObjectMerkleTree::build_from_positions(1, items.clone());
        let tree2 = ObjectMerkleTree::build_from_positions(1, items);

        let diffs = tree1.compare(&tree2);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_compare_different() {
        let tree1 = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"a"))]);
        let tree2 = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"b"))]);

        let diffs = tree1.compare(&tree2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_compare_one_empty() {
        let tree1 = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"a"))]);
        let tree2 = ObjectMerkleTree::new(1);

        let diffs = tree1.compare(&tree2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_compare_extra_item() {
        let items1: Vec<(u64, Oid)> = vec![
            (1000u64, make_oid(b"a")),
            (2000u64, make_oid(b"b")),
            (3000u64, make_oid(b"c")),
        ];
        let items2: Vec<(u64, Oid)> = vec![(1000u64, make_oid(b"a")), (2000u64, make_oid(b"b"))];

        let tree1 = ObjectMerkleTree::build_from_positions(1, items1);
        let tree2 = ObjectMerkleTree::build_from_positions(1, items2);

        let diffs = tree1.compare(&tree2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_diff_identical() {
        let items: Vec<(u64, Oid)> = vec![(1000u64, make_oid(b"a")), (2000u64, make_oid(b"b"))];
        let tree1 = ObjectMerkleTree::build_from_positions(1, items.clone());
        let tree2 = ObjectMerkleTree::build_from_positions(1, items.clone());

        let missing = tree1.diff(&tree2, items);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_diff_missing_on_remote() {
        let oid_a = make_oid(b"a");
        let oid_b = make_oid(b"b");

        let items1 = vec![(1000u64, oid_a), (2000u64, oid_b)];
        let items2 = vec![(1000u64, oid_a)];

        let tree1 = ObjectMerkleTree::build_from_positions(1, items1.clone());
        let tree2 = ObjectMerkleTree::build_from_positions(1, items2);

        let missing = tree1.diff(&tree2, items1);
        assert!(!missing.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_find_range_differences() {
        let tree1 = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"a"))]);
        let tree2 = ObjectMerkleTree::new(1);

        let ranges = tree1.find_range_differences(&tree2);
        assert!(!ranges.is_empty());
    }

    #[test]
    fn test_object_merkle_tree_equality() {
        let items: Vec<(u64, Oid)> = vec![(1000u64, make_oid(b"a"))];
        let tree1 = ObjectMerkleTree::build_from_positions(1, items.clone());
        let tree2 = ObjectMerkleTree::build_from_positions(1, items);

        assert_eq!(tree1, tree2);
    }

    #[test]
    fn test_object_merkle_tree_inequality() {
        let tree1 = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"a"))]);
        let tree2 = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"b"))]);

        assert_ne!(tree1, tree2);
    }

    #[test]
    fn test_merkle_diff_range_size() {
        let diff = MerkleDiff {
            position_start: 100,
            position_end: 500,
            local_hash: [0u8; 32],
            remote_hash: [0u8; 32],
            local_count: 10,
            remote_count: 5,
        };
        assert_eq!(diff.range_size(), 400);
    }

    #[test]
    fn test_merkle_diff_count_difference() {
        let diff = MerkleDiff {
            position_start: 0,
            position_end: 100,
            local_hash: [0u8; 32],
            remote_hash: [0u8; 32],
            local_count: 10,
            remote_count: 5,
        };
        assert_eq!(diff.count_difference(), 5);

        let diff2 = MerkleDiff {
            position_start: 0,
            position_end: 100,
            local_hash: [0u8; 32],
            remote_hash: [0u8; 32],
            local_count: 3,
            remote_count: 8,
        };
        assert_eq!(diff2.count_difference(), -5);
    }

    #[test]
    fn test_merkle_tree_builder_new() {
        let builder = MerkleTreeBuilder::new(1);
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn test_merkle_tree_builder_with_depth() {
        let builder = MerkleTreeBuilder::with_depth(1, 6);
        assert_eq!(builder.depth, 6);
    }

    #[test]
    fn test_merkle_tree_builder_add() {
        let mut builder = MerkleTreeBuilder::new(1);
        builder.add(1000, make_oid(b"a"));
        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());
    }

    #[test]
    fn test_merkle_tree_builder_add_batch() {
        let mut builder = MerkleTreeBuilder::new(1);
        let items = vec![
            (1000u64, make_oid(b"a")),
            (2000u64, make_oid(b"b")),
            (3000u64, make_oid(b"c")),
        ];
        builder.add_batch(items);
        assert_eq!(builder.len(), 3);
    }

    #[test]
    fn test_merkle_tree_builder_add_same_position() {
        let mut builder = MerkleTreeBuilder::new(1);
        builder.add(1000, make_oid(b"a"));
        builder.add(1000, make_oid(b"b"));
        assert_eq!(builder.len(), 2);
    }

    #[test]
    fn test_merkle_tree_builder_clear() {
        let mut builder = MerkleTreeBuilder::new(1);
        builder.add(1000, make_oid(b"a"));
        builder.add(2000, make_oid(b"b"));
        assert_eq!(builder.len(), 2);

        builder.clear();
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn test_merkle_tree_builder_build() {
        let mut builder = MerkleTreeBuilder::new(1);
        builder.add(1000, make_oid(b"a"));
        builder.add(2000, make_oid(b"b"));

        let tree = builder.build();
        assert_eq!(tree.ring_version(), 1);
        assert_eq!(tree.object_count(), 2);
    }

    #[test]
    fn test_merkle_tree_builder_build_empty() {
        let builder = MerkleTreeBuilder::new(1);
        let tree = builder.build();
        assert_eq!(tree.object_count(), 0);
    }

    #[test]
    fn test_merkle_tree_builder_build_range() {
        let mut builder = MerkleTreeBuilder::new(1);
        builder.add(1000, make_oid(b"a"));
        builder.add(5000, make_oid(b"b"));
        builder.add(9000, make_oid(b"c"));

        let tree = builder.build_range(0, 6000);
        assert_eq!(tree.object_count(), 2);
    }

    #[test]
    fn test_merkle_tree_builder_default() {
        let builder = MerkleTreeBuilder::default();
        assert!(builder.is_empty());
        assert_eq!(builder.ring_version, 0);
    }

    #[test]
    fn test_merkle_tree_builder_produces_same_tree() {
        let items: Vec<(u64, Oid)> = vec![
            (1000u64, make_oid(b"a")),
            (2000u64, make_oid(b"b")),
            (3000u64, make_oid(b"c")),
        ];

        let tree1 = ObjectMerkleTree::build_from_positions(1, items.clone());

        let mut builder = MerkleTreeBuilder::new(1);
        builder.add_batch(items);
        let tree2 = builder.build();

        assert_eq!(tree1, tree2);
    }

    #[test]
    fn test_tree_with_children_not_leaf() {
        let items: Vec<(u64, Oid)> = (0..100)
            .map(|i| (i as u64 * 1_000_000_000_000_000, make_oid(&[i as u8])))
            .collect();

        let tree = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items);
        assert!(!tree.root().is_leaf());
        assert!(tree.root().children().is_some());
    }

    #[test]
    fn test_compare_trees_with_children() {
        let items1: Vec<(u64, Oid)> = (0..50)
            .map(|i| (i as u64 * 1_000_000_000_000_000, make_oid(&[i as u8])))
            .collect();

        let items2: Vec<(u64, Oid)> = (0..50)
            .map(|i| (i as u64 * 1_000_000_000_000_000, make_oid(&[i as u8])))
            .collect();

        let tree1 = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items1);
        let tree2 = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items2);

        let diffs = tree1.compare(&tree2);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_compare_trees_with_children_different() {
        let items1: Vec<(u64, Oid)> = (0..50)
            .map(|i| (i as u64 * 1_000_000_000_000_000, make_oid(&[i as u8])))
            .collect();

        let items2: Vec<(u64, Oid)> = (0..49)
            .map(|i| (i as u64 * 1_000_000_000_000_000, make_oid(&[i as u8])))
            .collect();

        let tree1 = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items1);
        let tree2 = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items2);

        let diffs = tree1.compare(&tree2);
        assert!(!diffs.is_empty());
    }

    #[test]
    fn test_merkle_node_child_range() {
        let node = MerkleNode::new(0, u64::MAX);
        let (start, end) = node.child_range(0);
        assert_eq!(start, 0);
        assert!(end > 0);

        let (start_last, end_last) = node.child_range(15);
        assert!(start_last > 0);
        assert_eq!(end_last, u64::MAX);
    }

    #[test]
    fn test_merkle_diff_debug() {
        let diff = MerkleDiff {
            position_start: 0,
            position_end: 100,
            local_hash: [1u8; 32],
            remote_hash: [2u8; 32],
            local_count: 5,
            remote_count: 3,
        };
        let debug = format!("{:?}", diff);
        assert!(debug.contains("MerkleDiff"));
    }

    #[test]
    fn test_merkle_diff_clone() {
        let diff = MerkleDiff {
            position_start: 0,
            position_end: 100,
            local_hash: [1u8; 32],
            remote_hash: [2u8; 32],
            local_count: 5,
            remote_count: 3,
        };
        let cloned = diff.clone();
        assert_eq!(diff.position_start, cloned.position_start);
        assert_eq!(diff.local_hash, cloned.local_hash);
    }

    #[test]
    fn test_merkle_node_debug() {
        let node = MerkleNode::new(0, 100);
        let debug = format!("{:?}", node);
        assert!(debug.contains("MerkleNode"));
    }

    #[test]
    fn test_merkle_node_clone() {
        let mut node = MerkleNode::new(0, 100);
        node.compute_hash(&[make_oid(b"test")]);
        let cloned = node.clone();
        assert_eq!(node, cloned);
    }

    #[test]
    fn test_object_merkle_tree_debug() {
        let tree = ObjectMerkleTree::new(1);
        let debug = format!("{:?}", tree);
        assert!(debug.contains("ObjectMerkleTree"));
    }

    #[test]
    fn test_object_merkle_tree_clone() {
        let tree = ObjectMerkleTree::build_from_positions(1, [(1000u64, make_oid(b"a"))]);
        let cloned = tree.clone();
        assert_eq!(tree, cloned);
    }

    #[test]
    fn test_large_tree_performance() {
        let items: Vec<(u64, Oid)> = (0u32..10000)
            .map(|i| (i as u64 * 1000, make_oid(&i.to_le_bytes())))
            .collect();

        let tree = ObjectMerkleTree::build_from_positions(1, items);
        assert_eq!(tree.object_count(), 10000);
    }

    #[test]
    fn test_position_at_boundaries() {
        let items = vec![
            (0u64, make_oid(b"start")),
            (u64::MAX - 1, make_oid(b"near_end")),
        ];

        let tree = ObjectMerkleTree::build_from_positions(1, items);
        assert_eq!(tree.object_count(), 2);
    }

    #[test]
    fn test_duplicate_oids_same_position() {
        let oid = make_oid(b"duplicate");
        let items = vec![(1000u64, oid), (1000u64, oid)];

        let tree = ObjectMerkleTree::build_from_positions(1, items);
        assert_eq!(tree.object_count(), 2);
    }

    #[test]
    fn test_builder_incremental_vs_batch() {
        let items: Vec<(u64, Oid)> = vec![
            (1000u64, make_oid(b"a")),
            (2000u64, make_oid(b"b")),
            (3000u64, make_oid(b"c")),
        ];

        let mut builder1 = MerkleTreeBuilder::new(1);
        for (pos, oid) in items.clone() {
            builder1.add(pos, oid);
        }
        let tree1 = builder1.build();

        let mut builder2 = MerkleTreeBuilder::new(1);
        builder2.add_batch(items);
        let tree2 = builder2.build();

        assert_eq!(tree1, tree2);
    }

    #[test]
    fn test_zero_depth_tree() {
        let items = vec![(1000u64, make_oid(b"a")), (2000u64, make_oid(b"b"))];

        let tree = ObjectMerkleTree::build_from_positions_with_depth(1, 0, items);
        assert!(tree.root().is_leaf());
        assert_eq!(tree.object_count(), 2);
    }

    #[test]
    fn test_deep_tree() {
        let items: Vec<(u64, Oid)> = (0..100)
            .map(|i| (i as u64 * 1_000_000_000_000, make_oid(&[i as u8])))
            .collect();

        let tree = ObjectMerkleTree::build_from_positions_with_depth(1, 8, items);
        assert_eq!(tree.depth(), 8);
        assert_eq!(tree.object_count(), 100);
    }

    #[test]
    fn test_compare_different_depths() {
        let items: Vec<(u64, Oid)> = vec![(1000u64, make_oid(b"a"))];

        let tree1 = ObjectMerkleTree::build_from_positions_with_depth(1, 2, items.clone());
        let tree2 = ObjectMerkleTree::build_from_positions_with_depth(1, 4, items);

        assert_ne!(tree1, tree2);
    }

    #[test]
    fn test_find_range_differences_empty() {
        let tree1 = ObjectMerkleTree::new(1);
        let tree2 = ObjectMerkleTree::new(1);

        let ranges = tree1.find_range_differences(&tree2);
        assert!(ranges.is_empty());
    }
}
