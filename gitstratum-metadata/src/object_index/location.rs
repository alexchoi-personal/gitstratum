use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use gitstratum_core::Oid;

pub type NodeId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectLocation {
    pub oid: Oid,
    pub nodes: Vec<NodeId>,
    pub primary_node: Option<NodeId>,
    pub size_bytes: Option<u64>,
    pub object_type: ObjectType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    Commit,
    Tree,
    Blob,
    Tag,
    Unknown,
}

impl ObjectLocation {
    pub fn new(oid: Oid, nodes: Vec<NodeId>, object_type: ObjectType) -> Self {
        let primary_node = nodes.first().cloned();
        Self {
            oid,
            nodes,
            primary_node,
            size_bytes: None,
            object_type,
        }
    }

    pub fn with_size(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }

    pub fn add_node(&mut self, node_id: NodeId) {
        if !self.nodes.contains(&node_id) {
            self.nodes.push(node_id);
        }
    }

    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|n| n != node_id);
        if self.primary_node.as_deref() == Some(node_id) {
            self.primary_node = self.nodes.first().cloned();
        }
    }

    pub fn set_primary(&mut self, node_id: NodeId) {
        if self.nodes.contains(&node_id) {
            self.primary_node = Some(node_id);
        }
    }

    pub fn is_available(&self) -> bool {
        !self.nodes.is_empty()
    }

    pub fn replica_count(&self) -> usize {
        self.nodes.len()
    }
}

pub struct ObjectLocationIndex {
    locations: RwLock<HashMap<Oid, ObjectLocation>>,
    node_objects: RwLock<HashMap<NodeId, HashSet<Oid>>>,
}

impl ObjectLocationIndex {
    pub fn new() -> Self {
        Self {
            locations: RwLock::new(HashMap::new()),
            node_objects: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, oid: &Oid) -> Option<ObjectLocation> {
        self.locations.read().get(oid).cloned()
    }

    pub fn get_nodes(&self, oid: &Oid) -> Vec<NodeId> {
        self.locations
            .read()
            .get(oid)
            .map(|l| l.nodes.clone())
            .unwrap_or_default()
    }

    pub fn get_primary(&self, oid: &Oid) -> Option<NodeId> {
        self.locations
            .read()
            .get(oid)
            .and_then(|l| l.primary_node.clone())
    }

    pub fn put(&self, location: ObjectLocation) {
        let oid = location.oid;
        let nodes = location.nodes.clone();

        self.locations.write().insert(oid, location);

        let mut node_objects = self.node_objects.write();
        for node_id in nodes {
            node_objects.entry(node_id).or_default().insert(oid);
        }
    }

    pub fn add_node_to_object(&self, oid: &Oid, node_id: NodeId) -> bool {
        let mut locations = self.locations.write();
        if let Some(location) = locations.get_mut(oid) {
            location.add_node(node_id.clone());
            self.node_objects
                .write()
                .entry(node_id)
                .or_default()
                .insert(*oid);
            true
        } else {
            false
        }
    }

    pub fn remove_node_from_object(&self, oid: &Oid, node_id: &str) -> bool {
        let mut locations = self.locations.write();
        if let Some(location) = locations.get_mut(oid) {
            location.remove_node(node_id);
            if let Some(objects) = self.node_objects.write().get_mut(node_id) {
                objects.remove(oid);
            }
            true
        } else {
            false
        }
    }

    pub fn remove_object(&self, oid: &Oid) -> Option<ObjectLocation> {
        let location = self.locations.write().remove(oid);
        if let Some(ref loc) = location {
            let mut node_objects = self.node_objects.write();
            for node_id in &loc.nodes {
                if let Some(objects) = node_objects.get_mut(node_id) {
                    objects.remove(oid);
                }
            }
        }
        location
    }

    pub fn get_objects_on_node(&self, node_id: &str) -> HashSet<Oid> {
        self.node_objects
            .read()
            .get(node_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn remove_node(&self, node_id: &str) {
        let objects = self.node_objects.write().remove(node_id);
        if let Some(objects) = objects {
            let mut locations = self.locations.write();
            for oid in objects {
                if let Some(location) = locations.get_mut(&oid) {
                    location.remove_node(node_id);
                }
            }
        }
    }

    pub fn object_count(&self) -> usize {
        self.locations.read().len()
    }

    pub fn node_count(&self) -> usize {
        self.node_objects.read().len()
    }

    pub fn clear(&self) {
        self.locations.write().clear();
        self.node_objects.write().clear();
    }
}

impl Default for ObjectLocationIndex {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ObjectLocator: Send + Sync {
    fn locate(&self, oid: &Oid) -> Option<ObjectLocation>;
    fn locate_batch(&self, oids: &[Oid]) -> HashMap<Oid, ObjectLocation>;
}

impl ObjectLocator for ObjectLocationIndex {
    fn locate(&self, oid: &Oid) -> Option<ObjectLocation> {
        self.get(oid)
    }

    fn locate_batch(&self, oids: &[Oid]) -> HashMap<Oid, ObjectLocation> {
        let locations = self.locations.read();
        oids.iter()
            .filter_map(|oid| locations.get(oid).map(|l| (*oid, l.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_location_new() {
        let oid = Oid::hash(b"object");
        let nodes = vec!["node1".to_string(), "node2".to_string()];

        let location = ObjectLocation::new(oid, nodes.clone(), ObjectType::Blob);

        assert_eq!(location.oid, oid);
        assert_eq!(location.nodes, nodes);
        assert_eq!(location.primary_node, Some("node1".to_string()));
        assert!(location.size_bytes.is_none());
        assert!(location.is_available());
        assert_eq!(location.replica_count(), 2);
    }

    #[test]
    fn test_object_location_with_size() {
        let oid = Oid::hash(b"object");
        let location =
            ObjectLocation::new(oid, vec!["node1".to_string()], ObjectType::Blob).with_size(1024);

        assert_eq!(location.size_bytes, Some(1024));
    }

    #[test]
    fn test_object_location_add_remove_node() {
        let oid = Oid::hash(b"object");
        let mut location = ObjectLocation::new(oid, vec!["node1".to_string()], ObjectType::Blob);

        location.add_node("node2".to_string());
        assert_eq!(location.replica_count(), 2);

        location.add_node("node1".to_string());
        assert_eq!(location.replica_count(), 2);

        location.remove_node("node1");
        assert_eq!(location.replica_count(), 1);
        assert_eq!(location.primary_node, Some("node2".to_string()));
    }

    #[test]
    fn test_object_location_set_primary() {
        let oid = Oid::hash(b"object");
        let mut location = ObjectLocation::new(
            oid,
            vec!["node1".to_string(), "node2".to_string()],
            ObjectType::Blob,
        );

        location.set_primary("node2".to_string());
        assert_eq!(location.primary_node, Some("node2".to_string()));

        location.set_primary("node3".to_string());
        assert_eq!(location.primary_node, Some("node2".to_string()));
    }

    #[test]
    fn test_object_type() {
        assert_eq!(ObjectType::Commit, ObjectType::Commit);
        assert_ne!(ObjectType::Commit, ObjectType::Blob);
    }

    #[test]
    fn test_object_location_index_put_get() {
        let index = ObjectLocationIndex::new();
        let oid = Oid::hash(b"object");
        let location = ObjectLocation::new(oid, vec!["node1".to_string()], ObjectType::Blob);

        index.put(location);

        let retrieved = index.get(&oid).unwrap();
        assert_eq!(retrieved.oid, oid);
    }

    #[test]
    fn test_object_location_index_get_nodes() {
        let index = ObjectLocationIndex::new();
        let oid = Oid::hash(b"object");
        let location = ObjectLocation::new(
            oid,
            vec!["node1".to_string(), "node2".to_string()],
            ObjectType::Blob,
        );

        index.put(location);

        let nodes = index.get_nodes(&oid);
        assert_eq!(nodes.len(), 2);

        let primary = index.get_primary(&oid).unwrap();
        assert_eq!(primary, "node1");
    }

    #[test]
    fn test_object_location_index_add_remove_node() {
        let index = ObjectLocationIndex::new();
        let oid = Oid::hash(b"object");
        let location = ObjectLocation::new(oid, vec!["node1".to_string()], ObjectType::Blob);

        index.put(location);

        assert!(index.add_node_to_object(&oid, "node2".to_string()));
        assert_eq!(index.get_nodes(&oid).len(), 2);

        assert!(index.remove_node_from_object(&oid, "node1"));
        assert_eq!(index.get_nodes(&oid).len(), 1);
    }

    #[test]
    fn test_object_location_index_remove_object() {
        let index = ObjectLocationIndex::new();
        let oid = Oid::hash(b"object");
        let location = ObjectLocation::new(oid, vec!["node1".to_string()], ObjectType::Blob);

        index.put(location);
        assert!(index.get(&oid).is_some());

        let removed = index.remove_object(&oid);
        assert!(removed.is_some());
        assert!(index.get(&oid).is_none());
    }

    #[test]
    fn test_object_location_index_get_objects_on_node() {
        let index = ObjectLocationIndex::new();

        let oid1 = Oid::hash(b"object1");
        let oid2 = Oid::hash(b"object2");

        index.put(ObjectLocation::new(
            oid1,
            vec!["node1".to_string()],
            ObjectType::Blob,
        ));
        index.put(ObjectLocation::new(
            oid2,
            vec!["node1".to_string(), "node2".to_string()],
            ObjectType::Tree,
        ));

        let objects = index.get_objects_on_node("node1");
        assert_eq!(objects.len(), 2);

        let objects = index.get_objects_on_node("node2");
        assert_eq!(objects.len(), 1);
    }

    #[test]
    fn test_object_location_index_remove_node() {
        let index = ObjectLocationIndex::new();
        let oid = Oid::hash(b"object");
        let location = ObjectLocation::new(
            oid,
            vec!["node1".to_string(), "node2".to_string()],
            ObjectType::Blob,
        );

        index.put(location);
        index.remove_node("node1");

        let nodes = index.get_nodes(&oid);
        assert_eq!(nodes.len(), 1);
        assert!(!nodes.contains(&"node1".to_string()));
    }

    #[test]
    fn test_object_location_index_counts() {
        let index = ObjectLocationIndex::new();

        assert_eq!(index.object_count(), 0);
        assert_eq!(index.node_count(), 0);

        let oid = Oid::hash(b"object");
        index.put(ObjectLocation::new(
            oid,
            vec!["node1".to_string()],
            ObjectType::Blob,
        ));

        assert_eq!(index.object_count(), 1);
        assert_eq!(index.node_count(), 1);

        index.clear();
        assert_eq!(index.object_count(), 0);
        assert_eq!(index.node_count(), 0);
    }

    #[test]
    fn test_object_locator_trait() {
        let index = ObjectLocationIndex::new();
        let oid1 = Oid::hash(b"object1");
        let oid2 = Oid::hash(b"object2");

        index.put(ObjectLocation::new(
            oid1,
            vec!["node1".to_string()],
            ObjectType::Blob,
        ));
        index.put(ObjectLocation::new(
            oid2,
            vec!["node2".to_string()],
            ObjectType::Tree,
        ));

        let locator: &dyn ObjectLocator = &index;
        assert!(locator.locate(&oid1).is_some());

        let batch = locator.locate_batch(&[oid1, oid2, Oid::hash(b"missing")]);
        assert_eq!(batch.len(), 2);
    }
}
