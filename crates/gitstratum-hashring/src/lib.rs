mod builder;
mod error;
mod node;
mod ring;

pub use builder::HashRingBuilder;
pub use error::{HashRingError, Result};
pub use node::{NodeId, NodeInfo, NodeState};
pub use ring::ConsistentHashRing;

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Oid;

    fn create_test_node(id: &str) -> NodeInfo {
        NodeInfo::new(id, format!("10.0.0.{}", id.chars().last().unwrap()), 9002)
    }

    #[test]
    fn test_add_remove_node() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();

        let node1 = create_test_node("node-1");
        ring.add_node(node1.clone()).unwrap();
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.version(), 1);

        let node2 = create_test_node("node-2");
        ring.add_node(node2.clone()).unwrap();
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.version(), 2);

        ring.remove_node(&node1.id).unwrap();
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.version(), 3);
    }

    #[test]
    fn test_primary_node() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let key = b"test-key";
        let primary = ring.primary_node(key).unwrap();
        assert!(["node-1", "node-2", "node-3"].contains(&primary.id.as_str()));

        let primary2 = ring.primary_node(key).unwrap();
        assert_eq!(primary.id, primary2.id);
    }

    #[test]
    fn test_nodes_for_key() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let key = b"test-key";
        let nodes = ring.nodes_for_key(key).unwrap();
        assert_eq!(nodes.len(), 2);

        assert_ne!(nodes[0].id, nodes[1].id);
    }

    #[test]
    fn test_nodes_for_oid() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let oid = Oid::hash(b"test content");
        let nodes = ring.nodes_for_oid(&oid).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_insufficient_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(3)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        let result = ring.nodes_for_key(b"test");
        assert!(matches!(
            result,
            Err(HashRingError::InsufficientNodes(3, 2))
        ));
    }

    #[test]
    fn test_node_state() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Draining)
            .unwrap();

        let node = ring.get_node(&NodeId::new("node-1")).unwrap();
        assert_eq!(node.state, NodeState::Draining);
        assert!(node.state.can_serve_reads());
        assert!(!node.state.can_serve_writes());
    }

    #[test]
    fn test_distribution() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .build()
            .unwrap();

        let mut counts = std::collections::HashMap::new();

        for i in 0..1000 {
            let key = format!("key-{}", i);
            let primary = ring.primary_node(key.as_bytes()).unwrap();
            *counts.entry(primary.id.as_str().to_string()).or_insert(0) += 1;
        }

        for (node, count) in &counts {
            let percentage = (*count as f64 / 1000.0) * 100.0;
            assert!(
                percentage > 15.0 && percentage < 35.0,
                "Node {} has {}% of keys, expected ~25%",
                node,
                percentage
            );
        }
    }

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        let result = ring.primary_node(b"test");
        assert!(matches!(result, Err(HashRingError::EmptyRing)));
    }

    #[test]
    fn test_node_id() {
        let id = NodeId::new("node-1");
        assert_eq!(id.as_str(), "node-1");
        assert_eq!(format!("{}", id), "node-1");
    }

    #[test]
    fn test_node_info() {
        let node = NodeInfo::new("node-1", "10.0.0.1", 9002);
        assert_eq!(node.endpoint(), "10.0.0.1:9002");
        assert!(node.state.is_active());
        assert!(node.state.can_serve_reads());
        assert!(node.state.can_serve_writes());
    }

    #[test]
    fn test_node_state_draining() {
        let state = NodeState::Draining;
        assert!(!state.is_active());
        assert!(state.can_serve_reads());
        assert!(!state.can_serve_writes());
    }

    #[test]
    fn test_node_state_down() {
        let state = NodeState::Down;
        assert!(!state.is_active());
        assert!(!state.can_serve_reads());
        assert!(!state.can_serve_writes());
    }

    #[test]
    fn test_node_state_joining() {
        let state = NodeState::Joining;
        assert!(!state.is_active());
        assert!(!state.can_serve_reads());
        assert!(!state.can_serve_writes());
    }

    #[test]
    fn test_with_nodes() {
        let nodes = vec![create_test_node("node-1"), create_test_node("node-2")];
        let ring = ConsistentHashRing::with_nodes(nodes, 16, 2).unwrap();
        assert_eq!(ring.node_count(), 2);
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        let result = ring.remove_node(&NodeId::new("nonexistent"));
        assert!(matches!(result, Err(HashRingError::NodeNotFound(_))));
    }

    #[test]
    fn test_set_node_state_nonexistent() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        let result = ring.set_node_state(&NodeId::new("nonexistent"), NodeState::Draining);
        assert!(matches!(result, Err(HashRingError::NodeNotFound(_))));
    }

    #[test]
    fn test_get_nonexistent_node() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        assert!(ring.get_node(&NodeId::new("nonexistent")).is_none());
    }

    #[test]
    fn test_get_nodes() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        let nodes = ring.get_nodes();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_active_nodes() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();

        let active = ring.active_nodes();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn test_primary_node_for_oid() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        let oid = Oid::hash(b"test");
        let node = ring.primary_node_for_oid(&oid).unwrap();
        assert!(["node-1", "node-2"].contains(&node.id.as_str()));
    }

    #[test]
    fn test_nodes_for_prefix() {
        let ring = HashRingBuilder::new()
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        let nodes = ring.nodes_for_prefix(0xAB).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_nodes_for_prefix_empty_ring() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        let result = ring.nodes_for_prefix(0xAB);
        assert!(result.is_err());
    }

    #[test]
    fn test_replication_factor() {
        let ring = ConsistentHashRing::new(16, 3).unwrap();
        assert_eq!(ring.replication_factor(), 3);
    }

    #[test]
    fn test_clone_ring() {
        let ring = HashRingBuilder::new()
            .add_node(create_test_node("node-1"))
            .build()
            .unwrap();

        let cloned = ring.clone();
        assert_eq!(cloned.node_count(), 1);
        assert_eq!(cloned.version(), ring.version());
    }

    #[test]
    fn test_hash_ring_builder_default() {
        let builder = HashRingBuilder::default();
        let ring = builder.build().unwrap();
        assert_eq!(ring.node_count(), 0);
    }

    #[test]
    fn test_nodes_for_key_empty() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        let result = ring.nodes_for_key(b"test");
        assert!(matches!(result, Err(HashRingError::EmptyRing)));
    }

    #[test]
    fn test_version_increments() {
        let ring = ConsistentHashRing::new(16, 2).unwrap();
        assert_eq!(ring.version(), 0);

        ring.add_node(create_test_node("node-1")).unwrap();
        assert_eq!(ring.version(), 1);

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Draining)
            .unwrap();
        assert_eq!(ring.version(), 2);

        ring.remove_node(&NodeId::new("node-1")).unwrap();
        assert_eq!(ring.version(), 3);
    }

    #[test]
    fn test_nodes_for_key_with_draining() {
        let ring = HashRingBuilder::new()
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Draining)
            .unwrap();

        let nodes = ring.nodes_for_key(b"test").unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_hash_ring_error_display() {
        let err = HashRingError::NodeNotFound("test".to_string());
        assert!(format!("{}", err).contains("test"));

        let err = HashRingError::EmptyRing;
        assert!(format!("{}", err).contains("empty"));

        let err = HashRingError::InsufficientNodes(3, 2);
        assert!(format!("{}", err).contains("3"));
    }

    #[test]
    fn test_nodes_for_key_insufficient_active_after_state_change() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(3)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();
        ring.set_node_state(&NodeId::new("node-2"), NodeState::Joining)
            .unwrap();

        let result = ring.nodes_for_key(b"test-key");
        assert!(matches!(
            result,
            Err(HashRingError::InsufficientNodes(3, _))
        ));
    }

    #[test]
    fn test_nodes_for_key_with_all_down_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();
        ring.set_node_state(&NodeId::new("node-2"), NodeState::Down)
            .unwrap();

        let result = ring.nodes_for_key(b"test-key");
        assert!(matches!(
            result,
            Err(HashRingError::InsufficientNodes(2, 0))
        ));
    }

    #[test]
    fn test_distribution_variance() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .build()
            .unwrap();

        let mut counts = std::collections::HashMap::new();

        for i in 0..1000 {
            let key = format!("key-{}", i);
            let primary = ring.primary_node(key.as_bytes()).unwrap();
            *counts.entry(primary.id.as_str().to_string()).or_insert(0) += 1;
        }

        assert_eq!(counts.len(), 4);
        for count in counts.values() {
            assert!(*count > 100);
        }
    }

    #[test]
    fn test_zero_replication_factor_rejected() {
        let result = ConsistentHashRing::new(16, 0);
        assert!(matches!(result, Err(HashRingError::InvalidConfig(_))));
    }

    #[test]
    fn test_zero_replication_factor_rejected_via_builder() {
        let result = HashRingBuilder::new()
            .virtual_nodes(16)
            .replication_factor(0)
            .add_node(create_test_node("node-1"))
            .build();

        assert!(matches!(result, Err(HashRingError::InvalidConfig(_))));
    }

    #[test]
    fn test_nodes_for_key_iterates_multiple_nodes() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(3)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .add_node(create_test_node("node-5"))
            .build()
            .unwrap();

        for i in 0..100 {
            let key = format!("test-key-{}", i);
            let nodes = ring.nodes_for_key(key.as_bytes()).unwrap();
            assert_eq!(nodes.len(), 3);
            let unique: std::collections::HashSet<_> =
                nodes.iter().map(|n| n.id.as_str()).collect();
            assert_eq!(unique.len(), 3);
        }
    }

    #[test]
    fn test_nodes_for_key_skips_nodes_that_cannot_serve_reads() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(64)
            .replication_factor(2)
            .add_node(create_test_node("node-1"))
            .add_node(create_test_node("node-2"))
            .add_node(create_test_node("node-3"))
            .add_node(create_test_node("node-4"))
            .build()
            .unwrap();

        ring.set_node_state(&NodeId::new("node-1"), NodeState::Down)
            .unwrap();
        ring.set_node_state(&NodeId::new("node-3"), NodeState::Joining)
            .unwrap();

        for i in 0..100 {
            let key = format!("test-key-{}", i);
            let nodes = ring.nodes_for_key(key.as_bytes()).unwrap();
            assert_eq!(nodes.len(), 2);
            for node in &nodes {
                assert!(
                    node.id.as_str() == "node-2" || node.id.as_str() == "node-4",
                    "Expected only node-2 or node-4 but got {}",
                    node.id
                );
            }
        }
    }

    #[test]
    fn test_concurrent_reads_during_writes() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(2)
                .add_node(create_test_node("node-1"))
                .add_node(create_test_node("node-2"))
                .add_node(create_test_node("node-3"))
                .build()
                .unwrap(),
        );

        let mut handles = vec![];

        for i in 0..4 {
            let ring_clone = Arc::clone(&ring);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key-{}-{}", i, j);
                    let _ = ring_clone.primary_node(key.as_bytes());
                }
            });
            handles.push(handle);
        }

        let ring_writer = Arc::clone(&ring);
        let writer_handle = thread::spawn(move || {
            for i in 0..50 {
                let node = NodeInfo::new(
                    format!("temp-node-{}", i),
                    format!("10.0.1.{}", i % 256),
                    9002,
                );
                let _ = ring_writer.add_node(node);
                let _ = ring_writer.remove_node(&NodeId::new(format!("temp-node-{}", i)));
            }
        });
        handles.push(writer_handle);

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }

    #[test]
    fn test_concurrent_node_additions() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(ConsistentHashRing::new(16, 2).unwrap());

        let mut handles = vec![];

        for i in 0..8 {
            let ring_clone = Arc::clone(&ring);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let node = NodeInfo::new(
                        format!("node-{}-{}", i, j),
                        format!("10.0.{}.{}", i, j % 256),
                        9002,
                    );
                    let _ = ring_clone.add_node(node);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert_eq!(ring.node_count(), 800);
    }

    #[test]
    fn test_concurrent_lookups_with_state_changes() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(
            HashRingBuilder::new()
                .virtual_nodes(16)
                .replication_factor(2)
                .add_node(create_test_node("node-1"))
                .add_node(create_test_node("node-2"))
                .add_node(create_test_node("node-3"))
                .add_node(create_test_node("node-4"))
                .build()
                .unwrap(),
        );

        let mut handles = vec![];

        for i in 0..4 {
            let ring_clone = Arc::clone(&ring);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("lookup-key-{}-{}", i, j);
                    let _ = ring_clone.nodes_for_key(key.as_bytes());
                }
            });
            handles.push(handle);
        }

        for i in 0..2 {
            let ring_clone = Arc::clone(&ring);
            let handle = thread::spawn(move || {
                let node_id = NodeId::new(format!("node-{}", (i % 4) + 1));
                for _ in 0..100 {
                    let _ = ring_clone.set_node_state(&node_id, NodeState::Draining);
                    let _ = ring_clone.set_node_state(&node_id, NodeState::Active);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }

    #[test]
    fn test_node_info_with_state() {
        let node = NodeInfo::new("node-1", "10.0.0.1", 9002).with_state(NodeState::Draining);
        assert_eq!(node.state, NodeState::Draining);
    }

    #[test]
    fn test_get_ring_entries() {
        let ring = HashRingBuilder::new()
            .virtual_nodes(4)
            .add_node(create_test_node("node-1"))
            .build()
            .unwrap();

        let entries = ring.get_ring_entries();
        assert_eq!(entries.len(), 4);
        for (_, node_id) in entries {
            assert_eq!(node_id.as_str(), "node-1");
        }
    }
}
