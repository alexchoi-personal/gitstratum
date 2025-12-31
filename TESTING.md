# GitStratum Testing Guide

This guide covers testing patterns for GitStratum, with emphasis on simulating distributed cluster behavior using Tokio's multi-threaded runtime.

## Running Tests

```bash
# Run all tests
cargo nextest run --workspace

# Run with coverage
cargo llvm-cov nextest --workspace

# Run specific crate tests
cargo nextest run -p gitstratum-control-plane

# Run integration tests only
cargo nextest run --workspace -- integration
```

## Tokio Multi-Threaded Testing

GitStratum uses Tokio's multi-threaded runtime to simulate concurrent cluster operations. The `#[tokio::test]` macro defaults to a multi-threaded runtime, but you can explicitly configure it:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_cluster_operations() {
    // Test code runs on 4 worker threads
}
```

### When to Use Multi-Threaded Tests

| Scenario | Flavor | Reason |
|----------|--------|--------|
| Single async operation | `current_thread` | Simpler, deterministic |
| Multiple concurrent tasks | `multi_thread` | Realistic concurrency |
| Simulating multiple nodes | `multi_thread` | Parallel execution |
| Race condition testing | `multi_thread` | Exposes timing bugs |

## Simulating Cluster Nodes with Tokio Tasks

Each cluster node can be simulated as a Tokio task. This allows testing distributed behavior without actual network I/O.

### Basic Pattern: Node as Task

```rust
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, watch};

struct SimulatedNode {
    id: String,
    state: Arc<Mutex<NodeState>>,
}

#[derive(Default)]
struct NodeState {
    data: HashMap<String, Vec<u8>>,
    is_leader: bool,
}

impl SimulatedNode {
    async fn run(
        self,
        mut requests: mpsc::Receiver<Request>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                Some(req) = requests.recv() => {
                    self.handle_request(req).await;
                }
                Ok(()) = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_request(&self, req: Request) {
        let mut state = self.state.lock().await;
        // Process request...
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_cluster() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn 3 nodes as concurrent tasks
    let mut handles = Vec::new();
    let mut senders = Vec::new();

    for i in 0..3 {
        let (tx, rx) = mpsc::channel(100);
        let node = SimulatedNode {
            id: format!("node-{}", i),
            state: Arc::new(Mutex::new(NodeState::default())),
        };

        let shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            node.run(rx, shutdown).await;
        }));
        senders.push(tx);
    }

    // Send requests to nodes
    for (i, tx) in senders.iter().enumerate() {
        tx.send(Request::Write { key: format!("key-{}", i), value: vec![1, 2, 3] }).await.unwrap();
    }

    // Shutdown
    shutdown_tx.send(true).unwrap();
    for handle in handles {
        handle.await.unwrap();
    }
}
```

### Pattern: Simulating Raft Consensus

```rust
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct RaftNode {
    id: u64,
    term: AtomicU64,
    is_leader: AtomicBool,
    state: Arc<Mutex<Vec<LogEntry>>>,
}

struct LogEntry {
    term: u64,
    command: Command,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_raft_leader_election() {
    let nodes: Vec<Arc<RaftNode>> = (0..3)
        .map(|i| Arc::new(RaftNode {
            id: i,
            term: AtomicU64::new(0),
            is_leader: AtomicBool::new(false),
            state: Arc::new(Mutex::new(Vec::new())),
        }))
        .collect();

    let (shutdown_tx, _) = watch::channel(false);

    // Spawn election tasks
    let handles: Vec<_> = nodes.iter().map(|node| {
        let node = Arc::clone(node);
        let all_nodes: Vec<_> = nodes.iter().map(Arc::clone).collect();
        let shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            run_election_loop(node, all_nodes, shutdown).await
        })
    }).collect();

    // Wait for leader election
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify exactly one leader
    let leader_count = nodes.iter()
        .filter(|n| n.is_leader.load(Ordering::SeqCst))
        .count();
    assert_eq!(leader_count, 1);

    shutdown_tx.send(true).unwrap();
}

async fn run_election_loop(
    node: Arc<RaftNode>,
    peers: Vec<Arc<RaftNode>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut election_timeout = tokio::time::interval(Duration::from_millis(150));

    loop {
        tokio::select! {
            _ = election_timeout.tick() => {
                if !node.is_leader.load(Ordering::SeqCst) {
                    // Start election
                    let new_term = node.term.fetch_add(1, Ordering::SeqCst) + 1;
                    let mut votes = 1; // Vote for self

                    for peer in &peers {
                        if peer.id != node.id {
                            let peer_term = peer.term.load(Ordering::SeqCst);
                            if new_term > peer_term {
                                votes += 1;
                            }
                        }
                    }

                    if votes > peers.len() / 2 {
                        node.is_leader.store(true, Ordering::SeqCst);
                        // Demote others
                        for peer in &peers {
                            if peer.id != node.id {
                                peer.is_leader.store(false, Ordering::SeqCst);
                            }
                        }
                    }
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}
```

### Pattern: Hash Ring Simulation

```rust
use std::collections::HashMap;
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};

struct ObjectNode {
    id: String,
    objects: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
}

struct HashRing {
    nodes: Vec<Arc<ObjectNode>>,
    virtual_nodes: u32,
}

impl HashRing {
    fn get_node(&self, oid: &[u8; 32]) -> &Arc<ObjectNode> {
        let mut hasher = SipHasher13::new();
        oid.hash(&mut hasher);
        let hash = hasher.finish();
        let index = (hash as usize) % self.nodes.len();
        &self.nodes[index]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn test_object_distribution() {
    // Create 6 object nodes
    let nodes: Vec<Arc<ObjectNode>> = (0..6)
        .map(|i| Arc::new(ObjectNode {
            id: format!("object-node-{}", i),
            objects: Arc::new(Mutex::new(HashMap::new())),
        }))
        .collect();

    let ring = Arc::new(HashRing {
        nodes: nodes.clone(),
        virtual_nodes: 100,
    });

    // Spawn concurrent writers
    let mut handles = Vec::new();
    for i in 0..100 {
        let ring = Arc::clone(&ring);
        handles.push(tokio::spawn(async move {
            let oid = create_test_oid(i);
            let data = vec![i as u8; 1024];

            let node = ring.get_node(&oid);
            node.objects.lock().await.insert(oid, data);
        }));
    }

    // Wait for all writes
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify distribution
    let mut total = 0;
    for node in &nodes {
        let count = node.objects.lock().await.len();
        total += count;
        // Each node should have roughly 100/6 ≈ 16-17 objects
        assert!(count > 5 && count < 30, "Uneven distribution: {}", count);
    }
    assert_eq!(total, 100);
}

fn create_test_oid(seed: u32) -> [u8; 32] {
    let mut oid = [0u8; 32];
    oid[0..4].copy_from_slice(&seed.to_be_bytes());
    oid
}
```

### Pattern: Mock Service Traits

```rust
use async_trait::async_trait;

#[async_trait]
pub trait MetadataClient: Send + Sync {
    async fn get_refs(&self, repo_id: &str) -> Result<HashMap<String, Oid>>;
    async fn update_ref(&self, repo_id: &str, ref_name: &str, oid: Oid) -> Result<()>;
}

#[async_trait]
pub trait ObjectClient: Send + Sync {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Vec<u8>>>;
    async fn put_blob(&self, oid: Oid, data: Vec<u8>) -> Result<()>;
}

// Test implementation with in-memory storage
struct MockMetadata {
    refs: Mutex<HashMap<String, HashMap<String, Oid>>>,
}

#[async_trait]
impl MetadataClient for MockMetadata {
    async fn get_refs(&self, repo_id: &str) -> Result<HashMap<String, Oid>> {
        let refs = self.refs.lock().await;
        Ok(refs.get(repo_id).cloned().unwrap_or_default())
    }

    async fn update_ref(&self, repo_id: &str, ref_name: &str, oid: Oid) -> Result<()> {
        let mut refs = self.refs.lock().await;
        refs.entry(repo_id.to_string())
            .or_default()
            .insert(ref_name.to_string(), oid);
        Ok(())
    }
}

struct MockObjects {
    blobs: Mutex<HashMap<Oid, Vec<u8>>>,
}

#[async_trait]
impl ObjectClient for MockObjects {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Vec<u8>>> {
        let blobs = self.blobs.lock().await;
        Ok(blobs.get(oid).cloned())
    }

    async fn put_blob(&self, oid: Oid, data: Vec<u8>) -> Result<()> {
        let mut blobs = self.blobs.lock().await;
        blobs.insert(oid, data);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_frontend_with_mocks() {
    let metadata = Arc::new(MockMetadata {
        refs: Mutex::new(HashMap::new()),
    });

    let objects = Arc::new(MockObjects {
        blobs: Mutex::new(HashMap::new()),
    });

    // Create frontend with mock backends
    let frontend = Frontend::new(
        metadata.clone() as Arc<dyn MetadataClient>,
        objects.clone() as Arc<dyn ObjectClient>,
    );

    // Test push operation
    let blob_data = b"Hello, World!".to_vec();
    let oid = Oid::hash(&blob_data);

    frontend.push("test-repo", "refs/heads/main", oid, blob_data.clone()).await.unwrap();

    // Verify state
    let refs = metadata.get_refs("test-repo").await.unwrap();
    assert_eq!(refs.get("refs/heads/main"), Some(&oid));

    let blob = objects.get_blob(&oid).await.unwrap();
    assert_eq!(blob, Some(blob_data));
}
```

### Pattern: Concurrent Request Simulation

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_clones() {
    let cluster = setup_test_cluster().await;

    // Simulate 100 concurrent clone requests
    let handles: Vec<_> = (0..100)
        .map(|i| {
            let cluster = cluster.clone();
            tokio::spawn(async move {
                let start = Instant::now();
                cluster.clone_repo("test-repo", format!("client-{}", i)).await?;
                Ok::<_, Error>(start.elapsed())
            })
        })
        .collect();

    let mut latencies = Vec::new();
    for handle in handles {
        match handle.await.unwrap() {
            Ok(latency) => latencies.push(latency),
            Err(e) => panic!("Clone failed: {}", e),
        }
    }

    // Analyze latencies
    latencies.sort();
    let p50 = latencies[50];
    let p99 = latencies[99];

    println!("p50: {:?}, p99: {:?}", p50, p99);
    assert!(p99 < Duration::from_secs(1), "p99 too high: {:?}", p99);
}
```

### Pattern: Failure Injection

```rust
use std::sync::atomic::{AtomicU32, Ordering};

struct FaultyNode {
    inner: Arc<ObjectNode>,
    fail_rate: AtomicU32, // Percentage 0-100
}

impl FaultyNode {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Vec<u8>>> {
        let fail = rand::random::<u32>() % 100 < self.fail_rate.load(Ordering::Relaxed);
        if fail {
            return Err(Error::NodeUnavailable);
        }
        self.inner.get_blob(oid).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replica_failover() {
    // Create 3 replicas, one faulty
    let nodes = vec![
        Arc::new(FaultyNode { inner: create_node(0), fail_rate: AtomicU32::new(0) }),
        Arc::new(FaultyNode { inner: create_node(1), fail_rate: AtomicU32::new(50) }), // 50% fail
        Arc::new(FaultyNode { inner: create_node(2), fail_rate: AtomicU32::new(0) }),
    ];

    let oid = create_test_oid(42);
    let data = vec![1, 2, 3, 4];

    // Write to all replicas
    for node in &nodes {
        node.inner.put_blob(oid, data.clone()).await.unwrap();
    }

    // Read with failover - should succeed even with faulty node
    let mut success = 0;
    for _ in 0..100 {
        for node in &nodes {
            if let Ok(Some(_)) = node.get_blob(&oid).await {
                success += 1;
                break;
            }
        }
    }

    // Should succeed every time with failover
    assert_eq!(success, 100);
}
```

## Integration Test Structure

```
tests/
├── integration_tests.rs      # Full cluster integration
├── raft_tests.rs            # Consensus behavior
├── graph_tests.rs           # Commit graph traversal
├── server_tests.rs          # gRPC service tests
└── store_test.rs            # Storage layer tests
```

### Example Integration Test

```rust
// tests/integration_tests.rs

use gitstratum_frontend_cluster::Frontend;
use gitstratum_control_plane_cluster::ControlPlane;
use gitstratum_metadata_cluster::MetadataCluster;
use gitstratum_object_cluster::ObjectCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_full_git_workflow() {
    // Setup clusters
    let temp_dir = tempfile::tempdir().unwrap();

    let control_plane = ControlPlane::new_test(3).await;
    let metadata = MetadataCluster::new_test(&temp_dir, 3).await;
    let objects = ObjectCluster::new_test(&temp_dir, 6).await;
    let frontend = Frontend::new(control_plane, metadata, objects);

    // Create repository
    frontend.create_repo("test/repo").await.unwrap();

    // Simulate push
    let blob = b"console.log('hello')".to_vec();
    let tree = create_tree(vec![("index.js", blob.clone())]);
    let commit = create_commit(tree.oid, "Initial commit");

    frontend.push(
        "test/repo",
        vec![RefUpdate::new("refs/heads/main", Oid::ZERO, commit.oid)],
        vec![blob, tree.into(), commit.into()],
    ).await.unwrap();

    // Simulate clone
    let pack = frontend.clone("test/repo", "refs/heads/main", 1).await.unwrap();

    assert!(pack.len() > 0);
    assert!(pack.contains_object(&commit.oid));
}
```

## Best Practices

1. **Use `Arc<Mutex<T>>` for shared state** - Safe concurrent access across tasks
2. **Use channels for communication** - `mpsc` for requests, `watch` for shutdown
3. **Use `tokio::select!` for multiplexing** - Handle multiple event sources
4. **Set explicit worker threads** - Match production concurrency
5. **Use `tempfile` for storage tests** - Automatic cleanup
6. **Mock network I/O** - Faster, deterministic tests
7. **Inject failures** - Test resilience
8. **Measure latencies** - Catch performance regressions

## Common Pitfalls

| Issue | Solution |
|-------|----------|
| Deadlocks | Use `tokio::sync::Mutex`, avoid nested locks |
| Flaky tests | Use deterministic ordering, avoid `sleep` |
| Resource leaks | Ensure shutdown signals are sent |
| Slow tests | Mock expensive operations |
| Race conditions | Use barriers for synchronization |
