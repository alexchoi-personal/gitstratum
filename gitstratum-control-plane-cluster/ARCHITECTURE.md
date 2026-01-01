# Control Plane Architecture

## Overview

The Control Plane manages cluster coordination via Raft consensus. It uses the `k8s-operator` crate as the HA (High Availability) framework and extends it with GitStratum-specific state machine and reconcilers.

Key responsibilities:
- **Cluster State**: Tracks all nodes (ControlPlane, Metadata, Object, Frontend)
- **Hash Ring**: Manages object distribution topology
- **Distributed Locks**: Provides ref locks with TTL for coordinated operations
- **K8s Reconciliation**: Reconciles GitStratum CRDs (leader-only)

## Dependency on k8s-operator

The `k8s-operator` crate provides the Raft HA framework:

| Component | Purpose |
|-----------|---------|
| `RaftNodeManager<SM>` | Raft lifecycle, bootstrap, gRPC server |
| `LeaderElection` | Tracks leadership via `Arc<AtomicBool>` |
| `HeadlessServiceDiscovery` | K8s DNS peer discovery |
| `ClusterManager` | Dynamic membership management |
| `GracefulShutdown` | Clean cluster removal on SIGTERM |
| `CompactionManager` | Raft log compaction and snapshots |
| `StateMachine` trait | Allows custom state machine implementations |

The Control Plane implements the `StateMachine` trait to provide GitStratum-specific behavior while reusing the entire Raft infrastructure.

## GitStratum State Machine

```rust
use k8s_operator::raft::StateMachine;

pub struct GitStratumStateMachine {
    state: ClusterStateSnapshot,
}

impl StateMachine for GitStratumStateMachine {
    type Request = ControlPlaneRequest;
    type Response = ControlPlaneResponse;
    type Snapshot = ClusterStateSnapshot;

    fn apply(&mut self, req: &Self::Request) -> Self::Response {
        match req {
            ControlPlaneRequest::AddNode { node } => { ... }
            ControlPlaneRequest::RemoveNode { node_id } => { ... }
            ControlPlaneRequest::AcquireLock { key, lock } => { ... }
            ControlPlaneRequest::ReleaseLock { lock_id } => { ... }
            ControlPlaneRequest::UpdateHashRing { ring } => { ... }
            // ... other variants
        }
    }

    fn snapshot(&self) -> Self::Snapshot {
        self.state.clone()
    }

    fn restore(&mut self, snapshot: Self::Snapshot) {
        self.state = snapshot;
    }
}
```

## State Machine Operations

### Node Management
- `AddNode { node: ExtendedNodeInfo }` - Register a new node
- `RemoveNode { node_id: String }` - Deregister a node
- `UpdateNodeStatus { node_id, status }` - Update node health status

### Distributed Locks
- `AcquireLock { key: RefLockKey, lock: LockInfo }` - Acquire a ref lock
- `ReleaseLock { lock_id: String }` - Release a ref lock
- Locks have TTL and auto-expire

### Hash Ring
- `UpdateHashRing { topology: HashRingTopology }` - Update ring topology
- Includes vnode assignments and replication configuration

### Configuration
- `SetClusterConfig { config: ClusterConfig }` - Update cluster configuration
- `SetFeatureFlag { flag, enabled }` - Toggle feature flags

## Cluster State Snapshot

```rust
pub struct ClusterStateSnapshot {
    pub nodes: HashMap<String, ExtendedNodeInfo>,
    pub hash_ring: HashRingTopology,
    pub locks: HashMap<String, LockInfo>,
    pub cluster_config: ClusterConfig,
    pub feature_flags: HashMap<String, bool>,
    pub version: u64,
}
```

The snapshot is serialized to JSON for Raft log replication and persisted across restarts.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     k8s-operator crate                       │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │RaftNode     │ │ClusterMgr   │ │ LeaderElection          ││
│  │Manager<SM>  │ │             │ │ is_leader: AtomicBool   ││
│  └─────────────┘ └─────────────┘ └─────────────────────────┘│
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │Discovery    │ │Compaction   │ │ GracefulShutdown        ││
│  │(DNS)        │ │Manager      │ │                         ││
│  └─────────────┘ └─────────────┘ └─────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              gitstratum-control-plane                        │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ GitStratumStateMachine                                  ││
│  │ - ClusterStateSnapshot (nodes, locks, ring)             ││
│  │ - ControlPlaneRequest/Response types                    ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────┐│
│  │ gRPC Server (ControlPlaneService)                       ││
│  │ - GetClusterState, AddNode, RemoveNode                  ││
│  │ - AcquireLock, ReleaseLock, GetHashRing                 ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────┐│
│  │ GitStratum Reconcilers (leader only)                    ││
│  │ - GitStratumCluster, GitRepository, GitUser             ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## gRPC API

### Port 8080: Raft Consensus (from k8s-operator)
- `Vote` - Raft leader election
- `AppendEntries` - Log replication
- `InstallSnapshot` - State transfer

### Port 9090: GitStratum Control Plane API
- `GetClusterState` - Get current cluster topology
- `AddNode` / `RemoveNode` - Node management
- `AcquireLock` / `ReleaseLock` - Distributed locking
- `GetHashRing` - Get current hash ring topology
- `WatchTopology` - Stream topology changes

## Reconcilers (Leader Only)

When this node is the Raft leader, it also runs K8s reconcilers:

| Reconciler | Purpose |
|------------|---------|
| `GitStratumCluster` | Manages StatefulSets for cluster components |
| `GitRepository` | Creates per-repository configuration |
| `GitUser` | Manages user authentication |

Reconcilers are guarded by `LeaderElection::is_leader()` - they only run on the current leader.

## Startup Sequence

```rust
async fn main() {
    let config = RaftConfig::from_env();
    let state_machine = GitStratumStateMachine::new();

    // Use k8s-operator's RaftNodeManager with custom state machine
    let raft_manager = RaftNodeManager::new_with_state_machine(config, state_machine).await?;
    raft_manager.start_grpc_server(8080).await?;
    raft_manager.bootstrap_or_join().await?;

    let leadership = raft_manager.leader_election();
    raft_manager.start_leadership_watcher();

    // Start GitStratum gRPC server (port 9090)
    let control_server = ControlPlaneServer::new(raft_manager.raft());
    tokio::spawn(control_server.serve(9090));

    // Start reconciler (guarded by leadership)
    let reconciler = GitStratumReconciler::new(leadership.clone());
    tokio::spawn(reconciler.run());

    // Cluster manager for dynamic membership
    let discovery = HeadlessServiceDiscovery::new(&config);
    let cluster_mgr = ClusterManager::new(raft_manager, discovery);
    tokio::spawn(cluster_mgr.run());
}
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `kubernetes` | Enables k8s-operator HA features |

Without the `kubernetes` feature, the control plane can run with manual Raft cluster setup (useful for testing or non-K8s deployments).

## k8s-operator Integration Requirements

The `k8s-operator` crate uses newer versions of Kubernetes dependencies:

| Dependency | k8s-operator | gitstratum |
|------------|--------------|------------|
| `kube` | 2.0+ | 0.87 |
| `k8s-openapi` | 0.26+ | 0.20 |
| `schemars` | 1.0 | 0.8 |

To enable the `kubernetes` feature, update the vendor to include these newer versions:

1. Uncomment `k8s-operator` dependency in `Cargo.toml`
2. Uncomment the `kubernetes` feature
3. Run `cargo vendor` to update dependencies
4. Build with `--features kubernetes`

The `GitStratumStateMachine` struct is always available. Only the `StateMachine` trait implementation requires the feature flag.

## Client Integration

Other clusters (Metadata, Object, Frontend) connect to the Control Plane as clients:

```rust
// In Frontend cluster
let control_client = ControlPlaneClient::new(control_plane_addr).await?;
let cluster_state = control_client.get_cluster_state().await?;
let hash_ring = cluster_state.hash_ring;

// Watch for topology changes
let mut watch = control_client.watch_topology().await?;
while let Some(update) = watch.next().await {
    local_ring_cache.update(update);
}
```

## Consistency Model

- **Strong consistency** for writes via Raft consensus
- **Linearizable reads** from leader
- **Eventually consistent reads** from followers (with stale ring tolerance)

The hash ring topology is cached locally by Frontend/Metadata/Object nodes. Stale routing is tolerable because:
1. Objects are immutable (old replicas remain valid)
2. Refs are partitioned by repository (no cross-repo dependencies)
3. Lock operations always go to the leader
