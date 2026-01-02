# gitstratum-coordinator Architecture

Raft-based cluster coordinator for GitStratum. Manages node membership, topology distribution, and failure detection.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            COORDINATOR CLUSTER                               │
│                                                                             │
│    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐             │
│    │    Leader    │◄────►│   Follower   │◄────►│   Follower   │             │
│    │              │      │              │      │              │             │
│    │  • Writes    │      │  • Reads     │      │  • Reads     │             │
│    │  • Heartbeats│      │  • Replicate │      │  • Replicate │             │
│    │  • Watch     │      │  • Failover  │      │  • Failover  │             │
│    └──────────────┘      └──────────────┘      └──────────────┘             │
│                                                                             │
│    State Machine: ClusterTopology (object nodes, metadata nodes, hash ring) │
└─────────────────────────────────────────────────────────────────────────────┘
```

The coordinator uses `k8s-operator`'s Raft implementation for consensus. The state machine stores `ClusterTopology` as JSON, which includes:
- Object node registry with health state
- Metadata node registry with health state
- Hash ring configuration

## Module Structure

```
src/
├── lib.rs              # Public API exports
├── server.rs           # gRPC server implementation (CoordinatorService)
├── client.rs           # Client library (CoordinatorClient, SmartCoordinatorClient)
├── state_machine.rs    # Raft state machine (ClusterTopology, apply_command)
├── commands.rs         # Command types (ClusterCommand, ClusterResponse)
├── topology.rs         # Topology types (NodeEntry, HashRingConfig)
├── config.rs           # Configuration (CoordinatorConfig, TlsConfig)
├── heartbeat_batcher.rs# Heartbeat batching to reduce Raft writes
├── rate_limit.rs       # Token bucket rate limiting (per-client and global)
├── validation.rs       # Input validation (node ID, address, UUID)
├── tls.rs              # mTLS and CRL checking
├── metrics.rs          # Prometheus metrics
├── error.rs            # Error types
└── convert.rs          # Proto conversions
```

## API (6 RPCs)

| RPC | Description |
|-----|-------------|
| `GetTopology` | Get current cluster topology |
| `WatchTopology` | Stream topology changes (server push) |
| `RegisterNode` | Register a new node (object or metadata) |
| `DeregisterNode` | Remove a node from the cluster |
| `Heartbeat` | Node health check (batched internally) |
| `HealthCheck` | Coordinator health status |

## State Transitions

Nodes transition through these states:

```
                   heartbeat
    ┌─────────────────────┐
    │                     │
    ▼                     │
JOINING ──────► ACTIVE ───┴───► SUSPECT ──────► DOWN
    │              │                │
    │              │                │ (heartbeat resumes)
    │              │                │
    │              └────────────────┘
    │
    └──► (timeout: 5 min with no ACTIVE transition)
```

### State Descriptions

| State | Description | Included in Hash Ring |
|-------|-------------|----------------------|
| JOINING | Registered but not yet receiving traffic | No |
| ACTIVE | Healthy, receiving traffic | Yes |
| SUSPECT | Heartbeat missed, grace period | Yes (with warning) |
| DOWN | Removed from routing | No |
| DRAINING | Graceful shutdown in progress | No (traffic draining) |

### Timeouts

| Timeout | Default | Description |
|---------|---------|-------------|
| `heartbeat_interval` | 10s | How often nodes send heartbeats |
| `suspect_timeout` | 45s | Time before ACTIVE → SUSPECT |
| `down_timeout` | 90s | Time before SUSPECT → DOWN |
| `joining_timeout` | 5min | Time before JOINING → removed |
| `leader_grace_period` | 90s | Grace period after leader election |

## Failure Detection

### Flap Damping

Prevents oscillation when a node is unstable:

```rust
if suspect_count >= 3 && last_suspect_within(10.minutes()) {
    suspect_timeout *= 2;  // Double the timeout
}
```

### Leader Grace Period

After leader election, the new leader waits 2× `suspect_timeout` before marking nodes DOWN. This prevents false positives during leader failover.

### Generation ID

Each node has a `generation_id` (UUID) assigned at startup. This detects zombie nodes—old processes that come back after being replaced:

```rust
if heartbeat.generation_id != registered.generation_id {
    return Err(Status::failed_precondition("Zombie node detected"));
}
```

## Heartbeat Batching

To reduce Raft write amplification, heartbeats are batched:

```
┌─────────────────────────────────────────────────────────────────┐
│                    HEARTBEAT BATCHER                             │
│                                                                 │
│  Node1 ──► │                                                    │
│  Node2 ──► │  Collect for 1s  │──► Single Raft write           │
│  Node3 ──► │                  │    (BatchHeartbeat command)     │
│  ...   ──► │                                                    │
└─────────────────────────────────────────────────────────────────┘
```

With 1000 nodes heartbeating every 10s:
- Without batching: 100 Raft writes/sec
- With 1s batching: 1 Raft write/sec (100× reduction)

## Rate Limiting

### Per-Client Limits

| Operation | Rate | Burst |
|-----------|------|-------|
| Heartbeat | 10/min | 5 |
| Register | 10/min | 2 |
| Deregister | 10/min | 2 |
| GetTopology | 100/min | 10 |
| WatchTopology | 10 connections | - |

### Global Limits

| Operation | Rate | Burst |
|-----------|------|-------|
| Heartbeat | 10,000/min | 500 |
| Register | 1,000/min | 50 |
| Writes (total) | 50,000/min | 1000 |

## Security

### mTLS

All connections require mutual TLS:
- Certificate CN must match node ID
- Certificate SAN must include node address
- TLS 1.3 minimum

### CRL Checking

```yaml
tls:
  crl_url: https://ca.example.com/crl.pem
  crl_refresh: 5m
  crl_max_age: 1h
```

Revoked certificates are rejected immediately.

## Metrics

Prometheus metrics exported:

| Metric | Type | Description |
|--------|------|-------------|
| `coordinator_topology_version` | Gauge | Current topology version |
| `coordinator_object_nodes_total` | Gauge | Number of object nodes |
| `coordinator_metadata_nodes_total` | Gauge | Number of metadata nodes |
| `coordinator_nodes_by_state` | Gauge | Nodes per state (labels: state) |
| `coordinator_heartbeats_total` | Counter | Heartbeat count |
| `coordinator_heartbeat_batch_size` | Histogram | Batch sizes |
| `coordinator_raft_proposals_total` | Counter | Raft proposal count |
| `coordinator_raft_commits_total` | Counter | Raft commit count |
| `coordinator_watch_connections` | Gauge | Active watch connections |
| `coordinator_rate_limit_rejected_total` | Counter | Rate-limited requests |

## Client Library

### Basic Client

```rust
let client = CoordinatorClient::connect("http://coordinator:9000").await?;
let topology = client.get_topology().await?;
```

### Smart Client

Handles leader discovery and failover automatically:

```rust
let client = SmartCoordinatorClient::new(
    vec!["coord1:9000", "coord2:9000", "coord3:9000"],
    RetryConfig::default(),
);
let topology = client.get_topology().await?;
```

### Watch with Recovery

Handles `Lagged` notifications and reconnects:

```rust
watch_topology_with_recovery(
    &client,
    WatchConfig::default(),
    |event, version| {
        match event {
            WatchEvent::NodeAdded => refresh_routing(),
            WatchEvent::Lagged => full_sync(),
            _ => {}
        }
    },
).await;
```

## Configuration

```rust
CoordinatorConfig {
    heartbeat_interval: Duration::from_secs(10),
    suspect_timeout: Duration::from_secs(45),
    down_timeout: Duration::from_secs(90),
    joining_timeout: Duration::from_secs(300),
    leader_grace_period: Duration::from_secs(90),
    heartbeat_batch_interval: Duration::from_secs(1),
    flap_window: Duration::from_secs(600),
    flap_threshold: 3,
    ..Default::default()
}
```

## Testing

198 tests covering:
- Unit tests for all modules
- Integration tests for cluster operations
- Load tests for throughput
- mTLS integration tests
- Upgrade/rollback compatibility tests

Run tests:
```bash
cargo nextest run -p gitstratum-coordinator
```

Run benchmarks:
```bash
cargo bench -p gitstratum-coordinator
```
