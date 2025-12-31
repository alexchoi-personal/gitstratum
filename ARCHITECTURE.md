# GitStratum Cluster Architecture

GitStratum is a distributed Git hosting system designed for high-throughput CI/CD workloads. It consists of four independent clusters, each responsible for a specific domain and managing its own caching and optimization strategies.

## System Overview

```
                                    CI/CD RUNNERS
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │  GitLab     │  │  GitHub     │  │  Jenkins    │  │  Buildkite  │  │  ArgoCD     │
   │  Runner     │  │  Actions    │  │  Agent      │  │  Agent      │  │  Controller │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                │                │                │
          └────────────────┴────────────────┴────────────────┴────────────────┘
                                     git clone/fetch/push
                                            │
                                            ▼
                              ┌─────────────────────────────────┐
                              │         LOAD BALANCER           │
                              │    (HAProxy / Nginx / Cloud)    │
                              └─────────────────────────────────┘
                                            │
          ┌─────────────────────────────────┼─────────────────────────────────┐
          │                                 │                                 │
          ▼                                 ▼                                 ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                        │
│                           ╔════════════════════════════════╗                          │
│                           ║     1. FRONTEND CLUSTER        ║                          │
│                           ║        (Git Protocol)          ║                          │
│                           ╚════════════════════════════════╝                          │
│                                                                                        │
│    ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐           │
│    │  Frontend Node 1   │   │  Frontend Node 2   │   │  Frontend Node N   │           │
│    │  ┌──────────────┐  │   │  ┌──────────────┐  │   │  ┌──────────────┐  │           │
│    │  │ SSH/HTTPS    │  │   │  │ SSH/HTTPS    │  │   │  │ SSH/HTTPS    │  │           │
│    │  │ Git Proto v2 │  │   │  │ Git Proto v2 │  │   │  │ Git Proto v2 │  │           │
│    │  │ Middleware   │  │   │  │ Middleware   │  │   │  │ Middleware   │  │           │
│    │  │ Local Cache  │  │   │  │ Local Cache  │  │   │  │ Local Cache  │  │           │
│    │  └──────────────┘  │   │  └──────────────┘  │   │  └──────────────┘  │           │
│    └────────────────────┘   └────────────────────┘   └────────────────────┘           │
│                                                                                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
          │                                 │                                 │
          │ gRPC                            │ gRPC                            │ gRPC
          ▼                                 ▼                                 ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                        │
│                           ╔════════════════════════════════╗                          │
│                           ║   2. CONTROL PLANE CLUSTER     ║                          │
│                           ║       (Raft Consensus)         ║                          │
│                           ╚════════════════════════════════╝                          │
│                                                                                        │
│    ┌─────────────────────────────────────────────────────────────────────────────┐    │
│    │                           RAFT CONSENSUS LAYER                               │    │
│    │     ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │    │
│    │     │ Control Node 1  │◀──▶│ Control Node 2  │◀──▶│ Control Node 3  │        │    │
│    │     │    (LEADER)     │    │   (FOLLOWER)    │    │   (FOLLOWER)    │        │    │
│    │     └─────────────────┘    └─────────────────┘    └─────────────────┘        │    │
│    └─────────────────────────────────────────────────────────────────────────────┘    │
│    │ Membership │ Auth/RBAC │ Rate Limiting │ Hash Ring │ Audit Log │ Config │        │
│                                                                                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
          │                                                           │
          │ gRPC                                                      │ gRPC
          ▼                                                           ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                        │
│                           ╔════════════════════════════════╗                          │
│                           ║     3. METADATA CLUSTER        ║                          │
│                           ║      (Refs & Repo Data)        ║                          │
│                           ╚════════════════════════════════╝                          │
│                                                                                        │
│    ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐           │
│    │  Metadata Node 1   │   │  Metadata Node 2   │   │  Metadata Node N   │           │
│    │  ┌──────────────┐  │   │  ┌──────────────┐  │   │  ┌──────────────┐  │           │
│    │  │ RocksDB      │  │   │  │ RocksDB      │  │   │  │ RocksDB      │  │           │
│    │  │ ──────────── │  │   │  │ ──────────── │  │   │  │ ──────────── │  │           │
│    │  │ CF: refs     │  │   │  │ CF: refs     │  │   │  │ CF: refs     │  │           │
│    │  │ CF: config   │  │   │  │ CF: config   │  │   │  │ CF: config   │  │           │
│    │  │ CF: graph    │  │   │  │ CF: graph    │  │   │  │ CF: graph    │  │           │
│    │  │ CF: pack_cache│ │   │  │ CF: pack_cache│ │   │  │ CF: pack_cache│ │           │
│    │  └──────────────┘  │   │  └──────────────┘  │   │  └──────────────┘  │           │
│    │  In-Memory Cache   │   │  In-Memory Cache   │   │  In-Memory Cache   │           │
│    └────────────────────┘   └────────────────────┘   └────────────────────┘           │
│                                                                                        │
│    Partitioned by repo_id hash, replicated for durability                             │
│                                                                                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
          │
          │ gRPC (object locations)
          ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                        │
│                           ╔════════════════════════════════╗                          │
│                           ║      4. OBJECT CLUSTER         ║                          │
│                           ║        (Blob Storage)          ║                          │
│                           ╚════════════════════════════════╝                          │
│                                                                                        │
│    ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐           │
│    │  Object Node 1     │   │  Object Node 2     │   │  Object Node N     │           │
│    │  ┌──────────────┐  │   │  ┌──────────────┐  │   │  ┌──────────────┐  │           │
│    │  │ RocksDB      │  │   │  │ RocksDB      │  │   │  │ RocksDB      │  │           │
│    │  │ ──────────── │  │   │  │ ──────────── │  │   │  │ ──────────── │  │           │
│    │  │ CF: objects  │  │   │  │ CF: objects  │  │   │  │ CF: objects  │  │           │
│    │  │ CF: deltas   │  │   │  │ CF: deltas   │  │   │  │ CF: deltas   │  │           │
│    │  └──────────────┘  │   │  └──────────────┘  │   │  └──────────────┘  │           │
│    │  Hot Object Cache  │   │  Hot Object Cache  │   │  Hot Object Cache  │           │
│    │  Delta Engine      │   │  Delta Engine      │   │  Delta Engine      │           │
│    └────────────────────┘   └────────────────────┘   └────────────────────┘           │
│                                                                                        │
│    Consistent Hash Ring: SHA256(object_id) -> Primary Node                            │
│    Replication Factor: 3 (configurable)                                               │
│                                                                                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                           KUBERNETES ORCHESTRATION                                     │
│                                                                                        │
│    ┌─────────────────────────────────────────────────────────────────────────────┐    │
│    │                         GitStratum Operator                                  │    │
│    │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐ ┌────────────────┐│    │
│    │  │ Frontend       │ │ Control Plane  │ │ Metadata       │ │ Object         ││    │
│    │  │ Controller     │ │ Controller     │ │ Controller     │ │ Controller     ││    │
│    │  └────────────────┘ └────────────────┘ └────────────────┘ └────────────────┘│    │
│    └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                        │
│    ┌─────────────────────────────────────────────────────────────────────────────┐    │
│    │                         Prometheus + Grafana                                 │    │
│    │  Per-cluster metrics: latency, throughput, cache hits, errors, queue depth  │    │
│    └─────────────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

## Cluster Responsibilities

### 1. Frontend Cluster

The Frontend Cluster handles all Git protocol interactions with clients. It is stateless and can scale horizontally.

**Responsibilities:**
- SSH server (port 22)
- HTTPS server (port 443)
- Git protocol v2 parsing
- `upload-pack` (clone/fetch operations)
- `receive-pack` (push operations)
- `ls-refs` (reference listing)
- Pack file assembly
- Shallow clone handling
- Partial clone (filter support)
- Request middleware (auth, rate limit, metrics)

**Caching & Optimization:**
- Hot refs LRU cache (30s TTL) - avoids repeated metadata lookups
- Negotiation cache - speeds up repeated fetches with same wants/haves
- Session state - maintains state for multi-round fetch negotiations
- Connection pooling - reuses connections to other clusters
- Request coalescing - batches concurrent requests for same refs
- Streaming pack assembly - reduces memory usage
- Parallel object fetch - fetches from multiple object nodes concurrently

### 2. Control Plane Cluster

The Control Plane Cluster manages cluster-wide state using Raft consensus. It provides strongly consistent operations for auth, rate limiting, and cluster membership.

**Responsibilities:**
- Raft consensus for cluster state
- Node membership registry
- Health monitoring and failure detection
- Leader election
- Authentication token validation
- SSH key validation
- RBAC policy enforcement
- Rate limit state management
- Hash ring topology management
- Audit logging
- Feature flags
- Global configuration

**Caching & Optimization:**
- Auth decision cache (5min TTL) - avoids repeated token validation
- Rate limit state in Raft state machine - consistent across cluster
- Hash ring cached locally with watch updates - avoids repeated lookups
- Config cache with invalidation on change
- Batch audit log writes (async) - reduces write amplification
- Read from any follower for stale-OK queries
- Snapshot-based log compaction

### 3. Metadata Cluster

The Metadata Cluster stores repository metadata including references, configuration, and commit graphs. Data is partitioned by repository ID and replicated for durability.

**Responsibilities:**
- Reference storage (branches, tags, HEAD)
- Repository configuration
- Repository statistics
- Commit graph storage (ancestry queries)
- Pack cache (pre-computed packs)
- Object location index (object -> nodes mapping)
- Hook configuration
- Access patterns and analytics

**Caching & Optimization:**
- Hot refs in-memory cache (per-node LRU)
- Commit graph cache for fast ancestry queries
- Pack manifests cache (object lists for common refs)
- Bloom filters for ref existence checks
- Write-behind for statistics updates (batched)
- Partitioned by repo_id for locality
- Read replicas for hot repositories
- Prefix compression for refs
- Background pack precomputation for hot repos

**RocksDB Column Families:**
- `refs` - Reference storage: `{repo_id}:{ref_name}` -> `ObjectId`
- `repo_config` - Repository configuration
- `repo_stats` - Repository statistics (clone count, push count, etc.)
- `commit_graph` - Commit ancestry for fast negotiation
- `pack_cache` - Pre-computed pack files with TTL
- `object_index` - Object location: `{oid}` -> `Vec<NodeId>`

### 4. Object Cluster

The Object Cluster stores raw Git objects (blobs, trees, commits, tags). Objects are distributed across nodes using consistent hashing.

**Responsibilities:**
- Raw object/blob storage
- Object compression (zlib/zstd)
- Delta computation and storage
- Object replication (write to N nodes)
- Garbage collection
- Integrity verification (SHA verification)
- Tiered storage (hot/cold)

**Caching & Optimization:**
- Hot objects LRU cache (decompressed) - avoids repeated decompression
- Pre-computed deltas for common bases
- Delta base cache
- Recent writes buffer
- Batch reads with prefetch (tree objects)
- Locality-aware reads (nearest replica)
- Background delta computation
- Streaming compression
- Tiered storage: SSD (hot) -> HDD (cold)

**RocksDB Column Families:**
- `objects` - Raw object storage: `{oid}` -> `compressed_blob`
- `deltas` - Delta storage: `{oid}:{base_oid}` -> `delta_data`

## Request Flows

### Clone Request (depth=1)

```
CI Runner              Frontend           Control Plane        Metadata           Object
   │                      │                     │                 │                  │
   │ ── git clone ──────▶ │                     │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── ValidateToken ─▶ │                 │                  │
   │                      │ ◀── OK + perms ──── │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── CheckRateLimit ▶ │                 │                  │
   │                      │ ◀── Allowed ─────── │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ─────────────────────── GetRefs ────▶ │                  │
   │                      │ ◀──────────────────── refs map ───── │                  │
   │                      │                     │                 │                  │
   │                      │ ───────────────────── CheckPackCache ▶│                  │
   │                      │                     │                 │                  │
   │                      │             ┌───────┴───────┐         │                  │
   │                      │             │  CACHE HIT    │         │                  │
   │                      │             │  (95% of CI)  │         │                  │
   │                      │             └───────┬───────┘         │                  │
   │                      │ ◀─────────────── pack data ────────── │                  │
   │ ◀── stream pack ──── │                     │                 │                  │
   │                      │                     │                 │                  │
   │                      │             ┌───────┴───────┐         │                  │
   │                      │             │  CACHE MISS   │         │                  │
   │                      │             └───────┬───────┘         │                  │
   │                      │ ◀────────────── object list ──────── │                  │
   │                      │                     │                 │                  │
   │                      │ ── GetHashRing ───▶ │                 │                  │
   │                      │ ◀── ring topology ─ │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ─────────────────────────────────── GetObjects(batch) ─▶│
   │                      │ ◀────────────────────────────────── object data ────────│
   │                      │                     │                 │                  │
   │                      │  [assemble pack]    │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ──────────────────── StorePackCache ─▶│                  │
   │ ◀── stream pack ──── │                     │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── LogAudit(async)▶ │                 │                  │
```

### Push Request

```
CI Runner              Frontend           Control Plane        Metadata           Object
   │                      │                     │                 │                  │
   │ ── git push ───────▶ │                     │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── ValidateToken ─▶ │                 │                  │
   │                      │ ◀── OK (write) ──── │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── CheckRateLimit ▶ │                 │                  │
   │                      │ ◀── Allowed ─────── │                 │                  │
   │                      │                     │                 │                  │
   │ ── pack data ──────▶ │                     │                 │                  │
   │                      │                     │                 │                  │
   │                      │  [unpack objects]   │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── GetHashRing ───▶ │                 │                  │
   │                      │ ◀── ring topology ─ │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ──────────────────────────────────── StoreObjects ─────▶│
   │                      │ ◀───────────────────────────────────── OK ──────────────│
   │                      │                     │                 │                  │
   │                      │ ────────────────────── UpdateRefs ──▶ │                  │
   │                      │ ◀─────────────────────── OK ──────── │                  │
   │                      │                     │                 │                  │
   │                      │ ─────────────────── InvalidateCache ▶ │                  │
   │                      │                     │                 │                  │
   │ ◀── OK ───────────── │                     │                 │                  │
   │                      │                     │                 │                  │
   │                      │ ── LogAudit(async)▶ │                 │                  │
```

## Crate Structure

```
gitstratum-cluster/
│
├── gitstratum-core/               # Shared types
│   └── src/
│       ├── lib.rs
│       ├── object_id.rs           # ObjectId, BlobId
│       ├── repo_id.rs             # RepoId
│       └── error.rs               # Common errors
│
├── gitstratum-proto/              # gRPC definitions
│   └── proto/
│       ├── frontend.proto         # Git protocol messages
│       ├── control.proto          # Auth, rate limit, cluster
│       ├── metadata.proto         # Refs, config, pack cache
│       └── object.proto           # Blob storage
│
├── gitstratum-hashring/           # Consistent hashing
│   └── src/
│       ├── lib.rs
│       ├── ring.rs
│       └── virtual_nodes.rs
│
│
│   ════════════════════════════════════════════════════════════════
│                         FRONTEND CLUSTER
│   ════════════════════════════════════════════════════════════════
│
├── gitstratum-frontend/
│   └── src/
│       ├── lib.rs
│       ├── server.rs              # SSH + HTTPS server
│       ├── protocol/
│       │   ├── mod.rs
│       │   ├── v2.rs              # Git protocol v2
│       │   ├── capabilities.rs
│       │   └── pktline.rs
│       ├── commands/
│       │   ├── mod.rs
│       │   ├── upload_pack.rs     # Clone/fetch
│       │   ├── receive_pack.rs    # Push
│       │   └── ls_refs.rs
│       ├── pack/
│       │   ├── mod.rs
│       │   ├── assembly.rs        # Pack file creation
│       │   ├── streaming.rs       # Streaming assembly
│       │   └── shallow.rs         # Shallow clone handling
│       ├── middleware/
│       │   ├── mod.rs
│       │   ├── auth.rs            # Token validation
│       │   ├── rate_limit.rs      # Rate limiting
│       │   ├── timeout.rs
│       │   ├── metrics.rs
│       │   └── logging.rs
│       ├── cache/
│       │   ├── mod.rs
│       │   ├── refs.rs            # Hot refs LRU
│       │   ├── negotiation.rs     # Want/have cache
│       │   └── session.rs         # Multi-round state
│       └── client/
│           ├── mod.rs
│           ├── control.rs         # Control plane client
│           ├── metadata.rs        # Metadata client
│           └── object.rs          # Object cluster client
│
│
│   ════════════════════════════════════════════════════════════════
│                       CONTROL PLANE CLUSTER
│   ════════════════════════════════════════════════════════════════
│
├── gitstratum-control-plane/
│   └── src/
│       ├── lib.rs
│       ├── raft/
│       │   ├── mod.rs
│       │   ├── node.rs            # Raft node
│       │   ├── state_machine.rs   # State machine
│       │   ├── log.rs             # Raft log
│       │   └── network.rs         # Raft RPC
│       ├── membership/
│       │   ├── mod.rs
│       │   ├── registry.rs        # Node registry
│       │   ├── health.rs          # Health checks
│       │   └── failure.rs         # Failure detection
│       ├── auth/
│       │   ├── mod.rs
│       │   ├── token.rs           # JWT validation
│       │   ├── ssh_key.rs         # SSH key store
│       │   ├── rbac.rs            # Role-based access
│       │   └── cache.rs           # Auth decision cache
│       ├── ratelimit/
│       │   ├── mod.rs
│       │   ├── bucket.rs          # Token bucket
│       │   ├── state.rs           # Raft-replicated state
│       │   └── enforcement.rs
│       ├── hashring/
│       │   ├── mod.rs
│       │   ├── state.rs           # Ring topology
│       │   ├── rebalance.rs       # Rebalancing logic
│       │   └── notify.rs          # Change notifications
│       ├── audit/
│       │   ├── mod.rs
│       │   ├── logger.rs          # Audit log writer
│       │   └── retention.rs       # Log rotation
│       └── config/
│           ├── mod.rs
│           ├── cluster.rs         # Cluster config
│           └── feature_flags.rs
│
│
│   ════════════════════════════════════════════════════════════════
│                         METADATA CLUSTER
│   ════════════════════════════════════════════════════════════════
│
├── gitstratum-metadata/
│   └── src/
│       ├── lib.rs
│       ├── store/
│       │   ├── mod.rs
│       │   ├── rocksdb.rs         # RocksDB wrapper
│       │   ├── column_families.rs # CF definitions
│       │   └── replication.rs     # Data replication
│       ├── refs/
│       │   ├── mod.rs
│       │   ├── storage.rs         # Ref storage
│       │   ├── update.rs          # Atomic ref updates
│       │   └── cache.rs           # Hot refs cache
│       ├── repo/
│       │   ├── mod.rs
│       │   ├── config.rs          # Repo configuration
│       │   ├── stats.rs           # Repository statistics
│       │   └── hooks.rs           # Hook configuration
│       ├── graph/
│       │   ├── mod.rs
│       │   ├── commit_graph.rs    # Commit ancestry
│       │   ├── reachability.rs    # Reachability queries
│       │   └── cache.rs           # Graph cache
│       ├── pack_cache/
│       │   ├── mod.rs
│       │   ├── storage.rs         # Pack cache storage
│       │   ├── precompute.rs      # Background precomputation
│       │   ├── invalidation.rs    # Cache invalidation
│       │   └── ttl.rs             # TTL management
│       ├── object_index/
│       │   ├── mod.rs
│       │   └── location.rs        # Object -> nodes mapping
│       └── partition/
│           ├── mod.rs
│           ├── router.rs          # Request routing
│           └── rebalance.rs       # Partition rebalancing
│
│
│   ════════════════════════════════════════════════════════════════
│                          OBJECT CLUSTER
│   ════════════════════════════════════════════════════════════════
│
├── gitstratum-object/
│   └── src/
│       ├── lib.rs
│       ├── store/
│       │   ├── mod.rs
│       │   ├── rocksdb.rs         # RocksDB storage
│       │   ├── compression.rs     # zlib/zstd
│       │   └── tiered.rs          # Hot/cold tiering
│       ├── delta/
│       │   ├── mod.rs
│       │   ├── compute.rs         # Delta computation
│       │   ├── storage.rs         # Delta storage
│       │   ├── base_selection.rs  # Optimal base finding
│       │   └── cache.rs           # Delta cache
│       ├── cache/
│       │   ├── mod.rs
│       │   ├── hot_objects.rs     # Decompressed LRU
│       │   ├── write_buffer.rs    # Recent writes
│       │   └── prefetch.rs        # Related object prefetch
│       ├── replication/
│       │   ├── mod.rs
│       │   ├── writer.rs          # Multi-node writes
│       │   └── reader.rs          # Locality-aware reads
│       ├── gc/
│       │   ├── mod.rs
│       │   ├── mark.rs            # Mark phase
│       │   ├── sweep.rs           # Sweep phase
│       │   └── scheduler.rs       # GC scheduling
│       └── integrity/
│           ├── mod.rs
│           └── verify.rs          # SHA verification
│
│
├── gitstratum-operator/           # Kubernetes operator
│   └── src/
│       ├── lib.rs
│       ├── controllers/
│       │   ├── mod.rs
│       │   ├── frontend.rs        # Frontend scaling
│       │   ├── control_plane.rs   # Control plane management
│       │   ├── metadata.rs        # Metadata cluster
│       │   └── object.rs          # Object cluster
│       └── crd/
│           ├── mod.rs
│           └── gitstratum.rs      # CRD definitions
│
├── gitstratum-metrics/            # Observability
│   └── src/
│       ├── lib.rs
│       ├── prometheus.rs
│       └── collectors/
│           ├── mod.rs
│           ├── frontend.rs
│           ├── control.rs
│           ├── metadata.rs
│           └── object.rs
│
└── bins/                          # Binaries
    ├── gitstratum-frontend/
    ├── gitstratum-control/
    ├── gitstratum-metadata/
    ├── gitstratum-object/
    └── gitstratum-ctl/
```

## Kubernetes Deployment

```yaml
apiVersion: gitstratum.io/v1
kind: GitStratumCluster
metadata:
  name: ci-cluster
spec:
  # Frontend Cluster - stateless, scales horizontally
  frontend:
    replicas: 5
    resources:
      requests: { cpu: "2", memory: "4Gi" }
    cache:
      refsLruSize: 10000
      negotiationTtl: "5m"
    connectionPool:
      controlPlane: 10
      metadata: 50
      object: 100

  # Control Plane Cluster - Raft, 3 or 5 nodes
  controlPlane:
    replicas: 3
    resources:
      requests: { cpu: "1", memory: "2Gi" }
    raft:
      electionTimeout: "1s"
      heartbeatInterval: "100ms"
    auth:
      tokenTtl: "5m"
      tokenSecret: runner-tokens
    rateLimit:
      perClient: 100
      perRepo: 1000
      global: 10000

  # Metadata Cluster - partitioned, replicated
  metadata:
    replicas: 5
    replicationFactor: 3
    resources:
      requests: { cpu: "2", memory: "8Gi" }
    persistence:
      storageClass: fast-ssd
      size: 100Gi
    cache:
      hotRefsSize: 100000
      packCacheTtl: "5m"
      commitGraphSize: 50000

  # Object Cluster - consistent hash ring
  object:
    replicas: 10
    replicationFactor: 3
    resources:
      requests: { cpu: "4", memory: "16Gi" }
    persistence:
      storageClass: fast-ssd
      size: 500Gi
    cache:
      hotObjectsSize: "2Gi"
      deltaCache: true
    gc:
      schedule: "0 2 * * *"
      retentionDays: 30
```

## Design Principles

### 1. Separation of Concerns
Each cluster owns its domain completely, including data storage, caching, and optimization strategies. This allows independent evolution and scaling.

### 2. Independent Scaling
- **Frontend**: Scale for connection count and throughput
- **Control Plane**: Fixed size (3 or 5 nodes for Raft quorum)
- **Metadata**: Scale for repository count and ref density
- **Object**: Scale for storage capacity and read throughput

### 3. Fault Isolation
Failures in one cluster don't cascade to others:
- Control plane issues don't affect cached operations
- Metadata issues allow cached refs to still work
- Object node failures are handled by replication

### 4. CI/CD Optimized
The architecture is specifically designed for CI/CD workloads:
- **Pack cache** achieves 95%+ hit rate for repeated depth=1 clones
- **Shallow clone optimization** minimizes data transfer
- **Rate limiting** prevents runaway jobs from impacting others
- **Horizontal frontend scaling** handles burst traffic

### 5. Consistency Model
- **Control Plane**: Strongly consistent (Raft)
- **Metadata**: Configurable (strong for writes, eventual for reads)
- **Object**: Eventual consistency with replication factor guarantee

## Metrics and Observability

Each cluster exposes Prometheus metrics:

### Frontend Metrics
- `gitstratum_frontend_request_duration_seconds` - Request latency histogram
- `gitstratum_frontend_requests_total` - Request count by operation
- `gitstratum_frontend_cache_hits_total` - Cache hit count
- `gitstratum_frontend_active_connections` - Current connection count

### Control Plane Metrics
- `gitstratum_control_raft_leader` - Current leader node
- `gitstratum_control_auth_decisions_total` - Auth decisions by result
- `gitstratum_control_ratelimit_throttled_total` - Throttled requests

### Metadata Metrics
- `gitstratum_metadata_refs_total` - Total refs stored
- `gitstratum_metadata_pack_cache_size_bytes` - Pack cache size
- `gitstratum_metadata_pack_cache_hit_ratio` - Cache hit ratio

### Object Metrics
- `gitstratum_object_stored_bytes` - Total stored bytes
- `gitstratum_object_read_bytes_total` - Bytes read
- `gitstratum_object_compression_ratio` - Compression efficiency
- `gitstratum_object_gc_objects_collected` - GC collected objects
