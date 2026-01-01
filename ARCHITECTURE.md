# GitStratum Architecture

GitStratum is a distributed Git server designed for CI/CD workloads.

## The Problem

CI/CD systems clone repositories constantly. When a developer pushes to main, dozens of pipeline jobs spin up—each one cloning the same repo, at the same commit, with the same shallow depth. Traditional Git servers treat each clone as a fresh request: resolve refs, walk the commit graph, gather objects, build a pack file, compress, send. The same work, repeated for every runner.

GitStratum asks: what if the hundredth clone cost almost nothing?

## Overview

The system is split into four clusters:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FRONTEND                                    │
│                                                                             │
│   Git protocol, authentication, rate limiting, pack assembly.              │
│   Stateless. Scales with connection count.                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
┌───────────────────────┐  ┌───────────────────┐  ┌───────────────────────────┐
│     CONTROL PLANE     │  │      METADATA     │  │          OBJECT           │
│                       │  │                   │  │                           │
│  Cluster membership.  │  │  Refs, commits,   │  │  Blob storage.            │
│  Hash ring topology.  │  │  permissions.     │  │  Pack cache.              │
│  Background only—not  │  │  Partitioned by   │  │  Replicated across nodes. │
│  on the request path. │  │  repository.      │  │                           │
└───────────────────────┘  └───────────────────┘  └───────────────────────────┘
```

Each cluster scales independently:

- **Frontend** scales with traffic. More CI runners means more Frontend nodes.
- **Control Plane** stays small (3–5 nodes). It coordinates the cluster but doesn't serve requests.
- **Metadata** scales with repository count.
- **Object** scales with storage capacity.

## Crate Structure

```
gitstratum-cluster/
├── gitstratum-core/             # Core types: Oid, Repo, object parsing
├── gitstratum-proto/            # gRPC protocol definitions
├── gitstratum-hashring/         # Consistent hashing implementation
├── gitstratum-metrics/          # Metrics collection and export
├── gitstratum-storage/          # BucketStore: high-performance object storage
├── gitstratum-control-plane/    # Control plane: Raft, membership, config
├── gitstratum-metadata/         # Metadata: refs, graph, permissions
├── gitstratum-object-cluster/   # Object: blob storage, pack cache, replication, repair
├── gitstratum-frontend/         # Frontend: Git protocol, pack assembly
├── gitstratum-operator/         # Kubernetes operator
└── gitstratum-lfs/              # Git LFS support
```

## Request Flow

### Clone

A CI runner runs `git clone --depth=1`. Here's what happens:

```
Runner                              GitStratum
   │
   │────── connect ──────────────────▶│
   │                                  │
   │                           ┌──────┴──────┐
   │                           │  FRONTEND   │
   │                           │             │
   │                           │  • Validate token (local)
   │                           │  • Check rate limit (local)
   │                           └──────┬──────┘
   │                                  │
   │                                  ▼
   │                           ┌─────────────┐
   │                           │  METADATA   │
   │                           │             │
   │                           │  • Resolve ref to commit
   │                           │  • Check read permission
   │                           └──────┬──────┘
   │                                  │
   │                                  ▼
   │                           ┌─────────────┐
   │                           │   OBJECT    │
   │                           │             │
   │                           │  • Check pack cache
   │                           │  • Hit: stream cached pack
   │                           │  • Miss: build pack, cache, stream
   │                           └──────┬──────┘
   │                                  │
   │◀──────── pack data ──────────────┘
```

**Authentication** happens locally in Frontend. JWT tokens are validated against known public keys—no network call. SSH keys are cached locally and synced from Metadata periodically.

**Rate limiting** is approximate. Each Frontend tracks requests independently using a sliding window. With 10 Frontends and a 1000 req/min limit, each allows roughly 100 req/min per user. Good enough for abuse prevention.

**The pack cache** is the key optimization. The cache key includes repo, ref, commit hash, and depth. Same parameters means same pack bytes. First clone builds and caches the pack; subsequent clones stream it directly. Hit rates exceed 95% for typical CI/CD workloads.

### Push

Pushes are less frequent but more complex:

```
Developer                           GitStratum
   │
   │────── push ─────────────────────▶│
   │       (pack)                     │
   │                           ┌──────┴──────┐
   │                           │  FRONTEND   │
   │                           │             │
   │                           │  • Validate token (local)
   │                           │  • Unpack objects
   │                           └──────┬──────┘
   │                                  │
   │                                  ▼
   │                           ┌─────────────┐
   │                           │  METADATA   │  ─── Check write permission
   │                           └──────┬──────┘
   │                                  │
   │                                  ▼
   │                           ┌─────────────┐
   │                           │   OBJECT    │  ─── Store objects (3 replicas)
   │                           └──────┬──────┘
   │                                  │
   │                                  ▼
   │                           ┌─────────────┐
   │                           │  METADATA   │  ─── Update refs
   │                           └──────┬──────┘
   │                                  │
   │                                  ▼
   │                           ┌─────────────┐
   │                           │   OBJECT    │  ─── Invalidate affected pack caches
   │                           └─────────────┘
   │                                  │
   │◀──────── OK ─────────────────────┘
```

Objects are written to three nodes for durability. Writes use quorum—succeed on 2 of 3 before acknowledging. One slow node doesn't block the push.

After refs update, cached packs for affected branches are invalidated. The next clone rebuilds them.

## Cluster Details

### Frontend

Frontend nodes are stateless and interchangeable. They handle:

- Git protocol (SSH and HTTPS)
- Token validation (JWT signatures checked locally)
- Rate limiting (local sliding window)
- Pack assembly and streaming
- Request coalescing (multiple identical requests share one backend call)

A load balancer distributes connections. If a node dies, traffic routes to others. No data is lost.

Frontend caches two things from other clusters:
- **SSH keys** from Metadata (synced every few minutes)
- **Hash ring topology** from Control Plane (watched in real-time)

#### Request Coalescing

When multiple CI runners request the same pack simultaneously, Frontend coalesces them:

```
Runner A ──┐
Runner B ──┼──▶ Single backend request ──▶ Stream to all three
Runner C ──┘
```

The first request triggers the backend call. Subsequent identical requests (same repo, ref, commit, depth) join the in-flight request and receive the same response stream. This prevents thundering herds when a popular branch gets pushed.

### Control Plane

Control Plane tracks cluster membership and the hash ring. It uses Raft consensus—a protocol where nodes elect a leader, and all changes replicate through the leader to followers.

```
┌─────────────────────────────────────────────────────────────┐
│                      CONTROL PLANE                          │
│                                                             │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐          │
│   │  Leader  │◄────►│ Follower │◄────►│ Follower │          │
│   └──────────┘      └──────────┘      └──────────┘          │
│                                                             │
│   Manages:                                                  │
│     • Node membership (who's in the cluster)                │
│     • Hash ring (which nodes own which object ranges)       │
│     • Cluster configuration                                 │
│     • Distributed locks (ref update coordination)           │
│     • Audit logging                                         │
│                                                             │
│   Does NOT manage:                                          │
│     • Authentication (Frontend handles locally)             │
│     • Rate limits (Frontend handles locally)                │
│     • Permissions (Metadata stores these)                   │
└─────────────────────────────────────────────────────────────┘
```

Control Plane is **not on the request path**. Frontend watches for topology changes in the background and caches the hash ring locally. Requests never block on Control Plane.

Why 3 or 5 nodes? Raft needs a majority to agree. With 3 nodes you need 2 (tolerates 1 failure). With 5 you need 3 (tolerates 2). More nodes adds overhead without much benefit.

#### Ring Distribution

Frontend nodes subscribe to ring updates via `WatchTopology`—a streaming gRPC call that stays open. When the ring changes, Control Plane pushes the delta:

```
Frontend                          Control Plane
   │                                    │
   │─── WatchTopology() ───────────────►│
   │                                    │
   │◀─────────── ring v1 ──────────────│  (initial state)
   │                                    │
   │              ...                   │  (time passes)
   │                                    │
   │◀─────────── ring v2 ──────────────│  (node added)
```

Frontend caches the ring locally. All routing decisions use the local cache—no network call to Control Plane per request.

### Metadata

Metadata stores everything about repositories except the actual object bytes:

- Refs (branches, tags)
- Commit graph (parent relationships, for merge-base calculations)
- Permissions (who can read/write)
- SSH keys (synced to Frontend)
- Pack cache index (which packs exist, their TTLs)
- Repository configuration and hooks

It's partitioned by repository. All data for one repo lives on the same node, so ref updates are atomic without distributed transactions.

Key components:
- **RefStorage**: Atomic ref updates with optimistic locking
- **GraphCache**: Cached commit ancestry for fast merge-base calculations
- **PackCacheStorage**: Tracks precomputed packs with TTL-based eviction
- **ObjectLocationIndex**: Maps object IDs to storage locations

### Object

Object stores Git objects (blobs, trees, commits, tags) and the pack cache.

Objects are distributed across nodes using consistent hashing. Each object's position on a ring is determined by its ID—since Git object IDs are already SHA-256 hashes, they're uniformly distributed across the keyspace. No additional hashing needed.

Key components:
- **ObjectStore**: Primary object storage (BucketStore or RocksDB)
- **PackCache**: Precomputed pack files for hot repositories
- **QuorumWriter**: Writes to N replicas, waits for quorum
- **HotRepoTracker**: Identifies frequently-accessed repos for precomputation
- **RepairCoordinator**: Central orchestrator for crash recovery, anti-entropy, and rebalancing
- **MerkleTree**: Position-based tree for O(log N) object set comparison between nodes

#### The Hash Ring

The ring is a circular number line from 0 to 2^64. Every object and every node gets a position on this ring.

**Object positions** come directly from the OID bytes:

```
OID: a1b2c3d4e5f6...  (32 bytes, SHA-256 hash)
Position: first 8 bytes as u64 = 0xa1b2c3d4e5f60000...
```

Since OIDs are already SHA-256 hashes, they're uniformly distributed. No additional hashing needed.

**Node positions** use virtual nodes. Each physical node gets multiple positions:

```
3 nodes with 16 vnodes each:

    A₀  B₀  C₀  A₁  B₁  C₁  A₂  B₂  ...  A₁₅ B₁₅ C₁₅
     ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓        ↓   ↓   ↓
  ───●───●───●───●───●───●───●───●── ... ─●───●───●───

Each node owns ~33% (many small segments instead of one big one)
```

To find which node owns an object: find its position, walk clockwise to the next vnode, that vnode's physical node is the owner.

#### Replication

Each object is stored on N distinct physical nodes (default: 3). To find replicas, walk clockwise collecting distinct physical nodes:

```
OID position: 500000
Replication factor: 3

     A₃        B₇        A₁₁       C₂        B₃
      ↓         ↓         ↓         ↓         ↓
  ────●─────────●─────────●─────────●─────────●────
      400000    550000    600000    700000    800000
             ↑
          500000

Walk clockwise:
  1. B₇  → Node B ✓ (1st replica - primary)
  2. A₁₁ → Node A ✓ (2nd replica)
  3. C₂  → Node C ✓ (3rd replica)

Replicas: [B, A, C]
```

**Writes** go to all replicas in parallel, wait for quorum (2 of 3) before acknowledging:

```
Frontend ──┬──► Node B (primary)  ──┐
           ├──► Node A (replica)  ──┼──► Wait for 2 of 3 ──► ACK
           └──► Node C (replica)  ──┘
```

**Reads** can go to any replica. Frontend picks based on load or latency.

## Storage Layer: BucketStore

The Object cluster uses BucketStore for high-performance object storage. BucketStore is a custom key-value store optimized for Git object workloads.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BucketStore                              │
│                                                                  │
│  ┌──────────────┐    ┌──────────────────────────────────────┐   │
│  │ BucketIndex  │    │           Bucket File                 │   │
│  │              │    │  ┌────────┬────────┬────────┬─────┐   │   │
│  │ bucket_id(oid) ──►│  │Bucket 0│Bucket 1│Bucket 2│ ... │   │   │
│  │              │    │  │ 4KB    │ 4KB    │ 4KB    │     │   │   │
│  └──────────────┘    │  └────────┴────────┴────────┴─────┘   │   │
│                      └──────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                      Data Files                            │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │   │
│  │  │  data.0001  │  │  data.0002  │  │  data.0003  │  ...   │   │
│  │  │ (append-only)│  │ (append-only)│  │ (append-only)│        │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**Two-file design:**
- **Bucket file**: Fixed-size 4KB pages containing object metadata (OID suffix, file ID, offset, size)
- **Data files**: Append-only files containing actual object data

**How it works:**

1. **Put**: Hash OID to bucket ID → append data to active data file → update bucket entry
2. **Get**: Hash OID to bucket ID → read bucket → find entry → read data at (file_id, offset)
3. **Delete**: Soft delete (set flag) or hard delete (remove entry, compact later)

### Bucket Structure

Each 4KB bucket contains:

```
┌────────────────────────────────────────────────────────────┐
│ Header (32 bytes)                                          │
│   magic: u32      count: u16      crc32: u32    reserved   │
├────────────────────────────────────────────────────────────┤
│ Entry 0 (32 bytes)                                         │
│   oid_suffix: [u8; 16]  file_id: u16  offset: u40  size: u24  flags: u8 │
├────────────────────────────────────────────────────────────┤
│ Entry 1                                                    │
├────────────────────────────────────────────────────────────┤
│ ...                                                        │
├────────────────────────────────────────────────────────────┤
│ Entry 125 (max 126 entries per bucket)                     │
├────────────────────────────────────────────────────────────┤
│ Overflow pointer + padding (32 bytes)                      │
└────────────────────────────────────────────────────────────┘
```

With 128M buckets (default) and 126 entries per bucket, the store supports ~16 billion objects.

### io_uring Integration

BucketStore uses Linux io_uring for all asynchronous I/O. A single shared io_uring instance handles both bucket operations and data file operations:

```
┌─────────────────────────────────────────────────────────────────────┐
│                           BucketStore                                │
│                                                                      │
│  Creates shared AsyncMultiQueueIo, passes to all components          │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    AsyncMultiQueueIo                         │    │
│  │                                                              │    │
│  │    Queue 0         Queue 1         Queue 2         Queue 3   │    │
│  │   ┌───────┐       ┌───────┐       ┌───────┐       ┌───────┐  │    │
│  │   │ depth │       │ depth │       │ depth │       │ depth │  │    │
│  │   │  256  │       │  256  │       │  256  │       │  256  │  │    │
│  │   └───┬───┘       └───┬───┘       └───┬───┘       └───┬───┘  │    │
│  │       │               │               │               │      │    │
│  │   Reaper 0        Reaper 1        Reaper 2        Reaper 3   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│            │                                       │                 │
│            ▼                                       ▼                 │
│  ┌─────────────────────┐                ┌─────────────────────┐     │
│  │      BucketIo       │                │      DataFile       │     │
│  │                     │                │                     │     │
│  │ queue = bucket_id   │                │ queue = file_id     │     │
│  │        % num_queues │                │        % num_queues │     │
│  └─────────────────────┘                └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

**Queue distribution:**
- BucketIo: `bucket_id % num_queues` — spreads bucket operations across queues
- DataFile: `file_id % num_queues` — spreads data operations across queues

**Why unified io_uring:**
- No blocking I/O on tokio worker threads
- Single io_uring instance reduces kernel overhead
- Consistent async code path for all I/O
- Better throughput under concurrent load

**Configuration:**

```rust
BucketStoreConfig {
    bucket_count: 128_000_000,    // 128M buckets
    io_queue_count: 4,            // 4 parallel io_uring queues
    io_queue_depth: 256,          // 256 ops per queue (1024 total)
    bucket_cache_size: 16_384,    // 16K cached buckets (64MB)
    ...
}
```

**Tuning guidelines:**

| Workload | Queues | Depth | Rationale |
|----------|--------|-------|-----------|
| Many small random reads | 8 | 128 | More parallelism, lower latency |
| Large sequential writes | 2 | 512 | Less overhead, better batching |
| Mixed (default) | 4 | 256 | Balanced |

**Fallback:** On non-Linux platforms or without the `io_uring` feature, falls back to synchronous I/O.

### Durability Model

BucketStore does **not** call fsync on writes. Durability comes from quorum replication at the cluster level, not local disk sync.

```
put(oid, value)
       │
       ├─────────────────────────────────────────────────┐
       │                                                 │
       ▼                                                 ▼
┌─────────────────┐                           ┌─────────────────┐
│   LOCAL WRITE   │                           │    REPLICAS     │
│                 │                           │                 │
│  append data    │                           │   replica 1  ───┤
│  update bucket  │                           │   replica 2  ───┼─► wait for
│  (no fsync)     │                           │   replica 3  ───┘   2 of 3
└─────────────────┘                           └─────────────────┘
       │                                                 │
       └────────────────────┬────────────────────────────┘
                            │
                            ▼
                      return OK
               (data is durable across cluster)
```

**How it works:**

1. **Write path**: Data goes to kernel page cache (not disk). Returns immediately.
2. **Kernel writeback**: Linux flushes dirty pages based on age and memory pressure.
3. **Quorum**: Object cluster waits for 2 of 3 replicas before acknowledging.
4. **Recovery**: If a node crashes before kernel flush, it recovers from replicas.

**Kernel writeback timing** (controlled by sysctl, not the program):

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `dirty_writeback_centisecs` | 500 (5s) | How often writeback threads wake up |
| `dirty_expire_centisecs` | 3000 (30s) | Page age before eligible for writeback |
| `dirty_background_ratio` | 10% | Start background writeback at this dirty ratio |
| `dirty_ratio` | 20% | Block writers until dirty ratio drops below this |

Worst-case flush delay is **30 seconds** (dirty_expire). In practice, under write load, memory pressure triggers writeback much sooner.

**Why no fsync:**

| Concern | Solution |
|---------|----------|
| Node crash before flush | Data exists on 2+ other nodes |
| Corrupted write | CRC32 in every record; re-fetch from replica |
| All 3 nodes crash simultaneously | Extremely unlikely; pages flush within 30s max |

**Trade-off:** Single-node deployments (testing, development) have up to a 30 second durability window. Production deployments with replication factor ≥ 3 are durable immediately after quorum ACK.

**Performance impact:**

| Operation | With fsync | Without fsync |
|-----------|------------|---------------|
| `put()` latency | 1-10 ms | 100-200 µs |
| Write throughput | 1K-10K ops/s | 50K-200K ops/s |

### Performance Targets

Target latencies for BucketStore on NVMe SSD:

| Operation | p50 | p99 | p999 |
|-----------|-----|-----|------|
| `get()` (cache hit) | 1-5 µs | 10 µs | 50 µs |
| `get()` (cache miss) | 50-100 µs | 200 µs | 1 ms |
| `put()` (local only) | 100-200 µs | 500 µs | 2 ms |
| `put()` (with quorum) | 1-5 ms | 10 ms | 50 ms |

Target throughput per node:

| Metric | Target |
|--------|--------|
| Small reads (<4KB) | 100K-500K ops/s |
| Small writes (<4KB) | 50K-200K ops/s |
| Sequential read | 3-6 GB/s |
| Sequential write | 1-3 GB/s |

io_uring targets:

| Metric | Target |
|--------|--------|
| Queue utilization | < 80% |
| Completion latency | < 100 µs |
| IOPS (NVMe) | 100K-1M |

Cache targets:

| Metric | Target |
|--------|--------|
| Bucket cache hit rate | > 90% |
| Cache evictions/sec | < 1000 |

**Note:** These are design targets. Actual performance depends on hardware, workload, and configuration. Benchmarks should be run on production-equivalent hardware to validate.

### Compaction

Over time, deletions and updates create dead space. The compactor reclaims it:

1. Scan buckets for entries marked deleted or pointing to old data
2. Copy live entries to a new data file
3. Update bucket entries with new locations
4. Remove entries for deleted objects
5. Delete old data files

Compaction runs when fragmentation exceeds 40% (configurable).

## Component Communication

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  FRONTEND                                   │
│                                                                             │
│  Local: JWT validation, rate limiting                                       │
│  Cached: SSH keys (from Metadata), hash ring (from Control Plane)           │
└─────────────────────────────────────────────────────────────────────────────┘
         │                          │                          │
         │ background               │ request path             │ request path
         ▼                          ▼                          ▼
┌─────────────────┐        ┌─────────────────┐        ┌─────────────────┐
│  CONTROL PLANE  │        │    METADATA     │        │     OBJECT      │
│                 │        │                 │        │                 │
│  WatchTopology  │        │  GetRefs        │        │  GetObjects     │
│  GetHashRing    │        │  UpdateRefs     │        │  PutObjects     │
│  AcquireLock    │        │  CheckPermission│        │  GetPackCache   │
│  ReleaseLock    │        │  SyncSSHKeys    │        │  StreamBlobs    │
└─────────────────┘        └─────────────────┘        └─────────────────┘
```

All communication uses gRPC. Frontend maintains connection pools to each cluster.

**Control Plane calls are background only.** Frontend subscribes to topology changes and updates its local cache. No request ever waits on Control Plane.

**Metadata calls happen per-request** for ref resolution and permission checks. SSH key sync is periodic, not per-request.

**Object calls happen per-request** for cache lookups and object fetches. Frontend batches object requests by destination node—instead of 1000 individual calls, it makes 10 batch calls (one per relevant node).

## Failure Modes

| Component | What breaks | What keeps working |
|-----------|-------------|-------------------|
| Frontend node | Connections to that node | Other nodes handle traffic |
| Control Plane | Can't add/remove nodes | Requests continue with cached topology |
| Metadata partition | Affected repos can't push or check permissions | Other repos unaffected; cached packs still stream |
| Object node | That node's objects unavailable from it | Replicas serve reads; writes succeed with 2 of 3 |

**Frontend failure:** Stateless, so no data loss. Load balancer routes around dead nodes. Clients retry.

**Control Plane failure:** Not on request path, so requests continue. Topology is frozen—no nodes can join or leave—but existing topology works indefinitely.

**Metadata failure:** Partitioned by repo. If one partition is down, those repos are affected but others continue. Cached packs in Object cluster can still stream (cache key includes commit hash, so it's still valid).

**Object node failure:** Objects are on 3 nodes. One down means 2 remain. Reads succeed. Writes need 2 of 3, so they succeed too. A background repairer copies data to restore the third replica when the node recovers.

## Object Cluster Repair System

When nodes fail, restart, or join/leave the cluster, the repair system ensures data consistency. It handles three scenarios:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          REPAIR COORDINATOR                                  │
│                                                                             │
│   Orchestrates all repair types, manages sessions, enforces rate limits    │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ CrashRecovery   │ │  AntiEntropy    │ │   Rebalance     │
│     Handler     │ │    Repairer     │ │     Handler     │
│                 │ │                 │ │                 │
│ • Downtime      │ │ • Periodic      │ │ • Node join     │
│   tracking      │ │   background    │ │ • Node drain    │
│ • Startup       │ │   scans         │ │ • Range         │
│   recovery      │ │ • Random        │ │   streaming     │
│                 │ │   sampling      │ │                 │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             │
                             ▼
                  ┌────────────────────────┐
                  │      Merkle Trees      │
                  │                        │
                  │  Position-based tree   │
                  │  for efficient diff    │
                  └────────────────────────┘
```

### Repair Types

| Type | Trigger | Priority | Description |
|------|---------|----------|-------------|
| Failed Write | Quorum write partially fails | 1 (highest) | Retry writes that succeeded on <N nodes |
| Crash Recovery | Node restart | 2 | Recover objects missed during downtime |
| Rebalance | Hash ring change | 3 | Move objects when nodes join/leave |
| Anti-Entropy | Periodic timer | 4 (lowest) | Background consistency verification |

### Merkle Trees for Efficient Comparison

Comparing billions of objects between nodes would be expensive. Instead, each node maintains Merkle trees over its object positions:

```
Position Space: 0 ────────────────────────────────────────── 2^64
                │                                             │
                ▼                                             ▼
Level 0:    [Root Hash]
                │
Level 1:    [0x0..]  [0x1..]  [0x2..]  ...  [0xF..]     (16 children)
                │
Level 2:    [0x00..] [0x01..] ...                        (16 children)
                │
Level 3:    [0x000.] [0x001.] ...                        (16 children)
                │
Level 4:    [OID list] ← Leaf nodes contain actual OIDs
```

**How comparison works:**

1. Exchange root hashes. If equal, trees are identical—done.
2. If different, exchange Level 1 hashes. Find differing children.
3. Recurse into differing subtrees only.
4. At leaf level, exchange OID lists to find missing objects.

This reduces O(N) full-scan comparison to O(log N) tree traversal. For a billion objects, we exchange ~4 levels of hashes instead of a billion OIDs.

### Crash Recovery Flow

When a node restarts after a crash:

```
Node Restart
     │
     ▼
┌─────────────────────────────────────┐
│ 1. Read last_healthy_timestamp      │
│    from persistent storage          │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ 2. Compute downtime window          │
│    (last_healthy → now)             │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ 3. For each owned range:            │
│    a. Get Merkle tree from peer     │
│    b. Compare with local tree       │
│    c. Queue missing OIDs            │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ 4. Stream missing objects           │
│    with rate limiting               │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ 5. Verify (CRC32) and store         │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ 6. Update last_healthy_timestamp    │
│    Node ready to serve              │
└─────────────────────────────────────┘
```

The **DowntimeTracker** persists a `healthy_timestamp` file. A background heartbeat updates it every few seconds. On restart, the gap between the file's timestamp and current time indicates how long the node was down.

### Anti-Entropy

Continuous background process that detects and fixes inconsistencies:

```rust
loop {
    for range in owned_ranges.round_robin() {
        // Build local Merkle tree
        let local_tree = build_merkle_tree(store, range);

        // Compare with each replica peer
        for peer in peers_for_range(range) {
            let remote_tree = get_merkle_tree(peer, range);
            let missing = local_tree.diff(remote_tree);

            if !missing.is_empty() {
                repair_queue.extend(missing);
            }
        }
    }

    sleep(scan_interval);  // Default: 1 hour
}
```

Anti-entropy catches problems that other repair types miss:
- Bit rot (data corruption on disk)
- Objects lost during crashes before crash recovery ran
- Bugs in replication code

### Rebalancing

When the hash ring changes (node added or removed):

**Node Join (Incoming):**
```
New Node                              Existing Node
    │                                       │
    │  1. Compute ranges to receive         │
    │────────────────────────────────────►  │
    │                                       │
    │  2. GetMerkleTree(range)              │
    │────────────────────────────────────►  │
    │                                       │
    │  3. MerkleTreeResponse                │
    │◄────────────────────────────────────  │
    │                                       │
    │  [Find missing objects]               │
    │                                       │
    │  4. StreamBlobs(missing_oids)         │
    │────────────────────────────────────►  │
    │                                       │
    │  5. stream(Blob1, Blob2, ...)         │
    │◄────────────────────────────────────  │
    │                                       │
    │  [Verify CRC32, store]                │
    │                                       │
    ▼  Node transitions to Active           ▼
```

**Node Drain (Outgoing):**
1. Node enters `Draining` state (accepts reads, rejects writes)
2. Computes ranges that transfer to other nodes
3. Streams objects to new owners
4. Once complete, node leaves the ring

### Rate Limiting and Checkpointing

Repairs consume bandwidth. The RepairCoordinator enforces limits:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          RepairCoordinator                                   │
│                                                                             │
│   max_bandwidth_bytes_per_sec: 100 MB/s (configurable)                      │
│   max_concurrent_sessions: 5                                                │
│   pause_threshold_write_rate: 10K writes/sec                                │
│                                                                             │
│   If write load exceeds threshold → pause repairs                           │
│   Checkpoint every 1000 objects → resume after failure                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Checkpointing:** Every N objects, the repair session saves its position. If a repair is interrupted, it resumes from the last checkpoint instead of restarting.

### gRPC APIs

The ObjectRepairService provides:

```protobuf
service ObjectRepairService {
    // Get Merkle tree for position range (for comparison)
    rpc GetMerkleTree(GetMerkleTreeRequest) returns (MerkleTreeResponse);

    // Stream missing objects by OIDs
    rpc StreamMissingBlobs(StreamMissingBlobsRequest) returns (stream Blob);

    // Get repair session status
    rpc GetRepairStatus(GetRepairStatusRequest) returns (GetRepairStatusResponse);

    // List active repair sessions
    rpc ListRepairSessions(ListRepairSessionsRequest) returns (ListRepairSessionsResponse);

    // Cancel a repair session
    rpc CancelRepairSession(CancelRepairSessionRequest) returns (CancelRepairSessionResponse);
}
```

### Stale Ring Handling

What happens when the ring changes but Frontend hasn't received the update yet?

**Reads with stale ring:** Old replicas are still valid replicas. When a node is added, it becomes the new primary for some range, but the previous replicas still have the data. Reads succeed; they just might hit a secondary instead of the primary.

**Writes with stale ring:** Writes succeed because the old topology's replicas overlap with the new topology's replicas. The new node catches up via background repair.

**Why this works:**
1. **Objects are immutable.** No "wrong version" to read. If an object exists, it's the right one.
2. **Replicas overlap.** Adding one node shifts ownership by one position. Two of three replicas remain the same.
3. **Repair fixes gaps.** Background process continuously scans for missing replicas and copies data.

## Design Tradeoffs

This architecture optimizes for CI/CD workloads:

**Reads over writes.** CI pipelines clone constantly; pushes are occasional. Caching and read scaling are prioritized.

**Approximate over exact.** Rate limiting is per-Frontend, not globally coordinated. Good enough for abuse prevention, and it scales.

**Availability over consistency.** Cached packs can stream even if Metadata is down. Slightly stale data is acceptable; blocked pipelines are not.

**Horizontal over vertical.** Frontend and Object scale out. Control Plane stays small but doesn't bottleneck—it's not on the request path.

**Caching everywhere.** Refs are cached. Auth decisions are cached. Pack files are cached. Hash ring is cached. The goal is to avoid recomputation.

**io_uring for I/O parallelism.** BucketStore uses a unified io_uring instance for all I/O (buckets and data files), enabling parallel NVMe access without blocking tokio threads.

**Quorum over fsync.** Local writes don't wait for disk sync. Durability comes from replication—2 of 3 nodes must ACK before the write returns. This trades single-node durability (up to 30 second window, kernel-controlled) for 10-100x better write latency.

For workloads where every clone is unique, or writes dominate reads, this design would be overkill. But for CI/CD—where the same repo is cloned thousands of times daily, always at the same ref, always shallow—it fits well.
