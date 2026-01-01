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

A load balancer distributes connections. If a node dies, traffic routes to others. No data is lost.

Frontend caches two things from other clusters:
- **SSH keys** from Metadata (synced every few minutes)
- **Hash ring topology** from Control Plane (watched in real-time)

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
│                                                             │
│   Does NOT manage:                                          │
│     • Authentication (Frontend handles locally)             │
│     • Rate limits (Frontend handles locally)                │
│     • Permissions (Metadata stores these)                   │
└─────────────────────────────────────────────────────────────┘
```

Control Plane is **not on the request path**. Frontend watches for topology changes in the background and caches the hash ring locally. Requests never block on Control Plane.

#### Ring Topology Storage

Control Plane maintains the hash ring as part of the Raft state machine. For each node in the cluster, it stores:

```
NodeInfo {
    node_id: "object-node-7"     // Unique identifier
    address: "10.0.5.7"          // IP or hostname
    port: 9090                   // gRPC port
    role: ObjectNode             // Node type
    vnodes: [847293, 192847, ...]  // Precomputed vnode positions
}
```

When a node joins or leaves, Control Plane:
1. Updates the Raft log (replicated to all Control Plane nodes)
2. Recomputes vnode positions for affected nodes
3. Increments the ring version
4. Pushes changes to all watchers

#### Ring Distribution

Frontend nodes subscribe to ring updates via `WatchTopology`—a streaming gRPC call that stays open. When the ring changes, Control Plane pushes the delta:

```
Frontend                          Control Plane
   │                                    │
   │─── WatchTopology() ───────────────►│
   │                                    │
   │◄─────────── ring v1 ──────────────│  (initial state)
   │                                    │
   │              ...                   │  (time passes)
   │                                    │
   │◄─────────── ring v2 ──────────────│  (node added)
   │                                    │
   │              ...                   │
```

Frontend caches the ring locally. All routing decisions use the local cache—no network call to Control Plane per request. If the stream disconnects, Frontend reconnects and gets the current ring state.

Why 3 or 5 nodes? Raft needs a majority to agree. With 3 nodes you need 2 (tolerates 1 failure). With 5 you need 3 (tolerates 2). More nodes adds overhead without much benefit.

### Metadata

Metadata stores everything about repositories except the actual object bytes:

- Refs (branches, tags)
- Commit graph (parent relationships)
- Permissions (who can read/write)
- SSH keys (synced to Frontend)

It's partitioned by repository. All data for one repo lives on the same node, so ref updates are atomic without distributed transactions.

### Object

Object stores Git objects (blobs, trees, commits, tags) and the pack cache.

Objects are distributed across nodes using consistent hashing. Each object's position on a ring is determined by its ID—since Git object IDs are already SHA-256 hashes, they're uniformly distributed across the keyspace. No additional hashing needed. Nodes own ranges of the ring. When you need an object, its ID directly tells you which node has it.

#### The Hash Ring

The ring is a circular number line from 0 to 2^64. Every object and every node gets a position on this ring.

**Object positions** come directly from the OID bytes:

```
OID: a1b2c3d4e5f6...  (32 bytes, SHA-256 hash)
Position: first 8 bytes as u64 = 0xa1b2c3d4e5f60000...
```

Since OIDs are already SHA-256 hashes, they're uniformly distributed. No additional hashing needed.

**Node positions** come from hashing the node ID. But one position per node creates uneven segments:

```
3 nodes with 1 position each:

    Node A              Node B                    Node C
       ↓                   ↓                         ↓
  ─────●───────────────────●─────────────────────────●─────
       10%                 60%                       30%

Node B owns 60% of keyspace—unfair.
```

#### Virtual Nodes

The fix: give each physical node multiple positions (virtual nodes). With 16 vnodes per node:

```
    A₀  B₀  C₀  A₁  B₁  C₁  A₂  B₂  ...  A₁₅ B₁₅ C₁₅
     ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓        ↓   ↓   ↓
  ───●───●───●───●───●───●───●───●── ... ─●───●───●───

Each node owns ~33% (many small segments instead of one big one)
```

Vnode positions are computed by hashing the node ID with an index:

```
hash("node-a", 0) → position 847293...
hash("node-a", 1) → position 192847...
hash("node-a", 2) → position 583920...
```

To find which node owns an object: find its position, walk clockwise to the next vnode, that vnode's physical node is the owner.

```
OID position: 500000

     A₃        B₇        C₂
      ↓         ↓         ↓
  ────●─────────●─────────●────
      400000    550000    700000
             ↑
          500000 → next vnode is B₇ → owned by Node B
```

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

If we hit the same physical node twice (e.g., another B vnode), we skip it and keep walking.

**Edge case—not enough nodes:** If the cluster has fewer physical nodes than the replication factor, objects get fewer replicas. A 2-node cluster with replication factor 3 stores each object on both nodes (2 replicas). Quorum math adjusts: 2 nodes means quorum is 2, so both must succeed.

**Writes** go to all replicas in parallel, wait for quorum (2 of 3) before acknowledging:

```
Frontend ──┬──► Node B (primary)  ──┐
           ├──► Node A (replica)  ──┼──► Wait for 2 of 3 ──► ACK
           └──► Node C (replica)  ──┘
```

**Reads** can go to any replica. Frontend picks based on load or latency.

#### Batched Parallel Fetches

A large clone might need millions of objects spread across many nodes. Fetching them one-by-one would be slow. Instead, Frontend groups objects by destination node and makes parallel batch requests:

```
10 million OIDs to fetch, 10 Object nodes

Step 1: Group by owner (using hash ring)
  Node A: [oid1, oid7, oid12, ...]   ~1M OIDs
  Node B: [oid2, oid5, oid19, ...]   ~1M OIDs
  ...
  Node J: [oid4, oid8, oid23, ...]   ~1M OIDs

Step 2: Parallel batch requests
  ┌─────────────┐
  │  Frontend   │
  └──────┬──────┘
         │
   ┌─────┼─────┬─────┬─────┬─────┐
   ▼     ▼     ▼     ▼     ▼     ▼
  [A]   [B]   [C]   [D]   ...   [J]    ← 10 parallel gRPC streams
   │     │     │     │           │
   └─────┴─────┴─────┴───────────┘
                  │
                  ▼
         Merge results, stream to client
```

Each gRPC call uses streaming—objects flow back as they're fetched, no waiting for all objects before sending.

**Load balancing across replicas:** Since each object has 3 replicas, Frontend can spread load:

```
Instead of always hitting primary:
  Node A (primary): 1M reads
  Node B (replica): 0 reads
  Node C (replica): 0 reads

Spread across replicas:
  Node A: ~333K reads
  Node B: ~333K reads
  Node C: ~333K reads

3x read throughput, even load distribution.
```

This matters for hot repositories where many CI runners clone simultaneously.

#### Stale Ring Handling

What happens when the ring changes but Frontend hasn't received the update yet?

**Reads with stale ring:**
```
Scenario: Node D added, Frontend doesn't know yet

Frontend thinks: OID X → [A, B, C]
Reality:         OID X → [D, A, B]  (D is new primary)

Frontend asks Node A for OID X:
  - If A has it (was a replica): Success, returns data
  - If A doesn't have it: Returns "not found"

On "not found", Frontend:
  1. Refreshes ring from Control Plane
  2. Retries with correct node
```

The key insight: **old replicas are still valid replicas**. When a node is added, it becomes the new primary for some range, but the previous replicas still have the data. Reads succeed; they just might hit a secondary instead of the primary.

**Writes with stale ring:**
```
Scenario: Node D added, Frontend doesn't know yet

Frontend writes to: [A, B, C]
Should write to:    [D, A, B]

Result:
  - A and B receive the write (they're still replicas)
  - C receives the write (but shouldn't be a replica anymore)
  - D misses the write (new primary, but Frontend doesn't know)

Quorum (2 of 3) succeeds on [A, B, C].
D catches up via background repair.
```

Writes succeed because the old topology's replicas overlap with the new topology's replicas. The new node catches up via repair.

**Why this works:**
1. **Objects are immutable.** No "wrong version" to read. If an object exists, it's the right one.
2. **Replicas overlap.** Adding one node shifts ownership by one position. Two of three replicas remain the same.
3. **Repair fixes gaps.** Background process continuously scans for missing replicas and copies data.

The system favors **availability over immediate consistency**. A brief window of stale routing is acceptable; blocking requests while waiting for topology propagation is not.

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
│                 │        │  CheckPermission│        │  GetPackCache   │
│                 │        │  SyncSSHKeys    │        │  StreamBlobs    │
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

## Design Tradeoffs

This architecture optimizes for CI/CD workloads:

**Reads over writes.** CI pipelines clone constantly; pushes are occasional. Caching and read scaling are prioritized.

**Approximate over exact.** Rate limiting is per-Frontend, not globally coordinated. Good enough for abuse prevention, and it scales.

**Availability over consistency.** Cached packs can stream even if Metadata is down. Slightly stale data is acceptable; blocked pipelines are not.

**Horizontal over vertical.** Frontend and Object scale out. Control Plane stays small but doesn't bottleneck—it's not on the request path.

**Caching everywhere.** Refs are cached. Auth decisions are cached. Pack files are cached. Hash ring is cached. The goal is to avoid recomputation.

For workloads where every clone is unique, or writes dominate reads, this design would be overkill. But for CI/CD—where the same repo is cloned thousands of times daily, always at the same ref, always shallow—it fits well.
