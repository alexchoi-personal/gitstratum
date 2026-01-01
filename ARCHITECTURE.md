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

Each physical node gets multiple positions on the ring (virtual nodes). This smooths out the distribution—adding a node takes a little from everyone rather than a lot from one neighbor.

Objects replicate to 3 nodes for durability. If one node is down, reads go to replicas. Writes need 2 of 3 to succeed (quorum).

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
