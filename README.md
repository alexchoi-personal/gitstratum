# GitStratum

A distributed Git server optimized for CI/CD workloads.

## The Problem

CI/CD systems clone repositories constantly. When a developer pushes to main, dozens of pipeline jobs spin up—each cloning the same repo, at the same commit, with the same shallow depth. Traditional Git servers treat each clone as fresh work: resolve refs, walk the commit graph, gather objects, build a pack file, compress, send.

GitStratum asks: what if the hundredth clone cost almost nothing?

## How It Works

GitStratum caches assembled pack files. The first clone does the work. Every subsequent clone of the same ref at the same commit streams the cached result. For CI/CD workloads, cache hit rates exceed 95%.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FRONTEND                                    │
│                                                                             │
│   Git protocol (SSH/HTTPS), authentication, rate limiting, pack assembly   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
             ┌───────────────────────┼───────────────────────┐
             ▼                       ▼                       ▼
┌───────────────────────┐  ┌───────────────────┐  ┌───────────────────────────┐
│     CONTROL PLANE     │  │      METADATA     │  │          OBJECT           │
│                       │  │                   │  │                           │
│  Cluster membership   │  │  Refs, commits,   │  │  Blob storage (BucketStore│
│  Hash ring topology   │  │  trees, perms     │  │  Pack cache               │
│  Raft consensus       │  │  Partitioned by   │  │  Consistent hashing with  │
│  (background only)    │  │  repository       │  │  vnodes, 3x replication   │
└───────────────────────┘  └───────────────────┘  └───────────────────────────┘
```

**Frontend** handles Git protocol and streams data. Stateless—scales with connection count.

**Control Plane** coordinates cluster membership and hash ring topology via Raft. Not on the request path—Frontend caches the ring locally.

**Metadata** stores refs, commits, trees, and permissions. Partitioned by repository for atomic ref updates.

**Object** stores Git objects (blobs, trees, commits, tags) distributed across nodes via consistent hashing. Each object lives on 3 nodes. Writes use quorum (2 of 3). The pack cache stores pre-assembled pack files for fast clones.

## Key Design Choices

- **Pack caching**: Same clone request = same pack bytes. Cache it once, stream it many times.
- **Consistent hashing with vnodes**: Objects distributed evenly across nodes. Adding a node shifts ~1/N of data, not 50%.
- **Quorum replication**: Write to 3 nodes, wait for 2. One slow node doesn't block.
- **Eventual consistency**: Objects are immutable. Stale routing still works—old replicas remain valid.
- **Control Plane off the hot path**: Frontend caches topology. Requests never wait on Raft.

## Crates

| Crate | Description |
|-------|-------------|
| `gitstratum-core` | Core types: Oid, Blob, Commit, Tree, Ref |
| `gitstratum-control-plane-cluster` | Raft consensus, membership, hash ring management |
| `gitstratum-metadata-cluster` | Ref/commit/tree storage, partitioned by repo |
| `gitstratum-object-cluster` | Object storage, pack cache, replication |
| `gitstratum-frontend-cluster` | Git protocol (SSH/HTTPS), pack assembly |
| `gitstratum-storage` | BucketStore: bucket-based key-value storage for objects |
| `gitstratum-hashring` | Consistent hashing with virtual nodes |
| `gitstratum-lfs` | Git LFS support with cloud object storage backends |
| `gitstratum-operator` | Kubernetes operator for cluster management |

## Building

```bash
cargo build --workspace
```

## Testing

```bash
cargo nextest run --workspace
```

## Pre-commit Hooks

```bash
git config core.hooksPath .githooks
```

## Documentation

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## License

MIT OR Apache-2.0
