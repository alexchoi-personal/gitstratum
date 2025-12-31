# GitStratum (WIP)

A distributed Git storage system built in Rust.

## What is GitStratum?

GitStratum is a horizontally scalable Git hosting backend that separates storage concerns into specialized clusters:

- **Control Plane** - Raft-based coordination for cluster state, authentication, and rate limiting
- **Metadata Cluster** - Stores commits, trees, and refs with partition-based routing
- **Object Cluster** - Blob storage with consistent hashing and tiered compression
- **Frontend** - Git protocol (HTTP/SSH) serving layer

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Clients   │────▶│    Frontend     │────▶│  Control Plane  │
│  (git cli)  │     │  (Git Protocol) │     │     (Raft)      │
└─────────────┘     └────────┬────────┘     └─────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
       ┌──────────┐   ┌──────────┐   ┌──────────┐
       │ Metadata │   │ Metadata │   │ Metadata │
       │  Node 1  │   │  Node 2  │   │  Node N  │
       └──────────┘   └──────────┘   └──────────┘
              │              │              │
       ┌──────────┐   ┌──────────┐   ┌──────────┐
       │  Object  │   │  Object  │   │  Object  │
       │  Node 1  │   │  Node 2  │   │  Node N  │
       └──────────┘   └──────────┘   └──────────┘
```

## Crates

| Crate | Description |
|-------|-------------|
| `gitstratum-core` | Core types (Oid, Blob, Commit, Tree, etc.) |
| `gitstratum-control-plane` | Raft consensus and cluster coordination |
| `gitstratum-metadata` | Commit/tree/ref storage |
| `gitstratum-object` | Blob storage with compression |
| `gitstratum-frontend` | Git protocol implementation |
| `gitstratum-operator` | Kubernetes operator |
| `gitstratum-storage` | NVMe-optimized storage engine |
| `gitstratum-hashring` | Consistent hashing |

## Building

```bash
cargo build --workspace
```

## Testing

```bash
cargo nextest run --workspace
```

## Pre-commit Hooks

Set up the pre-commit hook to check formatting and lints:

```bash
git config core.hooksPath .githooks
```

## License

MIT OR Apache-2.0
