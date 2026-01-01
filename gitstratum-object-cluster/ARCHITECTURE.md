# Object Cluster Architecture

The Object cluster stores Git objects—blobs, trees, commits, tags. These are immutable, content-addressed chunks of data identified by their SHA-256 hash. Once written, an object never changes; if its content changed, it would have a different hash.

This immutability shapes everything about the storage design.

## Why BucketStore

The storage engine is BucketStore, a custom key-value store designed for this workload.

BucketStore splits storage into two parts: a **bucket file** that maps keys to locations, and **data files** that hold actual values. The bucket file is divided into fixed-size buckets (4KB each). 4KB is chosen to align disk block size. To find a key, use its first 4 bytes to determine which bucket it's in, read that bucket, scan through its entries, then read the actual data from the data file.

```
get(key):
    bucket_id = key[0..4] % number_of_buckets  # first 4 bytes, no hashing
    bucket = read_bucket(bucket_id)             # 4KB read, often cached
    entry = find_in_bucket(bucket, key)         # scan ~100 entries
    value = read_data(entry.location)           # read actual bytes
```

Data files are append-only—new values are written to the end, never overwriting existing data. This is safe and fast for immutable objects.

This works well for Git objects because:

- **Writes are append-only.** Objects never change. No read-modify-write cycles.
- **Keys are already hashes.** Git object IDs are SHA-256 hashes, uniformly distributed. Using their first bytes directly for bucket assignment gives even load across buckets—no additional hashing needed.
- **Access has locality.** Cloning a repo accesses objects from that repo. Those objects cluster into a subset of buckets that stay cached.

The tradeoff is that lookups may hit disk twice (bucket + data) if the bucket isn't cached. For workloads with good locality—like CI/CD cloning the same repos repeatedly—the bucket cache is effective and this rarely happens.

## Distributing Objects Across Nodes

With multiple storage nodes, we need to decide which node stores which objects. We use **consistent hashing**—a technique that maps both objects and nodes onto a circular number line (a "ring").

Here's how it works:

1. Use the object ID directly as its position on the ring (0 to 2^64). Since Git object IDs are SHA-256 hashes, they're already uniformly distributed—no additional hashing needed.
2. Hash each node ID to get positions on the ring
3. An object belongs to the first node clockwise from its position

```
        Node A                    Node B
           ↓                         ↓
    ───────●─────────────────────────●─────────────────────────●───
           0                                                   max
                    ↑
              Object X lands here, belongs to Node B
```

Why not just `oid % num_nodes`? Because when nodes are added or removed, that formula reassigns almost every object. Consistent hashing only reassigns objects in the affected range—the rest stay put.

### Virtual Nodes

A problem: with few nodes, the ring has uneven segments. One node might own 60% of the keyspace, another 10%.

The fix: give each physical node multiple positions on the ring, called **virtual nodes**. With 16 virtual nodes per physical node, the segments become roughly equal even with small clusters.

```
    A₀    B₀    A₁    B₁    A₂    B₂    A₃    B₃   ...
    ↓     ↓     ↓     ↓     ↓     ↓     ↓     ↓
────●─────●─────●─────●─────●─────●─────●─────●────
```

Now adding a node takes a little from everyone instead of a lot from one neighbor.

## Replication

Every object is stored on multiple nodes (3 by default) for durability. If one node dies, the data still exists on others.

The replicas are the N nodes clockwise from the object's ring position. Any node can compute this without asking anyone—just use the object ID as the position and walk the ring.

### Quorum Writes

When storing an object, we write to all 3 replicas but only wait for 2 to acknowledge success. This is called **quorum**—a majority is enough.

Why not wait for all 3? Because one slow or dead node would block every write. With quorum, writes succeed as long as most replicas are healthy. The third replica catches up eventually.

### Reading from Replicas

Reads can go to any replica. If one is slow or down, try another. The Frontend picks based on which node is likely fastest (considering latency and current load).

### Repair

A background process compares replicas and fixes inconsistencies. If one replica is missing an object that others have, it copies the object over. This handles the case where a node was down during a write and missed some objects.

## Frontend Routing

Frontend doesn't talk to "the Object cluster" as a single entity. It routes requests to specific nodes based on the hash ring.

### Position Calculation

Both Frontend and Object nodes compute positions the same way:

```rust
fn oid_position(oid: &Oid) -> u64 {
    // OIDs are SHA-256 hashes—use first 8 bytes directly
    u64::from_le_bytes(oid.as_bytes()[..8].try_into().unwrap())
}
```

This must match exactly. If Frontend thinks OID X belongs to Node A but Node A computes a different position, the object won't be found.

### Batched Parallel Fetches

When Frontend needs many objects (e.g., assembling a pack for a clone), it groups OIDs by destination node:

```
Input: 10 million OIDs

Step 1: Partition by owner
  for each oid:
      position = oid_position(oid)
      node = ring.find_owner(position)  // walk to next vnode
      by_node[node].push(oid)

Result:
  Node A: ~1M OIDs
  Node B: ~1M OIDs
  ...
  Node J: ~1M OIDs

Step 2: Parallel gRPC streams
  futures = []
  for (node, oids) in by_node:
      futures.push(node.get_blobs_streaming(oids))

  results = join_all(futures)  // 10 concurrent streams
```

This turns 10M individual requests into ~10 parallel streams. Each stream uses gRPC server-side streaming—objects flow back as they're fetched.

### Load Balancing Reads

Each object exists on 3 replicas. Frontend can choose which replica to read from:

```
OID X has replicas: [Node B (primary), Node A, Node C]

Option 1: Always read from primary
  - Simple, consistent
  - Primary becomes hot spot for popular repos

Option 2: Round-robin across replicas
  - Spreads load evenly
  - 3x read throughput

Option 3: Least-loaded replica
  - Track pending requests per node
  - Route to node with fewest in-flight requests
  - Best for uneven workloads
```

For batch fetches, Frontend can distribute OIDs across replicas:

```
10M OIDs, each has 3 replicas

Naive: send all to primaries
  Node A: 3M reads (hot)
  Node B: 4M reads (hotter)
  Node C: 3M reads (hot)

Smart: balance across replicas
  Node A: ~3.3M reads
  Node B: ~3.3M reads
  Node C: ~3.3M reads

Even distribution, no hot spots.
```

### Write Path

Writes go to all replicas, wait for quorum:

```
put_blob(blob):
    replicas = ring.nodes_for_oid(blob.oid)  // [B, A, C]

    // Write to all in parallel
    results = parallel_write(replicas, blob)

    // Count successes
    success_count = results.filter(ok).count()

    // Quorum: majority must succeed
    if success_count >= 2:
        return Ok(())
    else:
        return Err(QuorumFailed)
```

The third replica catches up via repair if it missed the write.

### Consistency with Ring Changes

When the ring changes (node added/removed), Frontend and Object nodes must agree on the new topology. This is coordinated via Control Plane:

1. Control Plane updates ring, increments version
2. All nodes watch for changes, update local cache
3. Brief inconsistency window during propagation
4. Reads may hit wrong node, get forwarded or retry
5. Writes to old primary still reach replicas (quorum handles it)

The system tolerates brief inconsistency because objects are immutable—there's no "wrong version" to read.

## Compression

All objects are compressed before storage using zlib (the same algorithm Git uses internally). This happens transparently—callers work with uncompressed data.

Git objects compress well. Source code is repetitive text. Even binary assets often have patterns. Compression typically saves 50-70% of storage space.

## The Pack Cache

The most important optimization is caching assembled pack files.

When a Git client clones or fetches, the server doesn't send individual objects. It assembles them into a **pack file**—a single binary blob containing all the requested objects, often delta-compressed against each other.

Assembling a pack is expensive: find all needed objects, compute deltas, write the pack format. But here's the key insight: CI/CD runners clone the same thing over and over. A hundred runners cloning `main` at depth 1 all need the exact same pack file.

So we cache it. The first clone does the work. Every subsequent clone just streams the cached result.

```
Clone request: repo=foo, ref=main, depth=1
    ↓
Check cache: key = (foo, main@abc123, depth=1)
    ↓
Cache hit? → Stream cached pack (fast)
Cache miss? → Build pack, cache it, stream it
```

For CI/CD workloads, cache hit rates exceed 95%. The hundredth clone of the same commit costs almost nothing.

## Delta Storage

Git already uses **delta compression** in pack files: instead of storing two similar files separately, store one file plus the differences (delta) to create the other.

The Object cluster can do the same in storage. When a new object is similar to an existing one, we compute and store the delta instead of the full object. Reading requires fetching the base object and applying the delta, but the storage savings can be significant.

This runs in the background and is optional. Choosing a good base object matters—a bad choice produces a delta larger than just storing the object.

## Garbage Collection

Objects become unreferenced when branches are deleted or history is rewritten. Garbage collection reclaims this space.

The algorithm is **mark-and-sweep**:

1. **Mark**: Start from known roots (branch tips, tags) and walk the object graph, marking everything reachable. A commit references a tree, a tree references blobs and other trees, etc.

2. **Sweep**: Delete everything not marked.

For BucketStore, "delete" means flagging entries as dead. The actual space in data files is reclaimed during **compaction**—a background process that rewrites data files, copying only live entries and discarding dead ones.

## Why This Design

The design makes specific tradeoffs for CI/CD workloads:

- **Optimized for reads.** Writes happen (pushes), but reads (clones) happen far more often.
- **Caching everywhere.** Bucket cache, pack cache, OS page cache. Repeated access is cheap.
- **Eventual consistency is fine.** A replica being slightly behind doesn't break anything. Objects are immutable—once written, they're the same forever.
- **Simple operations.** Get, put, delete, iterate. No transactions, no queries, no joins.

A general-purpose database would work but would spend resources on features Git storage doesn't need. BucketStore is faster because it does less.
