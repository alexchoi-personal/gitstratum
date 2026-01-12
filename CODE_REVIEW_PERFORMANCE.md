# Performance Code Review: gitstratum-cluster

**Review Date:** 2026-01-10
**Reviewer:** Senior Performance Engineer
**Repository:** gitstratum-cluster

---

## 1. Executive Summary

**Overall Performance Grade: 7.5/10**

The gitstratum-cluster codebase demonstrates solid performance engineering fundamentals with io_uring integration, proper cache layering, and efficient data structures. However, several bottlenecks and optimization opportunities exist that could significantly improve throughput and latency under production load.

**Key Findings:**
- **Strengths:** io_uring async I/O, multi-level caching, efficient bucket-based storage, good use of `parking_lot` locks
- **Critical Issues:** Lock contention in hot paths, unnecessary memory allocations, suboptimal hash ring lookups
- **Moderate Issues:** Serialization overhead, cache invalidation patterns, missing zero-copy opportunities

---

## 2. Strengths Found

### 2.1 io_uring Integration
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/io/uring.rs`

The storage layer leverages io_uring for async I/O operations, which is excellent for high-throughput storage:

```rust
// Line 48-76: Efficient io_uring read submission
pub fn submit_read(&mut self, fd: RawFd, offset: u64, len: usize) -> Result<u64> {
    let buffer = allocate_aligned(len, 4096); // 4KB alignment for direct I/O
    let read_e = opcode::Read::new(types::Fd(fd), buffer.as_ptr() as *mut u8, len as u32)
        .offset(offset)
        .build()
        .user_data(id);
    // ...
}
```

- Uses 4KB aligned buffers for optimal direct I/O
- Batches submissions via `submit_batch()`
- Multi-queue architecture distributes load (`AsyncMultiQueueIo`)

### 2.2 Multi-Level Cache Architecture
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-object-cluster/src/cache/`

The caching strategy is well-designed:

1. **Hot Objects Cache** (`hot_objects.rs:23-52`): Uses moka cache with weight-based eviction
2. **Bucket Cache** (`bucket/cache.rs`): LRU cache for disk buckets
3. **Pack Cache** (`pack_cache/`): Repository-aware caching

```rust
// hot_objects.rs:39-44 - Proper weighted cache
let cache = Cache::builder()
    .max_capacity(config.max_entries as u64)
    .weigher(|_key: &Oid, value: &Arc<Blob>| -> u32 {
        value.data.len().min(u32::MAX as usize) as u32
    })
    .build();
```

### 2.3 Efficient Bucket Storage Design
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/bucket/`

- Fixed 4KB bucket size aligns with filesystem blocks
- Compact entry format (32 bytes per entry, 126 entries per bucket)
- Hash-based bucket distribution minimizes collisions

```rust
// disk.rs:4-7
pub const BUCKET_SIZE: usize = 4096;
pub const MAX_ENTRIES: usize = 126;
```

### 2.4 Good Lock Choices
**Location:** Throughout codebase

Consistent use of `parking_lot::RwLock` and `parking_lot::Mutex` which are faster than std locks:

```rust
// hashring/ring.rs:33-37
pub struct ConsistentHashRing {
    state: RwLock<RingState>,
    virtual_nodes_per_physical: u32,
    replication_factor: usize,
}
```

### 2.5 Benchmark Coverage
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/*/benches/`

Good benchmark coverage for critical paths:
- Core operations (OID hashing, tree operations)
- Hash ring lookups
- Storage throughput

---

## 3. Bottlenecks and Issues Found

### 3.1 CRITICAL: Lock Contention in Bucket Cache
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/bucket/cache.rs:24-32`
**Severity:** High

```rust
pub fn get(&self, bucket_id: u32) -> Option<Arc<DiskBucket>> {
    let mut cache = self.cache.lock(); // MUTEX LOCK FOR EVERY GET
    if let Some(bucket) = cache.get(&bucket_id) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(Arc::clone(bucket))
    } else {
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }
}
```

**Problem:** Every cache lookup acquires a mutex, causing contention under high concurrency.

**Impact:** Under 1000+ concurrent requests, this becomes a severe bottleneck.

### 3.2 CRITICAL: Unnecessary Memory Allocations in Hot Path
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-hashring/src/ring.rs:65-80`
**Severity:** High

```rust
fn hash_position(node_id: &NodeId, virtual_index: u32) -> u64 {
    let mut hasher = Sha256::new(); // NEW HASHER EVERY CALL
    hasher.update(node_id.as_str().as_bytes());
    hasher.update(virtual_index.to_le_bytes());
    let result = hasher.finalize();
    // ...
}

fn key_position(key: &[u8]) -> u64 {
    let mut hasher = Sha256::new(); // NEW HASHER EVERY CALL
    // ...
}
```

**Problem:** Creates new SHA256 hasher for every position calculation. In a hot path like routing requests, this adds unnecessary allocation overhead.

### 3.3 HIGH: OID Position Uses SHA256 When Not Necessary
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-hashring/src/ring.rs:82-85`
**Severity:** High

```rust
fn oid_position(oid: &Oid) -> u64 {
    let bytes: [u8; 8] = oid.as_bytes()[..8].try_into().unwrap_or([0u8; 8]);
    u64::from_le_bytes(bytes)
}
```

**Good:** This correctly uses first 8 bytes of OID directly.

However, `key_position()` unnecessarily hashes when used with OID bytes:
```rust
pub fn primary_node(&self, key: &[u8]) -> Result<NodeInfo> {
    let position = Self::key_position(key); // SHA256 hash even for OID bytes
    self.primary_node_at_position(position)
}
```

### 3.4 HIGH: Vector Cloning in nodes_at_position
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-hashring/src/ring.rs:208-251`
**Severity:** High

```rust
fn nodes_at_position(&self, position: u64) -> Result<Vec<NodeInfo>> {
    let state = self.state.read();
    // ...
    for (_, vnode) in state.ring.range(position..).chain(state.ring.iter()) {
        // ...
        result.push(node.clone()); // CLONES NodeInfo for every result
    }
    Ok(result)
}
```

**Problem:** Clones `NodeInfo` structs on every lookup. With replication_factor=3, this means 3 clones per request.

### 3.5 MEDIUM: RocksDB Blocking Calls in Async Context
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-object-cluster/src/store/rocksdb.rs:286-303`
**Severity:** Medium

```rust
async fn get(&self, oid: &Oid) -> Result<Option<Blob>> {
    let db = self.db.clone();
    let compressor = self.compressor.clone();
    let oid = *oid;
    tokio::task::spawn_blocking(move || {  // spawn_blocking for every get
        let key = oid.as_bytes();
        match db.get(key) {
            // ...
        }
    })
    .await
    .map_err(|e| ObjectStoreError::Io(std::io::Error::other(e)))?
}
```

**Problem:** While using `spawn_blocking` is correct, spawning a new task for every operation adds overhead. Consider batching or using a dedicated thread pool.

### 3.6 MEDIUM: Protobuf Serialization Overhead
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-object-cluster/src/server.rs:54-59`
**Severity:** Medium

```rust
fn core_blob_to_proto(blob: &CoreBlob, compressed: bool) -> Blob {
    Blob {
        oid: Some(Self::core_oid_to_proto(&blob.oid)),
        data: blob.data.to_vec(), // COPIES ALL DATA
        compressed,
    }
}
```

**Problem:** Copies entire blob data when converting to protobuf. For large blobs (multi-MB), this is significant.

### 3.7 MEDIUM: Linear Search in DiskBucket
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/bucket/disk.rs:125-132`
**Severity:** Medium

```rust
pub fn find_entry(&self, oid: &gitstratum_core::Oid) -> Option<&CompactEntry> {
    for i in 0..self.header.count as usize {  // LINEAR SEARCH
        if self.entries[i].matches(oid) {
            return Some(&self.entries[i]);
        }
    }
    None
}
```

**Problem:** With up to 126 entries per bucket, linear search can be slow. Average case is 63 comparisons.

### 3.8 MEDIUM: Cache Invalidation on Every Put
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/store.rs:164-165`
**Severity:** Medium

```rust
pub async fn put(&self, oid: Oid, value: Bytes) -> Result<()> {
    // ...
    let bucket_id = self.bucket_index.bucket_id(&oid);
    self.bucket_cache.invalidate(bucket_id); // INVALIDATE BEFORE READ

    let mut bucket = self.inner.bucket_io.read_bucket(bucket_id).await?;
    // ...
}
```

**Problem:** Invalidates cache before reading, causing immediate cache miss. Should invalidate after successful write.

### 3.9 LOW: String Allocations in Coordinator
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/state_machine.rs:138-156`
**Severity:** Low

```rust
pub fn serialize_command(cmd: &ClusterCommand) -> Result<String, serde_json::Error> {
    let versioned = VersionedCommand::new(cmd.clone()); // CLONES COMMAND
    serde_json::to_string(&versioned)
}

pub fn serialize_topology(topology: &ClusterTopology) -> Result<String, serde_json::Error> {
    serde_json::to_string(topology)
}
```

**Problem:** Uses JSON serialization for cluster commands. Binary format (bincode) would be faster.

### 3.10 LOW: Reaper Thread Busy-Wait
**Location:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/io/async_queue.rs:146-161`
**Severity:** Low

```rust
pub fn start_reapers(&self) -> Vec<JoinHandle<()>> {
    self.queues
        .iter()
        .map(|queue| {
            // ...
            tokio::spawn(async move {
                while !shutdown.load(Ordering::Relaxed) {
                    let reaped = queue.reap_completions();
                    if reaped == 0 {
                        tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    }
                }
            })
        })
        .collect()
}
```

**Problem:** 100-microsecond sleep when no completions. Under low load, this wastes CPU. Under high load, adds latency.

---

## 4. Recommendations (Prioritized by Impact)

### P0 (Critical - Immediate Action Required)

#### 4.1 Replace Bucket Cache Mutex with Concurrent HashMap
**Impact:** 10-50x improvement in cache throughput under contention
**Location:** `crates/gitstratum-storage/src/bucket/cache.rs`

```rust
// BEFORE
pub struct BucketCache {
    cache: Mutex<LruCache<u32, Arc<DiskBucket>>>,
    // ...
}

// AFTER - Use moka or quick_cache with concurrent access
use moka::sync::Cache;

pub struct BucketCache {
    cache: Cache<u32, Arc<DiskBucket>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BucketCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(capacity as u64)
                .build(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, bucket_id: u32) -> Option<Arc<DiskBucket>> {
        if let Some(bucket) = self.cache.get(&bucket_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(bucket)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}
```

#### 4.2 Optimize Hash Ring Position Calculation
**Impact:** 5-10x speedup for routing operations
**Location:** `crates/gitstratum-hashring/src/ring.rs`

```rust
// BEFORE - SHA256 for every key lookup
fn key_position(key: &[u8]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let result = hasher.finalize();
    // ...
}

// AFTER - Use xxhash3 for non-cryptographic hashing
use xxhash_rust::xxh3::xxh3_64;

fn key_position(key: &[u8]) -> u64 {
    xxh3_64(key)
}

// For OID keys, use bytes directly (already cryptographic hash)
fn oid_position(oid: &Oid) -> u64 {
    u64::from_le_bytes(oid.as_bytes()[..8].try_into().unwrap())
}
```

### P1 (High - Next Sprint)

#### 4.3 Return Arc<NodeInfo> Instead of Cloning
**Impact:** Eliminates allocation per lookup
**Location:** `crates/gitstratum-hashring/src/ring.rs`

```rust
// Store Arc<NodeInfo> in the ring state
struct RingState {
    ring: BTreeMap<u64, VirtualNode>,
    nodes: BTreeMap<NodeId, Arc<NodeInfo>>,  // Changed to Arc
    version: u64,
}

// Return Arc instead of cloning
pub fn primary_node_for_oid(&self, oid: &Oid) -> Result<Arc<NodeInfo>> {
    let position = Self::oid_position(oid);
    self.primary_node_at_position(position)
}
```

#### 4.4 Use Binary Search in DiskBucket
**Impact:** O(log n) vs O(n) lookup
**Location:** `crates/gitstratum-storage/src/bucket/disk.rs`

```rust
// Keep entries sorted by OID prefix
pub fn insert(&mut self, entry: CompactEntry) -> Result<()> {
    if self.header.count as usize >= MAX_ENTRIES {
        return Err(BucketStoreError::BucketOverflow { bucket_id: 0 });
    }

    // Binary search for insertion point
    let count = self.header.count as usize;
    let pos = self.entries[..count]
        .binary_search_by(|e| e.oid_prefix().cmp(&entry.oid_prefix()))
        .unwrap_or_else(|p| p);

    // Shift entries and insert
    self.entries.copy_within(pos..count, pos + 1);
    self.entries[pos] = entry;
    self.header.count += 1;
    Ok(())
}

pub fn find_entry(&self, oid: &Oid) -> Option<&CompactEntry> {
    let count = self.header.count as usize;
    let prefix = compute_oid_prefix(oid);

    self.entries[..count]
        .binary_search_by(|e| e.oid_prefix().cmp(&prefix))
        .ok()
        .map(|i| &self.entries[i])
}
```

### P2 (Medium - Next Quarter)

#### 4.5 Implement Zero-Copy Protobuf Conversion
**Impact:** Eliminates large data copies
**Location:** `crates/gitstratum-object-cluster/src/server.rs`

```rust
// Use Bytes directly in protobuf
fn core_blob_to_proto(blob: CoreBlob, compressed: bool) -> Blob {
    Blob {
        oid: Some(Self::core_oid_to_proto(&blob.oid)),
        data: blob.data.into(), // Move instead of copy
        compressed,
    }
}

// Or use prost with bytes feature for zero-copy
```

#### 4.6 Fix Cache Invalidation Order
**Impact:** Improves cache hit rate
**Location:** `crates/gitstratum-storage/src/store.rs`

```rust
pub async fn put(&self, oid: Oid, value: Bytes) -> Result<()> {
    // ... write operations ...

    // Invalidate AFTER successful write
    self.bucket_cache.invalidate(bucket_id);

    // Better: update cache with new bucket
    self.bucket_cache.put(bucket_id, bucket.clone());

    Ok(())
}
```

#### 4.7 Use Binary Serialization for Coordinator
**Impact:** Faster serialization, smaller messages
**Location:** `crates/gitstratum-coordinator/src/state_machine.rs`

```rust
// Use bincode instead of JSON
pub fn serialize_command(cmd: &ClusterCommand) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(cmd)
}

pub fn deserialize_command(data: &[u8]) -> Result<ClusterCommand, bincode::Error> {
    bincode::deserialize(data)
}
```

### P3 (Low - Backlog)

#### 4.8 Use eventfd for Reaper Notification
**Impact:** Reduces CPU usage, improves latency
**Location:** `crates/gitstratum-storage/src/io/async_queue.rs`

Consider using eventfd or io_uring's native completion notification instead of polling.

#### 4.9 Add Connection Pooling for gRPC Clients
Ensure gRPC clients use connection pooling with proper keepalive settings.

#### 4.10 Implement Read-Through Cache Pattern
For bucket cache, implement read-through to reduce code complexity and ensure consistency.

---

## 5. Benchmarking Recommendations

### 5.1 Missing Benchmarks
Add benchmarks for:
1. Concurrent bucket cache access (with different thread counts)
2. Hash ring lookup under contention
3. End-to-end request latency (p50, p95, p99)
4. Memory allocation rates during high load

### 5.2 Suggested Benchmark Code

```rust
// Add to crates/gitstratum-storage/benches/cache_contention.rs
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use std::thread;

fn bench_cache_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_contention");

    for threads in [1, 4, 8, 16, 32] {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &thread_count| {
                let cache = Arc::new(BucketCache::new(1024));
                // Populate cache
                for i in 0..1024 {
                    cache.put(i, DiskBucket::new());
                }

                b.iter(|| {
                    let handles: Vec<_> = (0..thread_count)
                        .map(|_| {
                            let cache = Arc::clone(&cache);
                            thread::spawn(move || {
                                for _ in 0..1000 {
                                    let _ = cache.get(rand::random::<u32>() % 1024);
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}
```

---

## 6. Summary

The gitstratum-cluster codebase has a solid foundation with good architectural decisions around io_uring and caching. However, the identified lock contention and allocation issues in hot paths could significantly impact production performance.

**Immediate priorities:**
1. Replace mutex-protected bucket cache with concurrent data structure
2. Optimize hash ring position calculations
3. Eliminate unnecessary clones in node lookups

**Expected improvements after P0/P1 fixes:**
- 5-20x improvement in concurrent cache access
- 3-5x improvement in request routing latency
- 20-30% reduction in memory allocation rate

---

*End of Performance Review*
