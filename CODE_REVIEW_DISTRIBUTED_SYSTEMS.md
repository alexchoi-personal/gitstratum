# Distributed Systems Code Review: gitstratum-cluster

**Reviewer**: Senior Distributed Systems Engineer
**Date**: 2026-01-10
**Focus Areas**: Consensus, Cluster Membership, Replication, Partition Tolerance, Failure Handling

---

## Executive Summary

**Overall Grade: 7.0/10**

The gitstratum-cluster codebase demonstrates a solid foundation for distributed systems with well-designed components for cluster coordination, consistent hashing, and quorum-based writes. The architecture properly separates concerns between the coordinator (Raft-based membership), object storage (consistent hash ring), and metadata storage (partition-based routing).

However, several critical distributed systems concerns need attention before production deployment:

1. **No actual Raft implementation visible** - The coordinator relies on external `k8s-operator` Raft
2. **Missing split-brain prevention mechanisms** beyond generation IDs
3. **Incomplete network partition handling** in replication layer
4. **No read consistency guarantees** documented or enforced
5. **Simulated write operations** in quorum writer (placeholder code)

---

## Strengths

### 1. Well-Designed Consistent Hash Ring
**File**: `crates/gitstratum-hashring/src/ring.rs`

The consistent hash ring implementation is clean and correct:

```rust
// Lines 215-251: Proper node selection respecting state
for (_, vnode) in state.ring.range(position..).chain(state.ring.iter()) {
    if !seen.insert(&vnode.node_id) {
        continue;
    }
    let Some(node) = state.nodes.get(&vnode.node_id) else {
        continue;
    };
    if !node.state.can_serve_reads() {
        continue;
    }
    result.push(node.clone());
    if result.len() >= self.replication_factor {
        break;
    }
}
```

**Strengths**:
- Virtual nodes prevent hotspots (configurable via `virtual_nodes_per_physical`)
- Node state filtering ensures only healthy nodes are selected
- Proper wraparound for ring traversal
- SHA-256 hashing for uniform distribution

### 2. Heartbeat Batching for Raft Write Reduction
**File**: `crates/gitstratum-coordinator/src/heartbeat_batcher.rs:111-206`

Excellent optimization to reduce Raft consensus overhead:

```rust
pub async fn run_heartbeat_flush_loop<F, Fut, L, E>(
    batcher: Arc<HeartbeatBatcher>,
    flush_fn: F,
    is_leader_fn: L,
    mut shutdown_rx: watch::Receiver<bool>,
)
```

This reduces O(n) Raft writes per heartbeat interval to O(1), critical for clusters with many nodes.

### 3. Generation ID Zombie Node Detection
**File**: `crates/gitstratum-coordinator/src/state_machine.rs:20-43`

Proper handling of zombie nodes that may rejoin after failure:

```rust
pub fn validate_generation_id(
    topology: &ClusterTopology,
    node_id: &str,
    generation_id: &str,
) -> Result<(), String> {
    match node {
        None => Ok(()),
        Some(entry) => {
            if entry.generation_id == generation_id || entry.state == NODE_STATE_DOWN {
                Ok(())
            } else {
                Err(format!(
                    "Generation ID mismatch for node {}: expected {}, got {}",
                    node_id, entry.generation_id, generation_id
                ))
            }
        }
    }
}
```

### 4. Flap Damping in Failure Detection
**File**: `crates/gitstratum-coordinator/src/server.rs:272-291`

Smart exponential backoff for flapping nodes:

```rust
pub fn get_timeout_for_node(&self, node_id: &str) -> (Duration, Duration) {
    if let Some(flap) = flap_info.get(node_id) {
        let now = Instant::now();
        if flap.suspect_count >= self.config.flap_threshold
            && now.duration_since(flap.last_suspect_at) < self.config.flap_window
        {
            let multiplier = self.config.flap_multiplier;
            return (
                base_suspect.mul_f32(multiplier),
                base_down.mul_f32(multiplier),
            );
        }
    }
    (base_suspect, base_down)
}
```

### 5. Leader Grace Period After Election
**File**: `crates/gitstratum-coordinator/src/server.rs:312-314`

Prevents false positives during leader failover:

```rust
let in_grace_period =
    now.duration_since(leader_start) < self.config.leader_grace_period;
```

### 6. Circuit Breaker Pattern
**File**: `crates/gitstratum-object-cluster/src/client.rs:47-94`

Proper circuit breaker implementation for node failures:

```rust
fn is_open(&self) -> bool {
    let failures = self.failure_count.load(Ordering::Relaxed);
    if failures < self.threshold {
        return false;
    }
    let last_failure = self.last_failure_epoch_secs.load(Ordering::Relaxed);
    let now_secs = self.start_time.elapsed().as_secs();
    let elapsed = now_secs.saturating_sub(last_failure);
    if elapsed >= self.recovery_secs {
        self.failure_count.store(0, Ordering::Relaxed);
        return false;
    }
    true
}
```

---

## Issues and Risks

### CRITICAL

#### C1. Simulated Write Operations in QuorumWriter
**File**: `crates/gitstratum-object-cluster/src/replication/writer.rs:408-410`

```rust
async fn simulate_write_to_node(&self, _node: &NodeClient, _oid: &Oid, _data: &[u8]) -> bool {
    true
}
```

**Risk**: This is placeholder code - actual writes always succeed. In production, network failures, timeouts, and node failures would not be detected.

**Impact**: Data loss risk - system believes data is replicated when it may not be.

#### C2. Missing Quorum Read Implementation
**File**: `crates/gitstratum-object-cluster/src/client.rs:218-251`

The `get` operation reads from nodes sequentially until finding data:

```rust
for node in &nodes {
    if !self.is_node_available(node) {
        continue;
    }
    match self.get_from_node(node, oid).await {
        Ok(Some(blob)) => {
            self.record_node_success(node);
            return Ok(Some(blob));
        }
```

**Risk**: No read quorum means stale reads are possible. After a partial write (quorum achieved but not all replicas), a read might hit an old replica.

**Impact**: Read-after-write consistency not guaranteed.

#### C3. No Explicit Split-Brain Prevention
**Files**: Multiple coordinator files

While generation IDs help detect zombie nodes, there's no fencing mechanism to prevent a partitioned leader from accepting writes. The system relies entirely on the external Raft implementation (`k8s-operator`) for this.

**Risk**: If the Raft implementation has bugs or the integration is incorrect, split-brain scenarios are possible.

**Impact**: Data corruption, inconsistent cluster state.

### HIGH

#### H1. Partition Router Lock Contention
**File**: `crates/gitstratum-metadata-cluster/src/partition/router.rs:100-118`

```rust
pub fn assign_primary(&self, partition_id: PartitionId, node_id: NodeId) {
    let mut partitions = self.partitions.write();
    if let Some(partition) = partitions.get_mut(&partition_id) {
        if let Some(ref old_primary) = partition.primary_node {
            let mut node_partitions = self.node_partitions.write();
            // ... nested write lock
        }
        partition.primary_node = Some(node_id.clone());
        let mut node_partitions = self.node_partitions.write();
        // ... second write lock acquisition
    }
}
```

**Risk**: Nested RwLock acquisitions can cause deadlocks if acquired in different order elsewhere.

**Impact**: System hangs under concurrent partition reassignment.

#### H2. RebalancePlanner RemoveReplica Bug
**File**: `crates/gitstratum-metadata-cluster/src/partition/rebalance.rs:186-189`

```rust
RebalanceAction::RemoveReplica {
    partition_id,
    node_id,
} => {
    if let Some(mut partition) = router.get_partition(*partition_id) {
        partition.replica_nodes.retain(|n| n != node_id);
    }
}
```

**Risk**: `get_partition` returns a cloned `Partition`, not a mutable reference. The `retain` call modifies the clone, not the actual partition in the router.

**Impact**: Replica removal silently fails - the system thinks replicas are removed but they persist.

#### H3. Hash Ring Insufficient Nodes Error vs Graceful Degradation
**File**: `crates/gitstratum-hashring/src/ring.rs:238-248`

```rust
if result.len() < self.replication_factor {
    let active_count = state.nodes.values().filter(|n| n.state.can_serve_reads()).count();
    return Err(HashRingError::InsufficientNodes(
        self.replication_factor,
        active_count,
    ));
}
```

**Risk**: If fewer nodes are available than the replication factor, all operations fail even though some replication is possible.

**Impact**: Reduced availability during partial failures. A 3-node cluster with 1 failure becomes completely unavailable if replication factor is 3.

#### H4. No Consistency Level Configuration
**Files**: `crates/gitstratum-object-cluster/src/replication/writer.rs`, `crates/gitstratum-object-cluster/src/client.rs`

The system hardcodes quorum writes (`min_success: 2` for RF=3) but has no read consistency level:

```rust
impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            min_success: 2,  // Write quorum
            timeout_ms: 5000,
        }
    }
}
```

**Risk**: Users cannot tune consistency vs. availability tradeoff.

**Impact**: Cannot achieve linearizable reads or tune for higher availability.

### MEDIUM

#### M1. Atomic Counter Memory Ordering Issues
**File**: `crates/gitstratum-object-cluster/src/replication/reader.rs:68-79`

```rust
pub fn record_read_result(&self, success: bool, was_local: bool) {
    self.reads_attempted.fetch_add(1, Ordering::Relaxed);
    if success {
        self.reads_succeeded.fetch_add(1, Ordering::Relaxed);
        if was_local {
            self.local_reads.fetch_add(1, Ordering::Relaxed);
        }
    } else {
        self.reads_failed.fetch_add(1, Ordering::Relaxed);
    }
}
```

**Risk**: `Ordering::Relaxed` means counters may appear inconsistent when read from different threads. `reads_attempted` might be less than `reads_succeeded + reads_failed`.

**Impact**: Misleading metrics, debugging difficulty.

#### M2. Downtime Tracker File-Based Persistence
**File**: `crates/gitstratum-object-cluster/src/repair/downtime.rs:60-65`

```rust
pub fn save(&self, timestamp: u64) -> Result<()> {
    let path = self.data_dir.join(HEALTHY_TIMESTAMP_FILE);
    std::fs::create_dir_all(&self.data_dir)?;
    std::fs::write(&path, timestamp.to_string())?;
    Ok(())
}
```

**Risk**: Non-atomic file write. Power failure during write could corrupt the file.

**Impact**: Node may not correctly identify downtime window after crash during timestamp write.

#### M3. Missing Read Repair
**File**: `crates/gitstratum-object-cluster/src/client.rs`

When a read finds data on one node but not others, no repair is triggered:

```rust
Ok(Some(blob)) => {
    self.record_node_success(node);
    return Ok(Some(blob));  // No check if other replicas have it
}
```

**Risk**: Replicas can drift apart over time without anti-entropy repair.

**Impact**: Degraded durability, increased data loss risk during subsequent failures.

#### M4. Replication Manager Select Replica Nodes Determinism Issue
**File**: `crates/gitstratum-metadata-cluster/src/store/replication.rs:71-75`

```rust
pub fn select_replica_nodes(&self, _repo_id: &RepoId) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = self.peer_nodes.iter().cloned().collect();
    nodes.truncate(self.config.factor.saturating_sub(1));
    nodes
}
```

**Risk**: `HashSet::iter()` order is non-deterministic. Different calls may return different nodes for the same repo.

**Impact**: Inconsistent replica placement, complicating recovery and debugging.

#### M5. No Request Timeout in Async Operations
**File**: `crates/gitstratum-object-cluster/src/replication/writer.rs:322-406`

The `write` method has a `timeout_ms` config but doesn't actually enforce it:

```rust
pub async fn write(&self, oid: &Oid, data: &[u8]) -> Result<WriteResult> {
    let start = Instant::now();
    // ... no timeout enforcement
    for node in target_nodes.iter().take(self.quorum_size) {
        if self.simulate_write_to_node(node, oid, data).await {
```

**Risk**: Slow nodes can block indefinitely.

**Impact**: Cascading failures, reduced throughput under partial failure.

### LOW

#### L1. Version Overflow in Hash Ring
**File**: `crates/gitstratum-hashring/src/ring.rs:103`

```rust
state.version = state.version.wrapping_add(1);
```

Using `wrapping_add` means version can wrap to 0, potentially confusing version-based comparisons.

#### L2. Clone-Heavy Code Patterns
**Files**: Multiple

Frequent cloning of `NodeInfo`, `Partition`, etc. in hot paths:

```rust
// ring.rs:146-147
pub fn get_nodes(&self) -> Vec<NodeInfo> {
    self.state.read().nodes.values().cloned().collect()
}
```

Could impact performance at scale.

---

## Recommendations (Prioritized by Reliability Impact)

### Priority 1: Critical Fixes

1. **Implement actual network writes in QuorumWriter**
   - Replace `simulate_write_to_node` with real gRPC calls
   - Add proper timeout handling using `tokio::time::timeout`
   - Implement retry logic with exponential backoff

2. **Add quorum reads for consistency**
   - Implement read quorum (read from R nodes where R + W > N)
   - Add configurable consistency levels (ONE, QUORUM, ALL)
   - Consider read repair on inconsistent reads

3. **Add explicit fencing tokens**
   - Include Raft term/epoch in all write operations
   - Reject writes with stale fencing tokens
   - Document consistency guarantees

### Priority 2: High Impact Fixes

4. **Fix RebalancePlanner RemoveReplica**
   ```rust
   // Instead of cloning, use interior mutability or refactor
   pub fn remove_replica(&self, partition_id: PartitionId, node_id: &str) {
       let mut partitions = self.partitions.write();
       if let Some(partition) = partitions.get_mut(&partition_id) {
           partition.replica_nodes.retain(|n| n != node_id);
       }
   }
   ```

5. **Implement graceful degradation for insufficient replicas**
   ```rust
   // Return available nodes even if below replication factor
   pub fn nodes_for_oid_degraded(&self, oid: &Oid) -> Result<(Vec<NodeInfo>, bool)> {
       let nodes = self.nodes_at_position_best_effort(position);
       let degraded = nodes.len() < self.replication_factor;
       Ok((nodes, degraded))
   }
   ```

6. **Fix nested lock acquisition pattern**
   - Use a single lock for partition state
   - Or use `parking_lot::RwLock` which is reentrant

### Priority 3: Medium Impact Improvements

7. **Implement read repair**
   ```rust
   async fn get_with_repair(&self, oid: &Oid) -> Result<Option<CoreBlob>> {
       let nodes = self.ring.nodes_for_oid(oid)?;
       let results = self.parallel_get(&nodes, oid).await;
       if let Some(blob) = results.first_success() {
           self.repair_missing_replicas(&nodes, &results, &blob).await;
           return Ok(Some(blob));
       }
       Ok(None)
   }
   ```

8. **Use atomic file writes for downtime tracker**
   ```rust
   pub fn save(&self, timestamp: u64) -> Result<()> {
       let path = self.data_dir.join(HEALTHY_TIMESTAMP_FILE);
       let tmp_path = path.with_extension("tmp");
       std::fs::write(&tmp_path, timestamp.to_string())?;
       std::fs::rename(&tmp_path, &path)?;  // Atomic on POSIX
       Ok(())
   }
   ```

9. **Add request timeouts**
   ```rust
   let result = tokio::time::timeout(
       Duration::from_millis(self.config.timeout_ms),
       self.write_to_node(node, oid, data)
   ).await??;
   ```

### Priority 4: Low Impact / Technical Debt

10. **Fix memory ordering for metrics counters**
    - Use `Ordering::AcqRel` for updates
    - Use `Ordering::Acquire` for reads in `stats()`

11. **Add determinism to replica selection**
    - Use `BTreeSet` instead of `HashSet` for peer nodes
    - Or sort before selection

12. **Consider Arc<T> for shared NodeInfo to reduce cloning**

---

## Consistency Guarantees Summary

| Operation | Current Guarantee | Recommended |
|-----------|------------------|-------------|
| Write | Quorum (W=2 of 3) | Configurable W |
| Read | None (first success) | Configurable R |
| Cluster membership | Linearizable (Raft) | No change |
| Metadata routing | Eventual | Document clearly |
| Hash ring updates | Sequential (leader only) | No change |

---

## Testing Gaps Identified

1. **No chaos/fault injection tests** for network partitions
2. **No tests for partial write scenarios** (quorum achieved, some replicas fail)
3. **No tests for split-brain recovery**
4. **No tests for clock skew** scenarios
5. **No tests for slow/hung nodes** (only connection failures)

---

## Conclusion

The codebase shows good distributed systems fundamentals but has several gaps that would cause issues in production:

- The quorum write system is currently a stub
- Read consistency is not implemented
- Some critical bugs in the rebalancing code

With the recommended fixes, particularly implementing real network operations and adding read consistency options, this system could be suitable for production use. The architecture is sound, and the failure detection mechanisms are well thought out.

**Estimated effort to reach production readiness**: 2-3 weeks for critical fixes, 4-6 weeks including high priority items.
