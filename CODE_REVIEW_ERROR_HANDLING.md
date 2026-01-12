# Error Handling Code Review

**Repository:** gitstratum-cluster
**Review Focus:** Error Handling Patterns
**Reviewer:** Senior Rust Engineer
**Date:** 2026-01-10

---

## Executive Summary

**Grade: 7.5/10**

The gitstratum-cluster project demonstrates solid error handling foundations with consistent use of `thiserror` for error type definitions and well-designed error hierarchies across crates. The codebase shows mature patterns in retry logic, graceful degradation, and gRPC error code mapping. However, there are concerning uses of `expect()` in production code paths and several instances where error context could be improved. The test code appropriately uses `unwrap()` and `panic!()`, but a few production code paths need attention.

---

## 1. Strengths Found

### 1.1 Consistent Error Type Design with thiserror

All crates consistently use `thiserror` for error definitions, providing clear, structured error hierarchies:

**Example - Well-designed error type (gitstratum-metadata-cluster/src/error.rs:5-60):**
```rust
#[derive(Error, Debug)]
pub enum MetadataStoreError {
    #[error("repository not found: {0}")]
    RepoNotFound(String),

    #[error("compare-and-swap failed: expected {expected}, found {found}")]
    CasFailed { expected: String, found: String },

    #[error("bincode error")]
    Bincode(#[source] bincode::Error),

    #[error("rocksdb error: {0}")]
    RocksDb(#[source] rocksdb::Error),
}
```

### 1.2 Comprehensive Error-to-gRPC Status Mapping

Error types properly map to appropriate gRPC status codes:

**Example - Proper gRPC status conversion (gitstratum-coordinator/src/error.rs:28-48):**
```rust
impl From<CoordinatorError> for tonic::Status {
    fn from(err: CoordinatorError) -> Self {
        match err {
            CoordinatorError::NotLeader(leader_addr) => {
                let mut status = tonic::Status::failed_precondition("Not the leader");
                if let Some(addr) = leader_addr {
                    if let Ok(value) = MetadataValue::try_from(&addr) {
                        status.metadata_mut().insert("leader-address", value);
                    }
                }
                status
            }
            CoordinatorError::NodeNotFound(_) => tonic::Status::not_found(err.to_string()),
            CoordinatorError::InvalidNodeType => tonic::Status::invalid_argument(err.to_string()),
            // ... comprehensive mapping
        }
    }
}
```

### 1.3 Robust Retry Logic with Exponential Backoff

The coordinator client implements production-grade retry mechanisms:

**Example - Retry configuration (gitstratum-coordinator/src/client.rs:68-99):**
```rust
#[derive(Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub timeout_per_attempt: Duration,
    pub jitter_fraction: f64,
}

impl RetryConfig {
    pub fn compute_backoff_with_jitter(&self, attempt: u32) -> Duration {
        let base = self.initial_backoff.saturating_mul(2u32.saturating_pow(attempt));
        let capped = base.min(self.max_backoff);
        let jitter_ms = (capped.as_millis() as f64 * self.jitter_fraction * rand_fraction()) as u64;
        capped + Duration::from_millis(jitter_ms)
    }
}
```

### 1.4 Type-Safe Result Aliases

Each crate defines type aliases for cleaner code:

```rust
pub type Result<T> = std::result::Result<T, MetadataStoreError>;
pub type Result<T> = std::result::Result<T, ObjectStoreError>;
pub type Result<T> = std::result::Result<T, FrontendError>;
```

### 1.5 Proper Error Source Chaining

Errors preserve source information for debugging:

```rust
#[error("io error: {0}")]
Io(#[from] std::io::Error),

#[error("hash ring error")]
HashRing(#[source] gitstratum_hashring::HashRingError),
```

### 1.6 Comprehensive Error Testing

Error types have thorough test coverage:

**Example (gitstratum-coordinator/src/error.rs:50-145):**
Tests verify error display messages, debug output, and status code conversions.

### 1.7 HTTP Status Code Mapping for LFS

**Example (gitstratum-lfs/src/error.rs:42-58):**
```rust
impl LfsError {
    pub fn status_code(&self) -> u16 {
        match self {
            LfsError::NotFound(_) => 404,
            LfsError::AlreadyExists(_) => 409,
            LfsError::RateLimitExceeded { .. } => 429,
            LfsError::ObjectTooLarge { .. } => 413,
            // ...
        }
    }
}
```

---

## 2. Issues Found

### 2.1 CRITICAL: `expect()` in Production Code Paths

Several files use `expect()` in production code, which can cause panics:

**File: gitstratum-metadata-cluster/src/auth/store.rs:40, 46**
```rust
fn cf_auth(&self) -> &ColumnFamily {
    self.db
        .cf_handle("auth")
        .expect("auth column family validated in constructor")
}
```
**Risk:** While constructor validation exists, database state could change. A corrupted DB or race condition could cause a panic.

**File: gitstratum-metadata-cluster/src/refs/storage.rs:37, 43, 49**
```rust
let head_ref = RefName::new("HEAD").expect("HEAD is a valid ref name");
let main_ref = RefName::new("refs/heads/main").expect("refs/heads/main is a valid ref name");
```
**Risk:** Low (these are compile-time constants), but pattern sets bad precedent.

**File: gitstratum-storage/src/store.rs:403**
```rust
fn compute_position(oid: &Oid) -> u64 {
    u64::from_le_bytes(
        oid.as_bytes()[..8]
            .try_into()
            .expect("Oid is always 32 bytes, first 8 bytes always valid"),
    )
}
```
**Risk:** Medium - relies on invariant that could be violated by future changes.

**File: gitstratum-object-cluster/src/store/rocksdb.rs:270**
```rust
fn oid_position(&self, oid: &Oid) -> u64 {
    u64::from_le_bytes(
        oid.as_bytes()[..8]
            .try_into()
            .expect("OID is always 32 bytes"),
    )
}
```
**Risk:** Same as above.

**File: gitstratum-storage/src/bucket/disk.rs:39-41, 70, 91, 99**
Multiple `expect()` calls in byte slice conversions:
```rust
magic: u32::from_le_bytes(bytes[0..4].try_into().expect("slice is exactly 4 bytes")),
```
**Risk:** Low for known-size slices, but a parsing error could cause panic instead of returning an error.

**File: gitstratum-frontend-cluster/src/protocol/pktline.rs:59**
```rust
write!(&mut len_buf[..], "{:04x}", len).expect("4 hex digits fit in 4 bytes");
```
**Risk:** Low - mathematically correct, but pattern inconsistent with other error handling.

**File: gitstratum-object-cluster/src/cache/hot_objects.rs:139**
```rust
impl Default for HotObjectsCache {
    fn default() -> Self {
        Self::new(HotObjectsCacheConfig::default()).expect("default config should always be valid")
    }
}
```
**Risk:** Low - default config is controlled, but violates "no panic in production" principle.

### 2.2 HIGH: Inconsistent Error Context Preservation

Some error conversions lose context:

**File: gitstratum-metadata-cluster/src/error.rs:74-77**
```rust
impl From<tonic::Status> for MetadataStoreError {
    fn from(err: tonic::Status) -> Self {
        MetadataStoreError::Grpc(err.message().to_string())  // Loses status code and metadata
    }
}
```
**Recommendation:** Preserve the full status or include code in message.

**File: gitstratum-object-cluster/src/error.rs:79-94**
```rust
impl From<ObjectStoreError> for tonic::Status {
    fn from(err: ObjectStoreError) -> Self {
        match err {
            // ...
            _ => tonic::Status::internal(err.to_string()),  // Many errors become generic Internal
        }
    }
}
```
**Recommendation:** Map more error types to specific gRPC codes.

### 2.3 MEDIUM: Missing Error Context in Some Areas

**File: gitstratum-coordinator/src/client.rs:392-395**
```rust
Err(CoordinatorError::Internal(format!(
    "Failed to register after {} attempts",
    self.retry_config.max_attempts
)))
```
**Issue:** Doesn't capture or log the actual errors that occurred during retries.

### 2.4 MEDIUM: Admin CLI `unwrap()` Usage

**File: bins/gitstratum-admin/src/main.rs:150**
```rust
let now = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_secs() as i64;
```
**Risk:** While SystemTime issues are rare, CLI tools should handle errors gracefully.

### 2.5 LOW: `unwrap_or(0)` for Version Numbers

**File: gitstratum-coordinator/src/client.rs:46**
```rust
pub fn get_version(&self) -> u64 {
    self.topology
        .read()
        .as_ref()
        .map(|t| t.version)
        .unwrap_or(0)
}
```
**Issue:** Version 0 could be confused with "no topology" vs "version 0 topology".

### 2.6 LOW: RocksDB Error Swallowing

**File: gitstratum-object-cluster/src/store/rocksdb.rs:183-190**
```rust
fn has_sync(&self, oid: &Oid) -> bool {
    match self.db.get(key) {
        Ok(v) => v.is_some(),
        Err(e) => {
            tracing::error!(error = %e, oid = %oid, "RocksDB error during has() check");
            false  // Silently returns false on error
        }
    }
}
```
**Issue:** Returns `false` on error, which could mask database issues.

---

## 3. Recommendations (Prioritized by Risk)

### P0 - Critical (Address Before Production)

1. **Replace `expect()` with proper error handling in production paths**
   - `gitstratum-metadata-cluster/src/auth/store.rs` - Return Result from `cf_auth()` and `cf_acl()`
   - `gitstratum-storage/src/bucket/disk.rs` - Convert parsing functions to return Result

2. **Audit all byte slice conversions**
   - The `try_into().expect()` pattern for byte slices should be replaced with proper error handling or use const generics where applicable

### P1 - High (Address Soon)

3. **Preserve error context in conversions**
   - Modify `From<tonic::Status> for MetadataStoreError` to preserve status code
   - Expand `From<ObjectStoreError> for tonic::Status` to map more errors

4. **Add last-error tracking to retry loops**
   - Store the last error encountered during retries
   - Include it in the final error message

### P2 - Medium (Address in Next Sprint)

5. **Make `has()` return Result instead of bool**
   - Current signature masks database errors
   - Consider `fn has(&self, oid: &Oid) -> Result<bool>`

6. **Add structured logging for error chains**
   - Use `tracing::error!(error = ?err)` to log full error chains

7. **Replace admin CLI `unwrap()` with `unwrap_or_default()`**
   - Or handle the edge case explicitly

### P3 - Low (Technical Debt)

8. **Consider using `lazy_static!` for constant RefNames**
   - Avoids repeated `expect()` on known-valid strings
   - Or use `const` functions if possible

9. **Add error documentation**
   - Document when each error variant is returned
   - Add recovery suggestions in error messages

10. **Consider `anyhow` for binary crates**
    - Main functions already use it; could provide better error chains

---

## 4. Code Examples

### Good Pattern: Comprehensive Error Handling in Async Code

**File: gitstratum-coordinator/src/client.rs:340-395**
```rust
pub async fn register_node(&self, node: NodeInfo) -> Result<RegisterNodeResponse, CoordinatorError> {
    let mut consecutive_leader_failures = 0u32;

    for attempt in 0..self.retry_config.max_attempts {
        if consecutive_leader_failures >= 2 {
            self.clear_leader_hint();
            consecutive_leader_failures = 0;
        }

        let result = tokio::time::timeout(self.retry_config.timeout_per_attempt, async {
            let channel = self.get_channel(&endpoint).await?;
            let mut client = CoordinatorServiceClient::new(channel);
            let response = client.register_node(request).await?;
            Ok::<_, CoordinatorError>(response.into_inner())
        }).await;

        match result {
            Ok(Ok(resp)) => return Ok(resp),
            Ok(Err(CoordinatorError::Grpc(status))) if status.code() == Code::FailedPrecondition => {
                self.update_leader_hint(&status);
                consecutive_leader_failures = 0;
            }
            Ok(Err(e)) => {
                tracing::warn!(attempt, error = %e, "Register attempt failed");
                // ... handle error
            }
            Err(_) => {
                tracing::warn!(attempt, "Register attempt timed out");
                // ... handle timeout
            }
        }

        let backoff = self.retry_config.compute_backoff_with_jitter(attempt);
        tokio::time::sleep(backoff).await;
    }

    Err(CoordinatorError::Internal(format!("Failed after {} attempts", self.retry_config.max_attempts)))
}
```

### Problematic Pattern: expect() in Production

**Current (gitstratum-metadata-cluster/src/auth/store.rs):**
```rust
fn cf_auth(&self) -> &ColumnFamily {
    self.db.cf_handle("auth").expect("auth column family validated in constructor")
}
```

**Recommended:**
```rust
fn cf_auth(&self) -> Result<&ColumnFamily, AuthStoreError> {
    self.db.cf_handle("auth").ok_or_else(|| {
        AuthStoreError { message: "auth column family not found - database may be corrupted".to_string() }
    })
}
```

### Good Pattern: Graceful Degradation

**File: gitstratum-coordinator/src/client.rs:552-576 (watch_topology_with_recovery)**
```rust
loop {
    let connect_result = CoordinatorClient::connect(endpoint).await;
    let mut client = match connect_result {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(endpoint, error = %e, "Failed to connect for watch");
            on_event(WatchEvent::Error, current_version);
            sleep_with_jitter(config.reconnect_backoff, config.reconnect_jitter_fraction).await;
            continue;  // Graceful retry instead of panic
        }
    };
    // ... handle stream with similar graceful error handling
}
```

---

## 5. Metrics Summary

| Metric | Count | Notes |
|--------|-------|-------|
| Error types defined | 9 | One per major crate |
| `expect()` in production code | 15 | Should be 0 |
| `expect()` in test code | 100+ | Acceptable |
| `panic!()` in production code | 0 | Excellent |
| `panic!()` in test code | 45 | Acceptable |
| Error conversions (From impls) | 12 | Good coverage |
| Retry implementations | 3 | Well-designed |
| Error tests | 50+ | Comprehensive |

---

## Conclusion

The gitstratum-cluster codebase demonstrates mature error handling practices overall, with consistent use of `thiserror`, proper gRPC status mapping, and robust retry logic. The main areas for improvement are:

1. Eliminating `expect()` from production code paths
2. Improving error context preservation in conversions
3. Making `has()` methods return `Result` instead of swallowing errors

Addressing the P0 and P1 items would raise this review to an 8.5/10 grade.
