# API Design Code Review: gitstratum-cluster

**Reviewer:** Senior API Architect
**Date:** 2026-01-10
**Repository:** gitstratum-cluster
**Focus:** gRPC Service Definitions, Proto Organization, Error Handling, Client SDK Usability

---

## 1. Executive Summary

**Grade: 7.5/10**

The gitstratum-cluster repository demonstrates a well-structured gRPC API design with thoughtful service separation, consistent naming conventions, and solid error handling patterns. The codebase shows evidence of multiple revisions addressing architectural concerns, as seen in the recent security and bug fix commits.

**Key Findings:**
- Strong points: Clear service boundaries, good streaming API design, comprehensive error-to-Status mapping
- Areas for improvement: Missing API versioning, inconsistent pagination patterns, some legacy API technical debt, limited documentation in proto files

---

## 2. Strengths

### 2.1 Well-Organized Service Boundaries

The proto file (`crates/gitstratum-proto/proto/gitstratum.proto`) cleanly separates concerns into distinct services:

- **CoordinatorService** - Cluster membership and topology management
- **MetadataService** - Git refs, commits, trees, and repository management
- **ObjectService** - Blob storage operations
- **ObjectRepairService** - Data repair and anti-entropy operations
- **AuthService** - Authentication and authorization

This separation follows the single responsibility principle and enables independent scaling of each service component.

### 2.2 Consistent Message Naming Conventions

The API follows a consistent `{Action}{Resource}Request`/`{Action}{Resource}Response` naming pattern:

```protobuf
// crates/gitstratum-proto/proto/gitstratum.proto:274-283
message GetRefRequest {
  string repo_id = 1;
  string ref_name = 2;
}

message GetRefResponse {
  Oid target = 1;
  bool found = 2;
}
```

This consistency makes the API predictable and easy to learn.

### 2.3 Strong Streaming API Design

The repository makes excellent use of gRPC streaming for appropriate operations:

```protobuf
// crates/gitstratum-proto/proto/gitstratum.proto:257-265
rpc GetCommits(GetCommitsRequest) returns (stream Commit);
rpc WalkCommits(WalkCommitsRequest) returns (stream Commit);
```

The WatchTopology streaming implementation (`crates/gitstratum-coordinator/src/server.rs:634-767`) includes:
- Delta updates via `TopologyUpdate` oneof discriminated union
- Keepalive messages for connection health
- Lagged notification when clients fall behind
- Full sync capability when version gap exceeds threshold

### 2.4 Comprehensive Error-to-Status Mapping

Each service has well-defined error types with proper gRPC status code mapping:

```rust
// crates/gitstratum-metadata-cluster/src/error.rs:86-116
impl From<MetadataStoreError> for tonic::Status {
    fn from(err: MetadataStoreError) -> Self {
        match err {
            MetadataStoreError::RepoNotFound(msg) => tonic::Status::not_found(msg),
            MetadataStoreError::RepoAlreadyExists(msg) => tonic::Status::already_exists(msg),
            MetadataStoreError::CasFailed { expected, found } => {
                tonic::Status::failed_precondition(format!(
                    "CAS failed: expected {}, found {}",
                    expected, found
                ))
            }
            MetadataStoreError::InvalidRefName(msg) => tonic::Status::invalid_argument(msg),
            MetadataStoreError::Connection(msg) => tonic::Status::unavailable(msg),
            // ...
        }
    }
}
```

The error codes follow gRPC semantic conventions correctly.

### 2.5 Leader Forwarding Pattern

The coordinator implements proper leader forwarding via metadata:

```rust
// crates/gitstratum-coordinator/src/error.rs:28-38
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
            // ...
        }
    }
}
```

This enables smart clients to discover and route to the leader automatically.

### 2.6 Robust Input Validation

The validation module provides thorough input checking:

```rust
// crates/gitstratum-coordinator/src/validation.rs:5-36
pub fn validate_node_info(node: &NodeInfo) -> Result<(), Status> {
    if !is_valid_node_id(&node.id) {
        return Err(Status::invalid_argument(
            "Node ID must be 1-63 alphanumeric characters or hyphens, without leading/trailing hyphens",
        ));
    }
    if !is_valid_address(&node.address) {
        return Err(Status::invalid_argument(
            "Address must be valid IPv4, IPv6, or hostname (max 253 chars)",
        ));
    }
    if node.port == 0 || node.port > 65535 {
        return Err(Status::invalid_argument("Port must be 1-65535"));
    }
    if !is_valid_uuid(&node.generation_id) {
        return Err(Status::invalid_argument(
            "Generation ID must be valid UUID (36 chars with hyphens)",
        ));
    }
    // ...
}
```

### 2.7 Rate Limiting with Informative Errors

The rate limiting implementation provides actionable error information:

```rust
// crates/gitstratum-coordinator/src/rate_limit.rs:6-22
pub enum RateLimitError {
    GlobalLimitExceeded {
        operation: String,
        limit: String,
        retry_after: Duration,
    },
    PerClientLimitExceeded {
        operation: String,
        limit: String,
        retry_after: Duration,
    },
    TooManyWatchers {
        current: u32,
        max: u32,
    },
}
```

This enables clients to implement proper backoff strategies.

---

## 3. Issues Found

### 3.1 CRITICAL: Missing API Versioning

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto:1-3`

```protobuf
syntax = "proto3";
package gitstratum;
```

**Issue:** The proto package has no version identifier. This makes breaking changes difficult to manage and doesn't follow best practices for API evolution.

**Recommendation:** Use versioned package names:
```protobuf
package gitstratum.v1;
```

### 3.2 HIGH: Inconsistent Pagination Patterns

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto:289-296, 391-400`

The `ListRefs` API lacks pagination:
```protobuf
message ListRefsRequest {
  string repo_id = 1;
  string prefix = 2;
  // Missing: limit, cursor/page_token
}

message ListRefsResponse {
  repeated RefEntry refs = 1;
  // Missing: next_cursor/next_page_token
}
```

While `ListRepos` has proper pagination:
```protobuf
message ListReposRequest {
  string prefix = 1;
  uint32 limit = 2;
  string cursor = 3;
}

message ListReposResponse {
  repeated string repo_ids = 1;
  string next_cursor = 2;
}
```

**Recommendation:** Add consistent pagination to all list operations.

### 3.3 HIGH: Legacy API Technical Debt

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto:55-60`

```protobuf
// Legacy APIs (deprecated, will be removed)
rpc GetClusterState(GetClusterStateRequest) returns (GetClusterStateResponse);
rpc AddNode(AddNodeRequest) returns (AddNodeResponse);
rpc RemoveNode(RemoveNodeRequest) returns (RemoveNodeResponse);
rpc SetNodeState(SetNodeStateRequest) returns (SetNodeStateResponse);
rpc GetHashRing(GetHashRingRequest) returns (GetHashRingResponse);
```

**Issue:** Deprecated APIs are still present without a deprecation timeline or version policy.

**Recommendation:**
1. Add `option deprecated = true;` to deprecated RPCs
2. Document removal timeline in comments
3. Consider using a separate proto file for v2 APIs

### 3.4 MEDIUM: Inconsistent Response Patterns

**Location:** Multiple response messages

Some responses use `success + error` pattern:
```protobuf
// crates/gitstratum-proto/proto/gitstratum.proto:183-186
message AddNodeResponse {
  bool success = 1;
  string error = 2;
}
```

While others use just `found`:
```protobuf
// crates/gitstratum-proto/proto/gitstratum.proto:315-319
message GetCommitResponse {
  Commit commit = 1;
  bool found = 2;
}
```

And registration uses different fields:
```protobuf
// crates/gitstratum-proto/proto/gitstratum.proto:135-138
message RegisterNodeResponse {
  uint64 topology_version = 1;
  bool already_registered = 2;
}
```

**Issue:** Inconsistent response patterns make client implementation more complex.

**Recommendation:** Standardize on one of:
1. `google.rpc.Status` for errors in response body
2. gRPC status codes only (preferred for simplicity)
3. A consistent `result` wrapper type

### 3.5 MEDIUM: Missing Field Documentation

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto` (throughout)

Most fields lack documentation comments explaining their purpose, valid ranges, or semantics:

```protobuf
message HashRingConfig {
  uint32 virtual_nodes_per_physical = 1;  // What are valid ranges?
  uint32 replication_factor = 2;          // What's the minimum/maximum?
}
```

**Recommendation:** Add comprehensive field documentation:
```protobuf
message HashRingConfig {
  // Number of virtual nodes per physical node.
  // Higher values provide better distribution but use more memory.
  // Valid range: 10-1000. Default: 100.
  uint32 virtual_nodes_per_physical = 1;

  // Number of replicas for each object.
  // Valid range: 1-5. Default: 3.
  uint32 replication_factor = 2;
}
```

### 3.6 MEDIUM: Permissions Field Uses Magic Numbers

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto:697-724`

```protobuf
message CheckPermissionRequest {
  string repo_id = 1;
  string user_id = 2;
  uint32 required = 3;  // What do the values mean?
}

message SetPermissionRequest {
  string repo_id = 1;
  string user_id = 2;
  uint32 permissions = 3;  // What do the values mean?
}
```

**Issue:** Permission values are opaque integers without defined enum values.

**Recommendation:** Define a proper enum or bitmask:
```protobuf
enum Permission {
  PERMISSION_NONE = 0;
  PERMISSION_READ = 1;
  PERMISSION_WRITE = 2;
  PERMISSION_ADMIN = 4;
}
```

### 3.7 MEDIUM: Missing Request ID for Tracing

**Location:** All request messages

**Issue:** No standard field for request tracing/correlation IDs, which complicates distributed debugging.

**Recommendation:** Either:
1. Use gRPC metadata for `x-request-id` header
2. Add optional `request_id` field to request messages

### 3.8 LOW: Inconsistent Timestamp Representation

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto:10-15, 69`

Timestamps use bare `int64`:
```protobuf
message Signature {
  int64 timestamp = 3;  // Unix timestamp? Milliseconds? Seconds?
}

message NodeInfo {
  int64 last_heartbeat_at = 6;  // Same question
}
```

**Recommendation:** Use `google.protobuf.Timestamp` for clarity and interoperability:
```protobuf
import "google/protobuf/timestamp.proto";

message Signature {
  google.protobuf.Timestamp timestamp = 3;
}
```

### 3.9 LOW: Token Scope as Boolean Fields

**Location:** `crates/gitstratum-proto/proto/gitstratum.proto:661-673`

```protobuf
message CreateTokenRequest {
  string user_id = 1;
  bool scope_read = 2;
  bool scope_write = 3;
  bool scope_admin = 4;
  int64 expires_at = 5;
}
```

**Issue:** Boolean flags don't scale well for future scope additions.

**Recommendation:** Use repeated enum or string list:
```protobuf
enum TokenScope {
  TOKEN_SCOPE_READ = 0;
  TOKEN_SCOPE_WRITE = 1;
  TOKEN_SCOPE_ADMIN = 2;
}

message CreateTokenRequest {
  string user_id = 1;
  repeated TokenScope scopes = 2;
  google.protobuf.Timestamp expires_at = 3;
}
```

### 3.10 LOW: Build Configuration Could Be Enhanced

**Location:** `crates/gitstratum-proto/build.rs:1-7`

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/gitstratum.proto"], &["proto"])?;
    Ok(())
}
```

**Issue:** Missing useful tonic-build options.

**Recommendation:** Enable additional features:
```rust
tonic_build::configure()
    .build_server(true)
    .build_client(true)
    .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
    .field_attribute(".", "#[serde(default)]")
    .compile_well_known_types(true)
    .compile(&["proto/gitstratum.proto"], &["proto"])?;
```

---

## 4. Recommendations Prioritized by Impact

### Priority 1: Critical (Do Before Production)

1. **Add API versioning to proto package** - Essential for backward compatibility
2. **Add pagination to ListRefs** - Required for large repositories

### Priority 2: High (Address in Next Sprint)

3. **Deprecate legacy APIs properly** - Use proto deprecation annotations
4. **Standardize response patterns** - Pick one approach and apply consistently

### Priority 3: Medium (Address in Next Quarter)

5. **Add comprehensive field documentation** - Improves developer experience
6. **Replace permission magic numbers with enums** - Type safety
7. **Add request ID support for tracing** - Operational necessity
8. **Use google.protobuf.Timestamp** - Better interoperability

### Priority 4: Low (Nice to Have)

9. **Refactor token scopes to use enum list** - Future-proofing
10. **Enhance build configuration** - Developer convenience

---

## 5. Specific Code Examples

### Example 1: Proper Proto Versioning

```protobuf
// gitstratum_v1.proto
syntax = "proto3";

package gitstratum.v1;

option go_package = "github.com/gitstratum/gitstratum/gen/go/v1;gitstratumv1";
option java_package = "com.gitstratum.proto.v1";

// Service definitions...
```

### Example 2: Consistent Pagination Pattern

```protobuf
message ListRefsRequest {
  string repo_id = 1;
  string prefix = 2;

  // Maximum number of refs to return. Default: 100, Max: 1000.
  int32 page_size = 3;

  // Page token from previous response for pagination.
  string page_token = 4;
}

message ListRefsResponse {
  repeated RefEntry refs = 1;

  // Token for next page. Empty if no more results.
  string next_page_token = 2;

  // Total count (may be estimate for performance).
  int64 total_count = 3;
}
```

### Example 3: Unified Error Response Pattern

```protobuf
import "google/rpc/status.proto";

message UpdateRefResponse {
  oneof result {
    // Set on success
    UpdateRefSuccess success = 1;
    // Set on application-level failure (not gRPC errors)
    google.rpc.Status error = 2;
  }
}

message UpdateRefSuccess {
  uint64 new_version = 1;
}
```

### Example 4: Documented Enum for Permissions

```protobuf
// Permission levels for repository access.
// Can be combined as a bitmask.
enum RepositoryPermission {
  // No access (default)
  REPOSITORY_PERMISSION_NONE = 0;

  // Read repository contents (clone, fetch)
  REPOSITORY_PERMISSION_READ = 1;

  // Write to repository (push, delete refs)
  REPOSITORY_PERMISSION_WRITE = 2;

  // Full administrative access (delete repo, manage collaborators)
  REPOSITORY_PERMISSION_ADMIN = 4;
}
```

---

## 6. Backward Compatibility Considerations

The current API lacks explicit versioning, which creates challenges:

1. **Field Additions** - Safe via proto3 unknown field handling
2. **Field Removals** - Would break clients; avoid without version bump
3. **Semantic Changes** - Cannot change field meanings; need new fields
4. **RPC Additions** - Safe; clients ignore unknown methods
5. **RPC Removals** - Breaking; deprecate first with version policy

**Recommended Migration Strategy:**
1. Create `gitstratum.v1` package as alias to current API
2. Future breaking changes go in `gitstratum.v2`
3. Support v1 for at least 12 months after v2 release
4. Use feature flags for experimental APIs

---

## 7. Summary

The gitstratum-cluster API design is fundamentally sound with good separation of concerns, appropriate use of streaming, and solid error handling. The main areas requiring attention are:

1. **Versioning** - Critical for production deployment
2. **Consistency** - Pagination, response patterns, documentation
3. **Future-proofing** - Enum-based permissions, timestamp types

With the recommended changes, particularly adding API versioning and standardizing patterns, this API would be well-positioned for long-term production use.

---

*Review completed by Claude Opus 4.5*
