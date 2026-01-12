# Observability Code Review: gitstratum-cluster

**Reviewer**: Alex (Senior SRE)
**Date**: 2026-01-10
**Scope**: Logging, Metrics, Tracing, Health Checks, Diagnostics

---

## Executive Summary

**Grade: 6.5/10**

The gitstratum-cluster codebase has a solid foundation for observability with structured logging via the `tracing` crate and comprehensive Prometheus metrics. However, there are significant gaps in distributed tracing, request correlation across services, and alerting integration that limit production operational readiness.

### Key Findings Summary
- **Structured logging**: Present but inconsistent across crates
- **Metrics**: Comprehensive Prometheus metrics with good coverage
- **Distributed tracing**: Not implemented (no OpenTelemetry/Jaeger integration)
- **Health checks**: Basic implementation, missing readiness checks
- **Request ID propagation**: Partial implementation in frontend only

---

## 1. Strengths Found

### 1.1 Comprehensive Prometheus Metrics
The codebase has an excellent metrics collection layer with dedicated collectors for each service type.

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-metrics/src/`

```rust
// crates/gitstratum-metrics/src/collectors/object.rs:19-22
counter!("gitstratum_object_blob_operations_total", &labels).increment(1);
histogram!("gitstratum_object_blob_operation_duration_seconds", &labels)
    .record(duration_secs);
counter!("gitstratum_object_blob_bytes_total", &labels).increment(bytes);
```

**Metrics Coverage**:
- **Frontend cluster**: Git operations, cache hits, connections, pack sizes
- **Metadata cluster**: Ref operations, commit operations, graph walks, cache metrics
- **Object cluster**: Blob operations, delta computation, GC runs, replication, compression ratios
- **Coordinator**: Topology versions, node states, Raft metrics, RPC durations, watch subscribers

### 1.2 Coordinator Health Check Implementation
The coordinator provides a detailed health check endpoint with Raft state and cluster health information.

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/server.rs:807-862`

```rust
async fn health_check(
    &self,
    _request: Request<HealthCheckRequest>,
) -> Result<Response<HealthCheckResponse>, Status> {
    // Returns: healthy status, raft_state, raft_term, committed/applied index,
    // leader_id, topology_version, node counts by state
    let healthy = metrics.current_leader.is_some();
    // ...
}
```

### 1.3 Request ID Support in Frontend
The frontend logging middleware includes request ID support for correlation.

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/middleware/logging.rs:35-53`

```rust
pub fn log_request_start(
    &self,
    request_id: &str,
    operation: &str,
    repo_id: &str,
) -> RequestLog {
    if self.include_request_id {
        info!(request_id, operation, repo_id, "request started");
    }
    // ...
}
```

### 1.4 Use of #[instrument] for Tracing Spans
Good use of the `tracing::instrument` attribute for automatic span creation in object cluster.

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-object-cluster/src/server.rs`

```rust
// crates/gitstratum-object-cluster/src/server.rs:76
#[instrument(skip(self, request))]
async fn get_blob(
    &self,
    request: Request<GetBlobRequest>,
) -> Result<Response<GetBlobResponse>, Status> {
```

Found 30+ `#[instrument]` usages across:
- `gitstratum-object-cluster/src/client.rs`
- `gitstratum-object-cluster/src/server.rs`
- `gitstratum-object-cluster/src/store/rocksdb.rs`
- `gitstratum-object-cluster/src/pack_cache/*.rs`
- `gitstratum-frontend-cluster/src/server.rs`

### 1.5 Structured Error Types with thiserror
All crates use `thiserror` for well-structured, typed errors with proper Display implementations.

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/error.rs`

```rust
#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Not the leader")]
    NotLeader(Option<String>),

    #[error("Node not found: {0}")]
    NodeNotFound(String),
    // ...
}
```

### 1.6 Consistent Log Levels in SSH Handler
The SSH handler demonstrates good log level discipline.

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/ssh/handler.rs`

```rust
// Debug for routine events
tracing::debug!(peer_addr = ?self.peer_addr, "session channel opened");

// Info for significant events
tracing::info!(peer_addr = ?self.peer_addr, user_id = %result.user_id, "ssh authentication successful");

// Warn for concerning but handled situations
tracing::warn!(peer_addr = ?self.peer_addr, fingerprint = %fingerprint, error = %e, "ssh authentication failed");
```

---

## 2. Gaps and Issues Found

### 2.1 CRITICAL: No Distributed Tracing Integration

**Severity**: High
**Impact**: Cannot trace requests across service boundaries

No OpenTelemetry, Jaeger, or Zipkin integration found in the entire codebase.

```bash
# Search returned no results
grep -r "opentelemetry\|otel\|jaeger\|zipkin" --include="*.rs" crates/
```

**Affected files**: All gRPC service implementations

### 2.2 CRITICAL: Request ID Not Propagated Between Services

**Severity**: High
**Impact**: Cannot correlate logs across frontend, metadata, object, and coordinator services

Request ID is only implemented in frontend logging middleware but is not:
- Included in gRPC metadata headers
- Propagated to downstream service calls
- Present in coordinator, metadata, or object cluster logs

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/middleware/logging.rs:8`

```rust
// Request ID exists in frontend but not propagated
include_request_id: bool,
```

### 2.3 HIGH: Missing Readiness/Liveness Probes for Kubernetes

**Severity**: High
**Impact**: Kubernetes cannot properly manage pod lifecycle

The health check endpoint exists but:
- No separate readiness probe (ready to accept traffic)
- No liveness probe (still running but maybe stuck)
- No startup probe (slow-starting containers)

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-proto/proto/gitstratum.proto:48`

```protobuf
// Only one generic health check
rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
```

### 2.4 HIGH: Inconsistent Logging Across Crates

**Severity**: Medium-High
**Impact**: Difficult to correlate issues across services

Some services log extensively, others minimally:

**Good logging** (object-cluster):
```rust
// crates/gitstratum-object-cluster/src/store/rocksdb.rs:186
tracing::error!(error = %e, oid = %oid, "RocksDB error during has() check");
```

**Missing logging** (metadata-cluster): Very sparse logging outside of `warn` on client retries.

### 2.5 MEDIUM: No Alerting Integration

**Severity**: Medium
**Impact**: No automated incident response

No integration with alerting systems found:
- No Alertmanager webhook endpoints
- No PagerDuty/Opsgenie integration
- No Slack/Teams notification hooks
- No metric thresholds for alerts defined in code

### 2.6 MEDIUM: Metrics Not Connected to Business Context

**Severity**: Medium
**Impact**: Harder to build meaningful dashboards

Metrics lack repository-level or user-level context in many places:

```rust
// crates/gitstratum-metrics/src/lib.rs:14
counter!("gitstratum_requests_total", &labels).increment(1);
// Missing: repository name, user ID, organization
```

### 2.7 MEDIUM: Missing Error Context in Some Paths

**Severity**: Medium
**Impact**: Harder to debug production issues

Some error paths don't include enough context:

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/server.rs:404`

```rust
tracing::warn!(node_id = %node_id, error = %e, "Failed to mark node as SUSPECT via Raft");
// Missing: What was the node's previous state? Which Raft operation failed?
```

### 2.8 LOW: No Trace Context in Log Output

**Severity**: Low
**Impact**: Cannot easily jump from logs to traces

Tracing subscriber setup does not include span/trace IDs in log output:

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/bins/gitstratum-coordinator/src/main.rs:55-57`

```rust
tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
    .init();
// Missing: .with_span_events(), .with_current_span(), trace ID formatting
```

### 2.9 LOW: MetricsMiddleware Not Connected to Prometheus

**Severity**: Low
**Impact**: Duplicate metrics tracking

Frontend has two metrics systems:
1. `MetricsMiddleware` with atomic counters (in-memory only)
2. Prometheus metrics via `metrics` crate

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/middleware/metrics.rs`

```rust
// This is NOT exported to Prometheus
requests_total: AtomicU64,
```

### 2.10 LOW: Hardcoded Timeouts Without Visibility

**Severity**: Low
**Impact**: Hard to tune in production

Constants like streaming timeouts are hardcoded:

**Location**: `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-object-cluster/src/server.rs:23`

```rust
const STREAMING_TASK_TIMEOUT: Duration = Duration::from_secs(300);
// No metric/log when this timeout is hit in normal path
```

---

## 3. Recommendations (Prioritized by Operational Impact)

### P0: Add Distributed Tracing (Week 1-2)

**Why**: Essential for debugging cross-service issues in production.

Add OpenTelemetry integration:

```toml
# Cargo.toml additions
opentelemetry = { version = "0.21", features = ["rt-tokio"] }
opentelemetry-otlp = "0.14"
tracing-opentelemetry = "0.22"
```

```rust
// In each binary's main.rs
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use tracing_opentelemetry::OpenTelemetryLayer;

let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(opentelemetry_otlp::new_exporter().tonic())
    .install_batch(opentelemetry::runtime::Tokio)?;

tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(OpenTelemetryLayer::new(tracer))
    .init();
```

### P0: Implement Request ID Propagation (Week 1)

**Why**: Critical for log correlation in distributed system.

1. Generate UUID request ID at frontend entry point
2. Add to gRPC metadata on all outgoing calls
3. Extract and log in all service handlers

```rust
// Frontend client calls
let mut request = Request::new(inner_request);
request.metadata_mut().insert("x-request-id", request_id.parse()?);

// Service handlers
let request_id = request.metadata()
    .get("x-request-id")
    .map(|v| v.to_str().unwrap_or("unknown"))
    .unwrap_or("unknown");
```

### P1: Add Kubernetes Readiness/Liveness Probes (Week 1)

**Why**: Required for proper Kubernetes orchestration.

```protobuf
// Add to gitstratum.proto
rpc ReadinessCheck(ReadinessRequest) returns (ReadinessResponse);
rpc LivenessCheck(LivenessRequest) returns (LivenessResponse);

message ReadinessResponse {
    bool ready = 1;
    repeated string not_ready_reasons = 2;
}
```

Readiness should check:
- Database connectivity
- Minimum replica count met
- Raft cluster formed (for coordinator)

### P1: Add Alerting Hooks (Week 2)

**Why**: Need automated incident response.

Create alerting rules based on existing metrics:

```yaml
# prometheus-alerts.yaml
groups:
  - name: gitstratum
    rules:
      - alert: HighErrorRate
        expr: rate(gitstratum_requests_error_total[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
      - alert: RaftLeaderMissing
        expr: coordinator_raft_state{state="leader"} == 0
        for: 30s
        labels:
          severity: critical
```

### P2: Standardize Logging Across Crates (Week 2-3)

**Why**: Consistent logging makes debugging easier.

Create a logging guideline and helper macro:

```rust
// crates/gitstratum-core/src/logging.rs
macro_rules! log_operation {
    ($level:ident, $request_id:expr, $operation:expr, $($field:tt)*) => {
        tracing::$level!(
            request_id = %$request_id,
            operation = %$operation,
            $($field)*
        )
    };
}
```

### P2: Add Repository/User Context to Metrics (Week 3)

**Why**: Enables per-repo and per-user dashboards and alerts.

```rust
// Enhanced metrics with context
pub fn record_git_operation(
    &self,
    operation: &str,
    repo_id: &str,
    user_id: Option<&str>,
    organization: Option<&str>,
    duration_secs: f64,
    success: bool,
) {
    let labels = [
        ("node_id", self.node_id.clone()),
        ("operation", operation.to_string()),
        ("repo", repo_id.to_string()),
        ("organization", organization.unwrap_or("unknown").to_string()),
    ];
    // ...
}
```

### P3: Expose Span/Trace IDs in Logs (Week 3)

**Why**: Allows jumping from logs to traces.

```rust
tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .with_span_events(FmtSpan::CLOSE)
    .json()  // Structured JSON logs
    .init();
```

### P3: Merge MetricsMiddleware with Prometheus (Week 4)

**Why**: Single source of truth for metrics.

Remove in-memory `MetricsMiddleware` counters and use only `metrics` crate throughout.

### P3: Add Debug/Diagnostic Endpoints (Week 4)

**Why**: Faster production debugging.

Add `/debug/pprof`, `/debug/vars`, `/debug/topology` endpoints:

```rust
// Add debug routes
.route("/debug/pprof", get(pprof_handler))
.route("/debug/vars", get(vars_handler))
.route("/debug/config", get(config_handler))
```

---

## 4. Specific Code Examples for Fixes

### Example: Request ID Middleware

```rust
// crates/gitstratum-core/src/observability/request_id.rs
use tonic::metadata::MetadataValue;
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-request-id";

pub fn generate_request_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn extract_or_generate<T>(request: &tonic::Request<T>) -> String {
    request
        .metadata()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(generate_request_id)
}

pub fn inject<T>(request: &mut tonic::Request<T>, request_id: &str) {
    if let Ok(value) = MetadataValue::try_from(request_id) {
        request.metadata_mut().insert(REQUEST_ID_HEADER, value);
    }
}
```

### Example: Enhanced Health Check

```rust
// Enhanced health check with readiness
pub struct HealthStatus {
    pub is_live: bool,
    pub is_ready: bool,
    pub details: HealthDetails,
}

pub struct HealthDetails {
    pub raft_connected: bool,
    pub min_replicas_met: bool,
    pub database_connected: bool,
    pub uptime_seconds: u64,
}

impl CoordinatorServer {
    pub fn readiness_check(&self) -> HealthStatus {
        let raft_connected = self.raft.raft().metrics().borrow().current_leader.is_some();
        let min_replicas_met = self.topology_cache.read().object_nodes.len() >= 3;

        HealthStatus {
            is_live: true,
            is_ready: raft_connected && min_replicas_met,
            details: HealthDetails {
                raft_connected,
                min_replicas_met,
                database_connected: true,
                uptime_seconds: self.start_time.elapsed().as_secs(),
            },
        }
    }
}
```

---

## Appendix: Files Reviewed

| File | Observations |
|------|--------------|
| `crates/gitstratum-metrics/src/lib.rs` | Core metrics functions, good coverage |
| `crates/gitstratum-metrics/src/prometheus.rs` | Prometheus setup, basic but functional |
| `crates/gitstratum-metrics/src/collectors/*.rs` | Per-service metrics, comprehensive |
| `crates/gitstratum-coordinator/src/server.rs` | Health check, logging, metrics usage |
| `crates/gitstratum-coordinator/src/metrics.rs` | Coordinator-specific metrics |
| `crates/gitstratum-coordinator/src/error.rs` | Error types with good structure |
| `crates/gitstratum-frontend-cluster/src/middleware/logging.rs` | Request ID support, good patterns |
| `crates/gitstratum-frontend-cluster/src/middleware/metrics.rs` | Duplicate in-memory metrics |
| `crates/gitstratum-frontend-cluster/src/ssh/handler.rs` | Good log level discipline |
| `crates/gitstratum-frontend-cluster/src/error.rs` | Comprehensive error types |
| `crates/gitstratum-object-cluster/src/server.rs` | Good #[instrument] usage |
| `crates/gitstratum-object-cluster/src/store/rocksdb.rs` | Good error logging |
| `crates/gitstratum-object-cluster/src/error.rs` | Typed errors with context |
| `bins/gitstratum-coordinator/src/main.rs` | Tracing init, metrics setup |
| `bins/gitstratum-frontend/src/main.rs` | Similar patterns |
| `crates/gitstratum-proto/proto/gitstratum.proto` | Health check definition |

---

*Review completed with focus on production operational readiness. Primary blockers are lack of distributed tracing and cross-service request correlation.*
