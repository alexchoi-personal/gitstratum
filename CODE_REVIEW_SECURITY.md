# Security Code Review: gitstratum-cluster

**Reviewer:** Security Engineer (Claude)
**Date:** 2026-01-10
**Repository:** gitstratum-cluster
**Scope:** Full codebase security review

---

## Executive Summary

**Overall Security Grade: 7.5/10**

The gitstratum-cluster codebase demonstrates a **strong security posture** for a distributed Git storage system. The team has implemented many security best practices including proper password hashing with Argon2, comprehensive rate limiting, TLS with CRL checking, and input validation. However, several areas require attention before production deployment.

### Key Findings Summary
- **Strengths:** 8 major security controls properly implemented
- **Critical Issues:** 0
- **High-Severity Issues:** 2
- **Medium-Severity Issues:** 4
- **Low-Severity Issues:** 3

---

## Strengths Found

### 1. Cryptographic Password Hashing (Excellent)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/auth/password.rs`

The codebase uses **Argon2** (the winner of the Password Hashing Competition) with cryptographically secure random salts:

```rust
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| AuthError::Storage(format!("password hashing failed: {}", e)))
}
```

**Why this is good:**
- Uses `OsRng` for cryptographically secure salt generation
- Argon2 is memory-hard, resisting GPU/ASIC attacks
- Each password gets a unique salt

### 2. Token Hashing with SHA-256 (Good)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/auth/validator.rs:174-179`

Tokens are hashed before storage lookup, preventing timing attacks:

```rust
fn sha2_hash(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}
```

### 3. Comprehensive Rate Limiting (Excellent)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/rate_limit.rs`

Multi-tier rate limiting with:
- Global rate limits (cluster-wide protection)
- Per-client rate limits (individual abuse prevention)
- Token bucket algorithm with refill
- Watch connection limits

```rust
pub struct GlobalRateLimiter {
    register_limiter: TokenBucket::new(200, 100),      // 100/sec
    heartbeat_limiter: TokenBucket::new(20_000, 10_000), // 10k/sec
    topology_limiter: TokenBucket::new(100_000, 50_000), // 50k/sec
    max_watch_connections: 1_000,
}
```

**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/auth/rate_limit.rs`

Authentication rate limiting with lockout:
```rust
pub struct AuthRateLimiter {
    failed_attempts: DashMap<String, (u32, Instant)>,
    max_attempts: u32,    // Default: 5
    window: Duration,     // Default: 300s
    lockout: Duration,    // Default: 900s (15 min)
}
```

### 4. TLS with CRL Support (Good)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/tls.rs`

Certificate Revocation List (CRL) checking is implemented:
- Cached CRL with configurable refresh interval
- Staleness detection with max age enforcement
- Async fetching to prevent blocking

### 5. Node Identity Validation (Good)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/tls.rs:158-178`

Validates that node certificates match claimed identity:

```rust
pub fn validate_node_identity(
    cert_cn: &str,
    cert_sans: &[String],
    node: &NodeInfo,
) -> Result<(), Status> {
    if node.id != cert_cn {
        return Err(Status::permission_denied(...));
    }
    if !cert_sans.contains(&node.address) {
        return Err(Status::permission_denied(...));
    }
    Ok(())
}
```

### 6. Input Validation for Node Info (Good)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/validation.rs`

Comprehensive validation:
- Node ID format validation (alphanumeric + hyphens, 1-63 chars)
- Address validation (IPv4, IPv6, hostname)
- Port range validation (1-65535)
- UUID validation for generation IDs
- Loopback address blocking in production

### 7. SSH Authentication with Fingerprint Validation (Good)
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/ssh/handler.rs`

- Public key authentication only (no password auth)
- Rate limiting on SSH key validation
- Proper session denial for unauthenticated users

### 8. Structured Logging for Security Events (Good)
**Files:** Multiple locations using `tracing` crate

Security-relevant events are logged:
```rust
tracing::warn!(
    peer_addr = ?self.peer_addr,
    fingerprint = %fingerprint,
    error = %e,
    "ssh authentication failed"
);
```

---

## Vulnerabilities / Issues Found

### HIGH SEVERITY

#### H1. TLS Disabled by Default
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/config.rs:88-102`
**Line:** 91

```rust
impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,  // <-- SECURITY RISK
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            verify_client_cert: true,
            ...
        }
    }
}
```

**Risk:** In a distributed system handling Git data, having TLS disabled by default allows:
- Man-in-the-middle attacks
- Credential interception
- Data tampering

**Recommendation:** Change default to `enabled: true` and provide clear documentation for TLS setup. At minimum, log a prominent warning when TLS is disabled.

#### H2. Missing Authentication on gRPC Coordinator Service
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/server.rs`

The CoordinatorService implements rate limiting but **no authentication check** before processing requests:

```rust
async fn register_node(
    &self,
    request: Request<RegisterNodeRequest>,
) -> Result<Response<RegisterNodeResponse>, Status> {
    let client_id = Self::extract_client_id(&request);
    self.global_limiter.try_register()?;      // Only rate limiting
    self.try_client_register(&client_id)?;    // No auth check!

    // ... processes registration
}
```

**Risk:** Any client with network access can:
- Register malicious nodes
- Disrupt cluster topology
- Exhaust rate limits for legitimate clients

**Recommendation:**
1. Require mTLS for all coordinator operations
2. Validate client certificates against an allowlist
3. Add explicit authentication interceptor

### MEDIUM SEVERITY

#### M1. Rate Limit Key Exposes Full Token Hash
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/auth/validator.rs:72`

```rust
let token_key = format!("token:{}", rate_limit_key_hash(token));
```

While the token is hashed, the rate limit key uses the **full hash** which could leak timing information or be used for enumeration if the rate limiter state is exposed.

**Recommendation:** Use only a prefix of the hash (e.g., first 8 bytes) for rate limiting purposes.

#### M2. No Audit Trail for Administrative Actions
**Files:** Authentication and authorization modules

While authentication failures are logged, successful administrative actions (permission changes, user creation) lack dedicated audit logging:

**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-metadata-cluster/src/auth/store.rs:99-106`
```rust
pub fn set_permission(
    &self,
    repo_id: &str,
    user_id: &str,
    perm: u8,
) -> Result<(), rocksdb::Error> {
    let key = format!("{}/acl/{}", repo_id, user_id);
    self.db.put_cf(self.cf_acl(), key.as_bytes(), [perm])
    // No audit log!
}
```

**Recommendation:** Add structured audit logging for all permission changes, user modifications, and administrative operations.

#### M3. Unsafe Code Without Full Safety Justification
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-storage/src/bucket/entry.rs:117,129`

While the code includes safety comments, the transmute operations could be improved:

```rust
pub fn to_bytes(&self) -> [u8; ENTRY_SIZE] {
    // SAFETY: CompactEntry is repr(C, packed) with compile-time size/alignment checks
    unsafe { std::mem::transmute_copy(self) }
}

pub fn from_bytes(bytes: &[u8; ENTRY_SIZE]) -> Self {
    // SAFETY: CompactEntry is repr(C, packed), all bit patterns valid
    unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
}
```

**Risk:** While the current implementation appears sound, `transmute` operations are fragile under refactoring.

**Recommendation:** Consider using `bytemuck::Pod` trait for safer zero-copy conversions, or add compile-time assertions using `static_assert!` equivalents.

#### M4. SSH Command Parsing May Allow Path Traversal
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/ssh/handler.rs:32-71`

```rust
pub fn parse_git_command(cmd: &str) -> Option<GitCommand> {
    // ... parsing logic ...
    let repo_id = repo_path.trim_start_matches('/').to_string();
    // Does not validate for '../' sequences!
}
```

While leading slashes are stripped, there's no validation against path traversal patterns like `../` or encoded variants.

**Recommendation:** Add explicit validation:
```rust
if repo_id.contains("..") || repo_id.contains('\0') {
    return None;
}
```

### LOW SEVERITY

#### L1. Empty Password Accepted
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/auth/password.rs:81-83` (test)

The test explicitly shows empty passwords are allowed:
```rust
#[test]
fn test_empty_password() {
    let hash = hash_password("").unwrap();
    assert!(verify_password("", &hash).unwrap());
}
```

**Recommendation:** Add minimum password length validation in the authentication layer.

#### L2. Client ID Based on IP Only
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/server.rs:96-102`

```rust
fn extract_client_id<T>(request: &Request<T>) -> String {
    if let Some(addr) = request.remote_addr() {
        addr.ip().to_string()
    } else {
        "unknown".to_string()
    }
}
```

**Risk:** Multiple legitimate clients behind NAT share the same IP, making them share rate limits.

**Recommendation:** When mTLS is enabled, use the client certificate fingerprint as the client ID.

#### L3. Hardcoded Default SSH Key Path
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/ssh/server.rs:47`

```rust
impl Default for SshServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:22".parse().unwrap(),
            host_key_path: PathBuf::from("/etc/ssh/ssh_host_ed25519_key"),
            // ...
        }
    }
}
```

**Risk:** Default paths may not exist or may have incorrect permissions in container environments.

**Recommendation:** Require explicit configuration rather than defaulting to system paths.

---

## Recommendations (Prioritized)

### Immediate (Pre-Production)

1. **Enable TLS by default** or add a prominent startup warning when disabled
2. **Add authentication to Coordinator gRPC service** - require mTLS for cluster operations
3. **Add path traversal validation** in SSH command parsing
4. **Add password minimum length validation**

### Short-Term (Within 30 Days)

5. **Implement audit logging** for all administrative operations
6. **Review unsafe code** - consider using `bytemuck` or similar safe alternatives
7. **Enhance client identification** for rate limiting when mTLS is enabled
8. **Add explicit configuration requirements** instead of fallback defaults

### Long-Term (Within 90 Days)

9. **Security testing** - implement fuzzing for SSH command parsing and protocol handlers
10. **Penetration testing** - conduct formal security assessment
11. **Secrets management** - integrate with HashiCorp Vault or similar for key management
12. **RBAC enhancement** - implement more granular role-based access control

---

## Code Examples

### Good Pattern: Token Validation with Rate Limiting
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-frontend-cluster/src/auth/validator.rs:70-124`

```rust
pub fn validate_token(&self, token: &str) -> Result<AuthResult, AuthError> {
    let start = Instant::now();
    let token_key = format!("token:{}", rate_limit_key_hash(token));

    // 1. Check rate limit FIRST
    if !self.rate_limiter.check(&token_key) {
        record_auth_attempt("pat", false);
        return Err(AuthError::RateLimitExceeded);
    }

    // 2. Hash token before lookup (prevents timing attacks)
    let hash = sha2_hash(token);
    let stored = match self.store.get_token_by_hash(&hash)? {
        Some(t) => t,
        None => {
            self.rate_limiter.record_failure(&token_key);  // Record failure
            record_auth_attempt("pat", false);
            return Err(AuthError::InvalidCredentials);
        }
    };

    // 3. Check expiration
    if let Some(exp) = stored.expires_at {
        let now = chrono::Utc::now().timestamp();
        if exp < now {
            record_auth_attempt("pat", false);
            return Err(AuthError::TokenExpired);
        }
    }

    // 4. Check user status
    let user = self.store.get_user(&stored.user_id)?
        .ok_or(AuthError::UserNotFound)?;

    if user.status != UserStatus::Active {
        record_auth_attempt("pat", false);
        return Err(AuthError::UserDisabled);
    }

    // 5. Clear rate limit on success
    self.rate_limiter.clear(&token_key);

    // 6. Record metrics
    record_auth_duration("pat", start.elapsed().as_secs_f64());
    record_auth_attempt("pat", true);

    Ok(AuthResult::authenticated(stored.user_id))
}
```

### Bad Pattern: Missing Authentication Check
**File:** `/Users/alex/Desktop/git/gitstratum-cluster/crates/gitstratum-coordinator/src/server.rs` (conceptual fix needed)

```rust
// CURRENT (Insecure):
async fn add_node(&self, request: Request<AddNodeRequest>) -> Result<Response<AddNodeResponse>, Status> {
    let req = request.into_inner();
    // Directly processes request without authentication
    ...
}

// RECOMMENDED:
async fn add_node(&self, request: Request<AddNodeRequest>) -> Result<Response<AddNodeResponse>, Status> {
    // Verify client certificate
    let cert = extract_client_certificate(&request)?;
    validate_admin_certificate(&cert)?;

    let req = request.into_inner();
    audit_log!("add_node", cert.subject(), req.node.id);
    ...
}
```

---

## Conclusion

The gitstratum-cluster codebase shows **mature security engineering** with proper use of modern cryptographic primitives, comprehensive rate limiting, and defense-in-depth patterns. The primary concerns are around **default configurations** and **missing authentication on internal services**.

With the recommended fixes implemented, this codebase would be suitable for production deployment handling sensitive Git repository data.

---

*This review was conducted by analyzing source code only. A complete security assessment would include dynamic testing, penetration testing, and infrastructure security review.*
