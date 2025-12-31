use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockInfo {
    pub lock_id: String,
    pub repo_id: String,
    pub ref_name: String,
    pub holder_id: String,
    pub timeout_ms: u64,
    pub acquired_at_epoch_ms: u64,
}

impl LockInfo {
    pub fn new(
        lock_id: impl Into<String>,
        repo_id: impl Into<String>,
        ref_name: impl Into<String>,
        holder_id: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            lock_id: lock_id.into(),
            repo_id: repo_id.into(),
            ref_name: ref_name.into(),
            holder_id: holder_id.into(),
            timeout_ms: timeout.as_millis() as u64,
            acquired_at_epoch_ms: now,
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        now > self.acquired_at_epoch_ms + self.timeout_ms
    }

    pub fn remaining_ms(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let expires_at = self.acquired_at_epoch_ms + self.timeout_ms;
        if now >= expires_at {
            0
        } else {
            expires_at - now
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireLockRequest {
    pub repo_id: String,
    pub ref_name: String,
    pub holder_id: String,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireLockResponse {
    pub acquired: bool,
    pub lock_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseLockRequest {
    pub lock_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseLockResponse {
    pub success: bool,
    pub error: Option<String>,
}
