use async_trait::async_trait;
use std::time::Duration;

use crate::error::Result;
use crate::server::ClusterState;

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub endpoints: Vec<String>,
    pub timeout: Duration,
    pub retry_count: u32,
}

impl ControlPlaneConfig {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            timeout: Duration::from_secs(5),
            retry_count: 3,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_retry_count(mut self, count: u32) -> Self {
        self.retry_count = count;
        self
    }
}

#[async_trait]
pub trait ControlPlaneClient: Send + Sync {
    async fn validate_token(&self, token: &str) -> Result<TokenValidation>;
    async fn validate_ssh_key(&self, fingerprint: &str) -> Result<TokenValidation>;
    async fn check_rate_limit(&self, user_id: &str, repo_id: &str) -> Result<bool>;
    async fn acquire_ref_lock(&self, repo_id: &str, ref_name: &str, holder_id: &str, timeout_ms: u64) -> Result<String>;
    async fn release_ref_lock(&self, lock_id: &str) -> Result<()>;
    async fn get_cluster_state(&self) -> Result<ClusterState>;
    async fn log_audit(&self, event: AuditEvent) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct TokenValidation {
    pub valid: bool,
    pub user_id: Option<String>,
    pub permissions: Vec<String>,
    pub expires_at: Option<i64>,
}

impl TokenValidation {
    pub fn invalid() -> Self {
        Self {
            valid: false,
            user_id: None,
            permissions: Vec::new(),
            expires_at: None,
        }
    }

    pub fn valid(user_id: String) -> Self {
        Self {
            valid: true,
            user_id: Some(user_id),
            permissions: Vec::new(),
            expires_at: None,
        }
    }

    pub fn with_permissions(mut self, permissions: Vec<String>) -> Self {
        self.permissions = permissions;
        self
    }

    pub fn with_expiry(mut self, expires_at: i64) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub fn can_read(&self) -> bool {
        self.permissions.contains(&"read".to_string())
            || self.permissions.contains(&"admin".to_string())
    }

    pub fn can_write(&self) -> bool {
        self.permissions.contains(&"write".to_string())
            || self.permissions.contains(&"admin".to_string())
    }
}

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub event_type: String,
    pub user_id: String,
    pub repo_id: String,
    pub action: String,
    pub details: String,
    pub timestamp: i64,
}

impl AuditEvent {
    pub fn new(event_type: &str, user_id: &str, repo_id: &str, action: &str) -> Self {
        Self {
            event_type: event_type.to_string(),
            user_id: user_id.to_string(),
            repo_id: repo_id.to_string(),
            action: action.to_string(),
            details: String::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        }
    }

    pub fn with_details(mut self, details: &str) -> Self {
        self.details = details.to_string();
        self
    }

    pub fn clone_event(user_id: &str, repo_id: &str) -> Self {
        Self::new("git", user_id, repo_id, "clone")
    }

    pub fn fetch_event(user_id: &str, repo_id: &str) -> Self {
        Self::new("git", user_id, repo_id, "fetch")
    }

    pub fn push_event(user_id: &str, repo_id: &str) -> Self {
        Self::new("git", user_id, repo_id, "push")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_plane_config() {
        let config = ControlPlaneConfig::new(vec!["localhost:5000".to_string()]);
        assert_eq!(config.endpoints.len(), 1);
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.retry_count, 3);
    }

    #[test]
    fn test_control_plane_config_builders() {
        let config = ControlPlaneConfig::new(vec!["localhost:5000".to_string()])
            .with_timeout(Duration::from_secs(10))
            .with_retry_count(5);

        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.retry_count, 5);
    }

    #[test]
    fn test_token_validation_invalid() {
        let validation = TokenValidation::invalid();
        assert!(!validation.valid);
        assert!(validation.user_id.is_none());
    }

    #[test]
    fn test_token_validation_valid() {
        let validation = TokenValidation::valid("user-1".to_string());
        assert!(validation.valid);
        assert_eq!(validation.user_id, Some("user-1".to_string()));
    }

    #[test]
    fn test_token_validation_permissions() {
        let validation = TokenValidation::valid("user-1".to_string())
            .with_permissions(vec!["read".to_string(), "write".to_string()]);

        assert!(validation.can_read());
        assert!(validation.can_write());
    }

    #[test]
    fn test_token_validation_admin() {
        let validation = TokenValidation::valid("admin".to_string())
            .with_permissions(vec!["admin".to_string()]);

        assert!(validation.can_read());
        assert!(validation.can_write());
    }

    #[test]
    fn test_token_validation_expiry() {
        let validation = TokenValidation::valid("user-1".to_string())
            .with_expiry(1704067200);

        assert_eq!(validation.expires_at, Some(1704067200));
    }

    #[test]
    fn test_audit_event_new() {
        let event = AuditEvent::new("git", "user-1", "repo-1", "clone");
        assert_eq!(event.event_type, "git");
        assert_eq!(event.user_id, "user-1");
        assert_eq!(event.repo_id, "repo-1");
        assert_eq!(event.action, "clone");
    }

    #[test]
    fn test_audit_event_with_details() {
        let event = AuditEvent::new("git", "user-1", "repo-1", "clone")
            .with_details("refs/heads/main");

        assert_eq!(event.details, "refs/heads/main");
    }

    #[test]
    fn test_audit_event_helpers() {
        let clone = AuditEvent::clone_event("user-1", "repo-1");
        assert_eq!(clone.action, "clone");

        let fetch = AuditEvent::fetch_event("user-1", "repo-1");
        assert_eq!(fetch.action, "fetch");

        let push = AuditEvent::push_event("user-1", "repo-1");
        assert_eq!(push.action, "push");
    }
}
