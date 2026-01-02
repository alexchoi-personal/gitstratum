use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(group = "gitstratum.io", version = "v1", kind = "GitUser")]
#[kube(namespaced, status = "GitUserStatus")]
#[kube(
    shortname = "guser",
    printcolumn = r#"{"name":"Username","type":"string","jsonPath":".spec.username"}"#,
    printcolumn = r#"{"name":"Email","type":"string","jsonPath":".spec.email"}"#,
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.state"}"#,
    printcolumn = r#"{"name":"SSH Keys","type":"integer","jsonPath":".status.activeSshKeys"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GitUserSpec {
    pub username: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub ssh_keys: Vec<SshKeySpec>,
    #[serde(default)]
    pub tokens: Vec<TokenSpec>,
    #[serde(default)]
    pub rate_limits: Option<RateLimitSpec>,
    #[serde(default)]
    pub repository_access: Vec<RepoAccessSpec>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SshKeySpec {
    pub name: String,
    pub public_key: String,
    #[serde(default)]
    pub fingerprint: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub last_used: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenSpec {
    pub name: String,
    #[serde(default)]
    pub token_hash: Option<String>,
    #[serde(default)]
    pub scopes: Vec<TokenScope>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub last_used: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TokenScope {
    Read,
    Write,
    Admin,
    Repo,
    RepoRead,
    RepoWrite,
    RepoDelete,
    User,
    UserRead,
    UserWrite,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitSpec {
    #[serde(default)]
    pub requests_per_minute: Option<u32>,
    #[serde(default)]
    pub requests_per_hour: Option<u32>,
    #[serde(default)]
    pub concurrent_connections: Option<u32>,
    #[serde(default)]
    pub bandwidth_bytes_per_second: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepoAccessSpec {
    pub repository: String,
    pub permission: Permission,
    #[serde(default)]
    pub granted_at: Option<String>,
    #[serde(default)]
    pub granted_by: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Read,
    Write,
    Admin,
    Owner,
}

impl Permission {
    pub fn as_str(&self) -> &'static str {
        match self {
            Permission::Read => "read",
            Permission::Write => "write",
            Permission::Admin => "admin",
            Permission::Owner => "owner",
        }
    }

    pub fn can_read(&self) -> bool {
        true
    }

    pub fn can_write(&self) -> bool {
        !matches!(self, Permission::Read)
    }

    pub fn can_admin(&self) -> bool {
        matches!(self, Permission::Admin | Permission::Owner)
    }

    pub fn is_owner(&self) -> bool {
        matches!(self, Permission::Owner)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitUserStatus {
    #[serde(default)]
    pub state: UserState,
    #[serde(default)]
    pub active_ssh_keys: u32,
    #[serde(default)]
    pub active_tokens: u32,
    #[serde(default)]
    pub last_activity: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub repositories_owned: u32,
    #[serde(default)]
    pub repositories_accessible: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum UserState {
    #[default]
    Pending,
    Active,
    Suspended,
    Deleting,
}

impl UserState {
    pub fn as_str(&self) -> &'static str {
        match self {
            UserState::Pending => "Pending",
            UserState::Active => "Active",
            UserState::Suspended => "Suspended",
            UserState::Deleting => "Deleting",
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, UserState::Active)
    }
}

impl GitUserStatus {
    pub fn pending() -> Self {
        Self {
            state: UserState::Pending,
            created_at: Some(chrono::Utc::now().to_rfc3339()),
            ..Default::default()
        }
    }

    pub fn active(ssh_keys: u32, tokens: u32) -> Self {
        Self {
            state: UserState::Active,
            active_ssh_keys: ssh_keys,
            active_tokens: tokens,
            last_activity: Some(chrono::Utc::now().to_rfc3339()),
            created_at: Some(chrono::Utc::now().to_rfc3339()),
            ..Default::default()
        }
    }

    pub fn suspended() -> Self {
        Self {
            state: UserState::Suspended,
            ..Default::default()
        }
    }

    pub fn deleting() -> Self {
        Self {
            state: UserState::Deleting,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_state_default() {
        assert_eq!(UserState::default(), UserState::Pending);
    }

    #[test]
    fn test_user_state_as_str() {
        assert_eq!(UserState::Pending.as_str(), "Pending");
        assert_eq!(UserState::Active.as_str(), "Active");
        assert_eq!(UserState::Suspended.as_str(), "Suspended");
        assert_eq!(UserState::Deleting.as_str(), "Deleting");
    }

    #[test]
    fn test_user_state_is_active() {
        assert!(!UserState::Pending.is_active());
        assert!(UserState::Active.is_active());
        assert!(!UserState::Suspended.is_active());
        assert!(!UserState::Deleting.is_active());
    }

    #[test]
    fn test_permission_as_str() {
        assert_eq!(Permission::Read.as_str(), "read");
        assert_eq!(Permission::Write.as_str(), "write");
        assert_eq!(Permission::Admin.as_str(), "admin");
        assert_eq!(Permission::Owner.as_str(), "owner");
    }

    #[test]
    fn test_permission_can_read() {
        assert!(Permission::Read.can_read());
        assert!(Permission::Write.can_read());
        assert!(Permission::Admin.can_read());
        assert!(Permission::Owner.can_read());
    }

    #[test]
    fn test_permission_can_write() {
        assert!(!Permission::Read.can_write());
        assert!(Permission::Write.can_write());
        assert!(Permission::Admin.can_write());
        assert!(Permission::Owner.can_write());
    }

    #[test]
    fn test_permission_can_admin() {
        assert!(!Permission::Read.can_admin());
        assert!(!Permission::Write.can_admin());
        assert!(Permission::Admin.can_admin());
        assert!(Permission::Owner.can_admin());
    }

    #[test]
    fn test_permission_is_owner() {
        assert!(!Permission::Read.is_owner());
        assert!(!Permission::Write.is_owner());
        assert!(!Permission::Admin.is_owner());
        assert!(Permission::Owner.is_owner());
    }

    #[test]
    fn test_git_user_status_pending() {
        let status = GitUserStatus::pending();
        assert_eq!(status.state, UserState::Pending);
        assert!(status.created_at.is_some());
        assert_eq!(status.active_ssh_keys, 0);
    }

    #[test]
    fn test_git_user_status_active() {
        let status = GitUserStatus::active(2, 3);
        assert_eq!(status.state, UserState::Active);
        assert_eq!(status.active_ssh_keys, 2);
        assert_eq!(status.active_tokens, 3);
        assert!(status.last_activity.is_some());
    }

    #[test]
    fn test_git_user_status_suspended() {
        let status = GitUserStatus::suspended();
        assert_eq!(status.state, UserState::Suspended);
    }

    #[test]
    fn test_git_user_status_deleting() {
        let status = GitUserStatus::deleting();
        assert_eq!(status.state, UserState::Deleting);
    }

    #[test]
    fn test_ssh_key_spec_default_enabled() {
        let json = r#"{
            "name": "my-key",
            "publicKey": "ssh-ed25519 AAAA..."
        }"#;

        let key: SshKeySpec = serde_json::from_str(json).unwrap();
        assert!(key.enabled);
    }

    #[test]
    fn test_token_spec_serialization() {
        let token = TokenSpec {
            name: "ci-token".to_string(),
            token_hash: Some("sha256:abc123".to_string()),
            scopes: vec![TokenScope::Read, TokenScope::RepoWrite],
            expires_at: Some("2025-12-31T23:59:59Z".to_string()),
            created_at: None,
            last_used: None,
            enabled: true,
        };

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: TokenSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "ci-token");
        assert_eq!(deserialized.scopes.len(), 2);
        assert!(deserialized.scopes.contains(&TokenScope::Read));
        assert!(deserialized.scopes.contains(&TokenScope::RepoWrite));
    }

    #[test]
    fn test_rate_limit_spec() {
        let rate_limit = RateLimitSpec {
            requests_per_minute: Some(60),
            requests_per_hour: Some(1000),
            concurrent_connections: Some(10),
            bandwidth_bytes_per_second: Some(1_048_576),
        };

        let json = serde_json::to_string(&rate_limit).unwrap();
        let deserialized: RateLimitSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.requests_per_minute, Some(60));
        assert_eq!(deserialized.requests_per_hour, Some(1000));
        assert_eq!(deserialized.concurrent_connections, Some(10));
        assert_eq!(deserialized.bandwidth_bytes_per_second, Some(1_048_576));
    }

    #[test]
    fn test_repo_access_spec() {
        let access = RepoAccessSpec {
            repository: "org/repo".to_string(),
            permission: Permission::Write,
            granted_at: Some("2024-01-01T00:00:00Z".to_string()),
            granted_by: Some("admin".to_string()),
        };

        let json = serde_json::to_string(&access).unwrap();
        let deserialized: RepoAccessSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.repository, "org/repo");
        assert_eq!(deserialized.permission, Permission::Write);
    }

    #[test]
    fn test_git_user_spec_full() {
        let spec = GitUserSpec {
            username: "test-user".to_string(),
            email: Some("test@example.com".to_string()),
            ssh_keys: vec![SshKeySpec {
                name: "laptop".to_string(),
                public_key: "ssh-ed25519 AAAA...".to_string(),
                fingerprint: Some("SHA256:abc".to_string()),
                created_at: None,
                last_used: None,
                enabled: true,
            }],
            tokens: vec![],
            rate_limits: Some(RateLimitSpec {
                requests_per_minute: Some(100),
                ..Default::default()
            }),
            repository_access: vec![RepoAccessSpec {
                repository: "myorg/myrepo".to_string(),
                permission: Permission::Admin,
                granted_at: None,
                granted_by: None,
            }],
        };

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: GitUserSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.username, "test-user");
        assert_eq!(deserialized.email, Some("test@example.com".to_string()));
        assert_eq!(deserialized.ssh_keys.len(), 1);
        assert_eq!(deserialized.repository_access.len(), 1);
    }

    #[test]
    fn test_git_user_spec_minimal() {
        let json = r#"{
            "username": "minimal-user"
        }"#;

        let spec: GitUserSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.username, "minimal-user");
        assert!(spec.email.is_none());
        assert!(spec.ssh_keys.is_empty());
        assert!(spec.tokens.is_empty());
        assert!(spec.rate_limits.is_none());
        assert!(spec.repository_access.is_empty());
    }

    #[test]
    fn test_token_scope_all_variants() {
        let scopes = vec![
            TokenScope::Read,
            TokenScope::Write,
            TokenScope::Admin,
            TokenScope::Repo,
            TokenScope::RepoRead,
            TokenScope::RepoWrite,
            TokenScope::RepoDelete,
            TokenScope::User,
            TokenScope::UserRead,
            TokenScope::UserWrite,
        ];

        let json = serde_json::to_string(&scopes).unwrap();
        let deserialized: Vec<TokenScope> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.len(), 10);
    }
}
