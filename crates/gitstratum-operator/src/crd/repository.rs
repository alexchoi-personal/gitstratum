use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(group = "gitstratum.io", version = "v1", kind = "GitRepository")]
#[kube(namespaced, status = "GitRepositoryStatus")]
#[kube(
    shortname = "grepo",
    printcolumn = r#"{"name":"Owner","type":"string","jsonPath":".spec.owner"}"#,
    printcolumn = r#"{"name":"Visibility","type":"string","jsonPath":".spec.visibility"}"#,
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.state"}"#,
    printcolumn = r#"{"name":"Size","type":"integer","jsonPath":".status.sizeBytes"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GitRepositorySpec {
    pub owner: String,
    pub name: String,
    pub visibility: Visibility,
    pub default_branch: String,
    #[serde(default)]
    pub size_limit_bytes: Option<u64>,
    #[serde(default)]
    pub pack_cache_policy: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitRepositoryStatus {
    #[serde(default)]
    pub state: RepoState,
    #[serde(default)]
    pub size_bytes: u64,
    #[serde(default)]
    pub object_count: u64,
    #[serde(default)]
    pub last_push: Option<String>,
    #[serde(default)]
    pub assigned_metadata_partition: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum Visibility {
    #[default]
    Private,
    Public,
    Internal,
}

impl Visibility {
    pub fn as_str(&self) -> &'static str {
        match self {
            Visibility::Private => "private",
            Visibility::Public => "public",
            Visibility::Internal => "internal",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum RepoState {
    #[default]
    Pending,
    Ready,
    Error,
    Deleting,
}

impl RepoState {
    pub fn as_str(&self) -> &'static str {
        match self {
            RepoState::Pending => "Pending",
            RepoState::Ready => "Ready",
            RepoState::Error => "Error",
            RepoState::Deleting => "Deleting",
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, RepoState::Ready)
    }

    pub fn is_error(&self) -> bool {
        matches!(self, RepoState::Error)
    }
}

impl GitRepositoryStatus {
    pub fn pending() -> Self {
        Self {
            state: RepoState::Pending,
            ..Default::default()
        }
    }

    pub fn ready(size_bytes: u64, object_count: u64) -> Self {
        Self {
            state: RepoState::Ready,
            size_bytes,
            object_count,
            last_push: Some(chrono::Utc::now().to_rfc3339()),
            assigned_metadata_partition: None,
        }
    }

    pub fn error() -> Self {
        Self {
            state: RepoState::Error,
            ..Default::default()
        }
    }

    pub fn deleting() -> Self {
        Self {
            state: RepoState::Deleting,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visibility_default() {
        assert_eq!(Visibility::default(), Visibility::Private);
    }

    #[test]
    fn test_visibility_as_str() {
        assert_eq!(Visibility::Private.as_str(), "private");
        assert_eq!(Visibility::Public.as_str(), "public");
        assert_eq!(Visibility::Internal.as_str(), "internal");
    }

    #[test]
    fn test_repo_state_default() {
        assert_eq!(RepoState::default(), RepoState::Pending);
    }

    #[test]
    fn test_repo_state_as_str() {
        assert_eq!(RepoState::Pending.as_str(), "Pending");
        assert_eq!(RepoState::Ready.as_str(), "Ready");
        assert_eq!(RepoState::Error.as_str(), "Error");
        assert_eq!(RepoState::Deleting.as_str(), "Deleting");
    }

    #[test]
    fn test_repo_state_is_ready() {
        assert!(!RepoState::Pending.is_ready());
        assert!(RepoState::Ready.is_ready());
        assert!(!RepoState::Error.is_ready());
        assert!(!RepoState::Deleting.is_ready());
    }

    #[test]
    fn test_repo_state_is_error() {
        assert!(!RepoState::Pending.is_error());
        assert!(!RepoState::Ready.is_error());
        assert!(RepoState::Error.is_error());
        assert!(!RepoState::Deleting.is_error());
    }

    #[test]
    fn test_git_repository_status_pending() {
        let status = GitRepositoryStatus::pending();
        assert_eq!(status.state, RepoState::Pending);
        assert_eq!(status.size_bytes, 0);
        assert_eq!(status.object_count, 0);
        assert!(status.last_push.is_none());
    }

    #[test]
    fn test_git_repository_status_ready() {
        let status = GitRepositoryStatus::ready(1024, 100);
        assert_eq!(status.state, RepoState::Ready);
        assert_eq!(status.size_bytes, 1024);
        assert_eq!(status.object_count, 100);
        assert!(status.last_push.is_some());
    }

    #[test]
    fn test_git_repository_status_error() {
        let status = GitRepositoryStatus::error();
        assert_eq!(status.state, RepoState::Error);
    }

    #[test]
    fn test_git_repository_status_deleting() {
        let status = GitRepositoryStatus::deleting();
        assert_eq!(status.state, RepoState::Deleting);
    }

    #[test]
    fn test_git_repository_spec_serialization() {
        let spec = GitRepositorySpec {
            owner: "test-user".to_string(),
            name: "test-repo".to_string(),
            visibility: Visibility::Public,
            default_branch: "main".to_string(),
            size_limit_bytes: Some(1_073_741_824),
            pack_cache_policy: Some("default".to_string()),
        };

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: GitRepositorySpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.owner, "test-user");
        assert_eq!(deserialized.name, "test-repo");
        assert_eq!(deserialized.visibility, Visibility::Public);
        assert_eq!(deserialized.default_branch, "main");
        assert_eq!(deserialized.size_limit_bytes, Some(1_073_741_824));
        assert_eq!(deserialized.pack_cache_policy, Some("default".to_string()));
    }

    #[test]
    fn test_git_repository_spec_minimal() {
        let json = r#"{
            "owner": "user",
            "name": "repo",
            "visibility": "Private",
            "defaultBranch": "main"
        }"#;

        let spec: GitRepositorySpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.owner, "user");
        assert_eq!(spec.name, "repo");
        assert_eq!(spec.visibility, Visibility::Private);
        assert_eq!(spec.default_branch, "main");
        assert!(spec.size_limit_bytes.is_none());
        assert!(spec.pack_cache_policy.is_none());
    }
}
