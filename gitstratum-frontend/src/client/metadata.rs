use async_trait::async_trait;
use gitstratum_core::{Commit, Oid, Tree};
use std::time::Duration;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct MetadataClientConfig {
    pub endpoints: Vec<String>,
    pub timeout: Duration,
    pub pool_size: u32,
}

impl MetadataClientConfig {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            timeout: Duration::from_secs(10),
            pool_size: 50,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_pool_size(mut self, size: u32) -> Self {
        self.pool_size = size;
        self
    }
}

#[async_trait]
pub trait MetadataClusterClient: Send + Sync {
    async fn get_refs(&self, repo_id: &str, prefix: &str) -> Result<Vec<(String, Oid)>>;
    async fn get_ref(&self, repo_id: &str, ref_name: &str) -> Result<Option<Oid>>;
    async fn update_ref(&self, repo_id: &str, ref_name: &str, old_oid: Oid, new_oid: Oid, force: bool) -> Result<bool>;
    async fn get_commit(&self, repo_id: &str, oid: &Oid) -> Result<Option<Commit>>;
    async fn get_commits(&self, repo_id: &str, oids: &[Oid]) -> Result<Vec<Commit>>;
    async fn put_commit(&self, repo_id: &str, commit: &Commit) -> Result<()>;
    async fn get_tree(&self, repo_id: &str, oid: &Oid) -> Result<Option<Tree>>;
    async fn get_trees(&self, repo_id: &str, oids: &[Oid]) -> Result<Vec<Tree>>;
    async fn put_tree(&self, repo_id: &str, tree: &Tree) -> Result<()>;
    async fn walk_commits(&self, repo_id: &str, from: Vec<Oid>, until: Vec<Oid>, limit: u32) -> Result<Vec<Commit>>;
    async fn get_commit_ancestry(&self, repo_id: &str, oid: &Oid, depth: u32) -> Result<Vec<Oid>>;
}

#[derive(Debug, Clone)]
pub struct RefUpdateRequest {
    pub ref_name: String,
    pub old_oid: Oid,
    pub new_oid: Oid,
    pub force: bool,
}

impl RefUpdateRequest {
    pub fn new(ref_name: String, old_oid: Oid, new_oid: Oid) -> Self {
        Self {
            ref_name,
            old_oid,
            new_oid,
            force: false,
        }
    }

    pub fn with_force(mut self) -> Self {
        self.force = true;
        self
    }

    pub fn is_create(&self) -> bool {
        self.old_oid.is_zero()
    }

    pub fn is_delete(&self) -> bool {
        self.new_oid.is_zero()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_client_config() {
        let config = MetadataClientConfig::new(vec!["localhost:6000".to_string()]);
        assert_eq!(config.endpoints.len(), 1);
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.pool_size, 50);
    }

    #[test]
    fn test_metadata_client_config_builders() {
        let config = MetadataClientConfig::new(vec!["localhost:6000".to_string()])
            .with_timeout(Duration::from_secs(30))
            .with_pool_size(100);

        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.pool_size, 100);
    }

    #[test]
    fn test_ref_update_request_new() {
        let old = Oid::hash(b"old");
        let new = Oid::hash(b"new");
        let request = RefUpdateRequest::new("refs/heads/main".to_string(), old, new);

        assert_eq!(request.ref_name, "refs/heads/main");
        assert_eq!(request.old_oid, old);
        assert_eq!(request.new_oid, new);
        assert!(!request.force);
    }

    #[test]
    fn test_ref_update_request_with_force() {
        let old = Oid::hash(b"old");
        let new = Oid::hash(b"new");
        let request = RefUpdateRequest::new("refs/heads/main".to_string(), old, new).with_force();

        assert!(request.force);
    }

    #[test]
    fn test_ref_update_request_is_create() {
        let new = Oid::hash(b"new");
        let request = RefUpdateRequest::new("refs/heads/main".to_string(), Oid::ZERO, new);

        assert!(request.is_create());
        assert!(!request.is_delete());
    }

    #[test]
    fn test_ref_update_request_is_delete() {
        let old = Oid::hash(b"old");
        let request = RefUpdateRequest::new("refs/heads/main".to_string(), old, Oid::ZERO);

        assert!(!request.is_create());
        assert!(request.is_delete());
    }
}
