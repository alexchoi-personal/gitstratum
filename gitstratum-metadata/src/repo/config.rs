use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use gitstratum_core::RepoId;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RepoConfig {
    pub default_branch: Option<String>,
    pub description: Option<String>,
    pub visibility: RepoVisibility,
    pub settings: RepoSettings,
    pub custom: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
pub enum RepoVisibility {
    #[default]
    Public,
    Private,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSettings {
    pub allow_force_push: bool,
    pub require_signed_commits: bool,
    pub max_file_size_mb: u32,
    pub protected_branches: Vec<String>,
}

impl Default for RepoSettings {
    fn default() -> Self {
        Self {
            allow_force_push: false,
            require_signed_commits: false,
            max_file_size_mb: 100,
            protected_branches: vec!["main".to_string(), "master".to_string()],
        }
    }
}

impl RepoConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_default_branch(mut self, branch: &str) -> Self {
        self.default_branch = Some(branch.to_string());
        self
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    pub fn with_visibility(mut self, visibility: RepoVisibility) -> Self {
        self.visibility = visibility;
        self
    }

    pub fn set_custom(&mut self, key: &str, value: &str) {
        self.custom.insert(key.to_string(), value.to_string());
    }

    pub fn get_custom(&self, key: &str) -> Option<&str> {
        self.custom.get(key).map(|s| s.as_str())
    }

    pub fn is_branch_protected(&self, branch: &str) -> bool {
        self.settings.protected_branches.iter().any(|b| b == branch)
    }

    pub fn add_protected_branch(&mut self, branch: &str) {
        if !self.is_branch_protected(branch) {
            self.settings.protected_branches.push(branch.to_string());
        }
    }

    pub fn remove_protected_branch(&mut self, branch: &str) {
        self.settings.protected_branches.retain(|b| b != branch);
    }
}

pub struct RepoConfigStore {
    configs: parking_lot::RwLock<HashMap<RepoId, RepoConfig>>,
}

impl RepoConfigStore {
    pub fn new() -> Self {
        Self {
            configs: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, repo_id: &RepoId) -> Option<RepoConfig> {
        self.configs.read().get(repo_id).cloned()
    }

    pub fn set(&self, repo_id: &RepoId, config: RepoConfig) {
        self.configs.write().insert(repo_id.clone(), config);
    }

    pub fn delete(&self, repo_id: &RepoId) {
        self.configs.write().remove(repo_id);
    }

    pub fn exists(&self, repo_id: &RepoId) -> bool {
        self.configs.read().contains_key(repo_id)
    }
}

impl Default for RepoConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repo_config_default() {
        let config = RepoConfig::new();
        assert!(config.default_branch.is_none());
        assert!(config.description.is_none());
        assert_eq!(config.visibility, RepoVisibility::Public);
    }

    #[test]
    fn test_repo_config_builder() {
        let config = RepoConfig::new()
            .with_default_branch("main")
            .with_description("Test repo")
            .with_visibility(RepoVisibility::Private);

        assert_eq!(config.default_branch, Some("main".to_string()));
        assert_eq!(config.description, Some("Test repo".to_string()));
        assert_eq!(config.visibility, RepoVisibility::Private);
    }

    #[test]
    fn test_repo_config_custom() {
        let mut config = RepoConfig::new();
        config.set_custom("key1", "value1");

        assert_eq!(config.get_custom("key1"), Some("value1"));
        assert!(config.get_custom("key2").is_none());
    }

    #[test]
    fn test_protected_branches() {
        let config = RepoConfig::new();
        assert!(config.is_branch_protected("main"));
        assert!(config.is_branch_protected("master"));
        assert!(!config.is_branch_protected("feature"));
    }

    #[test]
    fn test_add_remove_protected_branch() {
        let mut config = RepoConfig::new();

        config.add_protected_branch("develop");
        assert!(config.is_branch_protected("develop"));

        config.add_protected_branch("develop");
        assert_eq!(
            config
                .settings
                .protected_branches
                .iter()
                .filter(|b| *b == "develop")
                .count(),
            1
        );

        config.remove_protected_branch("develop");
        assert!(!config.is_branch_protected("develop"));
    }

    #[test]
    fn test_repo_settings_default() {
        let settings = RepoSettings::default();
        assert!(!settings.allow_force_push);
        assert!(!settings.require_signed_commits);
        assert_eq!(settings.max_file_size_mb, 100);
        assert_eq!(settings.protected_branches.len(), 2);
    }

    #[test]
    fn test_repo_config_store() {
        let store = RepoConfigStore::new();
        let repo_id = RepoId::new("test/repo").unwrap();
        let config = RepoConfig::new().with_default_branch("main");

        assert!(store.get(&repo_id).is_none());
        assert!(!store.exists(&repo_id));

        store.set(&repo_id, config.clone());
        assert!(store.exists(&repo_id));

        let retrieved = store.get(&repo_id).unwrap();
        assert_eq!(retrieved.default_branch, Some("main".to_string()));

        store.delete(&repo_id);
        assert!(!store.exists(&repo_id));
    }

    #[test]
    fn test_repo_visibility() {
        assert_eq!(RepoVisibility::default(), RepoVisibility::Public);
    }
}
