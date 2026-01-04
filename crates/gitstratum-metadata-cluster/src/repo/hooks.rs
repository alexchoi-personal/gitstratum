use std::collections::HashMap;
use std::str::FromStr;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use gitstratum_core::RepoId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookType {
    PreReceive,
    PostReceive,
    Update,
    PrePush,
    PostUpdate,
}

impl HookType {
    pub fn as_str(&self) -> &'static str {
        match self {
            HookType::PreReceive => "pre-receive",
            HookType::PostReceive => "post-receive",
            HookType::Update => "update",
            HookType::PrePush => "pre-push",
            HookType::PostUpdate => "post-update",
        }
    }
}

impl FromStr for HookType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pre-receive" => Ok(HookType::PreReceive),
            "post-receive" => Ok(HookType::PostReceive),
            "update" => Ok(HookType::Update),
            "pre-push" => Ok(HookType::PrePush),
            "post-update" => Ok(HookType::PostUpdate),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfig {
    pub hook_type: HookType,
    pub enabled: bool,
    pub script: String,
    pub timeout_secs: u32,
    pub environment: HashMap<String, String>,
}

impl HookConfig {
    pub fn new(hook_type: HookType, script: &str) -> Self {
        Self {
            hook_type,
            enabled: true,
            script: script.to_string(),
            timeout_secs: 30,
            environment: HashMap::new(),
        }
    }

    pub fn with_timeout(mut self, timeout_secs: u32) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.environment.insert(key.to_string(), value.to_string());
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoHooks {
    hooks: HashMap<HookType, HookConfig>,
}

impl RepoHooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_hook(&mut self, config: HookConfig) {
        self.hooks.insert(config.hook_type, config);
    }

    pub fn get_hook(&self, hook_type: HookType) -> Option<&HookConfig> {
        self.hooks.get(&hook_type)
    }

    pub fn remove_hook(&mut self, hook_type: HookType) {
        self.hooks.remove(&hook_type);
    }

    pub fn is_enabled(&self, hook_type: HookType) -> bool {
        self.hooks
            .get(&hook_type)
            .map(|h| h.enabled)
            .unwrap_or(false)
    }

    pub fn list_hooks(&self) -> Vec<&HookConfig> {
        self.hooks.values().collect()
    }

    pub fn list_enabled_hooks(&self) -> Vec<&HookConfig> {
        self.hooks.values().filter(|h| h.enabled).collect()
    }
}

pub struct HookConfigStore {
    configs: RwLock<HashMap<RepoId, RepoHooks>>,
}

impl HookConfigStore {
    pub fn new() -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, repo_id: &RepoId) -> Option<RepoHooks> {
        self.configs.read().get(repo_id).cloned()
    }

    pub fn set(&self, repo_id: &RepoId, hooks: RepoHooks) {
        self.configs.write().insert(repo_id.clone(), hooks);
    }

    pub fn set_hook(&self, repo_id: &RepoId, config: HookConfig) {
        let mut configs = self.configs.write();
        let hooks = configs.entry(repo_id.clone()).or_default();
        hooks.set_hook(config);
    }

    pub fn remove_hook(&self, repo_id: &RepoId, hook_type: HookType) {
        if let Some(hooks) = self.configs.write().get_mut(repo_id) {
            hooks.remove_hook(hook_type);
        }
    }

    pub fn delete(&self, repo_id: &RepoId) {
        self.configs.write().remove(repo_id);
    }
}

impl Default for HookConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_type_as_str() {
        assert_eq!(HookType::PreReceive.as_str(), "pre-receive");
        assert_eq!(HookType::PostReceive.as_str(), "post-receive");
        assert_eq!(HookType::Update.as_str(), "update");
        assert_eq!(HookType::PrePush.as_str(), "pre-push");
        assert_eq!(HookType::PostUpdate.as_str(), "post-update");
    }

    #[test]
    fn test_hook_type_from_str() {
        assert_eq!("pre-receive".parse::<HookType>(), Ok(HookType::PreReceive));
        assert_eq!(
            "post-receive".parse::<HookType>(),
            Ok(HookType::PostReceive)
        );
        assert_eq!("update".parse::<HookType>(), Ok(HookType::Update));
        assert_eq!("pre-push".parse::<HookType>(), Ok(HookType::PrePush));
        assert_eq!("post-update".parse::<HookType>(), Ok(HookType::PostUpdate));
        assert!("unknown".parse::<HookType>().is_err());
    }

    #[test]
    fn test_hook_config_new() {
        let config = HookConfig::new(HookType::PreReceive, "#!/bin/bash\nexit 0");
        assert_eq!(config.hook_type, HookType::PreReceive);
        assert!(config.enabled);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.environment.is_empty());
    }

    #[test]
    fn test_hook_config_builder() {
        let config = HookConfig::new(HookType::PreReceive, "#!/bin/bash\nexit 0")
            .with_timeout(60)
            .with_env("CI", "true")
            .disabled();

        assert!(!config.enabled);
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.environment.get("CI"), Some(&"true".to_string()));
    }

    #[test]
    fn test_repo_hooks() {
        let mut hooks = RepoHooks::new();

        let pre_receive = HookConfig::new(HookType::PreReceive, "exit 0");
        let post_receive = HookConfig::new(HookType::PostReceive, "exit 0").disabled();

        hooks.set_hook(pre_receive);
        hooks.set_hook(post_receive);

        assert!(hooks.is_enabled(HookType::PreReceive));
        assert!(!hooks.is_enabled(HookType::PostReceive));
        assert!(!hooks.is_enabled(HookType::Update));

        assert_eq!(hooks.list_hooks().len(), 2);
        assert_eq!(hooks.list_enabled_hooks().len(), 1);
    }

    #[test]
    fn test_repo_hooks_remove() {
        let mut hooks = RepoHooks::new();
        hooks.set_hook(HookConfig::new(HookType::PreReceive, "exit 0"));

        assert!(hooks.get_hook(HookType::PreReceive).is_some());

        hooks.remove_hook(HookType::PreReceive);
        assert!(hooks.get_hook(HookType::PreReceive).is_none());
    }

    #[test]
    fn test_hook_config_store() {
        let store = HookConfigStore::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        assert!(store.get(&repo_id).is_none());

        let mut hooks = RepoHooks::new();
        hooks.set_hook(HookConfig::new(HookType::PreReceive, "exit 0"));
        store.set(&repo_id, hooks);

        let retrieved = store.get(&repo_id).unwrap();
        assert!(retrieved.is_enabled(HookType::PreReceive));
    }

    #[test]
    fn test_hook_config_store_set_hook() {
        let store = HookConfigStore::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        store.set_hook(&repo_id, HookConfig::new(HookType::PreReceive, "exit 0"));
        store.set_hook(&repo_id, HookConfig::new(HookType::PostReceive, "exit 0"));

        let hooks = store.get(&repo_id).unwrap();
        assert_eq!(hooks.list_hooks().len(), 2);
    }

    #[test]
    fn test_hook_config_store_remove_hook() {
        let store = HookConfigStore::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        store.set_hook(&repo_id, HookConfig::new(HookType::PreReceive, "exit 0"));
        store.remove_hook(&repo_id, HookType::PreReceive);

        let hooks = store.get(&repo_id).unwrap();
        assert!(hooks.list_hooks().is_empty());
    }

    #[test]
    fn test_hook_config_store_delete() {
        let store = HookConfigStore::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        store.set_hook(&repo_id, HookConfig::new(HookType::PreReceive, "exit 0"));
        store.delete(&repo_id);

        assert!(store.get(&repo_id).is_none());
    }
}
