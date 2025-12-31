use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlag {
    pub name: String,
    pub enabled: bool,
    pub description: Option<String>,
    pub rollout_percentage: Option<u8>,
    pub allowed_users: Vec<String>,
    pub allowed_repos: Vec<String>,
    pub metadata: HashMap<String, String>,
}

impl FeatureFlag {
    pub fn new(name: impl Into<String>, enabled: bool) -> Self {
        Self {
            name: name.into(),
            enabled,
            description: None,
            rollout_percentage: None,
            allowed_users: Vec::new(),
            allowed_repos: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_rollout(mut self, percentage: u8) -> Self {
        self.rollout_percentage = Some(percentage.min(100));
        self
    }

    pub fn with_allowed_user(mut self, user_id: impl Into<String>) -> Self {
        self.allowed_users.push(user_id.into());
        self
    }

    pub fn with_allowed_repo(mut self, repo_id: impl Into<String>) -> Self {
        self.allowed_repos.push(repo_id.into());
        self
    }

    pub fn is_enabled_for(&self, user_id: Option<&str>, repo_id: Option<&str>) -> bool {
        if !self.enabled {
            return false;
        }

        if !self.allowed_users.is_empty() {
            if let Some(user_id) = user_id {
                if !self.allowed_users.iter().any(|u| u == user_id) {
                    return false;
                }
            } else {
                return false;
            }
        }

        if !self.allowed_repos.is_empty() {
            if let Some(repo_id) = repo_id {
                if !self.allowed_repos.iter().any(|r| r == repo_id) {
                    return false;
                }
            } else {
                return false;
            }
        }

        if let Some(percentage) = self.rollout_percentage {
            let hash = Self::hash_for_rollout(user_id, repo_id);
            return hash < percentage;
        }

        true
    }

    fn hash_for_rollout(user_id: Option<&str>, repo_id: Option<&str>) -> u8 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        if let Some(user_id) = user_id {
            user_id.hash(&mut hasher);
        }
        if let Some(repo_id) = repo_id {
            repo_id.hash(&mut hasher);
        }
        (hasher.finish() % 100) as u8
    }
}

pub struct FeatureFlags {
    flags: HashMap<String, FeatureFlag>,
    version: u64,
}

impl FeatureFlags {
    pub fn new() -> Self {
        Self {
            flags: HashMap::new(),
            version: 0,
        }
    }

    pub fn add_flag(&mut self, flag: FeatureFlag) {
        self.flags.insert(flag.name.clone(), flag);
        self.version += 1;
    }

    pub fn remove_flag(&mut self, name: &str) -> Option<FeatureFlag> {
        let flag = self.flags.remove(name);
        if flag.is_some() {
            self.version += 1;
        }
        flag
    }

    pub fn get_flag(&self, name: &str) -> Option<&FeatureFlag> {
        self.flags.get(name)
    }

    pub fn is_enabled(&self, name: &str) -> bool {
        self.flags.get(name).map(|f| f.enabled).unwrap_or(false)
    }

    pub fn is_enabled_for(&self, name: &str, user_id: Option<&str>, repo_id: Option<&str>) -> bool {
        self.flags
            .get(name)
            .map(|f| f.is_enabled_for(user_id, repo_id))
            .unwrap_or(false)
    }

    pub fn set_enabled(&mut self, name: &str, enabled: bool) -> bool {
        if let Some(flag) = self.flags.get_mut(name) {
            flag.enabled = enabled;
            self.version += 1;
            true
        } else {
            false
        }
    }

    pub fn all_flags(&self) -> impl Iterator<Item = &FeatureFlag> {
        self.flags.values()
    }

    pub fn enabled_flags(&self) -> impl Iterator<Item = &FeatureFlag> {
        self.flags.values().filter(|f| f.enabled)
    }

    pub fn flag_count(&self) -> usize {
        self.flags.len()
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self::new()
    }
}
