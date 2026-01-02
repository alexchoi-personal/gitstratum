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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_flag_construction_and_builder_pattern() {
        let flag = FeatureFlag::new("test_flag", true);
        assert_eq!(flag.name, "test_flag");
        assert!(flag.enabled);
        assert!(flag.description.is_none());
        assert!(flag.rollout_percentage.is_none());
        assert!(flag.allowed_users.is_empty());
        assert!(flag.allowed_repos.is_empty());
        assert!(flag.metadata.is_empty());

        let disabled_flag = FeatureFlag::new("disabled_flag", false);
        assert!(!disabled_flag.enabled);

        let complex_flag = FeatureFlag::new("complex", true)
            .with_description("Complex flag")
            .with_rollout(80)
            .with_allowed_user("alice")
            .with_allowed_user("bob")
            .with_allowed_repo("repo-a")
            .with_allowed_repo("repo-b");
        assert_eq!(complex_flag.description, Some("Complex flag".to_string()));
        assert_eq!(complex_flag.rollout_percentage, Some(80));
        assert_eq!(complex_flag.allowed_users.len(), 2);
        assert!(complex_flag.allowed_users.contains(&"alice".to_string()));
        assert!(complex_flag.allowed_users.contains(&"bob".to_string()));
        assert_eq!(complex_flag.allowed_repos.len(), 2);
        assert!(complex_flag.allowed_repos.contains(&"repo-a".to_string()));
        assert!(complex_flag.allowed_repos.contains(&"repo-b".to_string()));

        let capped_flag = FeatureFlag::new("test", true).with_rollout(150);
        assert_eq!(capped_flag.rollout_percentage, Some(100));

        let name = String::from("owned_name");
        let description = String::from("owned description");
        let user = String::from("owned_user");
        let repo = String::from("owned_repo");
        let owned_flag = FeatureFlag::new(name, true)
            .with_description(description)
            .with_allowed_user(user)
            .with_allowed_repo(repo);
        assert_eq!(owned_flag.name, "owned_name");
        assert_eq!(
            owned_flag.description,
            Some("owned description".to_string())
        );
        assert_eq!(owned_flag.allowed_users[0], "owned_user");
        assert_eq!(owned_flag.allowed_repos[0], "owned_repo");

        let cloned = complex_flag.clone();
        assert_eq!(cloned.name, complex_flag.name);
        assert_eq!(cloned.enabled, complex_flag.enabled);
        assert_eq!(cloned.description, complex_flag.description);
        assert_eq!(cloned.rollout_percentage, complex_flag.rollout_percentage);
        assert_eq!(cloned.allowed_users, complex_flag.allowed_users);
        assert_eq!(cloned.allowed_repos, complex_flag.allowed_repos);

        let debug_str = format!("{:?}", flag);
        assert!(debug_str.contains("FeatureFlag"));
        assert!(debug_str.contains("test_flag"));
    }

    #[test]
    fn test_feature_flag_evaluation_and_access_control() {
        let disabled_flag = FeatureFlag::new("test", false);
        assert!(!disabled_flag.is_enabled_for(None, None));
        assert!(!disabled_flag.is_enabled_for(Some("user"), None));
        assert!(!disabled_flag.is_enabled_for(None, Some("repo")));
        assert!(!disabled_flag.is_enabled_for(Some("user"), Some("repo")));

        let open_flag = FeatureFlag::new("test", true);
        assert!(open_flag.is_enabled_for(None, None));
        assert!(open_flag.is_enabled_for(Some("user"), None));
        assert!(open_flag.is_enabled_for(None, Some("repo")));
        assert!(open_flag.is_enabled_for(Some("user"), Some("repo")));

        let user_restricted = FeatureFlag::new("test", true).with_allowed_user("allowed_user");
        assert!(user_restricted.is_enabled_for(Some("allowed_user"), None));
        assert!(!user_restricted.is_enabled_for(Some("other_user"), None));
        assert!(!user_restricted.is_enabled_for(None, None));
        assert!(!user_restricted.is_enabled_for(None, Some("repo")));

        let repo_restricted = FeatureFlag::new("test", true).with_allowed_repo("allowed_repo");
        assert!(repo_restricted.is_enabled_for(None, Some("allowed_repo")));
        assert!(!repo_restricted.is_enabled_for(None, Some("other_repo")));
        assert!(!repo_restricted.is_enabled_for(None, None));
        assert!(!repo_restricted.is_enabled_for(Some("user"), None));

        let combined = FeatureFlag::new("test", true)
            .with_allowed_user("user1")
            .with_allowed_repo("repo1");
        assert!(combined.is_enabled_for(Some("user1"), Some("repo1")));
        assert!(!combined.is_enabled_for(Some("user1"), Some("repo2")));
        assert!(!combined.is_enabled_for(Some("user2"), Some("repo1")));
        assert!(!combined.is_enabled_for(Some("user2"), Some("repo2")));
        assert!(!combined.is_enabled_for(Some("user1"), None));

        let full_rollout = FeatureFlag::new("test", true).with_rollout(100);
        assert!(full_rollout.is_enabled_for(Some("any_user"), Some("any_repo")));

        let zero_rollout = FeatureFlag::new("test", true).with_rollout(0);
        assert!(!zero_rollout.is_enabled_for(Some("user"), Some("repo")));

        let partial_rollout = FeatureFlag::new("test", true).with_rollout(50);
        let result1 = partial_rollout.is_enabled_for(Some("user123"), Some("repo456"));
        let result2 = partial_rollout.is_enabled_for(Some("user123"), Some("repo456"));
        assert_eq!(result1, result2);

        let _ = partial_rollout.is_enabled_for(None, None);
        let _ = partial_rollout.is_enabled_for(Some("user"), None);
        let _ = partial_rollout.is_enabled_for(None, Some("repo"));
        let _ = partial_rollout.is_enabled_for(Some("user"), Some("repo"));

        let mut enabled_count = 0;
        let total = 1000;
        for i in 0..total {
            if partial_rollout.is_enabled_for(Some(&format!("user{}", i)), None) {
                enabled_count += 1;
            }
        }
        assert!(enabled_count > 400 && enabled_count < 600);
    }

    #[test]
    fn test_feature_flags_collection_management() {
        let flags = FeatureFlags::new();
        assert_eq!(flags.flag_count(), 0);
        assert_eq!(flags.version(), 0);

        let default_flags = FeatureFlags::default();
        assert_eq!(default_flags.flag_count(), 0);
        assert_eq!(default_flags.version(), 0);

        let mut flags = FeatureFlags::new();
        flags.add_flag(FeatureFlag::new("flag1", true));
        assert_eq!(flags.flag_count(), 1);
        assert_eq!(flags.version(), 1);

        flags.add_flag(FeatureFlag::new("flag2", false));
        assert_eq!(flags.flag_count(), 2);
        assert_eq!(flags.version(), 2);

        flags.add_flag(FeatureFlag::new("flag1", false));
        assert!(!flags.is_enabled("flag1"));
        assert_eq!(flags.flag_count(), 2);
        assert_eq!(flags.version(), 3);

        flags.add_flag(FeatureFlag::new("flag3", true));
        let all: Vec<_> = flags.all_flags().collect();
        assert_eq!(all.len(), 3);

        let enabled: Vec<_> = flags.enabled_flags().collect();
        assert_eq!(enabled.len(), 1);
        assert!(enabled.iter().all(|f| f.enabled));

        let removed = flags.remove_flag("flag1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "flag1");
        assert_eq!(flags.flag_count(), 2);
        assert_eq!(flags.version(), 5);

        let not_removed = flags.remove_flag("nonexistent");
        assert!(not_removed.is_none());
        assert_eq!(flags.version(), 5);

        let empty_flags = FeatureFlags::new();
        let all_empty: Vec<_> = empty_flags.all_flags().collect();
        assert!(all_empty.is_empty());
        let enabled_empty: Vec<_> = empty_flags.enabled_flags().collect();
        assert!(enabled_empty.is_empty());

        let mut all_disabled = FeatureFlags::new();
        all_disabled.add_flag(FeatureFlag::new("d1", false));
        all_disabled.add_flag(FeatureFlag::new("d2", false));
        let enabled_from_disabled: Vec<_> = all_disabled.enabled_flags().collect();
        assert!(enabled_from_disabled.is_empty());
    }

    #[test]
    fn test_feature_flags_query_and_state_operations() {
        let mut flags = FeatureFlags::new();
        flags.add_flag(FeatureFlag::new("enabled", true).with_description("desc"));
        flags.add_flag(FeatureFlag::new("disabled", false));
        flags.add_flag(FeatureFlag::new("user_restricted", true).with_allowed_user("user1"));
        flags.add_flag(FeatureFlag::new("repo_restricted", true).with_allowed_repo("repo1"));
        flags.add_flag(
            FeatureFlag::new("combined", true)
                .with_allowed_user("user1")
                .with_allowed_repo("repo1"),
        );

        let flag = flags.get_flag("enabled");
        assert!(flag.is_some());
        assert_eq!(flag.unwrap().name, "enabled");
        assert_eq!(flag.unwrap().description, Some("desc".to_string()));
        assert!(flags.get_flag("nonexistent").is_none());

        assert!(flags.is_enabled("enabled"));
        assert!(!flags.is_enabled("disabled"));
        assert!(!flags.is_enabled("nonexistent"));

        assert!(flags.is_enabled_for("user_restricted", Some("user1"), None));
        assert!(!flags.is_enabled_for("user_restricted", Some("user2"), None));
        assert!(!flags.is_enabled_for("nonexistent", Some("user1"), None));

        assert!(flags.is_enabled_for("repo_restricted", None, Some("repo1")));
        assert!(!flags.is_enabled_for("repo_restricted", None, Some("repo2")));

        assert!(flags.is_enabled_for("combined", Some("user1"), Some("repo1")));
        assert!(!flags.is_enabled_for("combined", Some("user2"), Some("repo1")));
        assert!(!flags.is_enabled_for("combined", Some("user1"), Some("repo2")));

        let initial_version = flags.version();
        let result = flags.set_enabled("disabled", true);
        assert!(result);
        assert!(flags.is_enabled("disabled"));
        assert_eq!(flags.version(), initial_version + 1);

        let result = flags.set_enabled("disabled", false);
        assert!(result);
        assert!(!flags.is_enabled("disabled"));
        assert_eq!(flags.version(), initial_version + 2);

        let result = flags.set_enabled("nonexistent", true);
        assert!(!result);
        assert_eq!(flags.version(), initial_version + 2);
    }

    #[test]
    fn test_feature_flag_serialization_and_metadata() {
        let flag = FeatureFlag::new("test_flag", true)
            .with_description("A test description")
            .with_rollout(75)
            .with_allowed_user("user1")
            .with_allowed_repo("repo1");

        let json = serde_json::to_string(&flag).unwrap();
        let deserialized: FeatureFlag = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test_flag");
        assert!(deserialized.enabled);
        assert_eq!(
            deserialized.description,
            Some("A test description".to_string())
        );
        assert_eq!(deserialized.rollout_percentage, Some(75));
        assert_eq!(deserialized.allowed_users, vec!["user1".to_string()]);
        assert_eq!(deserialized.allowed_repos, vec!["repo1".to_string()]);

        let mut meta_flag = FeatureFlag::new("meta_flag", true);
        assert!(meta_flag.metadata.is_empty());
        meta_flag
            .metadata
            .insert("env".to_string(), "production".to_string());
        meta_flag
            .metadata
            .insert("team".to_string(), "platform".to_string());
        assert_eq!(
            meta_flag.metadata.get("env"),
            Some(&"production".to_string())
        );
        assert_eq!(
            meta_flag.metadata.get("team"),
            Some(&"platform".to_string())
        );

        let json = serde_json::to_string(&meta_flag).unwrap();
        let deserialized: FeatureFlag = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.metadata.get("env"),
            Some(&"production".to_string())
        );
        assert_eq!(
            deserialized.metadata.get("team"),
            Some(&"platform".to_string())
        );
    }
}
