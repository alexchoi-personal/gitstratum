use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    RepoRead,
    RepoWrite,
    RepoPush,
    RepoAdmin,
    ClusterRead,
    ClusterWrite,
    ClusterAdmin,
    UserRead,
    UserWrite,
    UserAdmin,
}

impl Permission {
    pub fn implies(&self, other: &Permission) -> bool {
        match (self, other) {
            (Permission::RepoAdmin, Permission::RepoRead)
            | (Permission::RepoAdmin, Permission::RepoWrite)
            | (Permission::RepoAdmin, Permission::RepoPush) => true,

            (Permission::RepoWrite, Permission::RepoRead) => true,
            (Permission::RepoPush, Permission::RepoRead) => true,

            (Permission::ClusterAdmin, Permission::ClusterRead)
            | (Permission::ClusterAdmin, Permission::ClusterWrite) => true,
            (Permission::ClusterWrite, Permission::ClusterRead) => true,

            (Permission::UserAdmin, Permission::UserRead)
            | (Permission::UserAdmin, Permission::UserWrite) => true,
            (Permission::UserWrite, Permission::UserRead) => true,

            (a, b) => a == b,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
    pub description: Option<String>,
}

impl Role {
    pub fn new(name: impl Into<String>, permissions: Vec<Permission>) -> Self {
        Self {
            name: name.into(),
            permissions: permissions.into_iter().collect(),
            description: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.permissions
            .iter()
            .any(|p| p.implies(permission) || p == permission)
    }

    pub fn add_permission(&mut self, permission: Permission) {
        self.permissions.insert(permission);
    }

    pub fn remove_permission(&mut self, permission: &Permission) {
        self.permissions.remove(permission);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleBinding {
    pub user_id: String,
    pub role_name: String,
    pub resource_pattern: Option<String>,
    pub created_at: u64,
    pub expires_at: Option<u64>,
}

impl RoleBinding {
    pub fn new(user_id: impl Into<String>, role_name: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            user_id: user_id.into(),
            role_name: role_name.into(),
            resource_pattern: None,
            created_at: now,
            expires_at: None,
        }
    }

    pub fn with_resource_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.resource_pattern = Some(pattern.into());
        self
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            now >= expires_at
        } else {
            false
        }
    }

    pub fn matches_resource(&self, resource: &str) -> bool {
        match &self.resource_pattern {
            None => true,
            Some(pattern) => {
                if pattern.ends_with('*') {
                    let prefix = &pattern[..pattern.len() - 1];
                    resource.starts_with(prefix)
                } else {
                    resource == pattern
                }
            }
        }
    }
}

pub struct RoleBindings {
    roles: HashMap<String, Role>,
    bindings: HashMap<String, Vec<RoleBinding>>,
}

impl RoleBindings {
    pub fn new() -> Self {
        Self {
            roles: HashMap::new(),
            bindings: HashMap::new(),
        }
    }

    pub fn add_role(&mut self, role: Role) {
        self.roles.insert(role.name.clone(), role);
    }

    pub fn get_role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }

    pub fn remove_role(&mut self, name: &str) -> Option<Role> {
        self.roles.remove(name)
    }

    pub fn add_binding(&mut self, binding: RoleBinding) {
        self.bindings
            .entry(binding.user_id.clone())
            .or_default()
            .push(binding);
    }

    pub fn get_user_bindings(&self, user_id: &str) -> Vec<&RoleBinding> {
        self.bindings
            .get(user_id)
            .map(|bindings| bindings.iter().collect())
            .unwrap_or_default()
    }

    pub fn check_permission(
        &self,
        user_id: &str,
        permission: &Permission,
        resource: Option<&str>,
    ) -> bool {
        let bindings = match self.bindings.get(user_id) {
            Some(b) => b,
            None => return false,
        };

        for binding in bindings {
            if binding.is_expired() {
                continue;
            }

            if let Some(res) = resource {
                if !binding.matches_resource(res) {
                    continue;
                }
            }

            if let Some(role) = self.roles.get(&binding.role_name) {
                if role.has_permission(permission) {
                    return true;
                }
            }
        }

        false
    }

    pub fn role_count(&self) -> usize {
        self.roles.len()
    }

    pub fn binding_count(&self) -> usize {
        self.bindings.values().map(|v| v.len()).sum()
    }
}

impl Default for RoleBindings {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_hierarchy_and_traits() {
        let all_permissions = [
            Permission::RepoRead,
            Permission::RepoWrite,
            Permission::RepoPush,
            Permission::RepoAdmin,
            Permission::ClusterRead,
            Permission::ClusterWrite,
            Permission::ClusterAdmin,
            Permission::UserRead,
            Permission::UserWrite,
            Permission::UserAdmin,
        ];

        for perm in &all_permissions {
            assert!(perm.implies(perm));

            let cloned = perm.clone();
            assert_eq!(*perm, cloned);

            let copied: Permission = *perm;
            assert_eq!(*perm, copied);

            let debug_str = format!("{:?}", perm);
            assert!(!debug_str.is_empty());

            let serialized = serde_json::to_string(perm).unwrap();
            let deserialized: Permission = serde_json::from_str(&serialized).unwrap();
            assert_eq!(*perm, deserialized);
        }

        let mut set = HashSet::new();
        set.insert(Permission::RepoRead);
        set.insert(Permission::RepoWrite);
        set.insert(Permission::RepoRead);
        assert_eq!(set.len(), 2);

        assert!(Permission::RepoAdmin.implies(&Permission::RepoRead));
        assert!(Permission::RepoAdmin.implies(&Permission::RepoWrite));
        assert!(Permission::RepoAdmin.implies(&Permission::RepoPush));
        assert!(Permission::RepoWrite.implies(&Permission::RepoRead));
        assert!(Permission::RepoPush.implies(&Permission::RepoRead));
        assert!(!Permission::RepoWrite.implies(&Permission::RepoAdmin));
        assert!(!Permission::RepoPush.implies(&Permission::RepoWrite));
        assert!(!Permission::RepoRead.implies(&Permission::RepoWrite));

        assert!(Permission::ClusterAdmin.implies(&Permission::ClusterRead));
        assert!(Permission::ClusterAdmin.implies(&Permission::ClusterWrite));
        assert!(Permission::ClusterWrite.implies(&Permission::ClusterRead));
        assert!(!Permission::ClusterWrite.implies(&Permission::ClusterAdmin));
        assert!(!Permission::ClusterRead.implies(&Permission::ClusterWrite));

        assert!(Permission::UserAdmin.implies(&Permission::UserRead));
        assert!(Permission::UserAdmin.implies(&Permission::UserWrite));
        assert!(Permission::UserWrite.implies(&Permission::UserRead));
        assert!(!Permission::UserWrite.implies(&Permission::UserAdmin));
        assert!(!Permission::UserRead.implies(&Permission::UserWrite));

        assert!(!Permission::RepoAdmin.implies(&Permission::ClusterRead));
        assert!(!Permission::ClusterAdmin.implies(&Permission::UserRead));
        assert!(!Permission::UserAdmin.implies(&Permission::RepoRead));
    }

    #[test]
    fn test_role_management_and_permissions() {
        let role = Role::new(
            "admin",
            vec![Permission::RepoAdmin, Permission::ClusterAdmin],
        );
        assert_eq!(role.name, "admin");
        assert!(role.permissions.contains(&Permission::RepoAdmin));
        assert!(role.permissions.contains(&Permission::ClusterAdmin));
        assert!(role.description.is_none());

        let role_from_string = Role::new(String::from("user"), vec![Permission::RepoRead]);
        assert_eq!(role_from_string.name, "user");

        let role_with_desc =
            Role::new("admin", vec![Permission::RepoAdmin]).with_description("Full admin access");
        assert_eq!(
            role_with_desc.description,
            Some("Full admin access".to_string())
        );

        let role_with_string_desc = Role::new("admin", vec![Permission::RepoAdmin])
            .with_description(String::from("Admin role"));
        assert_eq!(
            role_with_string_desc.description,
            Some("Admin role".to_string())
        );

        let writer_role = Role::new("writer", vec![Permission::RepoWrite]);
        assert!(writer_role.has_permission(&Permission::RepoWrite));
        assert!(writer_role.has_permission(&Permission::RepoRead));
        assert!(!writer_role.has_permission(&Permission::RepoAdmin));
        assert!(!writer_role.has_permission(&Permission::RepoPush));

        let admin_role = Role::new("admin", vec![Permission::RepoAdmin]);
        assert!(admin_role.has_permission(&Permission::RepoRead));
        assert!(admin_role.has_permission(&Permission::RepoWrite));
        assert!(admin_role.has_permission(&Permission::RepoPush));
        assert!(admin_role.has_permission(&Permission::RepoAdmin));

        let mut mutable_role = Role::new("custom", vec![Permission::RepoRead]);
        assert!(!mutable_role.has_permission(&Permission::RepoWrite));
        mutable_role.add_permission(Permission::RepoWrite);
        assert!(mutable_role.has_permission(&Permission::RepoWrite));
        let len_before = mutable_role.permissions.len();
        mutable_role.add_permission(Permission::RepoRead);
        assert_eq!(mutable_role.permissions.len(), len_before);

        let mut removable_role =
            Role::new("custom", vec![Permission::RepoRead, Permission::RepoWrite]);
        removable_role.remove_permission(&Permission::RepoWrite);
        assert!(!removable_role.permissions.contains(&Permission::RepoWrite));
        assert!(removable_role.permissions.contains(&Permission::RepoRead));
        let len_after_remove = removable_role.permissions.len();
        removable_role.remove_permission(&Permission::RepoAdmin);
        assert_eq!(removable_role.permissions.len(), len_after_remove);

        let debug_str = format!("{:?}", role);
        assert!(debug_str.contains("Role"));
        assert!(debug_str.contains("admin"));

        let original =
            Role::new("test", vec![Permission::RepoRead]).with_description("Test role");
        let cloned = original.clone();
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.permissions, cloned.permissions);
        assert_eq!(original.description, cloned.description);

        let serializable = Role::new(
            "admin",
            vec![Permission::RepoAdmin, Permission::ClusterAdmin],
        )
        .with_description("Administrator");
        let serialized = serde_json::to_string(&serializable).unwrap();
        let deserialized: Role = serde_json::from_str(&serialized).unwrap();
        assert_eq!(serializable.name, deserialized.name);
        assert_eq!(serializable.permissions, deserialized.permissions);
        assert_eq!(serializable.description, deserialized.description);
    }

    #[test]
    fn test_role_binding_lifecycle() {
        let binding = RoleBinding::new("user-1", "admin");
        assert_eq!(binding.user_id, "user-1");
        assert_eq!(binding.role_name, "admin");
        assert!(binding.resource_pattern.is_none());
        assert!(binding.created_at > 0);
        assert!(binding.expires_at.is_none());

        let binding_from_strings =
            RoleBinding::new(String::from("user-2"), String::from("writer"));
        assert_eq!(binding_from_strings.user_id, "user-2");
        assert_eq!(binding_from_strings.role_name, "writer");

        let binding_with_pattern =
            RoleBinding::new("user-1", "admin").with_resource_pattern("repo:org/*");
        assert_eq!(
            binding_with_pattern.resource_pattern,
            Some("repo:org/*".to_string())
        );

        let binding_with_string_pattern =
            RoleBinding::new("user-1", "admin").with_resource_pattern(String::from("repo:test"));
        assert_eq!(
            binding_with_string_pattern.resource_pattern,
            Some("repo:test".to_string())
        );

        let non_expiring = RoleBinding::new("user-1", "admin");
        assert!(!non_expiring.is_expired());

        let mut future_expiring = RoleBinding::new("user-1", "admin");
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        future_expiring.expires_at = Some(future);
        assert!(!future_expiring.is_expired());

        let mut past_expired = RoleBinding::new("user-1", "admin");
        past_expired.expires_at = Some(1);
        assert!(past_expired.is_expired());

        let no_pattern = RoleBinding::new("user-1", "admin");
        assert!(no_pattern.matches_resource("any/resource"));
        assert!(no_pattern.matches_resource("another/resource"));

        let exact_pattern =
            RoleBinding::new("user-1", "admin").with_resource_pattern("repo:org/project");
        assert!(exact_pattern.matches_resource("repo:org/project"));
        assert!(!exact_pattern.matches_resource("repo:org/other"));
        assert!(!exact_pattern.matches_resource("repo:org/project/sub"));

        let wildcard_pattern =
            RoleBinding::new("user-1", "admin").with_resource_pattern("repo:org/*");
        assert!(wildcard_pattern.matches_resource("repo:org/project1"));
        assert!(wildcard_pattern.matches_resource("repo:org/project2"));
        assert!(wildcard_pattern.matches_resource("repo:org/"));
        assert!(!wildcard_pattern.matches_resource("repo:other/project"));

        let debug_binding = RoleBinding::new("user-1", "admin");
        let debug_str = format!("{:?}", debug_binding);
        assert!(debug_str.contains("RoleBinding"));
        assert!(debug_str.contains("user-1"));

        let original = RoleBinding::new("user-1", "admin").with_resource_pattern("repo:*");
        let cloned = original.clone();
        assert_eq!(original.user_id, cloned.user_id);
        assert_eq!(original.role_name, cloned.role_name);
        assert_eq!(original.resource_pattern, cloned.resource_pattern);
        assert_eq!(original.created_at, cloned.created_at);
        assert_eq!(original.expires_at, cloned.expires_at);

        let mut serializable =
            RoleBinding::new("user-1", "admin").with_resource_pattern("repo:*");
        serializable.expires_at = Some(9999999999);
        let serialized = serde_json::to_string(&serializable).unwrap();
        let deserialized: RoleBinding = serde_json::from_str(&serialized).unwrap();
        assert_eq!(serializable.user_id, deserialized.user_id);
        assert_eq!(serializable.role_name, deserialized.role_name);
        assert_eq!(serializable.resource_pattern, deserialized.resource_pattern);
        assert_eq!(serializable.expires_at, deserialized.expires_at);
    }

    #[test]
    fn test_role_bindings_registry_operations() {
        let new_bindings = RoleBindings::new();
        assert_eq!(new_bindings.role_count(), 0);
        assert_eq!(new_bindings.binding_count(), 0);

        let default_bindings = RoleBindings::default();
        assert_eq!(default_bindings.role_count(), 0);
        assert_eq!(default_bindings.binding_count(), 0);

        let mut bindings = RoleBindings::new();
        bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        assert_eq!(bindings.role_count(), 1);

        bindings.add_role(Role::new("admin", vec![Permission::RepoRead]));
        bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        assert_eq!(bindings.role_count(), 1);
        let role = bindings.get_role("admin").unwrap();
        assert!(role.has_permission(&Permission::RepoAdmin));

        let retrieved = bindings.get_role("admin");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "admin");
        assert!(bindings.get_role("nonexistent").is_none());

        let removed = bindings.remove_role("admin");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "admin");
        assert_eq!(bindings.role_count(), 0);
        assert!(bindings.remove_role("nonexistent").is_none());

        bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        bindings.add_binding(RoleBinding::new("user-1", "admin"));
        assert_eq!(bindings.binding_count(), 1);

        bindings.add_binding(RoleBinding::new("user-1", "writer"));
        assert_eq!(bindings.binding_count(), 2);
        let user_bindings = bindings.get_user_bindings("user-1");
        assert_eq!(user_bindings.len(), 2);

        bindings.add_binding(RoleBinding::new("user-2", "reader"));
        let user1_bindings = bindings.get_user_bindings("user-1");
        assert_eq!(user1_bindings.len(), 2);
        assert!(bindings.get_user_bindings("nonexistent").is_empty());

        let mut counter = RoleBindings::new();
        assert_eq!(counter.role_count(), 0);
        counter.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        assert_eq!(counter.role_count(), 1);
        counter.add_role(Role::new("reader", vec![Permission::RepoRead]));
        assert_eq!(counter.role_count(), 2);

        assert_eq!(counter.binding_count(), 0);
        counter.add_binding(RoleBinding::new("user-1", "admin"));
        assert_eq!(counter.binding_count(), 1);
        counter.add_binding(RoleBinding::new("user-1", "reader"));
        assert_eq!(counter.binding_count(), 2);
        counter.add_binding(RoleBinding::new("user-2", "admin"));
        assert_eq!(counter.binding_count(), 3);
    }

    #[test]
    fn test_permission_checking_comprehensive() {
        let mut bindings = RoleBindings::new();
        bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        bindings.add_binding(RoleBinding::new("user-1", "admin"));

        assert!(bindings.check_permission("user-1", &Permission::RepoAdmin, None));

        assert!(bindings.check_permission("user-1", &Permission::RepoRead, None));
        assert!(bindings.check_permission("user-1", &Permission::RepoWrite, None));
        assert!(bindings.check_permission("user-1", &Permission::RepoPush, None));

        assert!(!bindings.check_permission("unknown-user", &Permission::RepoRead, None));

        let mut orphan_bindings = RoleBindings::new();
        orphan_bindings.add_binding(RoleBinding::new("user-1", "nonexistent-role"));
        assert!(!orphan_bindings.check_permission("user-1", &Permission::RepoRead, None));

        let mut expired_bindings = RoleBindings::new();
        expired_bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        let mut expired_binding = RoleBinding::new("user-1", "admin");
        expired_binding.expires_at = Some(1);
        expired_bindings.add_binding(expired_binding);
        assert!(!expired_bindings.check_permission("user-1", &Permission::RepoRead, None));

        let mut scoped_bindings = RoleBindings::new();
        scoped_bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        scoped_bindings
            .add_binding(RoleBinding::new("user-1", "admin").with_resource_pattern("repo:org/*"));
        assert!(scoped_bindings.check_permission(
            "user-1",
            &Permission::RepoRead,
            Some("repo:org/project"),
        ));
        assert!(!scoped_bindings.check_permission(
            "user-1",
            &Permission::RepoRead,
            Some("repo:other/project"),
        ));

        let mut limited_bindings = RoleBindings::new();
        limited_bindings.add_role(Role::new("reader", vec![Permission::RepoRead]));
        limited_bindings.add_binding(RoleBinding::new("user-1", "reader"));
        assert!(!limited_bindings.check_permission("user-1", &Permission::RepoAdmin, None));
        assert!(!limited_bindings.check_permission("user-1", &Permission::RepoWrite, None));

        let mut multi_role_bindings = RoleBindings::new();
        multi_role_bindings.add_role(Role::new("reader", vec![Permission::RepoRead]));
        multi_role_bindings.add_role(Role::new("writer", vec![Permission::RepoWrite]));
        multi_role_bindings.add_binding(RoleBinding::new("user-1", "reader"));
        multi_role_bindings.add_binding(RoleBinding::new("user-1", "writer"));
        assert!(multi_role_bindings.check_permission("user-1", &Permission::RepoRead, None));
        assert!(multi_role_bindings.check_permission("user-1", &Permission::RepoWrite, None));

        let mut mixed_bindings = RoleBindings::new();
        mixed_bindings.add_role(Role::new("admin", vec![Permission::RepoAdmin]));
        mixed_bindings.add_role(Role::new("reader", vec![Permission::RepoRead]));
        let mut expired_admin = RoleBinding::new("user-1", "admin");
        expired_admin.expires_at = Some(1);
        mixed_bindings.add_binding(expired_admin);
        mixed_bindings.add_binding(RoleBinding::new("user-1", "reader"));
        assert!(mixed_bindings.check_permission("user-1", &Permission::RepoRead, None));
        assert!(!mixed_bindings.check_permission("user-1", &Permission::RepoAdmin, None));
    }
}
