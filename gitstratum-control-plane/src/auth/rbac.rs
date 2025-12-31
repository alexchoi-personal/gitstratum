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
