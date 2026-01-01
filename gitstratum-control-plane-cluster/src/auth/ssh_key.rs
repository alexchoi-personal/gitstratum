use crate::time::current_timestamp_secs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SshKey {
    pub id: String,
    pub user_id: String,
    pub key_type: String,
    pub public_key: String,
    pub fingerprint: String,
    pub created_at: u64,
    pub last_used_at: Option<u64>,
    pub expires_at: Option<u64>,
}

impl SshKey {
    pub fn new(
        id: impl Into<String>,
        user_id: impl Into<String>,
        key_type: impl Into<String>,
        public_key: impl Into<String>,
    ) -> Self {
        let now = current_timestamp_secs();

        let public_key = public_key.into();
        let fingerprint = Self::compute_fingerprint(&public_key);

        Self {
            id: id.into(),
            user_id: user_id.into(),
            key_type: key_type.into(),
            public_key,
            fingerprint,
            created_at: now,
            last_used_at: None,
            expires_at: None,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = current_timestamp_secs();

            now >= expires_at
        } else {
            false
        }
    }

    fn compute_fingerprint(public_key: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        public_key.hash(&mut hasher);
        format!("SHA256:{:016x}", hasher.finish())
    }

    pub fn record_usage(&mut self) {
        let now = current_timestamp_secs();
        self.last_used_at = Some(now);
    }
}

pub struct SshKeyStore {
    keys_by_fingerprint: HashMap<String, SshKey>,
    keys_by_user: HashMap<String, Vec<String>>,
}

impl SshKeyStore {
    pub fn new() -> Self {
        Self {
            keys_by_fingerprint: HashMap::new(),
            keys_by_user: HashMap::new(),
        }
    }

    pub fn add_key(&mut self, key: SshKey) {
        let fingerprint = key.fingerprint.clone();
        let user_id = key.user_id.clone();

        self.keys_by_fingerprint.insert(fingerprint.clone(), key);

        self.keys_by_user
            .entry(user_id)
            .or_default()
            .push(fingerprint);
    }

    pub fn remove_key(&mut self, fingerprint: &str) -> Option<SshKey> {
        if let Some(key) = self.keys_by_fingerprint.remove(fingerprint) {
            if let Some(user_keys) = self.keys_by_user.get_mut(&key.user_id) {
                user_keys.retain(|fp| fp != fingerprint);
            }
            Some(key)
        } else {
            None
        }
    }

    pub fn get_key(&self, fingerprint: &str) -> Option<&SshKey> {
        self.keys_by_fingerprint.get(fingerprint)
    }

    pub fn get_key_mut(&mut self, fingerprint: &str) -> Option<&mut SshKey> {
        self.keys_by_fingerprint.get_mut(fingerprint)
    }

    pub fn get_user_keys(&self, user_id: &str) -> Vec<&SshKey> {
        self.keys_by_user
            .get(user_id)
            .map(|fingerprints| {
                fingerprints
                    .iter()
                    .filter_map(|fp| self.keys_by_fingerprint.get(fp))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn validate_key(&self, fingerprint: &str) -> Result<&SshKey, SshKeyValidationError> {
        let key = self
            .keys_by_fingerprint
            .get(fingerprint)
            .ok_or(SshKeyValidationError::KeyNotFound)?;

        if key.is_expired() {
            return Err(SshKeyValidationError::KeyExpired);
        }

        Ok(key)
    }

    pub fn key_count(&self) -> usize {
        self.keys_by_fingerprint.len()
    }
}

impl Default for SshKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshKeyValidationError {
    KeyNotFound,
    KeyExpired,
    KeyRevoked,
}

impl std::fmt::Display for SshKeyValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNotFound => write!(f, "SSH key not found"),
            Self::KeyExpired => write!(f, "SSH key expired"),
            Self::KeyRevoked => write!(f, "SSH key revoked"),
        }
    }
}

impl std::error::Error for SshKeyValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssh_key_lifecycle() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let key = SshKey::new("key-1", "user-1", "ssh-ed25519", "AAAAC3NzaC1lZDI1NTE5");
        assert_eq!(key.id, "key-1");
        assert_eq!(key.user_id, "user-1");
        assert_eq!(key.key_type, "ssh-ed25519");
        assert_eq!(key.public_key, "AAAAC3NzaC1lZDI1NTE5");
        assert!(key.fingerprint.starts_with("SHA256:"));
        assert!(key.created_at > 0);
        assert!(key.last_used_at.is_none());
        assert!(key.expires_at.is_none());

        let key_from_strings = SshKey::new(
            String::from("key-2"),
            String::from("user-2"),
            String::from("ssh-rsa"),
            String::from("AAAAB3NzaC1yc2E"),
        );
        assert_eq!(key_from_strings.id, "key-2");
        assert_eq!(key_from_strings.user_id, "user-2");
        assert_eq!(key_from_strings.key_type, "ssh-rsa");

        let key1 = SshKey::new("k1", "u1", "ssh-ed25519", "same-public-key");
        let key2 = SshKey::new("k2", "u2", "ssh-rsa", "same-public-key");
        assert_eq!(key1.fingerprint, key2.fingerprint);

        let key3 = SshKey::new("k3", "u1", "ssh-ed25519", "different-public-key");
        assert_ne!(key1.fingerprint, key3.fingerprint);

        assert!(!key.is_expired());

        let mut key_with_future_expiry = SshKey::new("k", "u", "ssh-ed25519", "pk");
        key_with_future_expiry.expires_at = Some(now + 3600);
        assert!(!key_with_future_expiry.is_expired());

        let mut expired_key = SshKey::new("k", "u", "ssh-ed25519", "pk2");
        expired_key.expires_at = Some(1);
        assert!(expired_key.is_expired());

        let mut usage_key = SshKey::new("k", "u", "ssh-ed25519", "pk3");
        assert!(usage_key.last_used_at.is_none());
        usage_key.record_usage();
        assert!(usage_key.last_used_at.is_some());
        assert!(usage_key.last_used_at.unwrap() >= now);
        assert!(usage_key.last_used_at.unwrap() <= now + 1);
    }

    #[test]
    fn test_ssh_key_store_multi_user_operations() {
        let store = SshKeyStore::new();
        assert_eq!(store.key_count(), 0);

        let default_store = SshKeyStore::default();
        assert_eq!(default_store.key_count(), 0);

        let mut store = SshKeyStore::new();

        let key1 = SshKey::new("key-1", "user-1", "ssh-ed25519", "key1-data");
        let key2 = SshKey::new("key-2", "user-1", "ssh-rsa", "key2-data");
        let key3 = SshKey::new("key-3", "user-2", "ssh-ed25519", "key3-data");
        let fp1 = key1.fingerprint.clone();
        let fp2 = key2.fingerprint.clone();
        let fp3 = key3.fingerprint.clone();

        store.add_key(key1);
        assert_eq!(store.key_count(), 1);
        assert!(store.get_key(&fp1).is_some());

        store.add_key(key2);
        store.add_key(key3);
        assert_eq!(store.key_count(), 3);

        let user1_keys = store.get_user_keys("user-1");
        assert_eq!(user1_keys.len(), 2);

        let user2_keys = store.get_user_keys("user-2");
        assert_eq!(user2_keys.len(), 1);
        assert_eq!(user2_keys[0].id, "key-3");

        let nonexistent_user_keys = store.get_user_keys("nonexistent-user");
        assert!(nonexistent_user_keys.is_empty());

        let retrieved = store.get_key(&fp1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, "key-1");

        assert!(store.get_key("nonexistent").is_none());

        let key_mut = store.get_key_mut(&fp1);
        assert!(key_mut.is_some());
        let key_mut = key_mut.unwrap();
        key_mut.record_usage();
        assert!(key_mut.last_used_at.is_some());

        assert!(store.get_key_mut("nonexistent").is_none());

        let removed = store.remove_key(&fp1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, "key-1");
        assert_eq!(store.key_count(), 2);

        let user1_keys_after = store.get_user_keys("user-1");
        assert_eq!(user1_keys_after.len(), 1);
        assert_eq!(user1_keys_after[0].id, "key-2");

        let not_found = store.remove_key("nonexistent");
        assert!(not_found.is_none());

        store.remove_key(&fp2);
        store.remove_key(&fp3);
        assert_eq!(store.key_count(), 0);
    }

    #[test]
    fn test_ssh_key_validation_workflow() {
        let mut store = SshKeyStore::new();

        let valid_key = SshKey::new("key-1", "user-1", "ssh-ed25519", "valid-key-data");
        let fp_valid = valid_key.fingerprint.clone();
        store.add_key(valid_key);

        let result = store.validate_key(&fp_valid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "key-1");

        let result = store.validate_key("nonexistent-fingerprint");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), SshKeyValidationError::KeyNotFound);

        let mut expired_key = SshKey::new("key-2", "user-1", "ssh-ed25519", "expired-key-data");
        expired_key.expires_at = Some(1);
        let fp_expired = expired_key.fingerprint.clone();
        store.add_key(expired_key);

        let result = store.validate_key(&fp_expired);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), SshKeyValidationError::KeyExpired);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut future_key = SshKey::new("key-3", "user-1", "ssh-ed25519", "future-key-data");
        future_key.expires_at = Some(now + 3600);
        let fp_future = future_key.fingerprint.clone();
        store.add_key(future_key);

        let result = store.validate_key(&fp_future);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "key-3");
    }

    #[test]
    fn test_ssh_key_traits_and_error_handling() {
        let key = SshKey::new("key-1", "user-1", "ssh-ed25519", "AAAAC3NzaC1lZDI1NTE5");

        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("SshKey"));
        assert!(debug_str.contains("key-1"));

        let cloned = key.clone();
        assert_eq!(key, cloned);

        let serialized = serde_json::to_string(&key).unwrap();
        let deserialized: SshKey = serde_json::from_str(&serialized).unwrap();
        assert_eq!(key, deserialized);

        assert_eq!(
            format!("{}", SshKeyValidationError::KeyNotFound),
            "SSH key not found"
        );
        assert_eq!(
            format!("{}", SshKeyValidationError::KeyExpired),
            "SSH key expired"
        );
        assert_eq!(
            format!("{}", SshKeyValidationError::KeyRevoked),
            "SSH key revoked"
        );

        assert_eq!(
            format!("{:?}", SshKeyValidationError::KeyNotFound),
            "KeyNotFound"
        );
        assert_eq!(
            format!("{:?}", SshKeyValidationError::KeyExpired),
            "KeyExpired"
        );
        assert_eq!(
            format!("{:?}", SshKeyValidationError::KeyRevoked),
            "KeyRevoked"
        );

        let error = SshKeyValidationError::KeyExpired;
        let cloned_error = error.clone();
        assert_eq!(error, cloned_error);

        assert_eq!(
            SshKeyValidationError::KeyNotFound,
            SshKeyValidationError::KeyNotFound
        );
        assert_ne!(
            SshKeyValidationError::KeyNotFound,
            SshKeyValidationError::KeyExpired
        );
        assert_ne!(
            SshKeyValidationError::KeyExpired,
            SshKeyValidationError::KeyRevoked
        );
        assert_ne!(
            SshKeyValidationError::KeyNotFound,
            SshKeyValidationError::KeyRevoked
        );

        let boxed_error: Box<dyn std::error::Error> = Box::new(SshKeyValidationError::KeyNotFound);
        assert_eq!(boxed_error.to_string(), "SSH key not found");

        let boxed_expired: Box<dyn std::error::Error> = Box::new(SshKeyValidationError::KeyExpired);
        assert_eq!(boxed_expired.to_string(), "SSH key expired");

        let boxed_revoked: Box<dyn std::error::Error> = Box::new(SshKeyValidationError::KeyRevoked);
        assert_eq!(boxed_revoked.to_string(), "SSH key revoked");
    }
}
