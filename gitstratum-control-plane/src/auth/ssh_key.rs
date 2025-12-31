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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

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
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
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
