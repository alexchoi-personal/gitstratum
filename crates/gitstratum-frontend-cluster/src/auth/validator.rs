use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use super::error::AuthError;
use super::metrics::{record_auth_attempt, record_auth_duration};
use super::rate_limit::AuthRateLimiter;
use super::types::{StoredToken, User, UserStatus};

pub struct AuthResult {
    pub authenticated: bool,
    pub user_id: String,
    pub permissions: HashSet<String>,
}

impl AuthResult {
    pub fn authenticated(user_id: String) -> Self {
        Self {
            authenticated: true,
            user_id,
            permissions: HashSet::new(),
        }
    }

    pub fn unauthenticated() -> Self {
        Self {
            authenticated: false,
            user_id: String::new(),
            permissions: HashSet::new(),
        }
    }

    pub fn with_permission(mut self, perm: &str) -> Self {
        self.permissions.insert(perm.to_string());
        self
    }

    pub fn can_read(&self) -> bool {
        self.permissions.contains("read")
    }

    pub fn can_write(&self) -> bool {
        self.permissions.contains("write")
    }

    pub fn is_admin(&self) -> bool {
        self.permissions.contains("admin")
    }
}

pub trait AuthStore: Send + Sync {
    fn get_token_by_hash(&self, hash: &str) -> Result<Option<StoredToken>, AuthError>;
    fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError>;
    fn get_ssh_key_user(&self, fingerprint: &str) -> Result<Option<String>, AuthError>;
}

pub struct LocalValidator<S: AuthStore> {
    store: Arc<S>,
    rate_limiter: Arc<AuthRateLimiter>,
}

impl<S: AuthStore> LocalValidator<S> {
    pub fn new(store: Arc<S>, rate_limiter: Arc<AuthRateLimiter>) -> Self {
        Self {
            store,
            rate_limiter,
        }
    }

    pub fn validate_token(&self, token: &str) -> Result<AuthResult, AuthError> {
        let start = Instant::now();
        let token_key = format!("token:{}", &token[..token.len().min(8)]);

        if !self.rate_limiter.check(&token_key) {
            record_auth_attempt("pat", false);
            return Err(AuthError::RateLimitExceeded);
        }

        let hash = sha2_hash(token);
        let stored = match self.store.get_token_by_hash(&hash)? {
            Some(t) => t,
            None => {
                self.rate_limiter.record_failure(&token_key);
                record_auth_attempt("pat", false);
                return Err(AuthError::InvalidCredentials);
            }
        };

        if let Some(exp) = stored.expires_at {
            let now = chrono::Utc::now().timestamp();
            if exp < now {
                record_auth_attempt("pat", false);
                return Err(AuthError::TokenExpired);
            }
        }

        let user = self
            .store
            .get_user(&stored.user_id)?
            .ok_or(AuthError::UserNotFound)?;

        if user.status != UserStatus::Active {
            record_auth_attempt("pat", false);
            return Err(AuthError::UserDisabled);
        }

        self.rate_limiter.clear(&token_key);

        let mut result = AuthResult::authenticated(stored.user_id);
        if stored.scopes.read {
            result = result.with_permission("read");
        }
        if stored.scopes.write {
            result = result.with_permission("write");
        }
        if stored.scopes.admin {
            result = result.with_permission("admin");
        }

        record_auth_duration("pat", start.elapsed().as_secs_f64());
        record_auth_attempt("pat", true);

        Ok(result)
    }

    pub fn validate_ssh_key(&self, fingerprint: &str) -> Result<AuthResult, AuthError> {
        let start = Instant::now();
        let key_id = format!("ssh:{}", fingerprint);

        if !self.rate_limiter.check(&key_id) {
            record_auth_attempt("ssh", false);
            return Err(AuthError::RateLimitExceeded);
        }

        let user_id = match self.store.get_ssh_key_user(fingerprint)? {
            Some(uid) => uid,
            None => {
                self.rate_limiter.record_failure(&key_id);
                record_auth_attempt("ssh", false);
                return Err(AuthError::SshKeyNotFound);
            }
        };

        let user = self
            .store
            .get_user(&user_id)?
            .ok_or(AuthError::UserNotFound)?;

        if user.status != UserStatus::Active {
            record_auth_attempt("ssh", false);
            return Err(AuthError::UserDisabled);
        }

        self.rate_limiter.clear(&key_id);

        let result = AuthResult::authenticated(user_id)
            .with_permission("read")
            .with_permission("write");

        record_auth_duration("ssh", start.elapsed().as_secs_f64());
        record_auth_attempt("ssh", true);

        Ok(result)
    }
}

fn sha2_hash(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::types::TokenScopes;
    use std::collections::HashMap;
    use std::sync::RwLock;

    struct MockAuthStore {
        tokens: RwLock<HashMap<String, StoredToken>>,
        users: RwLock<HashMap<String, User>>,
        ssh_keys: RwLock<HashMap<String, String>>,
    }

    impl MockAuthStore {
        fn new() -> Self {
            Self {
                tokens: RwLock::new(HashMap::new()),
                users: RwLock::new(HashMap::new()),
                ssh_keys: RwLock::new(HashMap::new()),
            }
        }

        fn add_user(&self, user: User) {
            self.users
                .write()
                .unwrap()
                .insert(user.user_id.clone(), user);
        }

        fn add_token(&self, hash: &str, token: StoredToken) {
            self.tokens.write().unwrap().insert(hash.to_string(), token);
        }

        fn add_ssh_key(&self, fingerprint: &str, user_id: &str) {
            self.ssh_keys
                .write()
                .unwrap()
                .insert(fingerprint.to_string(), user_id.to_string());
        }
    }

    impl AuthStore for MockAuthStore {
        fn get_token_by_hash(&self, hash: &str) -> Result<Option<StoredToken>, AuthError> {
            Ok(self.tokens.read().unwrap().get(hash).cloned())
        }

        fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError> {
            Ok(self.users.read().unwrap().get(user_id).cloned())
        }

        fn get_ssh_key_user(&self, fingerprint: &str) -> Result<Option<String>, AuthError> {
            Ok(self.ssh_keys.read().unwrap().get(fingerprint).cloned())
        }
    }

    fn create_test_user(user_id: &str, status: UserStatus) -> User {
        User {
            user_id: user_id.to_string(),
            name: "Test User".to_string(),
            email: "test@example.com".to_string(),
            password_hash: "hash".to_string(),
            status,
            created_at: 0,
        }
    }

    fn create_test_token(
        user_id: &str,
        scopes: TokenScopes,
        expires_at: Option<i64>,
    ) -> StoredToken {
        StoredToken {
            token_id: "tok123".to_string(),
            user_id: user_id.to_string(),
            hashed_value: "hash".to_string(),
            scopes,
            created_at: 0,
            expires_at,
        }
    }

    #[test]
    fn test_auth_result_permissions() {
        let result = AuthResult::authenticated("user1".to_string())
            .with_permission("read")
            .with_permission("write");

        assert!(result.authenticated);
        assert!(result.can_read());
        assert!(result.can_write());
        assert!(!result.is_admin());
    }

    #[test]
    fn test_auth_result_unauthenticated() {
        let result = AuthResult::unauthenticated();
        assert!(!result.authenticated);
        assert!(!result.can_read());
    }

    #[test]
    fn test_validate_token_success() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));

        store.add_user(create_test_user("user1", UserStatus::Active));

        let token = "gst_test_token";
        let hash = sha2_hash(token);
        store.add_token(
            &hash,
            create_test_token(
                "user1",
                TokenScopes {
                    read: true,
                    write: true,
                    admin: false,
                },
                None,
            ),
        );

        let validator = LocalValidator::new(store, rate_limiter);
        let result = validator.validate_token(token).unwrap();

        assert!(result.authenticated);
        assert_eq!(result.user_id, "user1");
        assert!(result.can_read());
        assert!(result.can_write());
        assert!(!result.is_admin());
    }

    #[test]
    fn test_validate_token_invalid() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));

        let validator = LocalValidator::new(store, rate_limiter);
        let result = validator.validate_token("invalid_token");

        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[test]
    fn test_validate_token_expired() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));

        store.add_user(create_test_user("user1", UserStatus::Active));

        let token = "gst_expired_token";
        let hash = sha2_hash(token);
        store.add_token(
            &hash,
            create_test_token("user1", TokenScopes::default(), Some(0)),
        );

        let validator = LocalValidator::new(store, rate_limiter);
        let result = validator.validate_token(token);

        assert!(matches!(result, Err(AuthError::TokenExpired)));
    }

    #[test]
    fn test_validate_token_disabled_user() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));

        store.add_user(create_test_user("user1", UserStatus::Disabled));

        let token = "gst_disabled_user_token";
        let hash = sha2_hash(token);
        store.add_token(
            &hash,
            create_test_token("user1", TokenScopes::default(), None),
        );

        let validator = LocalValidator::new(store, rate_limiter);
        let result = validator.validate_token(token);

        assert!(matches!(result, Err(AuthError::UserDisabled)));
    }

    #[test]
    fn test_validate_ssh_key_success() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));

        store.add_user(create_test_user("user1", UserStatus::Active));
        store.add_ssh_key("SHA256:abc123", "user1");

        let validator = LocalValidator::new(store, rate_limiter);
        let result = validator.validate_ssh_key("SHA256:abc123").unwrap();

        assert!(result.authenticated);
        assert_eq!(result.user_id, "user1");
        assert!(result.can_read());
        assert!(result.can_write());
    }

    #[test]
    fn test_validate_ssh_key_not_found() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));

        let validator = LocalValidator::new(store, rate_limiter);
        let result = validator.validate_ssh_key("SHA256:unknown");

        assert!(matches!(result, Err(AuthError::SshKeyNotFound)));
    }
}
