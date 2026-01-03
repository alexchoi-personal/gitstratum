use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub name: String,
    pub email: String,
    pub password_hash: String,
    pub status: UserStatus,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshKey {
    pub key_id: String,
    pub user_id: String,
    pub fingerprint: String,
    pub public_key: String,
    pub title: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredToken {
    pub token_id: String,
    pub user_id: String,
    pub hashed_value: String,
    pub scopes: TokenScopes,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenScopes {
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_serialization() {
        let user = User {
            user_id: "user123".to_string(),
            name: "Test User".to_string(),
            email: "test@example.com".to_string(),
            password_hash: "hash123".to_string(),
            status: UserStatus::Active,
            created_at: 1704067200,
        };

        let json = serde_json::to_string(&user).unwrap();
        let deserialized: User = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.user_id, "user123");
        assert_eq!(deserialized.status, UserStatus::Active);
    }

    #[test]
    fn test_user_status_equality() {
        assert_eq!(UserStatus::Active, UserStatus::Active);
        assert_eq!(UserStatus::Disabled, UserStatus::Disabled);
        assert_ne!(UserStatus::Active, UserStatus::Disabled);
    }

    #[test]
    fn test_ssh_key_serialization() {
        let key = SshKey {
            key_id: "key123".to_string(),
            user_id: "user123".to_string(),
            fingerprint: "SHA256:abc123".to_string(),
            public_key: "ssh-ed25519 AAAA...".to_string(),
            title: "My Laptop".to_string(),
            created_at: 1704067200,
        };

        let json = serde_json::to_string(&key).unwrap();
        let deserialized: SshKey = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.key_id, "key123");
        assert_eq!(deserialized.fingerprint, "SHA256:abc123");
    }

    #[test]
    fn test_stored_token_serialization() {
        let token = StoredToken {
            token_id: "tok123".to_string(),
            user_id: "user123".to_string(),
            hashed_value: "sha256hash".to_string(),
            scopes: TokenScopes {
                read: true,
                write: true,
                admin: false,
            },
            created_at: 1704067200,
            expires_at: Some(1704153600),
        };

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: StoredToken = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.token_id, "tok123");
        assert!(deserialized.scopes.read);
        assert!(deserialized.scopes.write);
        assert!(!deserialized.scopes.admin);
        assert_eq!(deserialized.expires_at, Some(1704153600));
    }

    #[test]
    fn test_stored_token_no_expiry() {
        let token = StoredToken {
            token_id: "tok123".to_string(),
            user_id: "user123".to_string(),
            hashed_value: "sha256hash".to_string(),
            scopes: TokenScopes::default(),
            created_at: 1704067200,
            expires_at: None,
        };

        assert!(token.expires_at.is_none());
    }

    #[test]
    fn test_token_scopes_default() {
        let scopes = TokenScopes::default();
        assert!(!scopes.read);
        assert!(!scopes.write);
        assert!(!scopes.admin);
    }

    #[test]
    fn test_token_scopes_all_permissions() {
        let scopes = TokenScopes {
            read: true,
            write: true,
            admin: true,
        };

        assert!(scopes.read);
        assert!(scopes.write);
        assert!(scopes.admin);
    }
}
