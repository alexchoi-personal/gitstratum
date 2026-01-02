use async_trait::async_trait;
use std::collections::HashSet;

use crate::error::{FrontendError, Result};

#[derive(Debug, Clone)]
pub struct AuthResult {
    pub user_id: String,
    pub permissions: HashSet<String>,
    pub authenticated: bool,
}

impl AuthResult {
    pub fn authenticated(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            permissions: HashSet::new(),
            authenticated: true,
        }
    }

    pub fn anonymous() -> Self {
        Self {
            user_id: String::new(),
            permissions: HashSet::new(),
            authenticated: false,
        }
    }

    pub fn with_permission(mut self, permission: impl Into<String>) -> Self {
        self.permissions.insert(permission.into());
        self
    }

    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(permission)
    }

    pub fn can_read(&self) -> bool {
        self.has_permission("read") || self.has_permission("admin")
    }

    pub fn can_write(&self) -> bool {
        self.has_permission("write") || self.has_permission("admin")
    }
}

#[async_trait]
pub trait TokenValidator: Send + Sync {
    async fn validate_token(&self, token: &str) -> Result<AuthResult>;
    async fn validate_ssh_key(&self, key_fingerprint: &str) -> Result<AuthResult>;
}

pub struct AuthMiddleware<V> {
    validator: V,
    require_auth: bool,
}

impl<V> AuthMiddleware<V>
where
    V: TokenValidator,
{
    pub fn new(validator: V) -> Self {
        Self {
            validator,
            require_auth: true,
        }
    }

    pub fn allow_anonymous(mut self) -> Self {
        self.require_auth = false;
        self
    }

    pub async fn authenticate_token(&self, token: &str) -> Result<AuthResult> {
        let result = self.validator.validate_token(token).await?;
        if self.require_auth && !result.authenticated {
            return Err(FrontendError::InvalidProtocol(
                "authentication required".to_string(),
            ));
        }
        Ok(result)
    }

    pub async fn authenticate_ssh(&self, key_fingerprint: &str) -> Result<AuthResult> {
        let result = self.validator.validate_ssh_key(key_fingerprint).await?;
        if self.require_auth && !result.authenticated {
            return Err(FrontendError::InvalidProtocol(
                "authentication required".to_string(),
            ));
        }
        Ok(result)
    }

    pub fn check_read_permission(&self, auth: &AuthResult, repo_id: &str) -> Result<()> {
        if !auth.can_read() {
            return Err(FrontendError::InvalidProtocol(format!(
                "no read permission for repository: {}",
                repo_id
            )));
        }
        Ok(())
    }

    pub fn check_write_permission(&self, auth: &AuthResult, repo_id: &str) -> Result<()> {
        if !auth.can_write() {
            return Err(FrontendError::InvalidProtocol(format!(
                "no write permission for repository: {}",
                repo_id
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockValidator {
        valid_token: String,
    }

    #[async_trait]
    impl TokenValidator for MockValidator {
        async fn validate_token(&self, token: &str) -> Result<AuthResult> {
            if token == self.valid_token {
                Ok(AuthResult::authenticated("user-1")
                    .with_permission("read")
                    .with_permission("write"))
            } else {
                Ok(AuthResult::anonymous())
            }
        }

        async fn validate_ssh_key(&self, key_fingerprint: &str) -> Result<AuthResult> {
            if key_fingerprint == "valid-key" {
                Ok(AuthResult::authenticated("user-1").with_permission("read"))
            } else {
                Ok(AuthResult::anonymous())
            }
        }
    }

    #[test]
    fn test_auth_result_authenticated() {
        let result = AuthResult::authenticated("user-1");
        assert!(result.authenticated);
        assert_eq!(result.user_id, "user-1");
    }

    #[test]
    fn test_auth_result_anonymous() {
        let result = AuthResult::anonymous();
        assert!(!result.authenticated);
        assert!(result.user_id.is_empty());
    }

    #[test]
    fn test_auth_result_permissions() {
        let result = AuthResult::authenticated("user-1")
            .with_permission("read")
            .with_permission("write");

        assert!(result.has_permission("read"));
        assert!(result.has_permission("write"));
        assert!(!result.has_permission("admin"));
        assert!(result.can_read());
        assert!(result.can_write());
    }

    #[test]
    fn test_auth_result_admin() {
        let result = AuthResult::authenticated("admin").with_permission("admin");
        assert!(result.can_read());
        assert!(result.can_write());
    }

    #[tokio::test]
    async fn test_auth_middleware_token() {
        let validator = MockValidator {
            valid_token: "valid".to_string(),
        };
        let middleware = AuthMiddleware::new(validator);

        let result = middleware.authenticate_token("valid").await.unwrap();
        assert!(result.authenticated);

        let result = middleware.authenticate_token("invalid").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_auth_middleware_allow_anonymous() {
        let validator = MockValidator {
            valid_token: "valid".to_string(),
        };
        let middleware = AuthMiddleware::new(validator).allow_anonymous();

        let result = middleware.authenticate_token("invalid").await.unwrap();
        assert!(!result.authenticated);
    }

    #[tokio::test]
    async fn test_auth_middleware_ssh() {
        let validator = MockValidator {
            valid_token: "valid".to_string(),
        };
        let middleware = AuthMiddleware::new(validator);

        let result = middleware.authenticate_ssh("valid-key").await.unwrap();
        assert!(result.authenticated);
    }

    #[tokio::test]
    async fn test_check_read_permission() {
        let validator = MockValidator {
            valid_token: "valid".to_string(),
        };
        let middleware = AuthMiddleware::new(validator);

        let auth = AuthResult::authenticated("user").with_permission("read");
        assert!(middleware.check_read_permission(&auth, "repo").is_ok());

        let auth_no_read = AuthResult::authenticated("user");
        assert!(middleware
            .check_read_permission(&auth_no_read, "repo")
            .is_err());
    }

    #[tokio::test]
    async fn test_check_write_permission() {
        let validator = MockValidator {
            valid_token: "valid".to_string(),
        };
        let middleware = AuthMiddleware::new(validator);

        let auth = AuthResult::authenticated("user").with_permission("write");
        assert!(middleware.check_write_permission(&auth, "repo").is_ok());

        let auth_no_write = AuthResult::authenticated("user").with_permission("read");
        assert!(middleware
            .check_write_permission(&auth_no_write, "repo")
            .is_err());
    }
}
