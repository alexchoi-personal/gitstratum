use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: u64,
    pub iat: u64,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
}

impl Claims {
    pub fn new(subject: impl Into<String>, roles: Vec<String>, ttl: Duration) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            sub: subject.into(),
            exp: now + ttl.as_secs(),
            iat: now,
            roles,
            permissions: Vec::new(),
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now >= self.exp
    }

    pub fn subject(&self) -> &str {
        &self.sub
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.iter().any(|p| p == permission)
    }
}

#[derive(Debug, Clone)]
pub struct TokenValidatorConfig {
    pub secret: Vec<u8>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub leeway: Duration,
}

impl Default for TokenValidatorConfig {
    fn default() -> Self {
        Self {
            secret: Vec::new(),
            issuer: None,
            audience: None,
            leeway: Duration::from_secs(60),
        }
    }
}

pub struct TokenValidator {
    config: TokenValidatorConfig,
}

impl TokenValidator {
    pub fn new(config: TokenValidatorConfig) -> Self {
        Self { config }
    }

    pub fn validate(&self, token: &str) -> Result<Claims, TokenValidationError> {
        if token.is_empty() {
            return Err(TokenValidationError::EmptyToken);
        }

        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(TokenValidationError::InvalidFormat);
        }

        Err(TokenValidationError::NotImplemented)
    }

    pub fn config(&self) -> &TokenValidatorConfig {
        &self.config
    }
}

impl Default for TokenValidator {
    fn default() -> Self {
        Self::new(TokenValidatorConfig::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenValidationError {
    EmptyToken,
    InvalidFormat,
    InvalidSignature,
    Expired,
    NotYetValid,
    InvalidIssuer,
    InvalidAudience,
    NotImplemented,
}

impl std::fmt::Display for TokenValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyToken => write!(f, "empty token"),
            Self::InvalidFormat => write!(f, "invalid token format"),
            Self::InvalidSignature => write!(f, "invalid signature"),
            Self::Expired => write!(f, "token expired"),
            Self::NotYetValid => write!(f, "token not yet valid"),
            Self::InvalidIssuer => write!(f, "invalid issuer"),
            Self::InvalidAudience => write!(f, "invalid audience"),
            Self::NotImplemented => write!(f, "token validation not implemented"),
        }
    }
}

impl std::error::Error for TokenValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claims_lifecycle() {
        let claims = Claims::new(
            "user-123",
            vec!["admin".to_string()],
            Duration::from_secs(3600),
        );
        assert_eq!(claims.sub, "user-123");
        assert!(!claims.roles.is_empty());
        assert_eq!(claims.roles[0], "admin");
        assert!(claims.permissions.is_empty());
        assert!(claims.iat > 0);
        assert!(claims.exp > claims.iat);
        assert_eq!(claims.exp - claims.iat, 3600);

        let claims_string = Claims::new(
            String::from("user-456"),
            vec!["reader".to_string(), "writer".to_string()],
            Duration::from_secs(7200),
        );
        assert_eq!(claims_string.sub, "user-456");
        assert_eq!(claims_string.roles.len(), 2);

        let fresh_claims = Claims::new("user-123", vec![], Duration::from_secs(3600));
        assert!(!fresh_claims.is_expired());

        let mut expired_claims = Claims::new("user-123", vec![], Duration::from_secs(3600));
        expired_claims.exp = 1;
        assert!(expired_claims.is_expired());

        let subject_claims = Claims::new("test-subject", vec![], Duration::from_secs(100));
        assert_eq!(subject_claims.subject(), "test-subject");

        let role_claims = Claims::new(
            "user",
            vec!["admin".to_string(), "user".to_string()],
            Duration::from_secs(100),
        );
        assert!(role_claims.has_role("admin"));
        assert!(role_claims.has_role("user"));

        let limited_role_claims =
            Claims::new("user", vec!["user".to_string()], Duration::from_secs(100));
        assert!(!limited_role_claims.has_role("admin"));
        assert!(!limited_role_claims.has_role("superuser"));

        let no_role_claims = Claims::new("user", vec![], Duration::from_secs(100));
        assert!(!no_role_claims.has_role("any"));

        let mut perm_claims = Claims::new("user", vec![], Duration::from_secs(100));
        perm_claims.permissions = vec!["read".to_string(), "write".to_string()];
        assert!(perm_claims.has_permission("read"));
        assert!(perm_claims.has_permission("write"));

        let mut limited_perm_claims = Claims::new("user", vec![], Duration::from_secs(100));
        limited_perm_claims.permissions = vec!["read".to_string()];
        assert!(!limited_perm_claims.has_permission("write"));
        assert!(!limited_perm_claims.has_permission("delete"));

        let no_perm_claims = Claims::new("user", vec![], Duration::from_secs(100));
        assert!(!no_perm_claims.has_permission("any"));

        let serialized = serde_json::to_string(&claims).unwrap();
        let deserialized: Claims = serde_json::from_str(&serialized).unwrap();
        assert_eq!(claims.sub, deserialized.sub);
        assert_eq!(claims.exp, deserialized.exp);
        assert_eq!(claims.iat, deserialized.iat);
        assert_eq!(claims.roles, deserialized.roles);
        assert_eq!(claims.permissions, deserialized.permissions);

        let debug_claims = Claims::new("user", vec![], Duration::from_secs(100));
        let debug_str = format!("{:?}", debug_claims);
        assert!(debug_str.contains("Claims"));
        assert!(debug_str.contains("user"));

        let clone_claims = Claims::new("user", vec!["admin".to_string()], Duration::from_secs(100));
        let cloned = clone_claims.clone();
        assert_eq!(clone_claims.sub, cloned.sub);
        assert_eq!(clone_claims.exp, cloned.exp);
        assert_eq!(clone_claims.roles, cloned.roles);
    }

    #[test]
    fn test_token_validator_configuration_and_validation() {
        let default_config = TokenValidatorConfig::default();
        assert!(default_config.secret.is_empty());
        assert!(default_config.issuer.is_none());
        assert!(default_config.audience.is_none());
        assert_eq!(default_config.leeway, Duration::from_secs(60));

        let debug_str = format!("{:?}", default_config);
        assert!(debug_str.contains("TokenValidatorConfig"));

        let mut config = TokenValidatorConfig::default();
        config.secret = vec![1, 2, 3];
        config.issuer = Some("test-issuer".to_string());
        config.audience = Some("test-audience".to_string());
        let cloned = config.clone();
        assert_eq!(config.secret, cloned.secret);
        assert_eq!(config.issuer, cloned.issuer);
        assert_eq!(config.audience, cloned.audience);
        assert_eq!(config.leeway, cloned.leeway);

        let custom_config = TokenValidatorConfig {
            secret: vec![1, 2, 3, 4],
            issuer: Some("my-issuer".to_string()),
            audience: Some("my-audience".to_string()),
            leeway: Duration::from_secs(120),
        };
        let validator = TokenValidator::new(custom_config);
        assert_eq!(validator.config().secret, vec![1, 2, 3, 4]);
        assert_eq!(validator.config().issuer, Some("my-issuer".to_string()));
        assert_eq!(validator.config().audience, Some("my-audience".to_string()));
        assert_eq!(validator.config().leeway, Duration::from_secs(120));

        let default_validator = TokenValidator::default();
        assert!(default_validator.config().secret.is_empty());
        assert!(default_validator.config().issuer.is_none());
        assert!(default_validator.config().audience.is_none());
        assert_eq!(default_validator.config().leeway, Duration::from_secs(60));

        let validator = TokenValidator::default();

        let empty_result = validator.validate("");
        assert!(empty_result.is_err());
        assert_eq!(empty_result.unwrap_err(), TokenValidationError::EmptyToken);

        let no_dots_result = validator.validate("invalid-token-without-dots");
        assert!(no_dots_result.is_err());
        assert_eq!(
            no_dots_result.unwrap_err(),
            TokenValidationError::InvalidFormat
        );

        let one_dot_result = validator.validate("header.payload");
        assert!(one_dot_result.is_err());
        assert_eq!(
            one_dot_result.unwrap_err(),
            TokenValidationError::InvalidFormat
        );

        let many_dots_result = validator.validate("a.b.c.d");
        assert!(many_dots_result.is_err());
        assert_eq!(
            many_dots_result.unwrap_err(),
            TokenValidationError::InvalidFormat
        );

        let valid_format_result = validator.validate("header.payload.signature");
        assert!(valid_format_result.is_err());
        assert_eq!(
            valid_format_result.unwrap_err(),
            TokenValidationError::NotImplemented
        );

        let accessor_config = TokenValidatorConfig {
            secret: b"secret".to_vec(),
            issuer: Some("issuer".to_string()),
            audience: None,
            leeway: Duration::from_secs(30),
        };
        let accessor_validator = TokenValidator::new(accessor_config);
        let retrieved_config = accessor_validator.config();
        assert_eq!(retrieved_config.secret, b"secret".to_vec());
        assert_eq!(retrieved_config.issuer, Some("issuer".to_string()));
        assert!(retrieved_config.audience.is_none());
        assert_eq!(retrieved_config.leeway, Duration::from_secs(30));
    }

    #[test]
    fn test_token_validation_error_variants() {
        let variants = [
            (
                TokenValidationError::EmptyToken,
                "empty token",
                "EmptyToken",
            ),
            (
                TokenValidationError::InvalidFormat,
                "invalid token format",
                "InvalidFormat",
            ),
            (
                TokenValidationError::InvalidSignature,
                "invalid signature",
                "InvalidSignature",
            ),
            (TokenValidationError::Expired, "token expired", "Expired"),
            (
                TokenValidationError::NotYetValid,
                "token not yet valid",
                "NotYetValid",
            ),
            (
                TokenValidationError::InvalidIssuer,
                "invalid issuer",
                "InvalidIssuer",
            ),
            (
                TokenValidationError::InvalidAudience,
                "invalid audience",
                "InvalidAudience",
            ),
            (
                TokenValidationError::NotImplemented,
                "token validation not implemented",
                "NotImplemented",
            ),
        ];

        for (error, display_msg, debug_msg) in &variants {
            assert_eq!(format!("{}", error), *display_msg);
            assert_eq!(format!("{:?}", error), *debug_msg);

            let cloned = error.clone();
            assert_eq!(error, &cloned);
        }

        let boxed_error: Box<dyn std::error::Error> = Box::new(TokenValidationError::EmptyToken);
        assert_eq!(boxed_error.to_string(), "empty token");

        for (i, (v1, _, _)) in variants.iter().enumerate() {
            for (j, (v2, _, _)) in variants.iter().enumerate() {
                if i == j {
                    assert_eq!(v1, v2);
                } else {
                    assert_ne!(v1, v2);
                }
            }
        }
    }
}
