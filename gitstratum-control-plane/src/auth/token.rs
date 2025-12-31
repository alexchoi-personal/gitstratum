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
