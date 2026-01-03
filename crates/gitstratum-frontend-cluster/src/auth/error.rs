use thiserror::Error;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,

    #[error("token expired")]
    TokenExpired,

    #[error("user not found")]
    UserNotFound,

    #[error("user disabled")]
    UserDisabled,

    #[error("permission denied")]
    PermissionDenied,

    #[error("ssh key not found")]
    SshKeyNotFound,

    #[error("invalid ssh key format")]
    InvalidSshKeyFormat,

    #[error("rate limit exceeded")]
    RateLimitExceeded,

    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            AuthError::InvalidCredentials.to_string(),
            "invalid credentials"
        );
        assert_eq!(AuthError::TokenExpired.to_string(), "token expired");
        assert_eq!(AuthError::UserNotFound.to_string(), "user not found");
        assert_eq!(AuthError::UserDisabled.to_string(), "user disabled");
        assert_eq!(AuthError::PermissionDenied.to_string(), "permission denied");
        assert_eq!(AuthError::SshKeyNotFound.to_string(), "ssh key not found");
        assert_eq!(
            AuthError::InvalidSshKeyFormat.to_string(),
            "invalid ssh key format"
        );
        assert_eq!(
            AuthError::RateLimitExceeded.to_string(),
            "rate limit exceeded"
        );
        assert_eq!(
            AuthError::Storage("db error".to_string()).to_string(),
            "storage error: db error"
        );
    }

    #[test]
    fn test_error_debug() {
        let err = AuthError::InvalidCredentials;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("InvalidCredentials"));
    }

    #[test]
    fn test_storage_error_with_message() {
        let err = AuthError::Storage("connection timeout".to_string());
        assert_eq!(err.to_string(), "storage error: connection timeout");
    }
}
