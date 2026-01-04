pub mod error;
pub mod metrics;
pub mod password;
pub mod rate_limit;
pub mod token;
pub mod types;
pub mod validator;

pub use error::AuthError;
pub use metrics::{
    record_auth_attempt, record_auth_duration, record_permission_check, record_rate_limit_exceeded,
};
pub use password::{hash_password, verify_password};
pub use rate_limit::AuthRateLimiter;
pub use token::{generate_pat, hash_token, is_valid_pat_format};
pub use types::*;
pub use validator::{AuthResult, AuthStore, LocalValidator};
