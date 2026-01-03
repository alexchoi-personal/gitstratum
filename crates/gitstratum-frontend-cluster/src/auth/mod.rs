pub mod error;
pub mod metrics;
pub mod rate_limit;
pub mod types;

pub use error::AuthError;
pub use metrics::{
    record_auth_attempt, record_auth_duration, record_permission_check, record_rate_limit_exceeded,
};
pub use rate_limit::AuthRateLimiter;
pub use types::*;
