use thiserror::Error;

#[derive(Debug, Error)]
pub enum LfsError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("object already exists: {0}")]
    AlreadyExists(String),

    #[error("size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("invalid oid: {0}")]
    InvalidOid(String),

    #[error("upload failed: {0}")]
    UploadFailed(String),

    #[error("download failed: {0}")]
    DownloadFailed(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("rate limit exceeded, retry after {retry_after} seconds")]
    RateLimitExceeded { retry_after: u64 },

    #[error("object too large: {size} bytes exceeds limit of {limit} bytes")]
    ObjectTooLarge { size: u64, limit: u64 },

    #[error("metadata error: {0}")]
    Metadata(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

impl LfsError {
    pub fn status_code(&self) -> u16 {
        match self {
            LfsError::NotFound(_) => 404,
            LfsError::AlreadyExists(_) => 409,
            LfsError::SizeMismatch { .. } => 400,
            LfsError::InvalidOid(_) => 400,
            LfsError::UploadFailed(_) => 500,
            LfsError::DownloadFailed(_) => 500,
            LfsError::Backend(_) => 502,
            LfsError::PermissionDenied(_) => 403,
            LfsError::RateLimitExceeded { .. } => 429,
            LfsError::ObjectTooLarge { .. } => 413,
            LfsError::Metadata(_) => 500,
            LfsError::InvalidRequest(_) => 400,
        }
    }
}
