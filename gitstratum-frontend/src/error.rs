use thiserror::Error;

pub type Result<T> = std::result::Result<T, FrontendError>;

#[derive(Error, Debug)]
pub enum FrontendError {
    #[error("object not found: {0}")]
    ObjectNotFound(String),

    #[error("ref not found: {0}")]
    RefNotFound(String),

    #[error("invalid pack format: {0}")]
    InvalidPackFormat(String),

    #[error("invalid protocol: {0}")]
    InvalidProtocol(String),

    #[error("negotiation failed: {0}")]
    NegotiationFailed(String),

    #[error("lock acquisition failed: {0}")]
    LockFailed(String),

    #[error("ref update rejected: {0}")]
    RefUpdateRejected(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("metadata service error: {0}")]
    MetadataService(String),

    #[error("object service error: {0}")]
    ObjectService(String),

    #[error("control plane error: {0}")]
    ControlPlane(String),

    #[error("hash ring error: {0}")]
    HashRing(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("core error: {0}")]
    Core(#[from] gitstratum_core::Error),

    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("stream ended unexpectedly")]
    UnexpectedEndOfStream,

    #[error("invalid object type: expected {expected}, got {actual}")]
    InvalidObjectType { expected: String, actual: String },

    #[error("pack too large: {size} bytes exceeds limit of {limit} bytes")]
    PackTooLarge { size: usize, limit: usize },
}

impl From<gitstratum_hashring::HashRingError> for FrontendError {
    fn from(e: gitstratum_hashring::HashRingError) -> Self {
        FrontendError::HashRing(e.to_string())
    }
}
