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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_not_found_display() {
        let err = FrontendError::ObjectNotFound("abc123".to_string());
        assert!(err.to_string().contains("abc123"));
    }

    #[test]
    fn test_ref_not_found_display() {
        let err = FrontendError::RefNotFound("refs/heads/main".to_string());
        assert!(err.to_string().contains("refs/heads/main"));
    }

    #[test]
    fn test_invalid_pack_format_display() {
        let err = FrontendError::InvalidPackFormat("bad header".to_string());
        assert!(err.to_string().contains("bad header"));
    }

    #[test]
    fn test_invalid_protocol_display() {
        let err = FrontendError::InvalidProtocol("unknown version".to_string());
        assert!(err.to_string().contains("unknown version"));
    }

    #[test]
    fn test_negotiation_failed_display() {
        let err = FrontendError::NegotiationFailed("timeout".to_string());
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_lock_failed_display() {
        let err = FrontendError::LockFailed("already locked".to_string());
        assert!(err.to_string().contains("already locked"));
    }

    #[test]
    fn test_ref_update_rejected_display() {
        let err = FrontendError::RefUpdateRejected("non-fast-forward".to_string());
        assert!(err.to_string().contains("non-fast-forward"));
    }

    #[test]
    fn test_compression_display() {
        let err = FrontendError::Compression("zlib error".to_string());
        assert!(err.to_string().contains("zlib error"));
    }

    #[test]
    fn test_decompression_display() {
        let err = FrontendError::Decompression("invalid data".to_string());
        assert!(err.to_string().contains("invalid data"));
    }

    #[test]
    fn test_metadata_service_display() {
        let err = FrontendError::MetadataService("connection refused".to_string());
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn test_object_service_display() {
        let err = FrontendError::ObjectService("timeout".to_string());
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_control_plane_display() {
        let err = FrontendError::ControlPlane("not leader".to_string());
        assert!(err.to_string().contains("not leader"));
    }

    #[test]
    fn test_hash_ring_display() {
        let err = FrontendError::HashRing("no nodes".to_string());
        assert!(err.to_string().contains("no nodes"));
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: FrontendError = io_err.into();
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_core_error_from() {
        let core_err = gitstratum_core::Error::InvalidOid("bad oid".to_string());
        let err: FrontendError = core_err.into();
        assert!(err.to_string().contains("bad oid"));
    }

    #[test]
    fn test_grpc_error_from() {
        let status = tonic::Status::not_found("resource not found");
        let err: FrontendError = status.into();
        assert!(err.to_string().contains("resource not found"));
    }

    #[test]
    fn test_unexpected_end_of_stream_display() {
        let err = FrontendError::UnexpectedEndOfStream;
        assert!(err.to_string().contains("unexpectedly"));
    }

    #[test]
    fn test_invalid_object_type_display() {
        let err = FrontendError::InvalidObjectType {
            expected: "blob".to_string(),
            actual: "tree".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("blob"));
        assert!(msg.contains("tree"));
    }

    #[test]
    fn test_pack_too_large_display() {
        let err = FrontendError::PackTooLarge {
            size: 1000,
            limit: 500,
        };
        let msg = err.to_string();
        assert!(msg.contains("1000"));
        assert!(msg.contains("500"));
    }

    #[test]
    fn test_hashring_error_from() {
        let ring_err = gitstratum_hashring::HashRingError::EmptyRing;
        let err: FrontendError = ring_err.into();
        assert!(err.to_string().contains("hash ring"));
    }

    #[test]
    fn test_error_debug() {
        let err = FrontendError::ObjectNotFound("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("ObjectNotFound"));
    }
}
