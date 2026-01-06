use thiserror::Error;

pub type Result<T> = std::result::Result<T, ObjectStoreError>;

#[derive(Error, Debug)]
pub enum ObjectStoreError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[cfg(feature = "bucketstore")]
    #[error("bucketstore error: {0}")]
    BucketStore(#[from] gitstratum_storage::BucketStoreError),

    #[error("blob not found: {0}")]
    BlobNotFound(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("hashring error: {0}")]
    HashRing(#[from] gitstratum_hashring::HashRingError),

    #[error("invalid oid: {0}")]
    InvalidOid(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid uri: {0}")]
    InvalidUri(String),

    #[error("no available nodes")]
    NoAvailableNodes,

    #[error("all replicas failed")]
    AllReplicasFailed,

    #[error("delta too large: {size} bytes (max: {max})")]
    DeltaTooLarge { size: usize, max: usize },

    #[error("delta apply error: {0}")]
    DeltaApplyError(String),

    #[error("delta deserialize error: {0}")]
    DeltaDeserializeError(String),

    #[error("insufficient replicas: required {required}, achieved {achieved}")]
    InsufficientReplicas { required: usize, achieved: usize },

    #[error("integrity error: oid {oid}, expected {expected}, computed {computed}")]
    IntegrityError {
        oid: String,
        expected: String,
        computed: String,
    },

    #[error("internal error: {0}")]
    Internal(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("already started")]
    AlreadyStarted,
}

impl From<ObjectStoreError> for tonic::Status {
    fn from(err: ObjectStoreError) -> Self {
        match err {
            ObjectStoreError::BlobNotFound(oid) => {
                tonic::Status::not_found(format!("blob not found: {}", oid))
            }
            ObjectStoreError::InvalidOid(msg) => tonic::Status::invalid_argument(msg),
            ObjectStoreError::InvalidArgument(msg) => tonic::Status::invalid_argument(msg),
            ObjectStoreError::NoAvailableNodes => tonic::Status::unavailable("no available nodes"),
            ObjectStoreError::AllReplicasFailed => {
                tonic::Status::unavailable("all replicas failed")
            }
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_not_found_display() {
        let err = ObjectStoreError::BlobNotFound("abc123".to_string());
        assert!(err.to_string().contains("abc123"));
    }

    #[test]
    fn test_compression_display() {
        let err = ObjectStoreError::Compression("zlib error".to_string());
        assert!(err.to_string().contains("zlib error"));
    }

    #[test]
    fn test_decompression_display() {
        let err = ObjectStoreError::Decompression("invalid data".to_string());
        assert!(err.to_string().contains("invalid data"));
    }

    #[test]
    fn test_serialization_display() {
        let err = ObjectStoreError::Serialization("bad format".to_string());
        assert!(err.to_string().contains("bad format"));
    }

    #[test]
    fn test_invalid_oid_display() {
        let err = ObjectStoreError::InvalidOid("not hex".to_string());
        assert!(err.to_string().contains("not hex"));
    }

    #[test]
    fn test_invalid_uri_display() {
        let err = ObjectStoreError::InvalidUri("bad://uri".to_string());
        assert!(err.to_string().contains("bad://uri"));
    }

    #[test]
    fn test_no_available_nodes_display() {
        let err = ObjectStoreError::NoAvailableNodes;
        assert!(err.to_string().contains("no available nodes"));
    }

    #[test]
    fn test_all_replicas_failed_display() {
        let err = ObjectStoreError::AllReplicasFailed;
        assert!(err.to_string().contains("all replicas failed"));
    }

    #[test]
    fn test_delta_too_large_display() {
        let err = ObjectStoreError::DeltaTooLarge {
            size: 1000,
            max: 500,
        };
        let msg = err.to_string();
        assert!(msg.contains("1000"));
        assert!(msg.contains("500"));
    }

    #[test]
    fn test_delta_apply_error_display() {
        let err = ObjectStoreError::DeltaApplyError("bad base".to_string());
        assert!(err.to_string().contains("bad base"));
    }

    #[test]
    fn test_delta_deserialize_error_display() {
        let err = ObjectStoreError::DeltaDeserializeError("invalid format".to_string());
        assert!(err.to_string().contains("invalid format"));
    }

    #[test]
    fn test_insufficient_replicas_display() {
        let err = ObjectStoreError::InsufficientReplicas {
            required: 3,
            achieved: 1,
        };
        let msg = err.to_string();
        assert!(msg.contains("3"));
        assert!(msg.contains("1"));
    }

    #[test]
    fn test_integrity_error_display() {
        let err = ObjectStoreError::IntegrityError {
            oid: "abc".to_string(),
            expected: "123".to_string(),
            computed: "456".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("abc"));
        assert!(msg.contains("123"));
        assert!(msg.contains("456"));
    }

    #[test]
    fn test_grpc_error_from() {
        let status = tonic::Status::not_found("not found");
        let err: ObjectStoreError = status.into();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: ObjectStoreError = io_err.into();
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_hashring_error_from() {
        let ring_err = gitstratum_hashring::HashRingError::EmptyRing;
        let err: ObjectStoreError = ring_err.into();
        assert!(err.to_string().contains("hashring"));
    }

    #[test]
    fn test_blob_not_found_to_status() {
        let err = ObjectStoreError::BlobNotFound("abc".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_invalid_oid_to_status() {
        let err = ObjectStoreError::InvalidOid("bad".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_no_available_nodes_to_status() {
        let err = ObjectStoreError::NoAvailableNodes;
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_all_replicas_failed_to_status() {
        let err = ObjectStoreError::AllReplicasFailed;
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_compression_to_status() {
        let err = ObjectStoreError::Compression("error".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_error_debug() {
        let err = ObjectStoreError::BlobNotFound("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("BlobNotFound"));
    }
}
