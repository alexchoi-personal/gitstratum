use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetadataStoreError>;

#[derive(Error, Debug)]
pub enum MetadataStoreError {
    #[error("repository not found: {0}")]
    RepoNotFound(String),

    #[error("repository already exists: {0}")]
    RepoAlreadyExists(String),

    #[error("commit not found: {0}")]
    CommitNotFound(String),

    #[error("tree not found: {0}")]
    TreeNotFound(String),

    #[error("ref not found: {0}")]
    RefNotFound(String),

    #[error("compare-and-swap failed: expected {expected}, found {found}")]
    CasFailed { expected: String, found: String },

    #[error("invalid ref name: {0}")]
    InvalidRefName(String),

    #[error("invalid repo id: {0}")]
    InvalidRepoId(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("column family not found: {0}")]
    ColumnFamilyNotFound(String),

    #[error("grpc error: {0}")]
    Grpc(String),

    #[error("connection error: {0}")]
    Connection(String),
}

impl From<rocksdb::Error> for MetadataStoreError {
    fn from(err: rocksdb::Error) -> Self {
        MetadataStoreError::Storage(err.to_string())
    }
}

impl From<bincode::Error> for MetadataStoreError {
    fn from(err: bincode::Error) -> Self {
        MetadataStoreError::Serialization(err.to_string())
    }
}

impl From<tonic::Status> for MetadataStoreError {
    fn from(err: tonic::Status) -> Self {
        MetadataStoreError::Grpc(err.message().to_string())
    }
}

impl From<tonic::transport::Error> for MetadataStoreError {
    fn from(err: tonic::transport::Error) -> Self {
        MetadataStoreError::Connection(err.to_string())
    }
}

impl From<MetadataStoreError> for tonic::Status {
    fn from(err: MetadataStoreError) -> Self {
        match err {
            MetadataStoreError::RepoNotFound(msg) => tonic::Status::not_found(msg),
            MetadataStoreError::RepoAlreadyExists(msg) => tonic::Status::already_exists(msg),
            MetadataStoreError::CommitNotFound(msg) => tonic::Status::not_found(msg),
            MetadataStoreError::TreeNotFound(msg) => tonic::Status::not_found(msg),
            MetadataStoreError::RefNotFound(msg) => tonic::Status::not_found(msg),
            MetadataStoreError::CasFailed { expected, found } => {
                tonic::Status::failed_precondition(format!(
                    "CAS failed: expected {}, found {}",
                    expected, found
                ))
            }
            MetadataStoreError::InvalidRefName(msg) => tonic::Status::invalid_argument(msg),
            MetadataStoreError::InvalidRepoId(msg) => tonic::Status::invalid_argument(msg),
            MetadataStoreError::Serialization(msg) => tonic::Status::internal(msg),
            MetadataStoreError::Deserialization(msg) => tonic::Status::internal(msg),
            MetadataStoreError::Storage(msg) => tonic::Status::internal(msg),
            MetadataStoreError::Internal(msg) => tonic::Status::internal(msg),
            MetadataStoreError::ColumnFamilyNotFound(msg) => tonic::Status::internal(msg),
            MetadataStoreError::Grpc(msg) => tonic::Status::internal(msg),
            MetadataStoreError::Connection(msg) => tonic::Status::unavailable(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repo_not_found_display() {
        let err = MetadataStoreError::RepoNotFound("test-repo".to_string());
        assert!(err.to_string().contains("test-repo"));
    }

    #[test]
    fn test_repo_already_exists_display() {
        let err = MetadataStoreError::RepoAlreadyExists("test-repo".to_string());
        assert!(err.to_string().contains("test-repo"));
    }

    #[test]
    fn test_commit_not_found_display() {
        let err = MetadataStoreError::CommitNotFound("abc123".to_string());
        assert!(err.to_string().contains("abc123"));
    }

    #[test]
    fn test_tree_not_found_display() {
        let err = MetadataStoreError::TreeNotFound("def456".to_string());
        assert!(err.to_string().contains("def456"));
    }

    #[test]
    fn test_ref_not_found_display() {
        let err = MetadataStoreError::RefNotFound("refs/heads/main".to_string());
        assert!(err.to_string().contains("refs/heads/main"));
    }

    #[test]
    fn test_cas_failed_display() {
        let err = MetadataStoreError::CasFailed {
            expected: "abc".to_string(),
            found: "def".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("abc"));
        assert!(msg.contains("def"));
    }

    #[test]
    fn test_invalid_ref_name_display() {
        let err = MetadataStoreError::InvalidRefName("bad ref".to_string());
        assert!(err.to_string().contains("bad ref"));
    }

    #[test]
    fn test_invalid_repo_id_display() {
        let err = MetadataStoreError::InvalidRepoId("bad id".to_string());
        assert!(err.to_string().contains("bad id"));
    }

    #[test]
    fn test_serialization_display() {
        let err = MetadataStoreError::Serialization("bincode error".to_string());
        assert!(err.to_string().contains("bincode error"));
    }

    #[test]
    fn test_deserialization_display() {
        let err = MetadataStoreError::Deserialization("parse error".to_string());
        assert!(err.to_string().contains("parse error"));
    }

    #[test]
    fn test_storage_display() {
        let err = MetadataStoreError::Storage("rocksdb error".to_string());
        assert!(err.to_string().contains("rocksdb error"));
    }

    #[test]
    fn test_internal_display() {
        let err = MetadataStoreError::Internal("something went wrong".to_string());
        assert!(err.to_string().contains("something went wrong"));
    }

    #[test]
    fn test_grpc_display() {
        let err = MetadataStoreError::Grpc("connection reset".to_string());
        assert!(err.to_string().contains("connection reset"));
    }

    #[test]
    fn test_connection_display() {
        let err = MetadataStoreError::Connection("refused".to_string());
        assert!(err.to_string().contains("refused"));
    }

    #[test]
    fn test_bincode_error_from() {
        use bincode::Options;
        let opts = bincode::DefaultOptions::new().with_fixint_encoding();
        let bincode_err = opts
            .deserialize::<String>(&[0xff, 0xff, 0xff, 0xff])
            .unwrap_err();
        let err: MetadataStoreError = bincode_err.into();
        assert!(err.to_string().contains("serialization"));
    }

    #[test]
    fn test_tonic_status_from() {
        let status = tonic::Status::not_found("not found");
        let err: MetadataStoreError = status.into();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_repo_not_found_to_status() {
        let err = MetadataStoreError::RepoNotFound("test".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_repo_already_exists_to_status() {
        let err = MetadataStoreError::RepoAlreadyExists("test".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn test_commit_not_found_to_status() {
        let err = MetadataStoreError::CommitNotFound("abc".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_tree_not_found_to_status() {
        let err = MetadataStoreError::TreeNotFound("abc".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_ref_not_found_to_status() {
        let err = MetadataStoreError::RefNotFound("refs/heads/main".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn test_cas_failed_to_status() {
        let err = MetadataStoreError::CasFailed {
            expected: "a".to_string(),
            found: "b".to_string(),
        };
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn test_invalid_ref_name_to_status() {
        let err = MetadataStoreError::InvalidRefName("bad".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_invalid_repo_id_to_status() {
        let err = MetadataStoreError::InvalidRepoId("bad".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_serialization_to_status() {
        let err = MetadataStoreError::Serialization("err".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_deserialization_to_status() {
        let err = MetadataStoreError::Deserialization("err".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_storage_to_status() {
        let err = MetadataStoreError::Storage("err".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_internal_to_status() {
        let err = MetadataStoreError::Internal("err".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_grpc_to_status() {
        let err = MetadataStoreError::Grpc("err".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn test_connection_to_status() {
        let err = MetadataStoreError::Connection("err".to_string());
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn test_error_debug() {
        let err = MetadataStoreError::RepoNotFound("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("RepoNotFound"));
    }
}
