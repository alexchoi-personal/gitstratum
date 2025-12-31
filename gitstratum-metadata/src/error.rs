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
            MetadataStoreError::Grpc(msg) => tonic::Status::internal(msg),
            MetadataStoreError::Connection(msg) => tonic::Status::unavailable(msg),
        }
    }
}
