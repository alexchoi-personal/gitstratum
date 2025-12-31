use thiserror::Error;

pub type Result<T> = std::result::Result<T, ObjectStoreError>;

#[derive(Error, Debug)]
pub enum ObjectStoreError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

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
}

impl From<ObjectStoreError> for tonic::Status {
    fn from(err: ObjectStoreError) -> Self {
        match err {
            ObjectStoreError::BlobNotFound(oid) => {
                tonic::Status::not_found(format!("blob not found: {}", oid))
            }
            ObjectStoreError::InvalidOid(msg) => tonic::Status::invalid_argument(msg),
            ObjectStoreError::NoAvailableNodes => {
                tonic::Status::unavailable("no available nodes")
            }
            ObjectStoreError::AllReplicasFailed => {
                tonic::Status::unavailable("all replicas failed")
            }
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}
