use thiserror::Error;

pub type Result<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Error, Debug)]
pub enum ControlPlaneError {
    #[error("not leader: current leader is {0:?}")]
    NotLeader(Option<String>),

    #[error("raft error: {0}")]
    Raft(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("lock not found: {0}")]
    LockNotFound(String),

    #[error("lock already held by {0}")]
    LockHeld(String),

    #[error("lock expired")]
    LockExpired,

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("hash ring error: {0}")]
    HashRing(#[from] gitstratum_hashring::HashRingError),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<ControlPlaneError> for tonic::Status {
    fn from(err: ControlPlaneError) -> Self {
        match err {
            ControlPlaneError::NotLeader(leader) => {
                let msg = match leader {
                    Some(l) => format!("not leader, try {}", l),
                    None => "not leader, leader unknown".to_string(),
                };
                tonic::Status::failed_precondition(msg)
            }
            ControlPlaneError::NodeNotFound(id) => {
                tonic::Status::not_found(format!("node not found: {}", id))
            }
            ControlPlaneError::LockNotFound(id) => {
                tonic::Status::not_found(format!("lock not found: {}", id))
            }
            ControlPlaneError::LockHeld(holder) => {
                tonic::Status::already_exists(format!("lock held by {}", holder))
            }
            ControlPlaneError::LockExpired => tonic::Status::failed_precondition("lock expired"),
            ControlPlaneError::InvalidRequest(msg) => tonic::Status::invalid_argument(msg),
            ControlPlaneError::HashRing(e) => tonic::Status::internal(e.to_string()),
            ControlPlaneError::Raft(msg) => tonic::Status::internal(format!("raft: {}", msg)),
            ControlPlaneError::Storage(msg) => tonic::Status::internal(format!("storage: {}", msg)),
            ControlPlaneError::Serialization(msg) => {
                tonic::Status::internal(format!("serialization: {}", msg))
            }
            ControlPlaneError::Grpc(status) => status,
            ControlPlaneError::Io(e) => tonic::Status::internal(e.to_string()),
            ControlPlaneError::Internal(msg) => tonic::Status::internal(msg),
        }
    }
}

impl From<bincode::Error> for ControlPlaneError {
    fn from(err: bincode::Error) -> Self {
        ControlPlaneError::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for ControlPlaneError {
    fn from(err: serde_json::Error) -> Self {
        ControlPlaneError::Serialization(err.to_string())
    }
}

impl From<rocksdb::Error> for ControlPlaneError {
    fn from(err: rocksdb::Error) -> Self {
        ControlPlaneError::Storage(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashring_error_to_tonic_status() {
        let hashring_err = gitstratum_hashring::HashRingError::EmptyRing;
        let control_plane_err = ControlPlaneError::HashRing(hashring_err);
        let status: tonic::Status = control_plane_err.into();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("empty"));
    }

    #[test]
    fn test_grpc_error_to_tonic_status() {
        let original_status = tonic::Status::not_found("test resource not found");
        let control_plane_err = ControlPlaneError::Grpc(original_status);
        let status: tonic::Status = control_plane_err.into();
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "test resource not found");
    }

    #[test]
    fn test_rocksdb_error_to_control_plane_error() {
        use rocksdb::{Options, DB};
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_db");

        let mut opts = Options::default();
        opts.create_if_missing(true);
        let _db = DB::open(&opts, &path).unwrap();

        opts.set_error_if_exists(true);
        let rocksdb_err = DB::open(&opts, &path).unwrap_err();

        let control_plane_err: ControlPlaneError = rocksdb_err.into();
        match control_plane_err {
            ControlPlaneError::Storage(msg) => {
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected Storage variant"),
        }
    }
}
