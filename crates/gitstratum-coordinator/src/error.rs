use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Not the leader")]
    NotLeader,

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Invalid node type: expected metadata or object")]
    InvalidNodeType,

    #[error("Raft error: {0}")]
    Raft(String),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<CoordinatorError> for tonic::Status {
    fn from(err: CoordinatorError) -> Self {
        match err {
            CoordinatorError::NotLeader => tonic::Status::failed_precondition(err.to_string()),
            CoordinatorError::NodeNotFound(_) => tonic::Status::not_found(err.to_string()),
            CoordinatorError::InvalidNodeType => tonic::Status::invalid_argument(err.to_string()),
            CoordinatorError::Raft(_) => tonic::Status::internal(err.to_string()),
            CoordinatorError::Grpc(s) => s,
            CoordinatorError::Internal(_) => tonic::Status::internal(err.to_string()),
        }
    }
}
