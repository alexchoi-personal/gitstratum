use thiserror::Error;
use tonic::metadata::MetadataValue;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Not the leader")]
    NotLeader(Option<String>),

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

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<CoordinatorError> for tonic::Status {
    fn from(err: CoordinatorError) -> Self {
        match err {
            CoordinatorError::NotLeader(leader_addr) => {
                let mut status = tonic::Status::failed_precondition("Not the leader");
                if let Some(addr) = leader_addr {
                    if let Ok(value) = MetadataValue::try_from(&addr) {
                        status.metadata_mut().insert("leader-address", value);
                    }
                }
                status
            }
            CoordinatorError::NodeNotFound(_) => tonic::Status::not_found(err.to_string()),
            CoordinatorError::InvalidNodeType => tonic::Status::invalid_argument(err.to_string()),
            CoordinatorError::Raft(_) => tonic::Status::internal(err.to_string()),
            CoordinatorError::Grpc(s) => s,
            CoordinatorError::Internal(_) => tonic::Status::internal(err.to_string()),
            CoordinatorError::Serialization(_) => tonic::Status::internal(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn test_not_leader_error_without_address() {
        let err = CoordinatorError::NotLeader(None);
        assert_eq!(err.to_string(), "Not the leader");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.metadata().get("leader-address").is_none());
    }

    #[test]
    fn test_not_leader_error_with_address() {
        let err = CoordinatorError::NotLeader(Some("http://leader:9000".to_string()));
        assert_eq!(err.to_string(), "Not the leader");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(
            status
                .metadata()
                .get("leader-address")
                .unwrap()
                .to_str()
                .unwrap(),
            "http://leader:9000"
        );
    }

    #[test]
    fn test_node_not_found_error() {
        let err = CoordinatorError::NodeNotFound("node-1".to_string());
        assert_eq!(err.to_string(), "Node not found: node-1");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::NotFound);
    }

    #[test]
    fn test_invalid_node_type_error() {
        let err = CoordinatorError::InvalidNodeType;
        assert!(err.to_string().contains("Invalid node type"));
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn test_raft_error() {
        let err = CoordinatorError::Raft("consensus failed".to_string());
        assert_eq!(err.to_string(), "Raft error: consensus failed");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn test_grpc_error() {
        let inner_status = tonic::Status::unavailable("service unavailable");
        let err = CoordinatorError::Grpc(inner_status);
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn test_internal_error() {
        let err = CoordinatorError::Internal("unexpected error".to_string());
        assert_eq!(err.to_string(), "Internal error: unexpected error");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn test_serialization_error() {
        let err = CoordinatorError::Serialization("invalid json".to_string());
        assert_eq!(err.to_string(), "Serialization error: invalid json");
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn test_error_debug() {
        let err = CoordinatorError::NotLeader(None);
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("NotLeader"));
    }

    #[test]
    fn test_error_from_status() {
        let status = tonic::Status::cancelled("cancelled");
        let err = CoordinatorError::from(status);
        match err {
            CoordinatorError::Grpc(s) => assert_eq!(s.code(), Code::Cancelled),
            _ => panic!("Expected Grpc variant"),
        }
    }
}
