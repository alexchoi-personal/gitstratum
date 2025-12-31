use gitstratum_control_plane::ControlPlaneError;

#[test]
fn test_error_not_leader() {
    let err = ControlPlaneError::NotLeader(Some("node-1".to_string()));
    let msg = err.to_string();
    assert!(msg.contains("not leader"));
    assert!(msg.contains("node-1"));

    let err = ControlPlaneError::NotLeader(None);
    let msg = err.to_string();
    assert!(msg.contains("not leader"));
}

#[test]
fn test_error_raft() {
    let err = ControlPlaneError::Raft("raft error".to_string());
    let msg = err.to_string();
    assert!(msg.contains("raft error"));
}

#[test]
fn test_error_storage() {
    let err = ControlPlaneError::Storage("storage error".to_string());
    let msg = err.to_string();
    assert!(msg.contains("storage error"));
}

#[test]
fn test_error_node_not_found() {
    let err = ControlPlaneError::NodeNotFound("node-1".to_string());
    let msg = err.to_string();
    assert!(msg.contains("node not found"));
    assert!(msg.contains("node-1"));
}

#[test]
fn test_error_lock_not_found() {
    let err = ControlPlaneError::LockNotFound("lock-1".to_string());
    let msg = err.to_string();
    assert!(msg.contains("lock not found"));
    assert!(msg.contains("lock-1"));
}

#[test]
fn test_error_lock_held() {
    let err = ControlPlaneError::LockHeld("holder-1".to_string());
    let msg = err.to_string();
    assert!(msg.contains("lock already held"));
    assert!(msg.contains("holder-1"));
}

#[test]
fn test_error_lock_expired() {
    let err = ControlPlaneError::LockExpired;
    let msg = err.to_string();
    assert!(msg.contains("lock expired"));
}

#[test]
fn test_error_invalid_request() {
    let err = ControlPlaneError::InvalidRequest("invalid field".to_string());
    let msg = err.to_string();
    assert!(msg.contains("invalid request"));
    assert!(msg.contains("invalid field"));
}

#[test]
fn test_error_serialization() {
    let err = ControlPlaneError::Serialization("json error".to_string());
    let msg = err.to_string();
    assert!(msg.contains("serialization error"));
    assert!(msg.contains("json error"));
}

#[test]
fn test_error_internal() {
    let err = ControlPlaneError::Internal("internal error".to_string());
    let msg = err.to_string();
    assert!(msg.contains("internal error"));
}

#[test]
fn test_error_to_tonic_status_not_leader() {
    let err = ControlPlaneError::NotLeader(Some("node-1".to_string()));
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("node-1"));
}

#[test]
fn test_error_to_tonic_status_not_leader_unknown() {
    let err = ControlPlaneError::NotLeader(None);
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("unknown"));
}

#[test]
fn test_error_to_tonic_status_node_not_found() {
    let err = ControlPlaneError::NodeNotFound("node-1".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[test]
fn test_error_to_tonic_status_lock_not_found() {
    let err = ControlPlaneError::LockNotFound("lock-1".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[test]
fn test_error_to_tonic_status_lock_held() {
    let err = ControlPlaneError::LockHeld("holder-1".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::AlreadyExists);
}

#[test]
fn test_error_to_tonic_status_lock_expired() {
    let err = ControlPlaneError::LockExpired;
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn test_error_to_tonic_status_invalid_request() {
    let err = ControlPlaneError::InvalidRequest("bad field".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_error_to_tonic_status_raft() {
    let err = ControlPlaneError::Raft("raft error".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn test_error_to_tonic_status_storage() {
    let err = ControlPlaneError::Storage("storage error".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn test_error_to_tonic_status_serialization() {
    let err = ControlPlaneError::Serialization("json error".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn test_error_to_tonic_status_internal() {
    let err = ControlPlaneError::Internal("internal error".to_string());
    let status: tonic::Status = err.into();
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn test_error_from_bincode() {
    use std::io::{Error, ErrorKind};
    let io_err = Error::new(ErrorKind::Other, "bincode error");
    let bincode_err = bincode::Error::from(io_err);
    let err: ControlPlaneError = bincode_err.into();
    assert!(matches!(err, ControlPlaneError::Serialization(_)));
}

#[test]
fn test_error_from_serde_json() {
    let json_err = serde_json::from_str::<String>("invalid").unwrap_err();
    let err: ControlPlaneError = json_err.into();
    assert!(matches!(err, ControlPlaneError::Serialization(_)));
}

#[test]
fn test_error_debug() {
    let err = ControlPlaneError::NotLeader(Some("node-1".to_string()));
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("NotLeader"));
}
