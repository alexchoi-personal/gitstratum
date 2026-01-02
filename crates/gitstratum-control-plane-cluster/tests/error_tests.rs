use gitstratum_control_plane_cluster::ControlPlaneError;

#[test]
fn test_all_error_variants_display_and_debug() {
    let errors_with_expected = [
        (
            ControlPlaneError::NotLeader(Some("node-1".to_string())),
            "not leader",
            Some("node-1"),
        ),
        (ControlPlaneError::NotLeader(None), "not leader", None),
        (
            ControlPlaneError::Raft("raft error".to_string()),
            "raft error",
            None,
        ),
        (
            ControlPlaneError::Storage("storage error".to_string()),
            "storage error",
            None,
        ),
        (
            ControlPlaneError::NodeNotFound("node-1".to_string()),
            "node not found",
            Some("node-1"),
        ),
        (
            ControlPlaneError::LockNotFound("lock-1".to_string()),
            "lock not found",
            Some("lock-1"),
        ),
        (
            ControlPlaneError::LockHeld("holder-1".to_string()),
            "lock already held",
            Some("holder-1"),
        ),
        (ControlPlaneError::LockExpired, "lock expired", None),
        (
            ControlPlaneError::InvalidRequest("invalid field".to_string()),
            "invalid request",
            Some("invalid field"),
        ),
        (
            ControlPlaneError::Serialization("json error".to_string()),
            "serialization error",
            Some("json error"),
        ),
        (
            ControlPlaneError::Internal("internal error".to_string()),
            "internal error",
            None,
        ),
    ];

    for (err, expected_msg, expected_detail) in &errors_with_expected {
        let msg = err.to_string();
        assert!(
            msg.contains(expected_msg),
            "Error '{}' should contain '{}'",
            msg,
            expected_msg
        );
        if let Some(detail) = expected_detail {
            assert!(
                msg.contains(detail),
                "Error '{}' should contain '{}'",
                msg,
                detail
            );
        }
        let debug_str = format!("{:?}", err);
        assert!(!debug_str.is_empty());
    }
}

#[test]
fn test_all_error_to_tonic_status_conversions() {
    let conversions: Vec<(ControlPlaneError, tonic::Code)> = vec![
        (
            ControlPlaneError::NotLeader(Some("node-1".to_string())),
            tonic::Code::FailedPrecondition,
        ),
        (
            ControlPlaneError::NotLeader(None),
            tonic::Code::FailedPrecondition,
        ),
        (
            ControlPlaneError::NodeNotFound("node-1".to_string()),
            tonic::Code::NotFound,
        ),
        (
            ControlPlaneError::LockNotFound("lock-1".to_string()),
            tonic::Code::NotFound,
        ),
        (
            ControlPlaneError::LockHeld("holder-1".to_string()),
            tonic::Code::AlreadyExists,
        ),
        (
            ControlPlaneError::LockExpired,
            tonic::Code::FailedPrecondition,
        ),
        (
            ControlPlaneError::InvalidRequest("bad field".to_string()),
            tonic::Code::InvalidArgument,
        ),
        (
            ControlPlaneError::Raft("raft error".to_string()),
            tonic::Code::Internal,
        ),
        (
            ControlPlaneError::Storage("storage error".to_string()),
            tonic::Code::Internal,
        ),
        (
            ControlPlaneError::Serialization("json error".to_string()),
            tonic::Code::Internal,
        ),
        (
            ControlPlaneError::Internal("internal error".to_string()),
            tonic::Code::Internal,
        ),
    ];

    for (err, expected_code) in conversions {
        let status: tonic::Status = err.into();
        assert_eq!(
            status.code(),
            expected_code,
            "Error should map to {:?}",
            expected_code
        );
    }

    let not_leader_status: tonic::Status =
        ControlPlaneError::NotLeader(Some("node-1".to_string())).into();
    assert!(not_leader_status.message().contains("node-1"));

    let unknown_leader_status: tonic::Status = ControlPlaneError::NotLeader(None).into();
    assert!(unknown_leader_status.message().contains("unknown"));
}

#[test]
fn test_error_from_external_types() {
    use std::io::{Error, ErrorKind};

    let io_err = Error::new(ErrorKind::Other, "bincode error");
    let bincode_err = bincode::Error::from(io_err);
    let err: ControlPlaneError = bincode_err.into();
    assert!(matches!(err, ControlPlaneError::Serialization(_)));

    let json_err = serde_json::from_str::<String>("invalid").unwrap_err();
    let err: ControlPlaneError = json_err.into();
    assert!(matches!(err, ControlPlaneError::Serialization(_)));
}
