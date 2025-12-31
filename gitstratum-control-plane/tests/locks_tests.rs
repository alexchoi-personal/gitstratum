use gitstratum_control_plane::LockInfo;
use std::thread;
use std::time::Duration;

#[test]
fn test_lock_info_creation() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_secs(60),
    );

    assert_eq!(lock.lock_id, "lock-123");
    assert_eq!(lock.repo_id, "my-repo");
    assert_eq!(lock.ref_name, "refs/heads/main");
    assert_eq!(lock.holder_id, "worker-1");
    assert_eq!(lock.timeout_ms, 60000);
    assert!(lock.acquired_at_epoch_ms > 0);
}

#[test]
fn test_lock_info_not_expired() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_secs(60),
    );

    assert!(!lock.is_expired());
    assert!(lock.remaining_ms() > 0);
}

#[test]
fn test_lock_info_expired() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_millis(10),
    );

    thread::sleep(Duration::from_millis(20));

    assert!(lock.is_expired());
    assert_eq!(lock.remaining_ms(), 0);
}

#[test]
fn test_lock_info_remaining_time() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_secs(10),
    );

    let remaining = lock.remaining_ms();
    assert!(remaining > 9000);
    assert!(remaining <= 10000);
}

#[test]
fn test_lock_info_serialization() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_secs(60),
    );

    let serialized = serde_json::to_string(&lock).unwrap();
    let deserialized: LockInfo = serde_json::from_str(&serialized).unwrap();

    assert_eq!(lock.lock_id, deserialized.lock_id);
    assert_eq!(lock.repo_id, deserialized.repo_id);
    assert_eq!(lock.ref_name, deserialized.ref_name);
    assert_eq!(lock.holder_id, deserialized.holder_id);
    assert_eq!(lock.timeout_ms, deserialized.timeout_ms);
    assert_eq!(lock.acquired_at_epoch_ms, deserialized.acquired_at_epoch_ms);
}

#[test]
fn test_lock_info_clone() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_secs(60),
    );

    let cloned = lock.clone();

    assert_eq!(lock.lock_id, cloned.lock_id);
    assert_eq!(lock.timeout_ms, cloned.timeout_ms);
}

#[test]
fn test_lock_info_debug() {
    let lock = LockInfo::new(
        "lock-123",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_secs(60),
    );

    let debug_str = format!("{:?}", lock);
    assert!(debug_str.contains("lock-123"));
    assert!(debug_str.contains("my-repo"));
}
