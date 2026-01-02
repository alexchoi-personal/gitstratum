use gitstratum_control_plane_cluster::LockInfo;
use std::thread;
use std::time::Duration;

#[test]
fn test_lock_info_lifecycle_and_expiration() {
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

    assert!(!lock.is_expired());
    let remaining = lock.remaining_ms();
    assert!(remaining > 59000 && remaining <= 60000);

    let cloned = lock.clone();
    assert_eq!(lock.lock_id, cloned.lock_id);
    assert_eq!(lock.timeout_ms, cloned.timeout_ms);

    let debug_str = format!("{:?}", lock);
    assert!(debug_str.contains("lock-123"));
    assert!(debug_str.contains("my-repo"));

    let serialized = serde_json::to_string(&lock).unwrap();
    let deserialized: LockInfo = serde_json::from_str(&serialized).unwrap();
    assert_eq!(lock.lock_id, deserialized.lock_id);
    assert_eq!(lock.repo_id, deserialized.repo_id);
    assert_eq!(lock.ref_name, deserialized.ref_name);
    assert_eq!(lock.holder_id, deserialized.holder_id);
    assert_eq!(lock.timeout_ms, deserialized.timeout_ms);
    assert_eq!(lock.acquired_at_epoch_ms, deserialized.acquired_at_epoch_ms);

    let short_lock = LockInfo::new(
        "lock-456",
        "my-repo",
        "refs/heads/main",
        "worker-1",
        Duration::from_millis(10),
    );

    thread::sleep(Duration::from_millis(20));

    assert!(short_lock.is_expired());
    assert_eq!(short_lock.remaining_ms(), 0);
}
