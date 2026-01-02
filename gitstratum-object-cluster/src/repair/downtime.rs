use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::error::{ObjectStoreError, Result};

const HEALTHY_TIMESTAMP_FILE: &str = "healthy_timestamp";

/// Tracks node downtime for crash recovery by persisting healthy timestamps.
///
/// Periodically writes the current timestamp to disk. On restart, compares
/// the last healthy timestamp to the current time to determine the downtime
/// window that requires repair.
pub struct DowntimeTracker {
    data_dir: PathBuf,
    last_healthy: AtomicU64,
    update_interval: Duration,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DowntimeTracker {
    pub fn new(data_dir: &Path) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let tracker = Self {
            data_dir: data_dir.to_path_buf(),
            last_healthy: AtomicU64::new(0),
            update_interval: Duration::from_secs(10),
            shutdown_tx,
            shutdown_rx,
        };

        if let Some(ts) = tracker.load()? {
            tracker.last_healthy.store(ts, Ordering::SeqCst);
        }

        Ok(tracker)
    }

    pub fn load(&self) -> Result<Option<u64>> {
        let path = self.data_dir.join(HEALTHY_TIMESTAMP_FILE);
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                let timestamp = contents
                    .trim()
                    .parse::<u64>()
                    .map_err(|e| ObjectStoreError::Serialization(e.to_string()))?;
                Ok(Some(timestamp))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save(&self, timestamp: u64) -> Result<()> {
        let path = self.data_dir.join(HEALTHY_TIMESTAMP_FILE);
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::write(&path, timestamp.to_string())?;
        Ok(())
    }

    pub fn mark_healthy(&self) -> Result<()> {
        let now = crate::util::time::current_timestamp();
        self.last_healthy.store(now, Ordering::SeqCst);
        self.save(now)
    }

    pub fn downtime_window(&self) -> Option<(u64, u64)> {
        let last = self.last_healthy.load(Ordering::SeqCst);
        if last == 0 {
            return None;
        }

        let now = crate::util::time::current_timestamp();

        if now > last {
            Some((last, now))
        } else {
            None
        }
    }

    pub fn start_heartbeat(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let tracker = self.clone();
        let mut shutdown_rx = tracker.shutdown_rx.clone();

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let _ = tracker.mark_healthy();
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn last_healthy_timestamp(&self) -> u64 {
        self.last_healthy.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        assert_eq!(tracker.last_healthy_timestamp(), 0);
    }

    #[test]
    fn test_new_with_existing_timestamp() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);
        std::fs::write(&path, "1234567890").unwrap();

        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        assert_eq!(tracker.last_healthy_timestamp(), 1234567890);
    }

    #[test]
    fn test_load_no_file() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        assert_eq!(tracker.load().unwrap(), None);
    }

    #[test]
    fn test_load_existing_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);
        std::fs::write(&path, "9876543210").unwrap();

        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        assert_eq!(tracker.load().unwrap(), Some(9876543210));
    }

    #[test]
    fn test_load_invalid_content() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);
        std::fs::write(&path, "not a number").unwrap();

        let result = DowntimeTracker::new(temp_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_save() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        tracker.save(1111111111).unwrap();

        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "1111111111");
    }

    #[test]
    fn test_save_creates_dir() {
        let temp_dir = TempDir::new().unwrap();
        let nested_path = temp_dir.path().join("nested").join("dir");

        let tracker = DowntimeTracker::new(&nested_path).unwrap();
        tracker.save(2222222222).unwrap();

        let path = nested_path.join(HEALTHY_TIMESTAMP_FILE);
        assert!(path.exists());
    }

    #[test]
    fn test_mark_healthy() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        tracker.mark_healthy().unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let ts = tracker.last_healthy_timestamp();
        assert!(ts <= now);
        assert!(ts >= now - 1);
    }

    #[test]
    fn test_downtime_window_no_previous() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        assert_eq!(tracker.downtime_window(), None);
    }

    #[test]
    fn test_downtime_window_with_previous() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);

        let old_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;

        std::fs::write(&path, old_ts.to_string()).unwrap();

        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        let window = tracker.downtime_window().unwrap();

        assert_eq!(window.0, old_ts);
        assert!(window.1 > window.0);
    }

    #[test]
    fn test_downtime_window_recent() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        tracker.mark_healthy().unwrap();

        let window = tracker.downtime_window();
        assert!(window.is_none() || window.unwrap().1 - window.unwrap().0 <= 1);
    }

    #[tokio::test]
    async fn test_start_heartbeat() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = Arc::new(DowntimeTracker::new(temp_dir.path()).unwrap());

        let handle = tracker.clone().start_heartbeat(Duration::from_millis(10));

        tokio::time::sleep(Duration::from_millis(50)).await;

        tracker.shutdown();
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        let ts = tracker.last_healthy_timestamp();
        assert!(ts > 0);
    }

    #[tokio::test]
    async fn test_heartbeat_updates_file() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = Arc::new(DowntimeTracker::new(temp_dir.path()).unwrap());

        let handle = tracker.clone().start_heartbeat(Duration::from_millis(10));

        tokio::time::sleep(Duration::from_millis(50)).await;

        tracker.shutdown();
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;

        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);
        assert!(path.exists());
    }

    #[test]
    fn test_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        tracker.shutdown();
    }

    #[test]
    fn test_last_healthy_timestamp_initial() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        assert_eq!(tracker.last_healthy_timestamp(), 0);
    }

    #[test]
    fn test_mark_healthy_updates_memory_and_disk() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        tracker.mark_healthy().unwrap();

        let mem_ts = tracker.last_healthy_timestamp();
        let disk_ts = tracker.load().unwrap().unwrap();

        assert_eq!(mem_ts, disk_ts);
    }

    #[test]
    fn test_multiple_mark_healthy_calls() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        tracker.mark_healthy().unwrap();
        let ts1 = tracker.last_healthy_timestamp();

        std::thread::sleep(Duration::from_millis(10));

        tracker.mark_healthy().unwrap();
        let ts2 = tracker.last_healthy_timestamp();

        assert!(ts2 >= ts1);
    }

    #[test]
    fn test_save_load_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        let timestamp = 1609459200u64;
        tracker.save(timestamp).unwrap();

        let loaded = tracker.load().unwrap();
        assert_eq!(loaded, Some(timestamp));
    }

    #[test]
    fn test_load_whitespace_handling() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(HEALTHY_TIMESTAMP_FILE);
        std::fs::write(&path, "  1234567890  \n").unwrap();

        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();
        assert_eq!(tracker.load().unwrap(), Some(1234567890));
    }

    #[tokio::test]
    async fn test_concurrent_mark_healthy() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = Arc::new(DowntimeTracker::new(temp_dir.path()).unwrap());

        let mut handles = vec![];
        for _ in 0..10 {
            let t = tracker.clone();
            handles.push(tokio::spawn(async move {
                t.mark_healthy().unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let ts = tracker.last_healthy_timestamp();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(ts <= now);
        assert!(ts >= now - 1);
    }

    #[test]
    fn test_downtime_window_boundary() {
        let temp_dir = TempDir::new().unwrap();
        let tracker = DowntimeTracker::new(temp_dir.path()).unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        tracker.last_healthy.store(now + 100, Ordering::SeqCst);

        let window = tracker.downtime_window();
        assert!(window.is_none());
    }
}
