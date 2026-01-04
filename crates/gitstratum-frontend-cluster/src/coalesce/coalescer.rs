use std::sync::Arc;

use dashmap::DashMap;
use thiserror::Error;
use tokio::sync::broadcast;

use super::PackKey;

#[derive(Debug, Error)]
pub enum CoalesceError {
    #[error("channel send error: {0}")]
    SendError(String),
    #[error("channel receive error: {0}")]
    ReceiveError(String),
}

#[derive(Debug, Clone)]
pub struct CoalesceConfig {
    pub max_wait_ms: u64,
    pub channel_capacity: usize,
}

impl Default for CoalesceConfig {
    fn default() -> Self {
        Self {
            max_wait_ms: 100,
            channel_capacity: 16,
        }
    }
}

pub struct InflightRequest {
    sender: broadcast::Sender<Arc<Vec<u8>>>,
}

impl InflightRequest {
    fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }
}

pub enum CoalesceResult {
    Leader(broadcast::Sender<Arc<Vec<u8>>>),
    Follower(broadcast::Receiver<Arc<Vec<u8>>>),
}

pub struct PackCoalescer {
    inflight: DashMap<PackKey, Arc<InflightRequest>>,
    config: CoalesceConfig,
}

impl PackCoalescer {
    pub fn new(config: CoalesceConfig) -> Self {
        Self {
            inflight: DashMap::new(),
            config,
        }
    }

    pub fn try_coalesce(&self, key: PackKey) -> CoalesceResult {
        if let Some(existing) = self.inflight.get(&key) {
            let receiver = existing.sender.subscribe();
            return CoalesceResult::Follower(receiver);
        }

        let request = Arc::new(InflightRequest::new(self.config.channel_capacity));
        let sender = request.sender.clone();

        match self.inflight.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let receiver = entry.get().sender.subscribe();
                CoalesceResult::Follower(receiver)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(request);
                CoalesceResult::Leader(sender)
            }
        }
    }

    pub fn complete(&self, key: &PackKey) {
        self.inflight.remove(key);
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    pub fn config(&self) -> &CoalesceConfig {
        &self.config
    }
}

impl Default for PackCoalescer {
    fn default() -> Self {
        Self::new(CoalesceConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Oid;

    fn test_key(name: &str) -> PackKey {
        let oid = Oid::hash(name.as_bytes());
        PackKey::new("repo", &[oid], &[], None)
    }

    #[test]
    fn test_coalesce_config_default() {
        let config = CoalesceConfig::default();
        assert_eq!(config.max_wait_ms, 100);
        assert_eq!(config.channel_capacity, 16);
    }

    #[test]
    fn test_coalescer_new() {
        let config = CoalesceConfig {
            max_wait_ms: 200,
            channel_capacity: 32,
        };
        let coalescer = PackCoalescer::new(config);

        assert_eq!(coalescer.config().max_wait_ms, 200);
        assert_eq!(coalescer.config().channel_capacity, 32);
        assert_eq!(coalescer.inflight_count(), 0);
    }

    #[test]
    fn test_coalescer_default() {
        let coalescer = PackCoalescer::default();
        assert_eq!(coalescer.config().max_wait_ms, 100);
    }

    #[test]
    fn test_try_coalesce_leader() {
        let coalescer = PackCoalescer::default();
        let key = test_key("test");

        let result = coalescer.try_coalesce(key);

        assert!(matches!(result, CoalesceResult::Leader(_)));
        assert_eq!(coalescer.inflight_count(), 1);
    }

    #[test]
    fn test_try_coalesce_follower() {
        let coalescer = PackCoalescer::default();
        let key = test_key("test");

        let _leader = coalescer.try_coalesce(key.clone());
        let result = coalescer.try_coalesce(key);

        assert!(matches!(result, CoalesceResult::Follower(_)));
        assert_eq!(coalescer.inflight_count(), 1);
    }

    #[test]
    fn test_complete_removes_inflight() {
        let coalescer = PackCoalescer::default();
        let key = test_key("test");

        let _result = coalescer.try_coalesce(key.clone());
        assert_eq!(coalescer.inflight_count(), 1);

        coalescer.complete(&key);
        assert_eq!(coalescer.inflight_count(), 0);
    }

    #[test]
    fn test_complete_nonexistent_key() {
        let coalescer = PackCoalescer::default();
        let key = test_key("nonexistent");

        coalescer.complete(&key);
        assert_eq!(coalescer.inflight_count(), 0);
    }

    #[test]
    fn test_different_keys_are_leaders() {
        let coalescer = PackCoalescer::default();
        let key1 = test_key("test1");
        let key2 = test_key("test2");

        let result1 = coalescer.try_coalesce(key1);
        let result2 = coalescer.try_coalesce(key2);

        assert!(matches!(result1, CoalesceResult::Leader(_)));
        assert!(matches!(result2, CoalesceResult::Leader(_)));
        assert_eq!(coalescer.inflight_count(), 2);
    }

    #[tokio::test]
    async fn test_leader_can_broadcast() {
        let coalescer = PackCoalescer::default();
        let key = test_key("test");

        let sender = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Leader(s) => s,
            _ => panic!("Expected leader"),
        };

        let mut receiver = match coalescer.try_coalesce(key) {
            CoalesceResult::Follower(r) => r,
            _ => panic!("Expected follower"),
        };

        let data = Arc::new(vec![1, 2, 3, 4]);
        sender.send(data.clone()).unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(*received, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_multiple_followers_receive() {
        let coalescer = PackCoalescer::default();
        let key = test_key("test");

        let sender = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Leader(s) => s,
            _ => panic!("Expected leader"),
        };

        let mut receiver1 = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Follower(r) => r,
            _ => panic!("Expected follower"),
        };

        let mut receiver2 = match coalescer.try_coalesce(key) {
            CoalesceResult::Follower(r) => r,
            _ => panic!("Expected follower"),
        };

        let data = Arc::new(vec![5, 6, 7]);
        sender.send(data.clone()).unwrap();

        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        assert_eq!(*received1, vec![5, 6, 7]);
        assert_eq!(*received2, vec![5, 6, 7]);
    }

    #[test]
    fn test_coalesce_error_display() {
        let send_err = CoalesceError::SendError("test error".to_string());
        assert!(send_err.to_string().contains("channel send error"));

        let recv_err = CoalesceError::ReceiveError("test error".to_string());
        assert!(recv_err.to_string().contains("channel receive error"));
    }

    #[test]
    fn test_after_complete_new_leader() {
        let coalescer = PackCoalescer::default();
        let key = test_key("test");

        let _result1 = coalescer.try_coalesce(key.clone());
        coalescer.complete(&key);

        let result2 = coalescer.try_coalesce(key);
        assert!(matches!(result2, CoalesceResult::Leader(_)));
    }

    #[test]
    fn test_coalesce_error_debug() {
        let send_err = CoalesceError::SendError("send failed".to_string());
        let debug_str = format!("{:?}", send_err);
        assert!(debug_str.contains("SendError"));
        assert!(debug_str.contains("send failed"));

        let recv_err = CoalesceError::ReceiveError("recv failed".to_string());
        let debug_str = format!("{:?}", recv_err);
        assert!(debug_str.contains("ReceiveError"));
        assert!(debug_str.contains("recv failed"));
    }

    #[test]
    fn test_coalesce_config_clone() {
        let config = CoalesceConfig {
            max_wait_ms: 500,
            channel_capacity: 64,
        };
        let cloned = config.clone();
        assert_eq!(config.max_wait_ms, cloned.max_wait_ms);
        assert_eq!(config.channel_capacity, cloned.channel_capacity);
    }

    #[test]
    fn test_coalesce_config_debug() {
        let config = CoalesceConfig {
            max_wait_ms: 100,
            channel_capacity: 16,
        };
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("CoalesceConfig"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("16"));
    }

    #[tokio::test]
    async fn test_concurrent_coalesce_race_condition() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let coalescer = Arc::new(PackCoalescer::new(CoalesceConfig {
            max_wait_ms: 1000,
            channel_capacity: 32,
        }));
        let leader_count = Arc::new(AtomicUsize::new(0));
        let follower_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..20 {
            let coalescer = Arc::clone(&coalescer);
            let leader_count = Arc::clone(&leader_count);
            let follower_count = Arc::clone(&follower_count);
            let key = test_key("concurrent_test");

            let handle = tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_micros(i * 10)).await;
                match coalescer.try_coalesce(key) {
                    CoalesceResult::Leader(_) => {
                        leader_count.fetch_add(1, Ordering::SeqCst);
                    }
                    CoalesceResult::Follower(_) => {
                        follower_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(leader_count.load(Ordering::SeqCst), 1);
        assert_eq!(follower_count.load(Ordering::SeqCst), 19);
    }

    #[tokio::test]
    async fn test_broadcast_channel_closed() {
        let coalescer = PackCoalescer::default();
        let key = test_key("closed_channel");

        let sender = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Leader(s) => s,
            _ => panic!("Expected leader"),
        };

        let mut receiver = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Follower(r) => r,
            _ => panic!("Expected follower"),
        };

        drop(sender);
        coalescer.complete(&key);

        let result = receiver.recv().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_receiver_lagged() {
        let coalescer = PackCoalescer::new(CoalesceConfig {
            max_wait_ms: 100,
            channel_capacity: 2,
        });
        let key = test_key("lagged");

        let sender = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Leader(s) => s,
            _ => panic!("Expected leader"),
        };

        let mut receiver = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Follower(r) => r,
            _ => panic!("Expected follower"),
        };

        sender.send(Arc::new(vec![1])).unwrap();
        sender.send(Arc::new(vec![2])).unwrap();
        sender.send(Arc::new(vec![3])).unwrap();

        let result = receiver.recv().await;
        match result {
            Ok(data) => assert!(!data.is_empty()),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_send_to_empty_receivers() {
        let coalescer = PackCoalescer::default();
        let key = test_key("no_receivers");

        let sender = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Leader(s) => s,
            _ => panic!("Expected leader"),
        };

        let result = sender.send(Arc::new(vec![1, 2, 3]));
        assert!(result.is_err());

        coalescer.complete(&key);
        assert_eq!(coalescer.inflight_count(), 0);
    }

    #[test]
    fn test_multiple_complete_calls() {
        let coalescer = PackCoalescer::default();
        let key = test_key("double_complete");

        let _result = coalescer.try_coalesce(key.clone());
        assert_eq!(coalescer.inflight_count(), 1);

        coalescer.complete(&key);
        assert_eq!(coalescer.inflight_count(), 0);

        coalescer.complete(&key);
        assert_eq!(coalescer.inflight_count(), 0);
    }

    #[test]
    fn test_coalescer_with_custom_capacity() {
        let config = CoalesceConfig {
            max_wait_ms: 50,
            channel_capacity: 1,
        };
        let coalescer = PackCoalescer::new(config);

        let key = test_key("small_capacity");
        let sender = match coalescer.try_coalesce(key.clone()) {
            CoalesceResult::Leader(s) => s,
            _ => panic!("Expected leader"),
        };

        let mut receiver = match coalescer.try_coalesce(key) {
            CoalesceResult::Follower(r) => r,
            _ => panic!("Expected follower"),
        };

        sender.send(Arc::new(vec![42])).unwrap();

        let received = receiver.try_recv().unwrap();
        assert_eq!(*received, vec![42]);
    }

    #[tokio::test]
    async fn test_multiple_keys_concurrent() {
        use std::sync::Arc;

        let coalescer = Arc::new(PackCoalescer::default());
        let mut handles = Vec::new();

        for i in 0..5 {
            let coalescer = Arc::clone(&coalescer);
            let key = test_key(&format!("key_{}", i));

            let handle = tokio::spawn(async move {
                let result = coalescer.try_coalesce(key);
                matches!(result, CoalesceResult::Leader(_))
            });
            handles.push(handle);
        }

        let mut leader_count = 0;
        for handle in handles {
            if handle.await.unwrap() {
                leader_count += 1;
            }
        }

        assert_eq!(leader_count, 5);
        assert_eq!(coalescer.inflight_count(), 5);
    }
}
