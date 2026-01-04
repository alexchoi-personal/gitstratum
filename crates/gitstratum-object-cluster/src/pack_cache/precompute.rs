use bytes::Bytes;
use dashmap::DashSet;
use gitstratum_core::Oid;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use super::hot_repos::HotRepoTracker;
use super::{PackCache, PackCacheKey, PackData};
use crate::error::{ObjectStoreError, Result};
use crate::store::ObjectStorage;
use crate::store::ObjectStore;

#[derive(Debug, Clone)]
pub struct PrecomputeConfig {
    pub worker_count: usize,
    pub queue_size: usize,
    pub check_interval: Duration,
    pub default_depth: u32,
    pub max_depth: u32,
    pub shallow_depth: u32,
    pub hot_repo_threshold: u64,
}

impl PrecomputeConfig {
    pub fn new(
        worker_count: usize,
        queue_size: usize,
        check_interval: Duration,
        default_depth: u32,
        max_depth: u32,
    ) -> Self {
        Self {
            worker_count,
            queue_size,
            check_interval,
            default_depth,
            max_depth,
            shallow_depth: 1,
            hot_repo_threshold: 10,
        }
    }
}

impl Default for PrecomputeConfig {
    fn default() -> Self {
        Self {
            worker_count: 2,
            queue_size: 100,
            check_interval: Duration::from_secs(60),
            default_depth: 50,
            max_depth: 500,
            shallow_depth: 1,
            hot_repo_threshold: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrecomputeRequest {
    pub repo_id: String,
    pub ref_name: String,
    pub tip_oid: Oid,
    pub depth: u32,
}

impl PrecomputeRequest {
    pub fn new(
        repo_id: impl Into<String>,
        ref_name: impl Into<String>,
        tip_oid: Oid,
        depth: u32,
    ) -> Self {
        Self {
            repo_id: repo_id.into(),
            ref_name: ref_name.into(),
            tip_oid,
            depth,
        }
    }

    pub fn cache_key(&self) -> PackCacheKey {
        PackCacheKey::new(&self.repo_id, &self.ref_name, self.depth)
    }
}

pub struct PackPrecomputer {
    config: PrecomputeConfig,
    cache: Arc<PackCache>,
    store: Arc<ObjectStore>,
    pending: Arc<DashSet<String>>,
    request_tx: mpsc::Sender<PrecomputeRequest>,
    request_rx: Option<mpsc::Receiver<PrecomputeRequest>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    hot_tracker: Option<Arc<HotRepoTracker>>,
    ref_tips: Arc<dashmap::DashMap<String, Oid>>,
}

impl PackPrecomputer {
    pub fn new(config: PrecomputeConfig, cache: Arc<PackCache>, store: Arc<ObjectStore>) -> Self {
        let (request_tx, request_rx) = mpsc::channel(config.queue_size);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            config,
            cache,
            store,
            pending: Arc::new(DashSet::new()),
            request_tx,
            request_rx: Some(request_rx),
            shutdown_tx,
            shutdown_rx,
            hot_tracker: None,
            ref_tips: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub fn with_hot_tracker(mut self, tracker: Arc<HotRepoTracker>) -> Self {
        self.hot_tracker = Some(tracker);
        self
    }

    pub fn set_ref_tip(&self, repo_id: &str, ref_name: &str, tip_oid: Oid) {
        let key = format!("{}:{}", repo_id, ref_name);
        self.ref_tips.insert(key, tip_oid);
    }

    pub fn get_ref_tip(&self, repo_id: &str, ref_name: &str) -> Option<Oid> {
        let key = format!("{}:{}", repo_id, ref_name);
        self.ref_tips.get(&key).map(|entry| *entry.value())
    }

    #[instrument(skip(self))]
    pub async fn precompute_for_hot_repos(&self) {
        let tracker = match &self.hot_tracker {
            Some(t) => t,
            None => {
                debug!("no hot tracker configured, skipping hot repo precompute");
                return;
            }
        };

        let hot_repos = tracker.get_hot_repos(self.config.hot_repo_threshold);

        if hot_repos.is_empty() {
            debug!("no hot repos found");
            return;
        }

        info!(count = hot_repos.len(), "precomputing packs for hot repos");

        for repo_id in hot_repos {
            if let Some(tip_oid) = self.get_ref_tip(&repo_id, "refs/heads/main") {
                let full_clone_request = PrecomputeRequest::new(
                    &repo_id,
                    "refs/heads/main",
                    tip_oid,
                    self.config.default_depth,
                );
                self.submit(full_clone_request);

                let shallow_request = PrecomputeRequest::new(
                    &repo_id,
                    "refs/heads/main",
                    tip_oid,
                    self.config.shallow_depth,
                );
                self.submit(shallow_request);
            }
        }
    }

    pub fn hot_tracker(&self) -> Option<&Arc<HotRepoTracker>> {
        self.hot_tracker.as_ref()
    }

    #[instrument(skip(self))]
    pub fn submit(&self, request: PrecomputeRequest) -> bool {
        let key = request.cache_key().to_string_key();

        if self.cache.contains(&request.cache_key()) {
            debug!(key = %key, "pack already cached, skipping");
            return false;
        }

        if self.pending.contains(&key) {
            debug!(key = %key, "request already pending, skipping");
            return false;
        }

        match self.request_tx.try_send(request) {
            Ok(()) => {
                self.pending.insert(key);
                true
            }
            Err(_) => {
                warn!("precompute queue full, dropping request");
                false
            }
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut rx = self
            .request_rx
            .take()
            .ok_or(ObjectStoreError::AlreadyStarted)?;
        let mut shutdown_rx = self.shutdown_rx.clone();

        info!(
            workers = self.config.worker_count,
            "starting pack precomputer"
        );

        loop {
            tokio::select! {
                Some(request) = rx.recv() => {
                    self.process_request(request).await;
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("pack precomputer shutting down");
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn process_request(&self, request: PrecomputeRequest) {
        let key = request.cache_key();
        let key_str = key.to_string_key();

        debug!(key = %key_str, "processing precompute request");

        match self.build_pack(&request).await {
            Ok(pack) => {
                if let Err(e) = self.cache.put(key, pack) {
                    warn!(error = %e, "failed to cache precomputed pack");
                } else {
                    debug!(key = %key_str, "successfully cached precomputed pack");
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to build pack");
            }
        }

        self.pending.remove(&key_str);
    }

    async fn build_pack(&self, request: &PrecomputeRequest) -> crate::error::Result<PackData> {
        let mut objects: Vec<(Oid, Bytes)> = Vec::new();
        let mut visited: HashSet<Oid> = HashSet::new();
        let mut to_visit: Vec<Oid> = vec![request.tip_oid];

        while let Some(oid) = to_visit.pop() {
            if visited.contains(&oid) {
                continue;
            }

            if objects.len() >= request.depth as usize {
                break;
            }

            visited.insert(oid);

            if let Some(blob) = self.store.get(&oid).await? {
                objects.push((oid, blob.data));
            }
        }

        let mut pack_data = Vec::new();
        let mut total_size = 0u64;

        pack_data.extend_from_slice(b"PACK");
        pack_data.extend_from_slice(&2u32.to_be_bytes());
        pack_data.extend_from_slice(&(objects.len() as u32).to_be_bytes());

        for (oid, data) in &objects {
            let header = encode_object_header(3, data.len());
            pack_data.extend_from_slice(&header);
            pack_data.extend_from_slice(oid.as_bytes());
            pack_data.extend_from_slice(data);
            total_size += data.len() as u64;
        }

        Ok(PackData::new(
            Bytes::from(pack_data),
            objects.len() as u32,
            total_size,
            request.tip_oid,
        ))
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn config(&self) -> &PrecomputeConfig {
        &self.config
    }
}

fn encode_object_header(obj_type: u8, size: usize) -> Vec<u8> {
    let mut header = Vec::new();
    let mut s = size;
    let mut byte = (obj_type << 4) | ((s & 0x0f) as u8);
    s >>= 4;

    while s > 0 {
        header.push(byte | 0x80);
        byte = (s & 0x7f) as u8;
        s >>= 7;
    }

    header.push(byte);
    header
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ObjectStorage;
    use gitstratum_core::Blob;
    use tempfile::TempDir;

    #[cfg(feature = "bucketstore")]
    async fn create_test_store() -> (Arc<ObjectStore>, TempDir) {
        use gitstratum_storage::BucketStoreConfig;
        use std::time::Duration;

        let temp_dir = TempDir::new().unwrap();
        let config = BucketStoreConfig {
            data_dir: temp_dir.path().to_path_buf(),
            bucket_count: 64,
            bucket_cache_size: 16,
            max_data_file_size: 1024 * 1024,
            io_queue_depth: 4,
            io_queue_count: 1,
            compaction: gitstratum_storage::config::CompactionConfig {
                fragmentation_threshold: 0.4,
                check_interval: Duration::from_secs(300),
                max_concurrent: 1,
            },
        };
        let store = Arc::new(ObjectStore::new(config).await.unwrap());
        (store, temp_dir)
    }

    #[cfg(not(feature = "bucketstore"))]
    fn create_test_store() -> (Arc<ObjectStore>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(ObjectStore::new(temp_dir.path()).unwrap());
        (store, temp_dir)
    }

    #[test]
    fn test_precompute_config_default() {
        let config = PrecomputeConfig::default();
        assert_eq!(config.worker_count, 2);
        assert_eq!(config.queue_size, 100);
        assert_eq!(config.default_depth, 50);
        assert_eq!(config.max_depth, 500);
    }

    #[test]
    fn test_precompute_config_new() {
        let config = PrecomputeConfig::new(4, 200, Duration::from_secs(30), 100, 1000);
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.queue_size, 200);
        assert_eq!(config.default_depth, 100);
        assert_eq!(config.max_depth, 1000);
    }

    #[test]
    fn test_precompute_request() {
        let oid = Oid::hash(b"test");
        let request = PrecomputeRequest::new("repo1", "refs/heads/main", oid, 50);

        assert_eq!(request.repo_id, "repo1");
        assert_eq!(request.ref_name, "refs/heads/main");
        assert_eq!(request.tip_oid, oid);
        assert_eq!(request.depth, 50);

        let key = request.cache_key();
        assert_eq!(key.repo_id, "repo1");
        assert_eq!(key.ref_name, "refs/heads/main");
        assert_eq!(key.depth, 50);
    }

    #[test]
    fn test_encode_object_header_small() {
        let header = encode_object_header(3, 10);
        assert_eq!(header[0] & 0x70, 0x30);
    }

    #[test]
    fn test_encode_object_header_large() {
        let header = encode_object_header(3, 1000);
        assert!(header.len() > 1);
        assert!(header[0] & 0x80 != 0);
    }

    #[tokio::test]
    async fn test_precomputer_submit() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();

        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache, store);

        let request = PrecomputeRequest::new("repo1", "refs/heads/main", Oid::hash(b"tip"), 50);
        assert!(precomputer.submit(request.clone()));
        assert_eq!(precomputer.pending_count(), 1);

        assert!(!precomputer.submit(request));
    }

    #[tokio::test]
    async fn test_precomputer_submit_cached() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();

        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache.clone(), store);

        let request = PrecomputeRequest::new("repo1", "refs/heads/main", Oid::hash(b"tip"), 50);

        let pack = PackData::new(Bytes::from(vec![0u8; 100]), 10, 100, Oid::hash(b"tip"));
        cache.put(request.cache_key(), pack).unwrap();

        assert!(!precomputer.submit(request));
    }

    #[tokio::test]
    async fn test_precomputer_process_request() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"test content".to_vec());
        store.put(&blob).await.unwrap();

        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache.clone(), store);

        let request = PrecomputeRequest::new("repo1", "refs/heads/main", blob.oid, 50);

        precomputer.process_request(request.clone()).await;

        assert!(cache.contains(&request.cache_key()));
    }

    #[tokio::test]
    async fn test_precomputer_shutdown() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let mut precomputer = PackPrecomputer::new(config, cache, store);

        let handle = tokio::spawn(async move {
            let _ = precomputer.start().await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        handle.abort();
    }

    #[test]
    fn test_precompute_config_debug() {
        let config = PrecomputeConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("PrecomputeConfig"));
    }

    #[test]
    fn test_precompute_request_debug() {
        let request = PrecomputeRequest::new("repo1", "main", Oid::hash(b"test"), 50);
        let debug_str = format!("{:?}", request);
        assert!(debug_str.contains("PrecomputeRequest"));
    }

    #[test]
    fn test_precompute_config_clone() {
        let config = PrecomputeConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.worker_count, config.worker_count);
    }

    #[test]
    fn test_precompute_config_new_fields() {
        let config = PrecomputeConfig::default();
        assert_eq!(config.shallow_depth, 1);
        assert_eq!(config.hot_repo_threshold, 10);
    }

    #[tokio::test]
    async fn test_with_hot_tracker() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();

        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();

        let tracker = Arc::new(HotRepoTracker::new(Duration::from_secs(60)));
        let precomputer = PackPrecomputer::new(config, cache, store).with_hot_tracker(tracker);

        assert!(precomputer.hot_tracker().is_some());
    }

    #[tokio::test]
    async fn test_set_and_get_ref_tip() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();

        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache, store);

        let oid = Oid::hash(b"tip");
        precomputer.set_ref_tip("repo1", "refs/heads/main", oid);

        assert_eq!(
            precomputer.get_ref_tip("repo1", "refs/heads/main"),
            Some(oid)
        );
        assert_eq!(precomputer.get_ref_tip("repo1", "refs/heads/dev"), None);
        assert_eq!(precomputer.get_ref_tip("repo2", "refs/heads/main"), None);
    }

    #[tokio::test]
    async fn test_precompute_for_hot_repos_no_tracker() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache, store);

        precomputer.precompute_for_hot_repos().await;
        assert_eq!(precomputer.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_precompute_for_hot_repos_no_hot_repos() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();

        let tracker = Arc::new(HotRepoTracker::new(Duration::from_secs(60)));
        let precomputer = PackPrecomputer::new(config, cache, store).with_hot_tracker(tracker);

        precomputer.precompute_for_hot_repos().await;
        assert_eq!(precomputer.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_precompute_for_hot_repos_with_repos() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let mut config = PrecomputeConfig::default();
        config.hot_repo_threshold = 5;

        let tracker = Arc::new(HotRepoTracker::new(Duration::from_secs(60)));
        for _ in 0..10 {
            tracker.record_access("hot-repo");
        }

        let precomputer = PackPrecomputer::new(config, cache, store).with_hot_tracker(tracker);

        let tip_oid = Oid::hash(b"tip");
        precomputer.set_ref_tip("hot-repo", "refs/heads/main", tip_oid);

        precomputer.precompute_for_hot_repos().await;
        assert_eq!(precomputer.pending_count(), 2);
    }

    #[tokio::test]
    async fn test_hot_tracker_accessor() {
        #[cfg(feature = "bucketstore")]
        let (store, _dir) = create_test_store().await;
        #[cfg(not(feature = "bucketstore"))]
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();

        let precomputer_without_tracker =
            PackPrecomputer::new(config.clone(), cache.clone(), store.clone());
        assert!(precomputer_without_tracker.hot_tracker().is_none());

        let tracker = Arc::new(HotRepoTracker::new(Duration::from_secs(60)));
        let precomputer_with_tracker =
            PackPrecomputer::new(config, cache, store).with_hot_tracker(tracker.clone());
        assert!(precomputer_with_tracker.hot_tracker().is_some());
    }
}
