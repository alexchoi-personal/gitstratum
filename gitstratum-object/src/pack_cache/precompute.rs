use bytes::Bytes;
use dashmap::DashSet;
use gitstratum_core::Oid;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use super::{PackCache, PackCacheKey, PackData};
use crate::store::ObjectStore;

#[derive(Debug, Clone)]
pub struct PrecomputeConfig {
    pub worker_count: usize,
    pub queue_size: usize,
    pub check_interval: Duration,
    pub default_depth: u32,
    pub max_depth: u32,
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
}

impl PackPrecomputer {
    pub fn new(
        config: PrecomputeConfig,
        cache: Arc<PackCache>,
        store: Arc<ObjectStore>,
    ) -> Self {
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
        }
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

    pub async fn start(&mut self) {
        let mut rx = self.request_rx.take().expect("start called twice");
        let mut shutdown_rx = self.shutdown_rx.clone();

        info!(workers = self.config.worker_count, "starting pack precomputer");

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

            if let Some(blob) = self.store.get(&oid)? {
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
    use gitstratum_core::Blob;
    use tempfile::TempDir;

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
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache.clone(), store);

        let request = PrecomputeRequest::new("repo1", "refs/heads/main", Oid::hash(b"tip"), 50);

        let pack = PackData::new(
            Bytes::from(vec![0u8; 100]),
            10,
            100,
            Oid::hash(b"tip"),
        );
        cache.put(request.cache_key(), pack).unwrap();

        assert!(!precomputer.submit(request));
    }

    #[tokio::test]
    async fn test_precomputer_process_request() {
        let (store, _dir) = create_test_store();

        let blob = Blob::new(b"test content".to_vec());
        store.put(&blob).unwrap();

        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let precomputer = PackPrecomputer::new(config, cache.clone(), store);

        let request = PrecomputeRequest::new("repo1", "refs/heads/main", blob.oid, 50);

        precomputer.process_request(request.clone()).await;

        assert!(cache.contains(&request.cache_key()));
    }

    #[tokio::test]
    async fn test_precomputer_shutdown() {
        let (store, _dir) = create_test_store();
        let cache = Arc::new(PackCache::new(1024 * 1024, Duration::from_secs(300)));
        let config = PrecomputeConfig::default();
        let mut precomputer = PackPrecomputer::new(config, cache, store);

        let handle = tokio::spawn(async move {
            precomputer.start().await;
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
}
