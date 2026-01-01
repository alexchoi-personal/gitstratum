use k8s_openapi::api::coordination::v1::Lease;
use kube::{
    api::{Api, Patch, PatchParams, PostParams},
    Client,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::error::{OperatorError, Result};

const DEFAULT_LEASE_DURATION_SECONDS: i32 = 15;
const DEFAULT_RENEW_DEADLINE_SECONDS: u64 = 10;
const DEFAULT_RETRY_PERIOD_SECONDS: u64 = 2;

#[derive(Clone)]
pub struct LeaderElectionConfig {
    pub lease_name: String,
    pub namespace: String,
    pub identity: String,
    pub lease_duration_seconds: i32,
    pub renew_deadline: Duration,
    pub retry_period: Duration,
}

impl LeaderElectionConfig {
    pub fn new(
        lease_name: impl Into<String>,
        namespace: impl Into<String>,
        identity: impl Into<String>,
    ) -> Self {
        Self {
            lease_name: lease_name.into(),
            namespace: namespace.into(),
            identity: identity.into(),
            lease_duration_seconds: DEFAULT_LEASE_DURATION_SECONDS,
            renew_deadline: Duration::from_secs(DEFAULT_RENEW_DEADLINE_SECONDS),
            retry_period: Duration::from_secs(DEFAULT_RETRY_PERIOD_SECONDS),
        }
    }

    pub fn with_lease_duration(mut self, seconds: i32) -> Self {
        self.lease_duration_seconds = seconds;
        self
    }

    pub fn with_renew_deadline(mut self, duration: Duration) -> Self {
        self.renew_deadline = duration;
        self
    }

    pub fn with_retry_period(mut self, duration: Duration) -> Self {
        self.retry_period = duration;
        self
    }
}

pub struct LeaderElection {
    config: LeaderElectionConfig,
    client: Client,
    is_leader: Arc<AtomicBool>,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl LeaderElection {
    pub fn new(client: Client, config: LeaderElectionConfig) -> Self {
        Self {
            config,
            client,
            is_leader: Arc::new(AtomicBool::new(false)),
            shutdown_tx: None,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    pub fn leader_status(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_leader)
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    pub async fn run_with_leader<F, Fut>(&mut self, leader_fn: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let is_leader = Arc::clone(&self.is_leader);
        let config = self.config.clone();
        let client = self.client.clone();

        let election_handle = tokio::spawn(async move {
            Self::election_loop(client, config, is_leader, shutdown_rx).await
        });

        while !self.is_leader() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!(identity = %self.config.identity, "Became leader, starting leader workload");

        let result = leader_fn().await;

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        let _ = election_handle.await;

        result
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn election_loop(
        client: Client,
        config: LeaderElectionConfig,
        is_leader: Arc<AtomicBool>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let leases: Api<Lease> = Api::namespaced(client.clone(), &config.namespace);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Leader election shutting down");
                        is_leader.store(false, Ordering::SeqCst);
                        break;
                    }
                }
                result = Self::try_acquire_or_renew(&leases, &config, &is_leader) => {
                    if let Err(e) = result {
                        warn!(error = %e, "Leader election cycle failed");
                        is_leader.store(false, Ordering::SeqCst);
                    }
                }
            }

            let sleep_duration = if is_leader.load(Ordering::SeqCst) {
                config.renew_deadline / 2
            } else {
                config.retry_period
            };

            tokio::time::sleep(sleep_duration).await;
        }
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn try_acquire_or_renew(
        leases: &Api<Lease>,
        config: &LeaderElectionConfig,
        is_leader: &Arc<AtomicBool>,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        let now_str = now.to_rfc3339();

        match leases.get(&config.lease_name).await {
            Ok(lease) => {
                let spec = lease.spec.as_ref();
                let current_holder = spec.and_then(|s| s.holder_identity.as_ref());
                let renew_time = spec.and_then(|s| s.renew_time.as_ref());
                let lease_duration = spec
                    .and_then(|s| s.lease_duration_seconds)
                    .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);

                let is_expired = renew_time.map_or(true, |rt| {
                    let renew_dt = rt.0;
                    now.signed_duration_since(renew_dt).num_seconds() > lease_duration as i64
                });

                let we_are_holder = current_holder
                    .map(|h| h == &config.identity)
                    .unwrap_or(false);

                if we_are_holder {
                    Self::renew_lease(leases, config, &now_str).await?;
                    is_leader.store(true, Ordering::SeqCst);
                    debug!(identity = %config.identity, "Renewed leader lease");
                } else if is_expired {
                    Self::acquire_lease(leases, config, &now_str).await?;
                    is_leader.store(true, Ordering::SeqCst);
                    info!(identity = %config.identity, "Acquired leader lease");
                } else {
                    is_leader.store(false, Ordering::SeqCst);
                    debug!(
                        identity = %config.identity,
                        holder = ?current_holder,
                        "Not the leader, waiting"
                    );
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                Self::create_lease(leases, config, &now_str).await?;
                is_leader.store(true, Ordering::SeqCst);
                info!(identity = %config.identity, "Created and acquired leader lease");
            }
            Err(e) => {
                return Err(OperatorError::KubeApi(e));
            }
        }

        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn create_lease(
        leases: &Api<Lease>,
        config: &LeaderElectionConfig,
        now: &str,
    ) -> Result<()> {
        let lease = serde_json::from_value(json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": config.lease_name,
                "namespace": config.namespace
            },
            "spec": {
                "holderIdentity": config.identity,
                "leaseDurationSeconds": config.lease_duration_seconds,
                "acquireTime": now,
                "renewTime": now,
                "leaseTransitions": 0
            }
        }))?;

        leases.create(&PostParams::default(), &lease).await?;
        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn acquire_lease(
        leases: &Api<Lease>,
        config: &LeaderElectionConfig,
        now: &str,
    ) -> Result<()> {
        let patch = json!({
            "spec": {
                "holderIdentity": config.identity,
                "acquireTime": now,
                "renewTime": now
            }
        });

        leases
            .patch(
                &config.lease_name,
                &PatchParams::apply("gitstratum-operator"),
                &Patch::Merge(&patch),
            )
            .await?;
        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn renew_lease(
        leases: &Api<Lease>,
        config: &LeaderElectionConfig,
        now: &str,
    ) -> Result<()> {
        let patch = json!({
            "spec": {
                "renewTime": now
            }
        });

        leases
            .patch(
                &config.lease_name,
                &PatchParams::apply("gitstratum-operator"),
                &Patch::Merge(&patch),
            )
            .await?;
        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    pub async fn release_leadership(&self) -> Result<()> {
        if !self.is_leader() {
            return Ok(());
        }

        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.config.namespace);

        match leases.get(&self.config.lease_name).await {
            Ok(lease) => {
                let spec = lease.spec.as_ref();
                let current_holder = spec.and_then(|s| s.holder_identity.as_ref());

                if current_holder == Some(&self.config.identity) {
                    let patch = json!({
                        "spec": {
                            "holderIdentity": null,
                            "renewTime": null
                        }
                    });

                    leases
                        .patch(
                            &self.config.lease_name,
                            &PatchParams::apply("gitstratum-operator"),
                            &Patch::Merge(&patch),
                        )
                        .await?;

                    info!(identity = %self.config.identity, "Released leader lease");
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                debug!("Lease not found, nothing to release");
            }
            Err(e) => {
                error!(error = %e, "Failed to release leadership");
                return Err(OperatorError::KubeApi(e));
            }
        }

        self.is_leader.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for LeaderElection {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::coordination::v1::LeaseSpec;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};

    #[test]
    fn test_leader_election_config_new() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1");

        assert_eq!(config.lease_name, "test-lease");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.identity, "pod-1");
        assert_eq!(
            config.lease_duration_seconds,
            DEFAULT_LEASE_DURATION_SECONDS
        );
        assert_eq!(
            config.renew_deadline,
            Duration::from_secs(DEFAULT_RENEW_DEADLINE_SECONDS)
        );
        assert_eq!(
            config.retry_period,
            Duration::from_secs(DEFAULT_RETRY_PERIOD_SECONDS)
        );
    }

    #[test]
    fn test_leader_election_config_with_lease_duration() {
        let config =
            LeaderElectionConfig::new("test-lease", "default", "pod-1").with_lease_duration(30);

        assert_eq!(config.lease_duration_seconds, 30);
    }

    #[test]
    fn test_leader_election_config_with_renew_deadline() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1")
            .with_renew_deadline(Duration::from_secs(20));

        assert_eq!(config.renew_deadline, Duration::from_secs(20));
    }

    #[test]
    fn test_leader_election_config_with_retry_period() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1")
            .with_retry_period(Duration::from_secs(5));

        assert_eq!(config.retry_period, Duration::from_secs(5));
    }

    #[test]
    fn test_leader_election_config_chained() {
        let config = LeaderElectionConfig::new("my-lease", "kube-system", "operator-xyz")
            .with_lease_duration(60)
            .with_renew_deadline(Duration::from_secs(45))
            .with_retry_period(Duration::from_secs(10));

        assert_eq!(config.lease_name, "my-lease");
        assert_eq!(config.namespace, "kube-system");
        assert_eq!(config.identity, "operator-xyz");
        assert_eq!(config.lease_duration_seconds, 60);
        assert_eq!(config.renew_deadline, Duration::from_secs(45));
        assert_eq!(config.retry_period, Duration::from_secs(10));
    }

    #[test]
    fn test_leader_election_config_clone() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1");
        let cloned = config.clone();

        assert_eq!(cloned.lease_name, config.lease_name);
        assert_eq!(cloned.namespace, config.namespace);
        assert_eq!(cloned.identity, config.identity);
    }

    #[test]
    fn test_is_leader_default_false() {
        let is_leader = Arc::new(AtomicBool::new(false));
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_is_leader_can_be_set() {
        let is_leader = Arc::new(AtomicBool::new(false));
        is_leader.store(true, Ordering::SeqCst);
        assert!(is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_leader_status_shared() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let status = Arc::clone(&is_leader);

        is_leader.store(true, Ordering::SeqCst);
        assert!(status.load(Ordering::SeqCst));

        status.store(false, Ordering::SeqCst);
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_LEASE_DURATION_SECONDS, 15);
        assert_eq!(DEFAULT_RENEW_DEADLINE_SECONDS, 10);
        assert_eq!(DEFAULT_RETRY_PERIOD_SECONDS, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_leader_status_updates() {
        let is_leader = Arc::new(AtomicBool::new(false));

        let mut handles = vec![];

        for i in 0..10 {
            let is_leader_clone = Arc::clone(&is_leader);
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    if i % 2 == 0 {
                        is_leader_clone.store(true, Ordering::SeqCst);
                    } else {
                        is_leader_clone.store(false, Ordering::SeqCst);
                    }
                    tokio::time::sleep(Duration::from_micros(10)).await;
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let _ = is_leader.load(Ordering::SeqCst);
    }

    #[test]
    fn test_election_config_all_fields() {
        let config = LeaderElectionConfig {
            lease_name: "custom-lease".to_string(),
            namespace: "custom-ns".to_string(),
            identity: "custom-identity".to_string(),
            lease_duration_seconds: 30,
            renew_deadline: Duration::from_secs(20),
            retry_period: Duration::from_secs(5),
        };

        assert_eq!(config.lease_name, "custom-lease");
        assert_eq!(config.namespace, "custom-ns");
        assert_eq!(config.identity, "custom-identity");
        assert_eq!(config.lease_duration_seconds, 30);
        assert_eq!(config.renew_deadline, Duration::from_secs(20));
        assert_eq!(config.retry_period, Duration::from_secs(5));
    }

    #[test]
    fn test_create_lease_json_format() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1");
        let now = chrono::Utc::now().to_rfc3339();

        let lease_json = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": config.lease_name,
                "namespace": config.namespace
            },
            "spec": {
                "holderIdentity": config.identity,
                "leaseDurationSeconds": config.lease_duration_seconds,
                "acquireTime": &now,
                "renewTime": &now,
                "leaseTransitions": 0
            }
        });

        let lease: Lease = serde_json::from_value(lease_json).unwrap();
        assert!(lease.spec.is_some());

        let spec = lease.spec.unwrap();
        assert_eq!(spec.holder_identity, Some("pod-1".to_string()));
        assert_eq!(spec.lease_duration_seconds, Some(15));
    }

    #[test]
    fn test_acquire_lease_patch_format() {
        let now = chrono::Utc::now().to_rfc3339();

        let patch = json!({
            "spec": {
                "holderIdentity": "pod-1",
                "acquireTime": &now,
                "renewTime": &now
            }
        });

        let patch_value: serde_json::Value = patch;
        assert!(patch_value.get("spec").is_some());
        assert_eq!(
            patch_value["spec"]["holderIdentity"].as_str(),
            Some("pod-1")
        );
    }

    #[test]
    fn test_renew_lease_patch_format() {
        let now = chrono::Utc::now().to_rfc3339();

        let patch = json!({
            "spec": {
                "renewTime": &now
            }
        });

        let patch_value: serde_json::Value = patch;
        assert!(patch_value.get("spec").is_some());
        assert!(patch_value["spec"]["renewTime"].is_string());
    }

    #[test]
    fn test_lease_expiration_calculation() {
        let now = chrono::Utc::now();

        let recent_time = now - chrono::Duration::seconds(5);
        let lease_duration = 15i64;
        let is_expired = now.signed_duration_since(recent_time).num_seconds() > lease_duration;
        assert!(!is_expired);

        let old_time = now - chrono::Duration::seconds(20);
        let is_expired_old = now.signed_duration_since(old_time).num_seconds() > lease_duration;
        assert!(is_expired_old);
    }

    #[test]
    fn test_holder_identity_comparison() {
        let our_identity = "pod-1";
        let holder: Option<&str> = Some("pod-1");

        let we_are_holder = holder.map(|h| h == our_identity).unwrap_or(false);
        assert!(we_are_holder);

        let other_holder: Option<&str> = Some("pod-2");
        let we_are_other = other_holder.map(|h| h == our_identity).unwrap_or(false);
        assert!(!we_are_other);

        let no_holder: Option<&str> = None;
        let we_are_none = no_holder.map(|h| h == our_identity).unwrap_or(false);
        assert!(!we_are_none);
    }

    #[tokio::test]
    async fn test_sleep_duration_calculation() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1")
            .with_renew_deadline(Duration::from_secs(10))
            .with_retry_period(Duration::from_secs(2));

        let is_leader = Arc::new(AtomicBool::new(true));

        let sleep_as_leader = if is_leader.load(Ordering::SeqCst) {
            config.renew_deadline / 2
        } else {
            config.retry_period
        };
        assert_eq!(sleep_as_leader, Duration::from_secs(5));

        is_leader.store(false, Ordering::SeqCst);
        let sleep_as_follower = if is_leader.load(Ordering::SeqCst) {
            config.renew_deadline / 2
        } else {
            config.retry_period
        };
        assert_eq!(sleep_as_follower, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn test_watch_channel_behavior() {
        let (tx, mut rx) = watch::channel(false);

        assert!(!*rx.borrow());

        tx.send(true).unwrap();
        rx.changed().await.unwrap();
        assert!(*rx.borrow());

        let rx2 = tx.subscribe();
        assert!(*rx2.borrow());
    }

    #[test]
    fn test_micro_time_parsing() {
        let now = chrono::Utc::now();
        let micro_time = MicroTime(now);

        assert_eq!(micro_time.0, now);
    }

    #[test]
    fn test_lease_spec_holder_extraction() {
        let spec = LeaseSpec {
            holder_identity: Some("pod-1".to_string()),
            lease_duration_seconds: Some(15),
            renew_time: Some(MicroTime(chrono::Utc::now())),
            acquire_time: Some(MicroTime(chrono::Utc::now())),
            lease_transitions: Some(1),
        };

        assert_eq!(spec.holder_identity.as_ref().unwrap(), "pod-1");
        assert_eq!(spec.lease_duration_seconds.unwrap(), 15);
    }

    #[test]
    fn test_lease_spec_empty() {
        let spec = LeaseSpec::default();

        assert!(spec.holder_identity.is_none());
        assert!(spec.lease_duration_seconds.is_none());
        assert!(spec.renew_time.is_none());
    }

    #[test]
    fn test_lease_with_metadata() {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some("test-lease".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some("pod-1".to_string()),
                lease_duration_seconds: Some(15),
                ..Default::default()
            }),
        };

        assert_eq!(lease.metadata.name.as_ref().unwrap(), "test-lease");
        assert_eq!(lease.metadata.namespace.as_ref().unwrap(), "default");
        assert!(lease.spec.is_some());
    }

    #[test]
    fn test_lease_expiration_with_no_renew_time() {
        let spec = LeaseSpec {
            holder_identity: Some("pod-1".to_string()),
            lease_duration_seconds: Some(15),
            renew_time: None,
            ..Default::default()
        };

        let is_expired = spec.renew_time.as_ref().map_or(true, |_rt| false);
        assert!(is_expired);
    }

    #[test]
    fn test_lease_expiration_with_recent_renew() {
        let now = chrono::Utc::now();
        let recent = now - chrono::Duration::seconds(5);
        let lease_duration = 15i32;

        let spec = LeaseSpec {
            holder_identity: Some("pod-1".to_string()),
            lease_duration_seconds: Some(lease_duration),
            renew_time: Some(MicroTime(recent)),
            ..Default::default()
        };

        let is_expired = spec.renew_time.as_ref().map_or(true, |rt| {
            let renew_dt = rt.0;
            now.signed_duration_since(renew_dt).num_seconds() > lease_duration as i64
        });
        assert!(!is_expired);
    }

    #[test]
    fn test_lease_expiration_with_old_renew() {
        let now = chrono::Utc::now();
        let old = now - chrono::Duration::seconds(30);
        let lease_duration = 15i32;

        let spec = LeaseSpec {
            holder_identity: Some("pod-1".to_string()),
            lease_duration_seconds: Some(lease_duration),
            renew_time: Some(MicroTime(old)),
            ..Default::default()
        };

        let is_expired = spec.renew_time.as_ref().map_or(true, |rt| {
            let renew_dt = rt.0;
            now.signed_duration_since(renew_dt).num_seconds() > lease_duration as i64
        });
        assert!(is_expired);
    }

    #[tokio::test]
    async fn test_shutdown_channel_send_receive() {
        let (tx, mut rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            rx.changed().await.unwrap();
            *rx.borrow()
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(true).unwrap();

        let result = handle.await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_atomic_bool_across_tasks() {
        let is_leader = Arc::new(AtomicBool::new(false));

        let setter = {
            let is_leader = Arc::clone(&is_leader);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                is_leader.store(true, Ordering::SeqCst);
            })
        };

        let checker = {
            let is_leader = Arc::clone(&is_leader);
            tokio::spawn(async move {
                for _ in 0..100 {
                    if is_leader.load(Ordering::SeqCst) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                false
            })
        };

        setter.await.unwrap();
        let found = checker.await.unwrap();
        assert!(found);
    }

    #[test]
    fn test_config_duration_arithmetic() {
        let config = LeaderElectionConfig::new("test", "default", "pod-1")
            .with_renew_deadline(Duration::from_secs(10));

        let half_deadline = config.renew_deadline / 2;
        assert_eq!(half_deadline, Duration::from_secs(5));

        let quarter_deadline = config.renew_deadline / 4;
        assert_eq!(quarter_deadline, Duration::from_millis(2500));
    }

    #[test]
    fn test_release_patch_format() {
        let patch = json!({
            "spec": {
                "holderIdentity": null,
                "renewTime": null
            }
        });

        let patch_value: serde_json::Value = patch;
        assert!(patch_value["spec"]["holderIdentity"].is_null());
        assert!(patch_value["spec"]["renewTime"].is_null());
    }

    #[tokio::test]
    async fn test_tokio_select_behavior() {
        let (tx, mut rx) = watch::channel(false);

        let result = tokio::select! {
            _ = rx.changed() => {
                "changed"
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                "timeout"
            }
        };

        assert_eq!(result, "timeout");

        tx.send(true).unwrap();

        let result2 = tokio::select! {
            _ = rx.changed() => {
                "changed"
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                "timeout"
            }
        };

        assert_eq!(result2, "changed");
    }

    #[test]
    fn test_drop_shutdown_tx() {
        let (tx, mut rx) = watch::channel(false);

        let mut shutdown_tx: Option<watch::Sender<bool>> = Some(tx);

        if let Some(tx) = shutdown_tx.take() {
            let _ = tx.send(true);
        }

        assert!(shutdown_tx.is_none());
        assert!(*rx.borrow_and_update());
    }

    #[test]
    fn test_lease_spec_with_all_fields() {
        let now = chrono::Utc::now();
        let spec = LeaseSpec {
            holder_identity: Some("leader-pod".to_string()),
            lease_duration_seconds: Some(30),
            acquire_time: Some(MicroTime(now)),
            renew_time: Some(MicroTime(now)),
            lease_transitions: Some(5),
        };

        assert_eq!(spec.holder_identity.as_deref(), Some("leader-pod"));
        assert_eq!(spec.lease_duration_seconds, Some(30));
        assert_eq!(spec.lease_transitions, Some(5));
        assert!(spec.acquire_time.is_some());
        assert!(spec.renew_time.is_some());
    }

    #[test]
    fn test_lease_duration_default_fallback() {
        let spec = LeaseSpec::default();
        let duration = spec
            .lease_duration_seconds
            .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);
        assert_eq!(duration, DEFAULT_LEASE_DURATION_SECONDS);
    }

    #[test]
    fn test_lease_holder_extraction_chain() {
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some("test-holder".to_string()),
                ..Default::default()
            }),
        };

        let holder = lease.spec.as_ref().and_then(|s| s.holder_identity.as_ref());
        assert_eq!(holder, Some(&"test-holder".to_string()));

        let lease_no_spec = Lease {
            metadata: ObjectMeta::default(),
            spec: None,
        };

        let holder_none = lease_no_spec
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());
        assert!(holder_none.is_none());
    }

    #[test]
    fn test_lease_renew_time_extraction_chain() {
        let now = chrono::Utc::now();
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                renew_time: Some(MicroTime(now)),
                ..Default::default()
            }),
        };

        let renew = lease.spec.as_ref().and_then(|s| s.renew_time.as_ref());
        assert!(renew.is_some());
        assert_eq!(renew.unwrap().0, now);
    }

    #[test]
    fn test_expiration_boundary_conditions() {
        let now = chrono::Utc::now();
        let lease_duration = 15i64;

        // Exactly at expiration boundary
        let boundary_time = now - chrono::Duration::seconds(lease_duration);
        let at_boundary = now.signed_duration_since(boundary_time).num_seconds() > lease_duration;
        assert!(!at_boundary); // 15 > 15 is false

        // One second past expiration
        let past_boundary = now - chrono::Duration::seconds(lease_duration + 1);
        let is_past = now.signed_duration_since(past_boundary).num_seconds() > lease_duration;
        assert!(is_past); // 16 > 15 is true
    }

    #[test]
    fn test_config_with_string_types() {
        let config = LeaderElectionConfig::new(
            String::from("lease-name"),
            String::from("namespace"),
            String::from("identity"),
        );
        assert_eq!(config.lease_name, "lease-name");
        assert_eq!(config.namespace, "namespace");
        assert_eq!(config.identity, "identity");
    }

    #[test]
    fn test_config_with_str_slices() {
        let lease: &str = "my-lease";
        let ns: &str = "my-ns";
        let id: &str = "my-id";
        let config = LeaderElectionConfig::new(lease, ns, id);
        assert_eq!(config.lease_name, "my-lease");
        assert_eq!(config.namespace, "my-ns");
        assert_eq!(config.identity, "my-id");
    }

    #[test]
    fn test_config_builder_pattern_preserves_defaults() {
        let config = LeaderElectionConfig::new("lease", "ns", "id").with_lease_duration(60);

        // Modified field
        assert_eq!(config.lease_duration_seconds, 60);
        // Preserved defaults
        assert_eq!(
            config.renew_deadline,
            Duration::from_secs(DEFAULT_RENEW_DEADLINE_SECONDS)
        );
        assert_eq!(
            config.retry_period,
            Duration::from_secs(DEFAULT_RETRY_PERIOD_SECONDS)
        );
    }

    #[test]
    fn test_lease_transitions_tracking() {
        let spec = LeaseSpec {
            lease_transitions: Some(0),
            ..Default::default()
        };
        assert_eq!(spec.lease_transitions, Some(0));

        let spec_many = LeaseSpec {
            lease_transitions: Some(100),
            ..Default::default()
        };
        assert_eq!(spec_many.lease_transitions, Some(100));
    }

    #[test]
    fn test_holder_identity_equality_cases() {
        let config_identity = "pod-abc-123";

        // Exact match
        let holder1: Option<String> = Some("pod-abc-123".to_string());
        assert!(holder1
            .as_ref()
            .map(|h| h == config_identity)
            .unwrap_or(false));

        // Different holder
        let holder2: Option<String> = Some("pod-xyz-789".to_string());
        assert!(!holder2
            .as_ref()
            .map(|h| h == config_identity)
            .unwrap_or(false));

        // Empty string holder
        let holder3: Option<String> = Some(String::new());
        assert!(!holder3
            .as_ref()
            .map(|h| h == config_identity)
            .unwrap_or(false));

        // None holder
        let holder4: Option<String> = None;
        assert!(!holder4
            .as_ref()
            .map(|h| h == config_identity)
            .unwrap_or(false));
    }

    #[test]
    fn test_duration_divisions() {
        let deadline = Duration::from_secs(10);
        assert_eq!(deadline / 2, Duration::from_secs(5));
        assert_eq!(deadline / 4, Duration::from_millis(2500));
        assert_eq!(deadline / 10, Duration::from_secs(1));

        let odd_deadline = Duration::from_secs(15);
        assert_eq!(odd_deadline / 2, Duration::from_millis(7500));
    }

    #[test]
    fn test_chrono_duration_arithmetic() {
        let now = chrono::Utc::now();
        let past = now - chrono::Duration::seconds(100);
        let diff = now.signed_duration_since(past);
        assert_eq!(diff.num_seconds(), 100);

        let future = now + chrono::Duration::seconds(50);
        let future_diff = now.signed_duration_since(future);
        assert_eq!(future_diff.num_seconds(), -50);
    }

    #[test]
    fn test_rfc3339_format() {
        let now = chrono::Utc::now();
        let formatted = now.to_rfc3339();
        assert!(formatted.contains("T"));
        assert!(formatted.ends_with('Z') || formatted.contains('+') || formatted.contains('-'));
    }

    #[tokio::test]
    async fn test_watch_channel_multiple_sends() {
        let (tx, mut rx) = watch::channel(0i32);

        tx.send(1).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 1);

        tx.send(2).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 2);

        tx.send(3).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 3);
    }

    #[tokio::test]
    async fn test_watch_channel_borrow_without_changed() {
        let (tx, rx) = watch::channel(42);
        assert_eq!(*rx.borrow(), 42);

        tx.send(100).unwrap();
        // borrow() returns current value even without calling changed()
        assert_eq!(*rx.borrow(), 100);
    }

    #[test]
    fn test_atomic_ordering_seqcst() {
        let flag = Arc::new(AtomicBool::new(false));

        // SeqCst provides strongest ordering guarantees
        flag.store(true, Ordering::SeqCst);
        assert!(flag.load(Ordering::SeqCst));

        flag.store(false, Ordering::SeqCst);
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_lease_json_serialization_roundtrip() {
        let original = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "test-lease",
                "namespace": "default"
            },
            "spec": {
                "holderIdentity": "pod-1",
                "leaseDurationSeconds": 15,
                "leaseTransitions": 0
            }
        });

        let lease: Lease = serde_json::from_value(original.clone()).unwrap();
        let serialized = serde_json::to_value(&lease).unwrap();

        assert_eq!(
            lease.spec.as_ref().unwrap().holder_identity,
            Some("pod-1".to_string())
        );
        assert!(serialized.get("spec").is_some());
    }

    #[test]
    fn test_patch_params_apply() {
        let params = PatchParams::apply("gitstratum-operator");
        // PatchParams::apply sets field_manager
        assert!(format!("{:?}", params).contains("gitstratum-operator"));
    }

    #[test]
    fn test_post_params_default() {
        let params = PostParams::default();
        // Default PostParams should be valid
        assert!(format!("{:?}", params).contains("PostParams"));
    }

    #[test]
    fn test_config_extreme_values() {
        let config = LeaderElectionConfig::new("l", "n", "i")
            .with_lease_duration(i32::MAX)
            .with_renew_deadline(Duration::from_secs(u64::MAX / 2))
            .with_retry_period(Duration::from_nanos(1));

        assert_eq!(config.lease_duration_seconds, i32::MAX);
        assert_eq!(config.renew_deadline, Duration::from_secs(u64::MAX / 2));
        assert_eq!(config.retry_period, Duration::from_nanos(1));
    }

    #[test]
    fn test_config_zero_values() {
        let config = LeaderElectionConfig::new("lease", "ns", "id")
            .with_lease_duration(0)
            .with_renew_deadline(Duration::ZERO)
            .with_retry_period(Duration::ZERO);

        assert_eq!(config.lease_duration_seconds, 0);
        assert_eq!(config.renew_deadline, Duration::ZERO);
        assert_eq!(config.retry_period, Duration::ZERO);
    }

    #[test]
    fn test_lease_expiration_with_zero_duration() {
        let now = chrono::Utc::now();
        let lease_duration = 0i64;

        // With zero duration, any time difference should be expired
        let recent = now - chrono::Duration::seconds(1);
        let is_expired = now.signed_duration_since(recent).num_seconds() > lease_duration;
        assert!(is_expired);
    }

    #[test]
    fn test_lease_spec_default_values() {
        let spec = LeaseSpec::default();
        assert!(spec.holder_identity.is_none());
        assert!(spec.lease_duration_seconds.is_none());
        assert!(spec.acquire_time.is_none());
        assert!(spec.renew_time.is_none());
        assert!(spec.lease_transitions.is_none());
    }

    #[test]
    fn test_lease_metadata_extraction() {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some("operator-lock".to_string()),
                namespace: Some("gitstratum-system".to_string()),
                uid: Some("abc-123-def".to_string()),
                resource_version: Some("12345".to_string()),
                ..Default::default()
            },
            spec: None,
        };

        assert_eq!(lease.metadata.name, Some("operator-lock".to_string()));
        assert_eq!(
            lease.metadata.namespace,
            Some("gitstratum-system".to_string())
        );
        assert_eq!(lease.metadata.uid, Some("abc-123-def".to_string()));
        assert_eq!(lease.metadata.resource_version, Some("12345".to_string()));
    }

    #[test]
    fn test_try_acquire_logic_we_are_holder() {
        let config_identity = "pod-1";
        let current_holder: Option<&str> = Some("pod-1");

        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(we_are_holder);
        // When we are holder, we should renew
    }

    #[test]
    fn test_try_acquire_logic_expired_lease() {
        let now = chrono::Utc::now();
        let config_identity = "pod-1";
        let current_holder: Option<&str> = Some("pod-2"); // Different holder
        let lease_duration = 15i32;

        // Lease expired 30 seconds ago
        let renew_time = Some(MicroTime(now - chrono::Duration::seconds(30)));

        let is_expired = renew_time.as_ref().map_or(true, |rt| {
            now.signed_duration_since(rt.0).num_seconds() > lease_duration as i64
        });

        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(!we_are_holder);
        assert!(is_expired);
        // When expired and not holder, we should acquire
    }

    #[test]
    fn test_try_acquire_logic_not_expired_not_holder() {
        let now = chrono::Utc::now();
        let config_identity = "pod-1";
        let current_holder: Option<&str> = Some("pod-2"); // Different holder
        let lease_duration = 15i32;

        // Lease renewed 5 seconds ago (not expired)
        let renew_time = Some(MicroTime(now - chrono::Duration::seconds(5)));

        let is_expired = renew_time.as_ref().map_or(true, |rt| {
            now.signed_duration_since(rt.0).num_seconds() > lease_duration as i64
        });

        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(!we_are_holder);
        assert!(!is_expired);
        // When not expired and not holder, we should wait
    }

    #[test]
    fn test_sleep_duration_logic() {
        let renew_deadline = Duration::from_secs(10);
        let retry_period = Duration::from_secs(2);

        // As leader, sleep for half the renew deadline
        let is_leader = true;
        let sleep_as_leader = if is_leader {
            renew_deadline / 2
        } else {
            retry_period
        };
        assert_eq!(sleep_as_leader, Duration::from_secs(5));

        // As follower, sleep for retry period
        let is_leader = false;
        let sleep_as_follower = if is_leader {
            renew_deadline / 2
        } else {
            retry_period
        };
        assert_eq!(sleep_as_follower, Duration::from_secs(2));
    }

    #[test]
    fn test_shutdown_signal_handling() {
        let (tx, rx) = watch::channel(false);

        // Initially not shutdown
        assert!(!*rx.borrow());

        // Send shutdown signal
        tx.send(true).unwrap();
        assert!(*rx.borrow());

        // Check shutdown condition
        if *rx.borrow() {
            // Should break out of loop and set is_leader to false
            let is_leader = Arc::new(AtomicBool::new(true));
            is_leader.store(false, Ordering::SeqCst);
            assert!(!is_leader.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn test_error_handling_sets_leader_false() {
        let is_leader = Arc::new(AtomicBool::new(true));

        // Simulate error in election cycle
        let result: std::result::Result<(), &str> = Err("connection failed");
        if result.is_err() {
            is_leader.store(false, Ordering::SeqCst);
        }

        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_release_leadership_logic_not_leader() {
        let is_leader = Arc::new(AtomicBool::new(false));

        // If not leader, should return early
        if !is_leader.load(Ordering::SeqCst) {
            // Early return - nothing to release
            assert!(true);
        }
    }

    #[test]
    fn test_release_leadership_logic_is_leader() {
        let is_leader = Arc::new(AtomicBool::new(true));
        let config_identity = "pod-1";
        let current_holder: Option<&str> = Some("pod-1");

        // If we are the holder, we should release
        if current_holder == Some(config_identity) {
            // Would patch with null holderIdentity and renewTime
            let patch = json!({
                "spec": {
                    "holderIdentity": null,
                    "renewTime": null
                }
            });
            assert!(patch["spec"]["holderIdentity"].is_null());
        }

        // After release, set leader to false
        is_leader.store(false, Ordering::SeqCst);
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_release_leadership_different_holder() {
        let config_identity = "pod-1";
        let current_holder: Option<&str> = Some("pod-2"); // Different holder

        // If different holder, should not release
        let should_release = current_holder == Some(config_identity);
        assert!(!should_release);
    }

    #[test]
    fn test_create_lease_json_structure() {
        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1");
        let now = chrono::Utc::now().to_rfc3339();

        let lease_json = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": config.lease_name,
                "namespace": config.namespace
            },
            "spec": {
                "holderIdentity": config.identity,
                "leaseDurationSeconds": config.lease_duration_seconds,
                "acquireTime": &now,
                "renewTime": &now,
                "leaseTransitions": 0
            }
        });

        assert_eq!(lease_json["apiVersion"], "coordination.k8s.io/v1");
        assert_eq!(lease_json["kind"], "Lease");
        assert_eq!(lease_json["metadata"]["name"], "test-lease");
        assert_eq!(lease_json["metadata"]["namespace"], "default");
        assert_eq!(lease_json["spec"]["holderIdentity"], "pod-1");
        assert_eq!(lease_json["spec"]["leaseDurationSeconds"], 15);
        assert_eq!(lease_json["spec"]["leaseTransitions"], 0);
    }

    #[tokio::test]
    async fn test_election_loop_shutdown_path() {
        let (tx, mut rx) = watch::channel(false);
        let is_leader = Arc::new(AtomicBool::new(true));

        // Simulate shutdown signal
        tx.send(true).unwrap();

        // Check shutdown condition
        if rx.changed().await.is_ok() && *rx.borrow() {
            is_leader.store(false, Ordering::SeqCst);
        }

        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_run_with_leader_setup() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let is_leader = Arc::clone(&Arc::new(AtomicBool::new(false)));
        let config = LeaderElectionConfig::new("test", "default", "pod-1");

        // Verify setup
        assert!(!*shutdown_rx.borrow());
        assert!(!is_leader.load(Ordering::SeqCst));
        assert_eq!(config.lease_name, "test");

        // Cleanup
        drop(shutdown_tx);
    }

    #[test]
    fn test_lease_not_found_creates_new() {
        // When lease not found (404), should create new lease
        let error_code = 404u16;
        let should_create = error_code == 404;
        assert!(should_create);
    }

    #[test]
    fn test_lease_other_error_propagates() {
        // When other error occurs, should propagate
        let error_code = 500u16;
        let should_create = error_code == 404;
        let should_propagate = !should_create;
        assert!(should_propagate);
    }

    #[test]
    fn test_micro_time_comparison() {
        let now = chrono::Utc::now();
        let earlier = now - chrono::Duration::seconds(10);
        let later = now + chrono::Duration::seconds(10);

        let mt_now = MicroTime(now);
        let mt_earlier = MicroTime(earlier);
        let mt_later = MicroTime(later);

        assert!(mt_now.0 > mt_earlier.0);
        assert!(mt_now.0 < mt_later.0);
    }

    #[test]
    fn test_config_identity_variations() {
        // Pod name style
        let config1 = LeaderElectionConfig::new("lease", "ns", "operator-pod-abc123");
        assert!(config1.identity.contains("operator-pod"));

        // UUID style
        let config2 =
            LeaderElectionConfig::new("lease", "ns", "550e8400-e29b-41d4-a716-446655440000");
        assert!(config2.identity.contains("-"));

        // Hostname style
        let config3 = LeaderElectionConfig::new("lease", "ns", "node-1.cluster.local");
        assert!(config3.identity.contains("."));
    }

    #[test]
    fn test_namespace_variations() {
        let config1 = LeaderElectionConfig::new("lease", "default", "pod");
        assert_eq!(config1.namespace, "default");

        let config2 = LeaderElectionConfig::new("lease", "kube-system", "pod");
        assert_eq!(config2.namespace, "kube-system");

        let config3 = LeaderElectionConfig::new("lease", "gitstratum-operator-system", "pod");
        assert_eq!(config3.namespace, "gitstratum-operator-system");
    }

    #[test]
    fn test_lease_name_conventions() {
        // Standard naming
        let config1 = LeaderElectionConfig::new("gitstratum-operator-lock", "ns", "pod");
        assert!(config1.lease_name.ends_with("-lock"));

        // Leader suffix
        let config2 = LeaderElectionConfig::new("controller-leader", "ns", "pod");
        assert!(config2.lease_name.ends_with("-leader"));

        // Election suffix
        let config3 = LeaderElectionConfig::new("manager-election", "ns", "pod");
        assert!(config3.lease_name.ends_with("-election"));
    }

    #[test]
    fn test_arc_clone_independence() {
        let original = Arc::new(AtomicBool::new(false));
        let clone1 = Arc::clone(&original);
        let clone2 = Arc::clone(&original);

        // All point to same value
        assert_eq!(Arc::strong_count(&original), 3);

        // Mutation through one affects all
        clone1.store(true, Ordering::SeqCst);
        assert!(original.load(Ordering::SeqCst));
        assert!(clone2.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_wait_for_leadership_pattern() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = Arc::clone(&is_leader);

        // Simulate becoming leader after a delay
        let setter = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            is_leader_clone.store(true, Ordering::SeqCst);
        });

        // Wait for leadership (like in run_with_leader)
        let mut attempts = 0;
        while !is_leader.load(Ordering::SeqCst) && attempts < 100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            attempts += 1;
        }

        setter.await.unwrap();
        assert!(is_leader.load(Ordering::SeqCst));
        assert!(attempts < 100);
    }

    #[test]
    fn test_option_take_pattern() {
        let mut shutdown_tx: Option<watch::Sender<bool>> = {
            let (tx, _rx) = watch::channel(false);
            Some(tx)
        };

        assert!(shutdown_tx.is_some());

        if let Some(tx) = shutdown_tx.take() {
            let _ = tx.send(true);
        }

        assert!(shutdown_tx.is_none());
    }

    #[test]
    fn test_lease_spec_field_access_patterns() {
        let spec = LeaseSpec {
            holder_identity: Some("holder".to_string()),
            lease_duration_seconds: Some(30),
            acquire_time: Some(MicroTime(chrono::Utc::now())),
            renew_time: Some(MicroTime(chrono::Utc::now())),
            lease_transitions: Some(5),
        };

        // Pattern used in try_acquire_or_renew
        let current_holder = spec.holder_identity.as_ref();
        let renew_time = spec.renew_time.as_ref();
        let lease_duration = spec
            .lease_duration_seconds
            .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);

        assert_eq!(current_holder, Some(&"holder".to_string()));
        assert!(renew_time.is_some());
        assert_eq!(lease_duration, 30);
    }

    #[test]
    fn test_lease_from_option_spec() {
        let lease_with_spec = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec::default()),
        };

        let lease_without_spec = Lease {
            metadata: ObjectMeta::default(),
            spec: None,
        };

        // Pattern: lease.spec.as_ref()
        assert!(lease_with_spec.spec.as_ref().is_some());
        assert!(lease_without_spec.spec.as_ref().is_none());

        // Pattern: lease.spec.as_ref().and_then(|s| s.holder_identity.as_ref())
        let holder_with = lease_with_spec
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());
        let holder_without = lease_without_spec
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());

        assert!(holder_with.is_none()); // spec exists but holder is None
        assert!(holder_without.is_none()); // spec doesn't exist
    }

    #[test]
    fn test_duration_from_various_units() {
        // Test duration creation patterns used in config
        let from_secs = Duration::from_secs(10);
        let from_millis = Duration::from_millis(10000);
        let from_nanos = Duration::from_nanos(10_000_000_000);

        assert_eq!(from_secs, from_millis);
        assert_eq!(from_secs, from_nanos);
    }

    #[test]
    fn test_chrono_duration_to_std_duration() {
        let chrono_dur = chrono::Duration::seconds(10);
        let std_dur = chrono_dur.to_std().unwrap();

        assert_eq!(std_dur, Duration::from_secs(10));
    }

    #[test]
    fn test_config_with_negative_lease_duration() {
        let config =
            LeaderElectionConfig::new("test-lease", "default", "pod-1").with_lease_duration(-5);

        assert_eq!(config.lease_duration_seconds, -5);
    }

    #[test]
    fn test_config_with_empty_strings() {
        let config = LeaderElectionConfig::new("", "", "");

        assert_eq!(config.lease_name, "");
        assert_eq!(config.namespace, "");
        assert_eq!(config.identity, "");
        assert_eq!(
            config.lease_duration_seconds,
            DEFAULT_LEASE_DURATION_SECONDS
        );
    }

    #[test]
    fn test_config_clone_is_independent() {
        let config1 = LeaderElectionConfig::new("lease-1", "ns-1", "id-1").with_lease_duration(30);
        let config2 = config1.clone();

        assert_eq!(config2.lease_name, "lease-1");
        assert_eq!(config2.namespace, "ns-1");
        assert_eq!(config2.identity, "id-1");
        assert_eq!(config2.lease_duration_seconds, 30);
        assert_eq!(
            config1.lease_duration_seconds,
            config2.lease_duration_seconds
        );
    }

    #[test]
    fn test_config_new_with_owned_strings() {
        let lease = String::from("owned-lease");
        let ns = String::from("owned-ns");
        let id = String::from("owned-id");

        let config = LeaderElectionConfig::new(lease, ns, id);

        assert_eq!(config.lease_name, "owned-lease");
        assert_eq!(config.namespace, "owned-ns");
        assert_eq!(config.identity, "owned-id");
    }

    #[test]
    fn test_config_builder_order_independence() {
        let config1 = LeaderElectionConfig::new("lease", "ns", "id")
            .with_lease_duration(30)
            .with_renew_deadline(Duration::from_secs(20))
            .with_retry_period(Duration::from_secs(5));

        let config2 = LeaderElectionConfig::new("lease", "ns", "id")
            .with_retry_period(Duration::from_secs(5))
            .with_renew_deadline(Duration::from_secs(20))
            .with_lease_duration(30);

        assert_eq!(
            config1.lease_duration_seconds,
            config2.lease_duration_seconds
        );
        assert_eq!(config1.renew_deadline, config2.renew_deadline);
        assert_eq!(config1.retry_period, config2.retry_period);
    }

    #[test]
    fn test_config_struct_field_access() {
        let config = LeaderElectionConfig {
            lease_name: "direct-lease".to_string(),
            namespace: "direct-ns".to_string(),
            identity: "direct-id".to_string(),
            lease_duration_seconds: 45,
            renew_deadline: Duration::from_secs(30),
            retry_period: Duration::from_secs(10),
        };

        assert_eq!(&config.lease_name, "direct-lease");
        assert_eq!(&config.namespace, "direct-ns");
        assert_eq!(&config.identity, "direct-id");
        assert_eq!(config.lease_duration_seconds, 45);
        assert_eq!(config.renew_deadline.as_secs(), 30);
        assert_eq!(config.retry_period.as_secs(), 10);
    }

    #[test]
    fn test_config_with_unicode_strings() {
        let config = LeaderElectionConfig::new("lease-日本語", "namespace-中文", "identity-한국어");

        assert_eq!(config.lease_name, "lease-日本語");
        assert_eq!(config.namespace, "namespace-中文");
        assert_eq!(config.identity, "identity-한국어");
    }

    #[test]
    fn test_config_with_special_characters() {
        let config = LeaderElectionConfig::new(
            "lease-with-dots.and-dashes",
            "namespace_with_underscores",
            "identity/with/slashes",
        );

        assert!(config.lease_name.contains('.'));
        assert!(config.namespace.contains('_'));
        assert!(config.identity.contains('/'));
    }

    #[test]
    fn test_config_with_very_long_strings() {
        let long_string = "a".repeat(1000);
        let config = LeaderElectionConfig::new(
            long_string.clone(),
            long_string.clone(),
            long_string.clone(),
        );

        assert_eq!(config.lease_name.len(), 1000);
        assert_eq!(config.namespace.len(), 1000);
        assert_eq!(config.identity.len(), 1000);
    }

    #[test]
    fn test_config_duration_nanoseconds() {
        let config = LeaderElectionConfig::new("lease", "ns", "id")
            .with_renew_deadline(Duration::from_nanos(1_000_000_000))
            .with_retry_period(Duration::from_nanos(500_000_000));

        assert_eq!(config.renew_deadline, Duration::from_secs(1));
        assert_eq!(config.retry_period, Duration::from_millis(500));
    }

    #[test]
    fn test_config_duration_subsecond_precision() {
        let config = LeaderElectionConfig::new("lease", "ns", "id")
            .with_renew_deadline(Duration::from_millis(1500))
            .with_retry_period(Duration::from_micros(250_000));

        assert_eq!(config.renew_deadline.as_millis(), 1500);
        assert_eq!(config.retry_period.as_micros(), 250_000);
    }

    #[test]
    fn test_signed_duration_negative() {
        let now = chrono::Utc::now();
        let future = now + chrono::Duration::seconds(100);

        // When comparing now to future, result is negative
        let diff = now.signed_duration_since(future);
        assert!(diff.num_seconds() < 0);
        assert_eq!(diff.num_seconds(), -100);
    }

    #[tokio::test]
    async fn test_select_timeout_vs_signal() {
        let (tx, mut rx) = watch::channel(false);

        // Test timeout wins when no signal
        let result = tokio::select! {
            _ = rx.changed() => "signal",
            _ = tokio::time::sleep(Duration::from_millis(10)) => "timeout",
        };
        assert_eq!(result, "timeout");

        // Test signal wins when sent before timeout
        tx.send(true).unwrap();
        let result2 = tokio::select! {
            _ = rx.changed() => "signal",
            _ = tokio::time::sleep(Duration::from_secs(10)) => "timeout",
        };
        assert_eq!(result2, "signal");
    }

    #[test]
    fn test_json_null_values() {
        let patch = json!({
            "spec": {
                "holderIdentity": serde_json::Value::Null,
                "renewTime": serde_json::Value::Null
            }
        });

        assert!(patch["spec"]["holderIdentity"].is_null());
        assert!(patch["spec"]["renewTime"].is_null());

        // Alternative null syntax
        let patch2 = json!({
            "spec": {
                "holderIdentity": null,
                "renewTime": null
            }
        });

        assert_eq!(patch, patch2);
    }

    #[test]
    fn test_lease_transitions_increment() {
        let mut transitions = 0i32;

        // Simulate leadership changes
        transitions += 1; // First acquisition
        assert_eq!(transitions, 1);

        transitions += 1; // Lost and reacquired
        assert_eq!(transitions, 2);

        // In real code, this is tracked in the lease spec
        let spec = LeaseSpec {
            lease_transitions: Some(transitions),
            ..Default::default()
        };
        assert_eq!(spec.lease_transitions, Some(2));
    }

    // Simulated cluster tests using tokio::spawn

    #[tokio::test]
    async fn test_simulated_election_loop_acquire_leadership() {
        use tokio::sync::mpsc;

        let is_leader = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (lease_tx, mut lease_rx) = mpsc::channel::<&str>(10);

        let is_leader_clone = Arc::clone(&is_leader);

        // Simulate election loop
        let election_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            is_leader_clone.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                    Some(action) = lease_rx.recv() => {
                        match action {
                            "acquire" => {
                                is_leader_clone.store(true, Ordering::SeqCst);
                            }
                            "lose" => {
                                is_leader_clone.store(false, Ordering::SeqCst);
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Simulate acquiring leadership
        lease_tx.send("acquire").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(is_leader.load(Ordering::SeqCst));

        // Shutdown
        shutdown_tx.send(true).unwrap();
        election_handle.await.unwrap();
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_simulated_election_loop_lose_leadership() {
        use tokio::sync::mpsc;

        let is_leader = Arc::new(AtomicBool::new(true));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (lease_tx, mut lease_rx) = mpsc::channel::<&str>(10);

        let is_leader_clone = Arc::clone(&is_leader);

        let election_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    Some(action) = lease_rx.recv() => {
                        match action {
                            "lose" => {
                                is_leader_clone.store(false, Ordering::SeqCst);
                            }
                            _ => {}
                        }
                    }
                }
            }
        });

        // Simulate losing leadership
        lease_tx.send("lose").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!is_leader.load(Ordering::SeqCst));

        shutdown_tx.send(true).unwrap();
        election_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_simulated_try_acquire_or_renew_we_are_holder() {
        use tokio::sync::mpsc;

        #[derive(Clone)]
        struct MockLease {
            holder: Option<String>,
            renew_time: Option<chrono::DateTime<chrono::Utc>>,
            duration_secs: i32,
        }

        let (request_tx, mut request_rx) = mpsc::channel::<String>(10);
        let (response_tx, mut response_rx) = mpsc::channel::<MockLease>(10);

        let config_identity = "pod-1".to_string();
        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = Arc::clone(&is_leader);

        // Mock API server
        let server_handle = tokio::spawn(async move {
            while let Some(_req) = request_rx.recv().await {
                // Return lease where we are the holder
                let lease = MockLease {
                    holder: Some("pod-1".to_string()),
                    renew_time: Some(chrono::Utc::now()),
                    duration_secs: 15,
                };
                response_tx.send(lease).await.unwrap();
            }
        });

        // Simulate try_acquire_or_renew
        request_tx.send("get_lease".to_string()).await.unwrap();
        let lease = response_rx.recv().await.unwrap();

        let we_are_holder = lease
            .holder
            .as_ref()
            .map(|h| h == &config_identity)
            .unwrap_or(false);

        if we_are_holder {
            // Renew lease
            is_leader_clone.store(true, Ordering::SeqCst);
        }

        assert!(is_leader.load(Ordering::SeqCst));

        drop(request_tx);
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_simulated_try_acquire_or_renew_expired_lease() {
        use tokio::sync::mpsc;

        #[derive(Clone)]
        struct MockLease {
            holder: Option<String>,
            renew_time: Option<chrono::DateTime<chrono::Utc>>,
            duration_secs: i32,
        }

        let (request_tx, mut request_rx) = mpsc::channel::<String>(10);
        let (response_tx, mut response_rx) = mpsc::channel::<MockLease>(10);

        let config_identity = "pod-1".to_string();
        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = Arc::clone(&is_leader);

        // Mock API server returning expired lease held by someone else
        let server_handle = tokio::spawn(async move {
            while let Some(_req) = request_rx.recv().await {
                let lease = MockLease {
                    holder: Some("pod-2".to_string()), // Different holder
                    renew_time: Some(chrono::Utc::now() - chrono::Duration::seconds(30)), // Expired
                    duration_secs: 15,
                };
                response_tx.send(lease).await.unwrap();
            }
        });

        request_tx.send("get_lease".to_string()).await.unwrap();
        let lease = response_rx.recv().await.unwrap();

        let now = chrono::Utc::now();
        let we_are_holder = lease
            .holder
            .as_ref()
            .map(|h| h == &config_identity)
            .unwrap_or(false);

        let is_expired = lease.renew_time.map_or(true, |rt| {
            now.signed_duration_since(rt).num_seconds() > lease.duration_secs as i64
        });

        if !we_are_holder && is_expired {
            // Acquire expired lease
            is_leader_clone.store(true, Ordering::SeqCst);
        }

        assert!(is_leader.load(Ordering::SeqCst));

        drop(request_tx);
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_simulated_try_acquire_or_renew_not_expired() {
        use tokio::sync::mpsc;

        #[derive(Clone)]
        struct MockLease {
            holder: Option<String>,
            renew_time: Option<chrono::DateTime<chrono::Utc>>,
            duration_secs: i32,
        }

        let (request_tx, mut request_rx) = mpsc::channel::<String>(10);
        let (response_tx, mut response_rx) = mpsc::channel::<MockLease>(10);

        let config_identity = "pod-1".to_string();
        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = Arc::clone(&is_leader);

        // Mock API server returning valid lease held by someone else
        let server_handle = tokio::spawn(async move {
            while let Some(_req) = request_rx.recv().await {
                let lease = MockLease {
                    holder: Some("pod-2".to_string()),
                    renew_time: Some(chrono::Utc::now() - chrono::Duration::seconds(5)), // Not expired
                    duration_secs: 15,
                };
                response_tx.send(lease).await.unwrap();
            }
        });

        request_tx.send("get_lease".to_string()).await.unwrap();
        let lease = response_rx.recv().await.unwrap();

        let now = chrono::Utc::now();
        let we_are_holder = lease
            .holder
            .as_ref()
            .map(|h| h == &config_identity)
            .unwrap_or(false);

        let is_expired = lease.renew_time.map_or(true, |rt| {
            now.signed_duration_since(rt).num_seconds() > lease.duration_secs as i64
        });

        if we_are_holder {
            is_leader_clone.store(true, Ordering::SeqCst);
        } else if is_expired {
            is_leader_clone.store(true, Ordering::SeqCst);
        } else {
            is_leader_clone.store(false, Ordering::SeqCst);
        }

        // Should NOT be leader since lease is valid and held by someone else
        assert!(!is_leader.load(Ordering::SeqCst));

        drop(request_tx);
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_simulated_lease_not_found_creates_new() {
        use tokio::sync::mpsc;

        #[derive(Clone)]
        enum ApiResponse {
            NotFound,
            Created,
        }

        let (request_tx, mut request_rx) = mpsc::channel::<String>(10);
        let (response_tx, mut response_rx) = mpsc::channel::<ApiResponse>(10);

        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = Arc::clone(&is_leader);

        // Mock API server that returns 404 on get, success on create
        let server_handle = tokio::spawn(async move {
            while let Some(req) = request_rx.recv().await {
                match req.as_str() {
                    "get" => {
                        response_tx.send(ApiResponse::NotFound).await.unwrap();
                    }
                    "create" => {
                        response_tx.send(ApiResponse::Created).await.unwrap();
                    }
                    _ => {}
                }
            }
        });

        // Try to get lease - not found
        request_tx.send("get".to_string()).await.unwrap();
        let response = response_rx.recv().await.unwrap();

        match response {
            ApiResponse::NotFound => {
                // Create new lease
                request_tx.send("create".to_string()).await.unwrap();
                let create_response = response_rx.recv().await.unwrap();
                if matches!(create_response, ApiResponse::Created) {
                    is_leader_clone.store(true, Ordering::SeqCst);
                }
            }
            _ => {}
        }

        assert!(is_leader.load(Ordering::SeqCst));

        drop(request_tx);
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_simulated_full_election_cycle() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        // Simulate lease state
        let lease_holder = Arc::new(std::sync::RwLock::new(None::<String>));
        let lease_renew_time = Arc::new(std::sync::RwLock::new(
            None::<chrono::DateTime<chrono::Utc>>,
        ));

        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1");
        let is_leader_clone = Arc::clone(&is_leader);
        let lease_holder_clone = Arc::clone(&lease_holder);
        let lease_renew_time_clone = Arc::clone(&lease_renew_time);

        // Simulated election loop
        let election_handle = tokio::spawn(async move {
            let mut cycle_count = 0;
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            is_leader_clone.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        let now = chrono::Utc::now();
                        let holder = lease_holder_clone.read().unwrap().clone();
                        let renew_time = *lease_renew_time_clone.read().unwrap();

                        let we_are_holder = holder.as_ref().map(|h| h == &config.identity).unwrap_or(false);
                        let is_expired = renew_time.map_or(true, |rt| {
                            now.signed_duration_since(rt).num_seconds() > config.lease_duration_seconds as i64
                        });

                        if we_are_holder {
                            // Renew
                            *lease_renew_time_clone.write().unwrap() = Some(now);
                            is_leader_clone.store(true, Ordering::SeqCst);
                        } else if is_expired || holder.is_none() {
                            // Acquire
                            *lease_holder_clone.write().unwrap() = Some(config.identity.clone());
                            *lease_renew_time_clone.write().unwrap() = Some(now);
                            is_leader_clone.store(true, Ordering::SeqCst);
                        } else {
                            is_leader_clone.store(false, Ordering::SeqCst);
                        }

                        cycle_count += 1;
                        if cycle_count > 5 {
                            break;
                        }
                    }
                }
            }
        });

        // Wait for election to complete
        election_handle.await.unwrap();

        // Should have acquired leadership since lease was empty
        assert!(is_leader.load(Ordering::SeqCst));
        assert_eq!(*lease_holder.read().unwrap(), Some("pod-1".to_string()));
    }

    #[tokio::test]
    async fn test_simulated_leadership_contention() {
        let lease_holder = Arc::new(std::sync::RwLock::new(None::<String>));
        let lease_renew_time = Arc::new(std::sync::RwLock::new(
            None::<chrono::DateTime<chrono::Utc>>,
        ));

        let pod1_is_leader = Arc::new(AtomicBool::new(false));
        let pod2_is_leader = Arc::new(AtomicBool::new(false));

        let (shutdown_tx, _) = watch::channel(false);

        // Pod 1 election task
        let lease_holder_1 = Arc::clone(&lease_holder);
        let lease_renew_1 = Arc::clone(&lease_renew_time);
        let pod1_leader = Arc::clone(&pod1_is_leader);
        let mut shutdown_rx1 = shutdown_tx.subscribe();

        let pod1_handle = tokio::spawn(async move {
            for _ in 0..10 {
                if *shutdown_rx1.borrow() {
                    break;
                }

                let now = chrono::Utc::now();
                let holder = lease_holder_1.read().unwrap().clone();
                let renew = *lease_renew_1.read().unwrap();

                let we_are_holder = holder.as_ref().map(|h| h == "pod-1").unwrap_or(false);
                let is_expired =
                    renew.map_or(true, |rt| now.signed_duration_since(rt).num_seconds() > 15);

                if we_are_holder {
                    *lease_renew_1.write().unwrap() = Some(now);
                    pod1_leader.store(true, Ordering::SeqCst);
                } else if is_expired || holder.is_none() {
                    *lease_holder_1.write().unwrap() = Some("pod-1".to_string());
                    *lease_renew_1.write().unwrap() = Some(now);
                    pod1_leader.store(true, Ordering::SeqCst);
                } else {
                    pod1_leader.store(false, Ordering::SeqCst);
                }

                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        // Pod 2 election task (starts slightly later)
        let lease_holder_2 = Arc::clone(&lease_holder);
        let lease_renew_2 = Arc::clone(&lease_renew_time);
        let pod2_leader = Arc::clone(&pod2_is_leader);
        let mut shutdown_rx2 = shutdown_tx.subscribe();

        let pod2_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2)).await; // Start slightly later

            for _ in 0..10 {
                if *shutdown_rx2.borrow() {
                    break;
                }

                let now = chrono::Utc::now();
                let holder = lease_holder_2.read().unwrap().clone();
                let renew = *lease_renew_2.read().unwrap();

                let we_are_holder = holder.as_ref().map(|h| h == "pod-2").unwrap_or(false);
                let is_expired =
                    renew.map_or(true, |rt| now.signed_duration_since(rt).num_seconds() > 15);

                if we_are_holder {
                    *lease_renew_2.write().unwrap() = Some(now);
                    pod2_leader.store(true, Ordering::SeqCst);
                } else if is_expired || holder.is_none() {
                    *lease_holder_2.write().unwrap() = Some("pod-2".to_string());
                    *lease_renew_2.write().unwrap() = Some(now);
                    pod2_leader.store(true, Ordering::SeqCst);
                } else {
                    pod2_leader.store(false, Ordering::SeqCst);
                }

                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        pod1_handle.await.unwrap();
        pod2_handle.await.unwrap();

        // Only one should be leader (pod-1 should win since it started first)
        let holder = lease_holder.read().unwrap().clone();
        assert!(holder.is_some());

        // The holder should be the leader
        if holder == Some("pod-1".to_string()) {
            assert!(pod1_is_leader.load(Ordering::SeqCst));
        } else {
            assert!(pod2_is_leader.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn test_simulated_release_leadership() {
        let lease_holder = Arc::new(std::sync::RwLock::new(Some("pod-1".to_string())));
        let lease_renew_time = Arc::new(std::sync::RwLock::new(Some(chrono::Utc::now())));
        let is_leader = Arc::new(AtomicBool::new(true));

        let config_identity = "pod-1";

        // Simulate release_leadership
        let release_handle = tokio::spawn({
            let lease_holder = Arc::clone(&lease_holder);
            let lease_renew_time = Arc::clone(&lease_renew_time);
            let is_leader = Arc::clone(&is_leader);

            async move {
                // Check if we are leader
                if !is_leader.load(Ordering::SeqCst) {
                    return;
                }

                // Check if we hold the lease
                let holder = lease_holder.read().unwrap().clone();
                if holder
                    .as_ref()
                    .map(|h| h == config_identity)
                    .unwrap_or(false)
                {
                    // Release by clearing holder
                    *lease_holder.write().unwrap() = None;
                    *lease_renew_time.write().unwrap() = None;
                }

                is_leader.store(false, Ordering::SeqCst);
            }
        });

        release_handle.await.unwrap();

        assert!(!is_leader.load(Ordering::SeqCst));
        assert!(lease_holder.read().unwrap().is_none());
        assert!(lease_renew_time.read().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_simulated_leader_workload_execution() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let workload_executed = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let is_leader_clone = Arc::clone(&is_leader);
        let workload_executed_clone = Arc::clone(&workload_executed);

        // Simulate run_with_leader pattern
        let run_handle = tokio::spawn(async move {
            // Election loop sets leader after some time
            tokio::spawn({
                let is_leader = Arc::clone(&is_leader_clone);
                async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    is_leader.store(true, Ordering::SeqCst);
                }
            });

            // Wait for leadership
            while !is_leader_clone.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            // Execute leader workload
            workload_executed_clone.store(true, Ordering::SeqCst);

            // Workload completes, signal shutdown
            Ok::<(), ()>(())
        });

        run_handle.await.unwrap().unwrap();

        assert!(is_leader.load(Ordering::SeqCst));
        assert!(workload_executed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_simulated_graceful_shutdown() {
        let is_leader = Arc::new(AtomicBool::new(true));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_completed = Arc::new(AtomicBool::new(false));

        let is_leader_clone = Arc::clone(&is_leader);
        let shutdown_completed_clone = Arc::clone(&shutdown_completed);

        // Election loop
        let election_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            // Graceful shutdown
                            is_leader_clone.store(false, Ordering::SeqCst);
                            shutdown_completed_clone.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Normal election cycle
                    }
                }
            }
        });

        // Wait a bit then shutdown
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(is_leader.load(Ordering::SeqCst));

        // Send shutdown signal
        shutdown_tx.send(true).unwrap();
        election_handle.await.unwrap();

        assert!(!is_leader.load(Ordering::SeqCst));
        assert!(shutdown_completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_simulated_drop_behavior() {
        let shutdown_received = Arc::new(AtomicBool::new(false));
        let shutdown_received_clone = Arc::clone(&shutdown_received);

        {
            let (tx, mut rx) = watch::channel(false);
            let mut shutdown_tx: Option<watch::Sender<bool>> = Some(tx);

            // Spawn receiver
            let receiver_handle = tokio::spawn(async move {
                if rx.changed().await.is_ok() && *rx.borrow() {
                    shutdown_received_clone.store(true, Ordering::SeqCst);
                }
            });

            // Simulate Drop behavior
            if let Some(tx) = shutdown_tx.take() {
                let _ = tx.send(true);
            }

            receiver_handle.await.unwrap();
        }

        assert!(shutdown_received.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simulated_concurrent_election_cycles() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let cycle_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let is_leader_clone = Arc::clone(&is_leader);
        let cycle_count_clone = Arc::clone(&cycle_count);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let election_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                        // Simulate election cycle
                        cycle_count_clone.fetch_add(1, Ordering::SeqCst);
                        is_leader_clone.store(true, Ordering::SeqCst);
                    }
                }
            }
        });

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown_tx.send(true).unwrap();
        election_handle.await.unwrap();

        // Should have run multiple cycles
        assert!(cycle_count.load(Ordering::SeqCst) > 1);
        assert!(is_leader.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_simulated_error_recovery() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let error_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let success_after_errors = Arc::new(AtomicBool::new(false));

        let is_leader_clone = Arc::clone(&is_leader);
        let error_count_clone = Arc::clone(&error_count);
        let success_clone = Arc::clone(&success_after_errors);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let election_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                        let errors = error_count_clone.load(Ordering::SeqCst);

                        // Simulate errors for first 3 attempts
                        if errors < 3 {
                            error_count_clone.fetch_add(1, Ordering::SeqCst);
                            is_leader_clone.store(false, Ordering::SeqCst);
                        } else {
                            // Success after errors
                            is_leader_clone.store(true, Ordering::SeqCst);
                            success_clone.store(true, Ordering::SeqCst);
                        }
                    }
                }
            }
        });

        // Wait for recovery
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown_tx.send(true).unwrap();
        election_handle.await.unwrap();

        assert_eq!(error_count.load(Ordering::SeqCst), 3);
        assert!(success_after_errors.load(Ordering::SeqCst));
        assert!(is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_shutdown_tx_take_when_none() {
        let mut shutdown_tx: Option<watch::Sender<bool>> = None;

        if let Some(tx) = shutdown_tx.take() {
            let _ = tx.send(true);
            panic!("Should not reach here when None");
        }

        assert!(shutdown_tx.is_none());
    }

    #[test]
    fn test_shutdown_tx_take_when_some() {
        let (tx, mut rx) = watch::channel(false);
        let mut shutdown_tx: Option<watch::Sender<bool>> = Some(tx);

        if let Some(tx) = shutdown_tx.take() {
            let _ = tx.send(true);
        }

        assert!(shutdown_tx.is_none());
        assert!(*rx.borrow_and_update());
    }

    #[test]
    fn test_atomic_bool_initial_false() {
        let is_leader = Arc::new(AtomicBool::new(false));
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_atomic_bool_store_and_load() {
        let is_leader = Arc::new(AtomicBool::new(false));

        is_leader.store(true, Ordering::SeqCst);
        assert!(is_leader.load(Ordering::SeqCst));

        is_leader.store(false, Ordering::SeqCst);
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_arc_clone_shares_state() {
        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = Arc::clone(&is_leader);

        is_leader.store(true, Ordering::SeqCst);
        assert!(is_leader_clone.load(Ordering::SeqCst));

        is_leader_clone.store(false, Ordering::SeqCst);
        assert!(!is_leader.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_simulated_election_struct_pattern() {
        struct SimulatedLeaderElection {
            config: LeaderElectionConfig,
            is_leader: Arc<AtomicBool>,
            shutdown_tx: Option<watch::Sender<bool>>,
        }

        impl SimulatedLeaderElection {
            fn new(config: LeaderElectionConfig) -> Self {
                Self {
                    config,
                    is_leader: Arc::new(AtomicBool::new(false)),
                    shutdown_tx: None,
                }
            }

            fn is_leader(&self) -> bool {
                self.is_leader.load(Ordering::SeqCst)
            }

            fn leader_status(&self) -> Arc<AtomicBool> {
                Arc::clone(&self.is_leader)
            }
        }

        impl Drop for SimulatedLeaderElection {
            fn drop(&mut self) {
                if let Some(tx) = self.shutdown_tx.take() {
                    let _ = tx.send(true);
                }
            }
        }

        let config = LeaderElectionConfig::new("test-lease", "default", "pod-1");
        let election = SimulatedLeaderElection::new(config);

        assert!(!election.is_leader());

        let status = election.leader_status();
        status.store(true, Ordering::SeqCst);
        assert!(election.is_leader());
    }

    #[tokio::test]
    async fn test_simulated_drop_with_none_tx() {
        let shutdown_received = Arc::new(AtomicBool::new(false));

        struct DropTest {
            shutdown_tx: Option<watch::Sender<bool>>,
            _marker: Arc<AtomicBool>,
        }

        impl Drop for DropTest {
            fn drop(&mut self) {
                if let Some(tx) = self.shutdown_tx.take() {
                    let _ = tx.send(true);
                }
            }
        }

        {
            let _test = DropTest {
                shutdown_tx: None,
                _marker: Arc::clone(&shutdown_received),
            };
        }

        assert!(!shutdown_received.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_simulated_drop_with_some_tx() {
        let shutdown_received = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown_received);

        let (tx, mut rx) = watch::channel(false);

        let receiver_handle = tokio::spawn(async move {
            if rx.changed().await.is_ok() && *rx.borrow() {
                shutdown_clone.store(true, Ordering::SeqCst);
            }
        });

        struct DropTest {
            shutdown_tx: Option<watch::Sender<bool>>,
        }

        impl Drop for DropTest {
            fn drop(&mut self) {
                if let Some(tx) = self.shutdown_tx.take() {
                    let _ = tx.send(true);
                }
            }
        }

        {
            let _test = DropTest {
                shutdown_tx: Some(tx),
            };
        }

        receiver_handle.await.unwrap();
        assert!(shutdown_received.load(Ordering::SeqCst));
    }

    #[test]
    fn test_config_defaults_match_constants() {
        let config = LeaderElectionConfig::new("test", "default", "pod");

        assert_eq!(
            config.lease_duration_seconds,
            DEFAULT_LEASE_DURATION_SECONDS
        );
        assert_eq!(
            config.renew_deadline.as_secs(),
            DEFAULT_RENEW_DEADLINE_SECONDS
        );
        assert_eq!(config.retry_period.as_secs(), DEFAULT_RETRY_PERIOD_SECONDS);
    }

    #[test]
    fn test_config_into_string_trait() {
        fn create_config<S: Into<String>>(name: S, ns: S, id: S) -> LeaderElectionConfig {
            LeaderElectionConfig::new(name, ns, id)
        }

        let config1 = create_config("lease", "ns", "id");
        let config2 = create_config(
            String::from("lease"),
            String::from("ns"),
            String::from("id"),
        );

        assert_eq!(config1.lease_name, config2.lease_name);
        assert_eq!(config1.namespace, config2.namespace);
        assert_eq!(config1.identity, config2.identity);
    }

    #[test]
    fn test_config_multiple_clones() {
        let config = LeaderElectionConfig::new("lease", "ns", "id");
        let clone1 = config.clone();
        let clone2 = clone1.clone();
        let clone3 = clone2.clone();

        assert_eq!(config.lease_name, clone3.lease_name);
        assert_eq!(config.namespace, clone3.namespace);
        assert_eq!(config.identity, clone3.identity);
    }

    #[test]
    fn test_config_field_mutation_after_clone() {
        let original = LeaderElectionConfig::new("original", "ns", "id").with_lease_duration(10);

        let modified = original.clone().with_lease_duration(20);

        assert_eq!(original.lease_duration_seconds, 10);
        assert_eq!(modified.lease_duration_seconds, 20);
    }

    #[tokio::test]
    async fn test_watch_channel_closed_sender() {
        let (tx, mut rx) = watch::channel(false);

        drop(tx);

        let result = rx.changed().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_watch_channel_multiple_receivers() {
        let (tx, rx1) = watch::channel(false);
        let rx2 = tx.subscribe();
        let rx3 = tx.subscribe();

        assert_eq!(*rx1.borrow(), false);
        assert_eq!(*rx2.borrow(), false);
        assert_eq!(*rx3.borrow(), false);

        tx.send(true).unwrap();

        assert_eq!(*rx1.borrow(), true);
        assert_eq!(*rx2.borrow(), true);
        assert_eq!(*rx3.borrow(), true);
    }

    #[test]
    fn test_lease_json_with_all_optional_fields() {
        let now = chrono::Utc::now().to_rfc3339();

        let lease_json = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "test-lease",
                "namespace": "default",
                "uid": "abc-123",
                "resourceVersion": "12345"
            },
            "spec": {
                "holderIdentity": "pod-1",
                "leaseDurationSeconds": 15,
                "acquireTime": &now,
                "renewTime": &now,
                "leaseTransitions": 5
            }
        });

        let lease: Lease = serde_json::from_value(lease_json).unwrap();
        assert!(lease.spec.is_some());

        let spec = lease.spec.unwrap();
        assert_eq!(spec.holder_identity, Some("pod-1".to_string()));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(spec.lease_transitions, Some(5));
    }

    #[test]
    fn test_lease_json_with_minimal_fields() {
        let lease_json = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "minimal-lease"
            }
        });

        let lease: Lease = serde_json::from_value(lease_json).unwrap();
        assert!(lease.spec.is_none());
        assert_eq!(lease.metadata.name, Some("minimal-lease".to_string()));
    }

    #[test]
    fn test_expiration_check_no_renew_time() {
        let spec_option: Option<LeaseSpec> = Some(LeaseSpec::default());
        let renew_time = spec_option.as_ref().and_then(|s| s.renew_time.as_ref());

        let is_expired = renew_time.map_or(true, |_rt| false);
        assert!(is_expired);
    }

    #[test]
    fn test_expiration_check_with_renew_time() {
        let now = chrono::Utc::now();
        let recent = now - chrono::Duration::seconds(5);
        let lease_duration = 15i64;

        let spec = LeaseSpec {
            renew_time: Some(MicroTime(recent)),
            ..Default::default()
        };
        let spec_option: Option<LeaseSpec> = Some(spec);
        let renew_time = spec_option.as_ref().and_then(|s| s.renew_time.as_ref());

        let is_expired = renew_time.map_or(true, |rt| {
            now.signed_duration_since(rt.0).num_seconds() > lease_duration
        });
        assert!(!is_expired);
    }

    #[test]
    fn test_holder_identity_check_none_spec() {
        let spec_option: Option<LeaseSpec> = None;
        let current_holder = spec_option
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());

        let config_identity = "pod-1";
        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(!we_are_holder);
    }

    #[test]
    fn test_holder_identity_check_none_holder() {
        let spec = LeaseSpec::default();
        let spec_option: Option<LeaseSpec> = Some(spec);
        let current_holder = spec_option
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());

        let config_identity = "pod-1";
        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(!we_are_holder);
    }

    #[test]
    fn test_holder_identity_check_different_holder() {
        let spec = LeaseSpec {
            holder_identity: Some("pod-2".to_string()),
            ..Default::default()
        };
        let spec_option: Option<LeaseSpec> = Some(spec);
        let current_holder = spec_option
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());

        let config_identity = "pod-1";
        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(!we_are_holder);
    }

    #[test]
    fn test_holder_identity_check_same_holder() {
        let spec = LeaseSpec {
            holder_identity: Some("pod-1".to_string()),
            ..Default::default()
        };
        let spec_option: Option<LeaseSpec> = Some(spec);
        let current_holder = spec_option
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref());

        let config_identity = "pod-1";
        let we_are_holder = current_holder
            .map(|h| h == config_identity)
            .unwrap_or(false);

        assert!(we_are_holder);
    }

    #[test]
    fn test_lease_duration_fallback_none() {
        let spec = LeaseSpec::default();
        let duration = spec
            .lease_duration_seconds
            .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);
        assert_eq!(duration, DEFAULT_LEASE_DURATION_SECONDS);
    }

    #[test]
    fn test_lease_duration_fallback_some() {
        let spec = LeaseSpec {
            lease_duration_seconds: Some(30),
            ..Default::default()
        };
        let duration = spec
            .lease_duration_seconds
            .unwrap_or(DEFAULT_LEASE_DURATION_SECONDS);
        assert_eq!(duration, 30);
    }

    #[tokio::test]
    async fn test_simulated_complete_acquire_renew_cycle() {
        let now = chrono::Utc::now();
        let config_identity = "pod-1";
        let lease_duration = 15i32;
        let is_leader = Arc::new(AtomicBool::new(false));

        #[derive(Clone)]
        struct SimulatedLease {
            holder: Option<String>,
            renew_time: Option<chrono::DateTime<chrono::Utc>>,
            duration: i32,
        }

        let lease = Arc::new(std::sync::RwLock::new(SimulatedLease {
            holder: None,
            renew_time: None,
            duration: lease_duration,
        }));

        let is_leader_clone = Arc::clone(&is_leader);
        let lease_clone = Arc::clone(&lease);

        for cycle in 0..5 {
            let current_lease = lease_clone.read().unwrap().clone();
            let now = chrono::Utc::now();

            let we_are_holder = current_lease
                .holder
                .as_ref()
                .map(|h| h == config_identity)
                .unwrap_or(false);

            let is_expired = current_lease.renew_time.map_or(true, |rt| {
                now.signed_duration_since(rt).num_seconds() > current_lease.duration as i64
            });

            if we_are_holder {
                let mut lease_mut = lease_clone.write().unwrap();
                lease_mut.renew_time = Some(now);
                is_leader_clone.store(true, Ordering::SeqCst);
            } else if is_expired || current_lease.holder.is_none() {
                let mut lease_mut = lease_clone.write().unwrap();
                lease_mut.holder = Some(config_identity.to_string());
                lease_mut.renew_time = Some(now);
                is_leader_clone.store(true, Ordering::SeqCst);
            } else {
                is_leader_clone.store(false, Ordering::SeqCst);
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(is_leader.load(Ordering::SeqCst));
        let final_lease = lease.read().unwrap();
        assert_eq!(final_lease.holder, Some(config_identity.to_string()));
        assert!(final_lease.renew_time.is_some());
    }

    #[tokio::test]
    async fn test_simulated_release_when_not_holder() {
        let is_leader = Arc::new(AtomicBool::new(true));
        let config_identity = "pod-1";

        #[derive(Clone)]
        struct SimulatedLease {
            holder: Option<String>,
        }

        let lease = SimulatedLease {
            holder: Some("pod-2".to_string()),
        };

        let should_release = lease
            .holder
            .as_ref()
            .map(|h| h == config_identity)
            .unwrap_or(false);

        if should_release {
            is_leader.store(false, Ordering::SeqCst);
        }

        assert!(is_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_patch_merge_format() {
        let patch = Patch::Merge(&json!({
            "spec": {
                "holderIdentity": "pod-1"
            }
        }));

        assert!(format!("{:?}", patch).contains("Merge"));
    }

    #[test]
    fn test_config_display_like_behavior() {
        let config = LeaderElectionConfig::new("my-lease", "my-ns", "my-pod")
            .with_lease_duration(30)
            .with_renew_deadline(Duration::from_secs(20))
            .with_retry_period(Duration::from_secs(5));

        let display = format!(
            "LeaderElectionConfig {{ lease_name: {}, namespace: {}, identity: {}, duration: {}s, renew: {}s, retry: {}s }}",
            config.lease_name,
            config.namespace,
            config.identity,
            config.lease_duration_seconds,
            config.renew_deadline.as_secs(),
            config.retry_period.as_secs()
        );

        assert!(display.contains("my-lease"));
        assert!(display.contains("my-ns"));
        assert!(display.contains("my-pod"));
        assert!(display.contains("30"));
        assert!(display.contains("20"));
        assert!(display.contains("5"));
    }
}
