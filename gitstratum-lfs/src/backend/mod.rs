use crate::error::LfsError;
use crate::types::{BackendStats, Oid, SignedUrl};
use async_trait::async_trait;
use std::time::Duration;

/// Storage backend abstraction for LFS objects
///
/// Implementations generate signed URLs for direct client upload/download
/// to cloud storage, avoiding proxying large files through Frontend.
#[async_trait]
pub trait LfsBackend: Send + Sync {
    /// Generate a signed URL for uploading an object
    ///
    /// The client will PUT directly to this URL.
    async fn upload_url(
        &self,
        oid: &Oid,
        size: u64,
        expires: Duration,
    ) -> Result<SignedUrl, LfsError>;

    /// Generate a signed URL for downloading an object
    ///
    /// The client will GET directly from this URL.
    async fn download_url(&self, oid: &Oid, expires: Duration) -> Result<SignedUrl, LfsError>;

    /// Verify that an object exists and has the expected size
    ///
    /// Called after client reports upload complete.
    async fn verify(&self, oid: &Oid, expected_size: u64) -> Result<bool, LfsError>;

    /// Delete an object from storage
    ///
    /// Used by garbage collection when no repos reference the object.
    async fn delete(&self, oid: &Oid) -> Result<(), LfsError>;

    /// Check if an object exists
    async fn exists(&self, oid: &Oid) -> Result<bool, LfsError>;

    /// Get the size of an object
    async fn size(&self, oid: &Oid) -> Result<u64, LfsError>;

    /// Get storage statistics
    async fn stats(&self) -> Result<BackendStats, LfsError>;

    /// Backend identifier for logging/metrics
    fn name(&self) -> &str;
}

/// Configuration for creating a backend
#[derive(Debug, Clone)]
pub enum BackendConfig {
    S3 {
        region: String,
        bucket: String,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    },
    GCS {
        project: String,
        bucket: String,
        credentials_path: Option<String>,
    },
    Azure {
        account: String,
        container: String,
        access_key: Option<String>,
    },
    Local {
        path: String,
    },
}

// Cloud backend implementations (uncomment when SDKs are vendored)
// #[cfg(feature = "s3")]
// pub mod s3;
//
// #[cfg(feature = "gcs")]
// pub mod gcs;

pub mod local;

pub use local::LocalBackend;
