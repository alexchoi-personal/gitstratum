use serde::{Deserialize, Serialize};
use std::time::Duration;

/// SHA-256 hash identifying an LFS object
pub type Oid = [u8; 32];

/// LFS object metadata stored in the Metadata cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsObject {
    /// SHA-256 hash of the object content
    pub oid: Oid,
    /// Size in bytes
    pub size: u64,
    /// Storage backend where this object lives
    pub backend: StorageBackend,
    /// Bucket or container name
    pub bucket: String,
    /// Object key/path within the bucket
    pub key: String,
    /// Upload status
    pub status: ObjectStatus,
    /// When the object was created
    pub created_at: u64,
    /// Repositories that reference this object
    pub repos: Vec<String>,
}

/// Storage backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackend {
    /// Amazon S3 or S3-compatible storage
    S3 {
        region: String,
        endpoint: Option<String>,
    },
    /// Google Cloud Storage
    GCS { project: String },
    /// Azure Blob Storage
    Azure { account: String, container: String },
    /// Local filesystem (for testing)
    Local { path: String },
}

/// Object upload/verification status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectStatus {
    /// Upload started but not yet verified
    Pending,
    /// Upload verified, object available for download
    Complete,
    /// Upload failed or object missing from storage
    Failed,
}

/// A signed URL for upload or download
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedUrl {
    /// The signed URL
    pub href: String,
    /// HTTP headers to include with the request
    pub headers: Vec<(String, String)>,
    /// Seconds until the URL expires
    pub expires_in: u64,
}

impl SignedUrl {
    pub fn new(href: String, expires: Duration) -> Self {
        Self {
            href,
            headers: Vec::new(),
            expires_in: expires.as_secs(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Storage statistics
#[derive(Debug, Clone, Default)]
pub struct BackendStats {
    /// Total objects stored
    pub object_count: u64,
    /// Total bytes stored
    pub total_bytes: u64,
    /// Bytes uploaded in the last 24 hours
    pub bytes_uploaded_24h: u64,
    /// Bytes downloaded in the last 24 hours
    pub bytes_downloaded_24h: u64,
}

/// Generate the storage key for an OID
/// Uses sharded path: objects/ab/cd/abcd1234...
pub fn oid_to_key(oid: &Oid) -> String {
    let hex = hex::encode(oid);
    format!("objects/{}/{}/{}", &hex[0..2], &hex[2..4], hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_to_key() {
        let oid = [
            0xab, 0xcd, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44,
            0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22,
            0x33, 0x44, 0x55, 0x66,
        ];
        let key = oid_to_key(&oid);
        assert!(key.starts_with("objects/ab/cd/"));
        assert_eq!(key.len(), "objects/ab/cd/".len() + 64);
    }

    #[test]
    fn test_signed_url_with_headers() {
        let url = SignedUrl::new("https://example.com".to_string(), Duration::from_secs(3600))
            .with_header("Content-Type", "application/octet-stream")
            .with_header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

        assert_eq!(url.expires_in, 3600);
        assert_eq!(url.headers.len(), 2);
    }
}
