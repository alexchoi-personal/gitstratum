use crate::error::LfsError;
use crate::types::{oid_to_key, BackendStats, Oid, SignedUrl};
use crate::LfsBackend;
use async_trait::async_trait;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;

/// Local filesystem backend for testing and development
///
/// Not suitable for production—use S3/GCS/Azure instead.
pub struct LocalBackend {
    base_path: PathBuf,
    base_url: String,
}

impl LocalBackend {
    pub fn new(base_path: PathBuf, base_url: String) -> Self {
        Self { base_path, base_url }
    }

    fn object_path(&self, oid: &Oid) -> PathBuf {
        let key = oid_to_key(oid);
        self.base_path.join(key)
    }
}

#[async_trait]
impl LfsBackend for LocalBackend {
    async fn upload_url(
        &self,
        oid: &Oid,
        _size: u64,
        expires: Duration,
    ) -> Result<SignedUrl, LfsError> {
        let key = oid_to_key(oid);
        let path = self.base_path.join(&key);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| LfsError::Backend(e.to_string()))?;
        }

        let url = format!("{}/{}", self.base_url, key);
        Ok(SignedUrl::new(url, expires))
    }

    async fn download_url(&self, oid: &Oid, expires: Duration) -> Result<SignedUrl, LfsError> {
        let path = self.object_path(oid);

        if !path.exists() {
            return Err(LfsError::NotFound(hex::encode(oid)));
        }

        let key = oid_to_key(oid);
        let url = format!("{}/{}", self.base_url, key);
        Ok(SignedUrl::new(url, expires))
    }

    async fn verify(&self, oid: &Oid, expected_size: u64) -> Result<bool, LfsError> {
        let path = self.object_path(oid);

        match fs::metadata(&path).await {
            Ok(meta) => {
                let actual_size = meta.len();
                if actual_size != expected_size {
                    return Err(LfsError::SizeMismatch {
                        expected: expected_size,
                        actual: actual_size,
                    });
                }
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(LfsError::Backend(e.to_string())),
        }
    }

    async fn delete(&self, oid: &Oid) -> Result<(), LfsError> {
        let path = self.object_path(oid);

        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(LfsError::Backend(e.to_string())),
        }
    }

    async fn exists(&self, oid: &Oid) -> Result<bool, LfsError> {
        let path = self.object_path(oid);
        Ok(path.exists())
    }

    async fn size(&self, oid: &Oid) -> Result<u64, LfsError> {
        let path = self.object_path(oid);

        match fs::metadata(&path).await {
            Ok(meta) => Ok(meta.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(LfsError::NotFound(hex::encode(oid)))
            }
            Err(e) => Err(LfsError::Backend(e.to_string())),
        }
    }

    async fn stats(&self) -> Result<BackendStats, LfsError> {
        Ok(BackendStats::default())
    }

    fn name(&self) -> &str {
        "local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_backend_upload_download() {
        let dir = tempdir().unwrap();
        let backend = LocalBackend::new(
            dir.path().to_path_buf(),
            "http://localhost:8080".to_string(),
        );

        let oid = [0xab; 32];

        let upload_url = backend
            .upload_url(&oid, 1024, Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(upload_url.href.starts_with("http://localhost:8080/objects/"));

        let path = backend.object_path(&oid);
        fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        fs::write(&path, b"test content").await.unwrap();

        assert!(backend.exists(&oid).await.unwrap());
        assert_eq!(backend.size(&oid).await.unwrap(), 12);

        let download_url = backend
            .download_url(&oid, Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(download_url.href.starts_with("http://localhost:8080/objects/"));

        backend.delete(&oid).await.unwrap();
        assert!(!backend.exists(&oid).await.unwrap());
    }
}
