use crate::error::LfsError;
use crate::types::{LfsObject, ObjectStatus, Oid};
use async_trait::async_trait;

/// LFS metadata storage interface
///
/// Stores LFS object metadata in the Metadata cluster.
/// The actual object bytes live in cloud storage (S3/GCS/etc).
#[async_trait]
pub trait LfsMetadata: Send + Sync {
    /// Get metadata for an LFS object
    async fn get(&self, oid: &Oid) -> Result<Option<LfsObject>, LfsError>;

    /// Store metadata for an LFS object
    async fn put(&self, object: &LfsObject) -> Result<(), LfsError>;

    /// Delete metadata for an LFS object
    async fn delete(&self, oid: &Oid) -> Result<(), LfsError>;

    /// Update object status (Pending → Complete, etc.)
    async fn update_status(&self, oid: &Oid, status: ObjectStatus) -> Result<(), LfsError>;

    /// Add a repo reference to an object
    async fn add_repo_reference(&self, oid: &Oid, repo: &str) -> Result<(), LfsError>;

    /// Remove a repo reference from an object
    async fn remove_repo_reference(&self, oid: &Oid, repo: &str) -> Result<(), LfsError>;

    /// List all objects for a repository
    async fn list_by_repo(&self, repo: &str) -> Result<Vec<LfsObject>, LfsError>;

    /// List objects with no repo references (orphaned)
    async fn list_orphaned(&self) -> Result<Vec<LfsObject>, LfsError>;

    /// List objects in pending status older than the given timestamp
    async fn list_stale_pending(&self, older_than: u64) -> Result<Vec<LfsObject>, LfsError>;
}

/// In-memory metadata store for testing
#[derive(Default)]
pub struct InMemoryMetadata {
    objects: parking_lot::RwLock<std::collections::HashMap<Oid, LfsObject>>,
}

impl InMemoryMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LfsMetadata for InMemoryMetadata {
    async fn get(&self, oid: &Oid) -> Result<Option<LfsObject>, LfsError> {
        let objects = self.objects.read();
        Ok(objects.get(oid).cloned())
    }

    async fn put(&self, object: &LfsObject) -> Result<(), LfsError> {
        let mut objects = self.objects.write();
        objects.insert(object.oid, object.clone());
        Ok(())
    }

    async fn delete(&self, oid: &Oid) -> Result<(), LfsError> {
        let mut objects = self.objects.write();
        objects.remove(oid);
        Ok(())
    }

    async fn update_status(&self, oid: &Oid, status: ObjectStatus) -> Result<(), LfsError> {
        let mut objects = self.objects.write();
        if let Some(obj) = objects.get_mut(oid) {
            obj.status = status;
            Ok(())
        } else {
            Err(LfsError::NotFound(hex::encode(oid)))
        }
    }

    async fn add_repo_reference(&self, oid: &Oid, repo: &str) -> Result<(), LfsError> {
        let mut objects = self.objects.write();
        if let Some(obj) = objects.get_mut(oid) {
            if !obj.repos.contains(&repo.to_string()) {
                obj.repos.push(repo.to_string());
            }
            Ok(())
        } else {
            Err(LfsError::NotFound(hex::encode(oid)))
        }
    }

    async fn remove_repo_reference(&self, oid: &Oid, repo: &str) -> Result<(), LfsError> {
        let mut objects = self.objects.write();
        if let Some(obj) = objects.get_mut(oid) {
            obj.repos.retain(|r| r != repo);
            Ok(())
        } else {
            Err(LfsError::NotFound(hex::encode(oid)))
        }
    }

    async fn list_by_repo(&self, repo: &str) -> Result<Vec<LfsObject>, LfsError> {
        let objects = self.objects.read();
        Ok(objects
            .values()
            .filter(|o| o.repos.contains(&repo.to_string()))
            .cloned()
            .collect())
    }

    async fn list_orphaned(&self) -> Result<Vec<LfsObject>, LfsError> {
        let objects = self.objects.read();
        Ok(objects
            .values()
            .filter(|o| o.repos.is_empty())
            .cloned()
            .collect())
    }

    async fn list_stale_pending(&self, older_than: u64) -> Result<Vec<LfsObject>, LfsError> {
        let objects = self.objects.read();
        Ok(objects
            .values()
            .filter(|o| o.status == ObjectStatus::Pending && o.created_at < older_than)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StorageBackend;

    fn test_object(oid: Oid) -> LfsObject {
        LfsObject {
            oid,
            size: 1024,
            backend: StorageBackend::Local {
                path: "/tmp".to_string(),
            },
            bucket: "test".to_string(),
            key: "test/key".to_string(),
            status: ObjectStatus::Pending,
            created_at: 1000,
            repos: vec!["org/repo".to_string()],
        }
    }

    #[tokio::test]
    async fn test_in_memory_metadata() {
        let meta = InMemoryMetadata::new();
        let oid = [0xab; 32];
        let obj = test_object(oid);

        meta.put(&obj).await.unwrap();

        let retrieved = meta.get(&oid).await.unwrap().unwrap();
        assert_eq!(retrieved.size, 1024);
        assert_eq!(retrieved.status, ObjectStatus::Pending);

        meta.update_status(&oid, ObjectStatus::Complete)
            .await
            .unwrap();
        let updated = meta.get(&oid).await.unwrap().unwrap();
        assert_eq!(updated.status, ObjectStatus::Complete);

        meta.add_repo_reference(&oid, "other/repo").await.unwrap();
        let with_ref = meta.get(&oid).await.unwrap().unwrap();
        assert_eq!(with_ref.repos.len(), 2);

        meta.remove_repo_reference(&oid, "org/repo").await.unwrap();
        meta.remove_repo_reference(&oid, "other/repo")
            .await
            .unwrap();

        let orphaned = meta.list_orphaned().await.unwrap();
        assert_eq!(orphaned.len(), 1);

        meta.delete(&oid).await.unwrap();
        assert!(meta.get(&oid).await.unwrap().is_none());
    }
}
