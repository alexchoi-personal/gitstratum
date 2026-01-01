use async_trait::async_trait;
use gitstratum_core::{Blob, Oid};

use crate::error::Result;
use crate::store::StorageStats;

#[async_trait]
pub trait ObjectStorage: Send + Sync {
    async fn get(&self, oid: &Oid) -> Result<Option<Blob>>;
    async fn put(&self, blob: &Blob) -> Result<()>;
    async fn delete(&self, oid: &Oid) -> Result<bool>;
    fn has(&self, oid: &Oid) -> bool;
    fn stats(&self) -> StorageStats;
}
