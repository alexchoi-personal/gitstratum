use async_trait::async_trait;
use bytes::Bytes;
use gitstratum_core::{Blob, Commit, Oid, Tree};
use std::sync::Arc;
use tracing::{info, instrument};

use crate::cache::negotiation::NegotiationRequest;
use crate::commands::receive_pack::{GitReceivePack, PushResult, RefUpdate};
use crate::commands::upload_pack::GitUploadPack;
use crate::error::{FrontendError, Result};

#[async_trait]
pub trait ControlPlaneConnection: Send + Sync {
    async fn acquire_ref_lock(
        &self,
        repo_id: &str,
        ref_name: &str,
        holder_id: &str,
        timeout_ms: u64,
    ) -> Result<String>;
    async fn release_ref_lock(&self, lock_id: &str) -> Result<()>;
    async fn get_cluster_state(&self) -> Result<ClusterState>;
}

#[async_trait]
pub trait MetadataConnection: Send + Sync {
    async fn get_commit(&self, repo_id: &str, oid: &Oid) -> Result<Option<Commit>>;
    async fn get_tree(&self, repo_id: &str, oid: &Oid) -> Result<Option<Tree>>;
    async fn list_refs(&self, repo_id: &str, prefix: &str) -> Result<Vec<(String, Oid)>>;
    async fn walk_commits(
        &self,
        repo_id: &str,
        from: Vec<Oid>,
        until: Vec<Oid>,
        limit: u32,
    ) -> Result<Vec<Commit>>;
    async fn put_commit(&self, repo_id: &str, commit: &Commit) -> Result<()>;
    async fn put_tree(&self, repo_id: &str, tree: &Tree) -> Result<()>;
    async fn update_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        old_oid: Oid,
        new_oid: Oid,
        force: bool,
    ) -> Result<bool>;
    async fn get_ref(&self, repo_id: &str, ref_name: &str) -> Result<Option<Oid>>;
}

#[async_trait]
pub trait ObjectConnection: Send + Sync {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>>;
    async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>>;
    async fn put_blob(&self, blob: &Blob) -> Result<()>;
    async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct ClusterState {
    pub control_plane_nodes: Vec<gitstratum_hashring::NodeInfo>,
    pub metadata_nodes: Vec<gitstratum_hashring::NodeInfo>,
    pub object_nodes: Vec<gitstratum_hashring::NodeInfo>,
    pub frontend_nodes: Vec<gitstratum_hashring::NodeInfo>,
    pub leader_id: Option<String>,
    pub version: u64,
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            control_plane_nodes: Vec::new(),
            metadata_nodes: Vec::new(),
            object_nodes: Vec::new(),
            frontend_nodes: Vec::new(),
            leader_id: None,
            version: 0,
        }
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GitFrontend<C, M, O> {
    control_plane: Arc<C>,
    metadata: Arc<M>,
    object: Arc<O>,
    node_id: String,
}

impl<C, M, O> GitFrontend<C, M, O>
where
    C: ControlPlaneConnection + 'static,
    M: MetadataConnection + 'static,
    O: ObjectConnection + 'static,
{
    pub fn new(control_plane: Arc<C>, metadata: Arc<M>, object: Arc<O>, node_id: String) -> Self {
        Self {
            control_plane,
            metadata,
            object,
            node_id,
        }
    }

    #[instrument(skip(self))]
    pub async fn get_cluster_state(&self) -> Result<ClusterState> {
        self.control_plane.get_cluster_state().await
    }

    #[instrument(skip(self))]
    pub async fn advertise_refs(&self, repo_id: &str) -> Result<Vec<(String, Oid)>> {
        info!(repo_id, "advertising refs");
        self.metadata.list_refs(repo_id, "").await
    }

    #[instrument(skip(self, request))]
    pub async fn handle_fetch(&self, repo_id: &str, request: NegotiationRequest) -> Result<Bytes> {
        info!(
            repo_id,
            wants = request.wants.len(),
            haves = request.haves.len(),
            "handling fetch request"
        );

        let adapter = MetadataAdapter {
            client: Arc::clone(&self.metadata),
        };
        let object_adapter = ObjectAdapter {
            client: Arc::clone(&self.object),
        };

        let upload_pack = GitUploadPack::new(
            Arc::new(adapter),
            Arc::new(object_adapter),
            repo_id.to_string(),
        );

        upload_pack.handle_request(request).await
    }

    #[instrument(skip(self, updates, pack_data))]
    pub async fn handle_push(
        &self,
        repo_id: &str,
        updates: Vec<RefUpdate>,
        pack_data: Bytes,
    ) -> Result<PushResult> {
        info!(
            repo_id,
            updates = updates.len(),
            pack_size = pack_data.len(),
            "handling push request"
        );

        let control_adapter = ControlPlaneAdapter {
            client: Arc::clone(&self.control_plane),
        };
        let metadata_adapter = MetadataWriterAdapter {
            client: Arc::clone(&self.metadata),
        };
        let object_adapter = ObjectWriterAdapter {
            client: Arc::clone(&self.object),
        };

        let receive_pack = GitReceivePack::new(
            Arc::new(control_adapter),
            Arc::new(metadata_adapter),
            Arc::new(object_adapter),
            repo_id.to_string(),
            self.node_id.clone(),
        );

        receive_pack.handle_push(updates, pack_data).await
    }

    pub async fn get_ref(&self, repo_id: &str, ref_name: &str) -> Result<Option<Oid>> {
        self.metadata.get_ref(repo_id, ref_name).await
    }

    pub async fn get_commit(&self, repo_id: &str, oid: &Oid) -> Result<Option<Commit>> {
        self.metadata.get_commit(repo_id, oid).await
    }

    pub async fn get_tree(&self, repo_id: &str, oid: &Oid) -> Result<Option<Tree>> {
        self.metadata.get_tree(repo_id, oid).await
    }

    pub async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>> {
        self.object.get_blob(oid).await
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

struct MetadataAdapter<M> {
    client: Arc<M>,
}

#[async_trait]
#[cfg_attr(coverage_nightly, coverage(off))]
impl<M: MetadataConnection> crate::commands::upload_pack::MetadataClient for MetadataAdapter<M> {
    async fn get_commit(&self, repo_id: &str, oid: &Oid) -> Result<Option<Commit>> {
        self.client.get_commit(repo_id, oid).await
    }

    async fn get_tree(&self, repo_id: &str, oid: &Oid) -> Result<Option<Tree>> {
        self.client.get_tree(repo_id, oid).await
    }

    async fn list_refs(&self, repo_id: &str, prefix: &str) -> Result<Vec<(String, Oid)>> {
        self.client.list_refs(repo_id, prefix).await
    }

    async fn walk_commits(
        &self,
        repo_id: &str,
        from: Vec<Oid>,
        until: Vec<Oid>,
        limit: u32,
    ) -> Result<Vec<Commit>> {
        self.client.walk_commits(repo_id, from, until, limit).await
    }
}

struct ObjectAdapter<O> {
    client: Arc<O>,
}

#[async_trait]
#[cfg_attr(coverage_nightly, coverage(off))]
impl<O: ObjectConnection> crate::commands::upload_pack::ObjectClient for ObjectAdapter<O> {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>> {
        self.client.get_blob(oid).await
    }

    async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>> {
        self.client.get_blobs(oids).await
    }
}

struct ControlPlaneAdapter<C> {
    client: Arc<C>,
}

#[async_trait]
impl<C: ControlPlaneConnection> crate::commands::receive_pack::ControlPlaneClient
    for ControlPlaneAdapter<C>
{
    async fn acquire_ref_lock(
        &self,
        repo_id: &str,
        ref_name: &str,
        holder_id: &str,
        timeout_ms: u64,
    ) -> Result<String> {
        self.client
            .acquire_ref_lock(repo_id, ref_name, holder_id, timeout_ms)
            .await
    }

    async fn release_ref_lock(&self, lock_id: &str) -> Result<()> {
        self.client.release_ref_lock(lock_id).await
    }
}

struct MetadataWriterAdapter<M> {
    client: Arc<M>,
}

#[async_trait]
impl<M: MetadataConnection> crate::commands::receive_pack::MetadataWriter
    for MetadataWriterAdapter<M>
{
    async fn put_commit(&self, repo_id: &str, commit: &Commit) -> Result<()> {
        self.client.put_commit(repo_id, commit).await
    }

    async fn put_tree(&self, repo_id: &str, tree: &Tree) -> Result<()> {
        self.client.put_tree(repo_id, tree).await
    }

    async fn update_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        old_oid: Oid,
        new_oid: Oid,
        force: bool,
    ) -> Result<bool> {
        self.client
            .update_ref(repo_id, ref_name, old_oid, new_oid, force)
            .await
    }

    async fn get_ref(&self, repo_id: &str, ref_name: &str) -> Result<Option<Oid>> {
        self.client.get_ref(repo_id, ref_name).await
    }
}

struct ObjectWriterAdapter<O> {
    client: Arc<O>,
}

#[async_trait]
#[cfg_attr(coverage_nightly, coverage(off))]
impl<O: ObjectConnection> crate::commands::receive_pack::ObjectWriter for ObjectWriterAdapter<O> {
    async fn put_blob(&self, blob: &Blob) -> Result<()> {
        self.client.put_blob(blob).await
    }

    async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()> {
        self.client.put_blobs(blobs).await
    }
}

pub struct FrontendBuilder<C, M, O> {
    control_plane: Option<Arc<C>>,
    metadata: Option<Arc<M>>,
    object: Option<Arc<O>>,
    node_id: String,
}

impl<C, M, O> FrontendBuilder<C, M, O>
where
    C: ControlPlaneConnection + 'static,
    M: MetadataConnection + 'static,
    O: ObjectConnection + 'static,
{
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            control_plane: None,
            metadata: None,
            object: None,
            node_id: node_id.into(),
        }
    }

    pub fn control_plane(mut self, client: Arc<C>) -> Self {
        self.control_plane = Some(client);
        self
    }

    pub fn metadata(mut self, client: Arc<M>) -> Self {
        self.metadata = Some(client);
        self
    }

    pub fn object(mut self, client: Arc<O>) -> Self {
        self.object = Some(client);
        self
    }

    pub fn build(self) -> Result<GitFrontend<C, M, O>> {
        let control_plane = self.control_plane.ok_or_else(|| {
            FrontendError::InvalidProtocol("control plane client required".to_string())
        })?;
        let metadata = self.metadata.ok_or_else(|| {
            FrontendError::InvalidProtocol("metadata client required".to_string())
        })?;
        let object = self
            .object
            .ok_or_else(|| FrontendError::InvalidProtocol("object client required".to_string()))?;

        let frontend = GitFrontend::new(control_plane, metadata, object, self.node_id);

        Ok(frontend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::{Signature, TreeEntry};
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    struct MockControlPlane {
        locks: Mutex<HashMap<String, String>>,
        next_id: Mutex<u64>,
    }

    impl MockControlPlane {
        fn new() -> Self {
            Self {
                locks: Mutex::new(HashMap::new()),
                next_id: Mutex::new(1),
            }
        }
    }

    #[async_trait]
    impl ControlPlaneConnection for MockControlPlane {
        async fn acquire_ref_lock(
            &self,
            _repo_id: &str,
            ref_name: &str,
            _holder_id: &str,
            _timeout_ms: u64,
        ) -> Result<String> {
            let mut id = self.next_id.lock().await;
            let lock_id = format!("lock-{}", *id);
            *id += 1;
            self.locks
                .lock()
                .await
                .insert(lock_id.clone(), ref_name.to_string());
            Ok(lock_id)
        }

        async fn release_ref_lock(&self, lock_id: &str) -> Result<()> {
            self.locks.lock().await.remove(lock_id);
            Ok(())
        }

        async fn get_cluster_state(&self) -> Result<ClusterState> {
            Ok(ClusterState::new())
        }
    }

    struct MockMetadata {
        commits: Mutex<HashMap<Oid, Commit>>,
        trees: Mutex<HashMap<Oid, Tree>>,
        refs: Mutex<HashMap<String, Oid>>,
    }

    impl MockMetadata {
        fn new() -> Self {
            Self {
                commits: Mutex::new(HashMap::new()),
                trees: Mutex::new(HashMap::new()),
                refs: Mutex::new(HashMap::new()),
            }
        }

        async fn add_commit(&self, commit: Commit) {
            self.commits.lock().await.insert(commit.oid, commit);
        }

        async fn add_tree(&self, tree: Tree) {
            self.trees.lock().await.insert(tree.oid, tree);
        }

        async fn set_ref(&self, name: &str, oid: Oid) {
            self.refs.lock().await.insert(name.to_string(), oid);
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl MetadataConnection for MockMetadata {
        async fn get_commit(&self, _repo_id: &str, oid: &Oid) -> Result<Option<Commit>> {
            Ok(self.commits.lock().await.get(oid).cloned())
        }

        async fn get_tree(&self, _repo_id: &str, oid: &Oid) -> Result<Option<Tree>> {
            Ok(self.trees.lock().await.get(oid).cloned())
        }

        async fn list_refs(&self, _repo_id: &str, _prefix: &str) -> Result<Vec<(String, Oid)>> {
            Ok(self
                .refs
                .lock()
                .await
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect())
        }

        async fn walk_commits(
            &self,
            _repo_id: &str,
            _from: Vec<Oid>,
            _until: Vec<Oid>,
            _limit: u32,
        ) -> Result<Vec<Commit>> {
            Ok(self.commits.lock().await.values().cloned().collect())
        }

        async fn put_commit(&self, _repo_id: &str, commit: &Commit) -> Result<()> {
            self.commits.lock().await.insert(commit.oid, commit.clone());
            Ok(())
        }

        async fn put_tree(&self, _repo_id: &str, tree: &Tree) -> Result<()> {
            self.trees.lock().await.insert(tree.oid, tree.clone());
            Ok(())
        }

        async fn update_ref(
            &self,
            _repo_id: &str,
            ref_name: &str,
            _old_oid: Oid,
            new_oid: Oid,
            _force: bool,
        ) -> Result<bool> {
            if new_oid.is_zero() {
                self.refs.lock().await.remove(ref_name);
            } else {
                self.refs.lock().await.insert(ref_name.to_string(), new_oid);
            }
            Ok(true)
        }

        async fn get_ref(&self, _repo_id: &str, ref_name: &str) -> Result<Option<Oid>> {
            Ok(self.refs.lock().await.get(ref_name).copied())
        }
    }

    struct MockObject {
        blobs: Mutex<HashMap<Oid, Blob>>,
    }

    impl MockObject {
        fn new() -> Self {
            Self {
                blobs: Mutex::new(HashMap::new()),
            }
        }

        async fn add_blob(&self, blob: Blob) {
            self.blobs.lock().await.insert(blob.oid, blob);
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl ObjectConnection for MockObject {
        async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>> {
            Ok(self.blobs.lock().await.get(oid).cloned())
        }

        async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>> {
            let blobs = self.blobs.lock().await;
            Ok(oids
                .iter()
                .filter_map(|oid| blobs.get(oid).cloned())
                .collect())
        }

        async fn put_blob(&self, blob: &Blob) -> Result<()> {
            self.blobs.lock().await.insert(blob.oid, blob.clone());
            Ok(())
        }

        async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()> {
            let mut store = self.blobs.lock().await;
            for blob in blobs {
                store.insert(blob.oid, blob);
            }
            Ok(())
        }
    }

    fn create_test_commit(message: &str, tree_oid: Oid, parents: Vec<Oid>) -> Commit {
        let author = Signature::new("Test", "test@example.com", 1704067200, "+0000");
        Commit::new(tree_oid, parents, author.clone(), author, message)
    }

    #[tokio::test]
    async fn test_frontend_advertise_refs() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let oid = Oid::hash(b"commit");
        metadata.set_ref("refs/heads/main", oid).await;

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let refs = frontend.advertise_refs("test-repo").await.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].0, "refs/heads/main");
    }

    #[tokio::test]
    async fn test_frontend_handle_fetch() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let blob = Blob::new(b"content".to_vec());
        object.add_blob(blob.clone()).await;

        let tree = Tree::new(vec![TreeEntry::file("file.txt", blob.oid)]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("test", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit.oid);
        request.done = true;

        let pack = frontend.handle_fetch("test-repo", request).await.unwrap();
        assert!(!pack.is_empty());
    }

    #[tokio::test]
    async fn test_frontend_handle_push() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let pack_data = create_test_pack();
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            Oid::ZERO,
            Oid::hash(b"new"),
        )];

        let result = frontend
            .handle_push("test-repo", updates, pack_data)
            .await
            .unwrap();
        assert!(result.all_successful());
    }

    fn create_test_pack() -> Bytes {
        use crate::pack::PackWriter;
        use gitstratum_core::Object;

        let mut writer = PackWriter::new();
        let blob = Blob::new(b"test".to_vec());
        writer.add_object(&Object::Blob(blob)).unwrap();
        writer.build().unwrap()
    }

    #[tokio::test]
    async fn test_frontend_builder() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let frontend = FrontendBuilder::new("frontend-1")
            .control_plane(control)
            .metadata(metadata)
            .object(object)
            .build()
            .unwrap();

        assert_eq!(frontend.node_id(), "frontend-1");
    }

    #[tokio::test]
    async fn test_frontend_builder_missing_client() {
        let control = Arc::new(MockControlPlane::new());

        let result: Result<GitFrontend<_, MockMetadata, MockObject>> =
            FrontendBuilder::new("frontend-1")
                .control_plane(control)
                .build();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_ref() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let oid = Oid::hash(b"test");
        metadata.set_ref("refs/heads/main", oid).await;

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let result = frontend.get_ref("repo", "refs/heads/main").await.unwrap();
        assert_eq!(result, Some(oid));

        let missing = frontend
            .get_ref("repo", "refs/heads/missing")
            .await
            .unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_get_commit() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let tree = Tree::new(vec![]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("test", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let result = frontend.get_commit("repo", &commit.oid).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().message, "test");
    }

    #[tokio::test]
    async fn test_get_blob() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let blob = Blob::new(b"hello".to_vec());
        object.add_blob(blob.clone()).await;

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let result = frontend.get_blob(&blob.oid).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn test_get_cluster_state() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let state = frontend.get_cluster_state().await.unwrap();
        assert_eq!(state.version, 0);
    }

    #[test]
    fn test_cluster_state_default() {
        let state = ClusterState::default();
        assert!(state.control_plane_nodes.is_empty());
        assert!(state.metadata_nodes.is_empty());
        assert!(state.object_nodes.is_empty());
        assert!(state.frontend_nodes.is_empty());
        assert!(state.leader_id.is_none());
        assert_eq!(state.version, 0);
    }

    #[tokio::test]
    async fn test_get_tree() {
        let control = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadata::new());
        let object = Arc::new(MockObject::new());

        let tree = Tree::new(vec![]);
        metadata.add_tree(tree.clone()).await;

        let frontend = GitFrontend::new(control, metadata, object, "frontend-1".to_string());

        let result = frontend.get_tree("repo", &tree.oid).await.unwrap();
        assert!(result.is_some());
    }
}
