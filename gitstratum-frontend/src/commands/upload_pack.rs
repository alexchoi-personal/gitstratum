use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use gitstratum_core::{Blob, Commit, Object, Oid, Tree};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::cache::negotiation::{NegotiationRequest, NegotiationResponse, ObjectWalker};
use crate::error::{FrontendError, Result};
use crate::pack::assembly::{PackEntry, PackWriter};

#[async_trait]
pub trait MetadataClient: Send + Sync {
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
}

#[async_trait]
pub trait ObjectClient: Send + Sync {
    async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>>;
    async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>>;
}

pub struct GitUploadPack<M, O> {
    metadata_client: Arc<M>,
    object_client: Arc<O>,
    repo_id: String,
    max_pack_size: usize,
    blob_batch_size: usize,
}

impl<M, O> GitUploadPack<M, O>
where
    M: MetadataClient + 'static,
    O: ObjectClient + 'static,
{
    pub fn new(metadata_client: Arc<M>, object_client: Arc<O>, repo_id: String) -> Self {
        Self {
            metadata_client,
            object_client,
            repo_id,
            max_pack_size: 100 * 1024 * 1024,
            blob_batch_size: 100,
        }
    }

    pub fn with_max_pack_size(mut self, size: usize) -> Self {
        self.max_pack_size = size;
        self
    }

    pub fn with_blob_batch_size(mut self, size: usize) -> Self {
        self.blob_batch_size = size;
        self
    }

    pub async fn advertise_refs(&self) -> Result<Vec<(String, Oid)>> {
        self.metadata_client.list_refs(&self.repo_id, "").await
    }

    pub async fn negotiate(&self, request: &NegotiationRequest) -> Result<NegotiationResponse> {
        let mut available = HashSet::new();

        for have in &request.haves {
            if self
                .metadata_client
                .get_commit(&self.repo_id, have)
                .await?
                .is_some()
            {
                available.insert(*have);
            }
        }

        for want in &request.wants {
            if self
                .metadata_client
                .get_commit(&self.repo_id, want)
                .await?
                .is_some()
            {
                available.insert(*want);
            }
        }

        Ok(crate::cache::negotiation::negotiate_refs(
            request, &available,
        ))
    }

    pub async fn handle_request(&self, request: NegotiationRequest) -> Result<Bytes> {
        let objects = self.collect_objects(&request).await?;

        let mut writer = PackWriter::new();
        for obj in objects {
            writer.add_object(&obj)?;
        }

        writer.build()
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    pub fn handle_request_streaming(
        self: Arc<Self>,
        request: NegotiationRequest,
    ) -> ReceiverStream<Result<Bytes>> {
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            if let Err(e) = self.stream_pack_to_channel(request, tx.clone()).await {
                let _ = tx.send(Err(e)).await;
            }
        });

        ReceiverStream::new(rx)
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn stream_pack_to_channel(
        &self,
        request: NegotiationRequest,
        tx: mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        let objects = self.collect_objects(&request).await?;

        let mut writer = PackWriter::new();
        let mut current_size = 0;

        for obj in objects {
            let entry = PackEntry::from_object(&obj)?;
            let entry_size = entry.data.len();

            if current_size + entry_size > self.max_pack_size && current_size > 0 {
                let pack_data = writer.build()?;
                if tx.send(Ok(pack_data)).await.is_err() {
                    return Ok(());
                }
                writer = PackWriter::new();
                current_size = 0;
            }

            writer.add_entry(entry);
            current_size += entry_size;
        }

        if writer.entry_count() > 0 {
            let pack_data = writer.build()?;
            let _ = tx.send(Ok(pack_data)).await;
        }

        Ok(())
    }

    async fn collect_objects(&self, request: &NegotiationRequest) -> Result<Vec<Object>> {
        let mut walker = ObjectWalker::from_request(request);
        let mut objects = Vec::new();
        let mut commit_queue: VecDeque<Oid> = request.wants.iter().copied().collect();
        let mut tree_oids = Vec::new();
        let mut blob_oids = Vec::new();

        while let Some(commit_oid) = commit_queue.pop_front() {
            if !walker.needs_object(&commit_oid) {
                continue;
            }

            let commit = self
                .metadata_client
                .get_commit(&self.repo_id, &commit_oid)
                .await?
                .ok_or_else(|| FrontendError::ObjectNotFound(commit_oid.to_string()))?;

            walker.mark_to_send(commit_oid);
            tree_oids.push(commit.tree);

            for parent in &commit.parents {
                if !walker.is_boundary(parent) && walker.needs_object(parent) {
                    commit_queue.push_back(*parent);
                }
            }

            objects.push(Object::Commit(commit));
        }

        let mut tree_queue: VecDeque<Oid> = tree_oids.into_iter().collect();

        while let Some(tree_oid) = tree_queue.pop_front() {
            if !walker.needs_object(&tree_oid) {
                continue;
            }

            let tree = self
                .metadata_client
                .get_tree(&self.repo_id, &tree_oid)
                .await?
                .ok_or_else(|| FrontendError::ObjectNotFound(tree_oid.to_string()))?;

            walker.mark_to_send(tree_oid);

            for entry in &tree.entries {
                if entry.mode.is_tree() {
                    if walker.needs_object(&entry.oid) {
                        tree_queue.push_back(entry.oid);
                    }
                } else if walker.needs_object(&entry.oid) {
                    blob_oids.push(entry.oid);
                    walker.mark_visited(entry.oid);
                }
            }

            objects.push(Object::Tree(tree));
        }

        let blobs = self.fetch_blobs_parallel(blob_oids).await?;
        objects.extend(blobs.into_iter().map(Object::Blob));

        Ok(objects)
    }

    async fn fetch_blobs_parallel(&self, blob_oids: Vec<Oid>) -> Result<Vec<Blob>> {
        if blob_oids.is_empty() {
            return Ok(Vec::new());
        }

        let chunks: Vec<Vec<Oid>> = blob_oids
            .chunks(self.blob_batch_size)
            .map(|c| c.to_vec())
            .collect();

        let results: Vec<Result<Vec<Blob>>> = stream::iter(chunks)
            .map(|chunk| {
                let client = Arc::clone(&self.object_client);
                async move { client.get_blobs(chunk).await }
            })
            .buffer_unordered(4)
            .collect()
            .await;

        let mut blobs = Vec::new();
        for result in results {
            blobs.extend(result?);
        }

        Ok(blobs)
    }

    pub async fn wants_are_valid(&self, wants: &[Oid]) -> Result<bool> {
        for want in wants {
            if self
                .metadata_client
                .get_commit(&self.repo_id, want)
                .await?
                .is_none()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[derive(Clone)]
pub struct UploadPackCapabilities {
    pub multi_ack: bool,
    pub multi_ack_detailed: bool,
    pub side_band: bool,
    pub side_band_64k: bool,
    pub ofs_delta: bool,
    pub shallow: bool,
    pub deepen_since: bool,
    pub deepen_not: bool,
    pub no_progress: bool,
    pub include_tag: bool,
    pub thin_pack: bool,
    pub filter: bool,
}

impl UploadPackCapabilities {
    pub fn new() -> Self {
        Self {
            multi_ack: false,
            multi_ack_detailed: false,
            side_band: false,
            side_band_64k: false,
            ofs_delta: false,
            shallow: false,
            deepen_since: false,
            deepen_not: false,
            no_progress: false,
            include_tag: false,
            thin_pack: false,
            filter: false,
        }
    }

    pub fn default_server() -> Self {
        Self {
            multi_ack: true,
            multi_ack_detailed: true,
            side_band: true,
            side_band_64k: true,
            ofs_delta: true,
            shallow: true,
            deepen_since: true,
            deepen_not: true,
            no_progress: false,
            include_tag: true,
            thin_pack: true,
            filter: true,
        }
    }

    pub fn parse(caps_str: &str) -> Self {
        let mut caps = Self::new();

        for cap in caps_str.split_whitespace() {
            match cap {
                "multi_ack" => caps.multi_ack = true,
                "multi_ack_detailed" => caps.multi_ack_detailed = true,
                "side-band" => caps.side_band = true,
                "side-band-64k" => caps.side_band_64k = true,
                "ofs-delta" => caps.ofs_delta = true,
                "shallow" => caps.shallow = true,
                "deepen-since" => caps.deepen_since = true,
                "deepen-not" => caps.deepen_not = true,
                "no-progress" => caps.no_progress = true,
                "include-tag" => caps.include_tag = true,
                "thin-pack" => caps.thin_pack = true,
                "filter" => caps.filter = true,
                _ => {}
            }
        }

        caps
    }
}

impl fmt::Display for UploadPackCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut caps = Vec::new();

        if self.multi_ack {
            caps.push("multi_ack");
        }
        if self.multi_ack_detailed {
            caps.push("multi_ack_detailed");
        }
        if self.side_band {
            caps.push("side-band");
        }
        if self.side_band_64k {
            caps.push("side-band-64k");
        }
        if self.ofs_delta {
            caps.push("ofs-delta");
        }
        if self.shallow {
            caps.push("shallow");
        }
        if self.deepen_since {
            caps.push("deepen-since");
        }
        if self.deepen_not {
            caps.push("deepen-not");
        }
        if self.no_progress {
            caps.push("no-progress");
        }
        if self.include_tag {
            caps.push("include-tag");
        }
        if self.thin_pack {
            caps.push("thin-pack");
        }
        if self.filter {
            caps.push("filter");
        }

        write!(f, "{}", caps.join(" "))
    }
}

impl Default for UploadPackCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

pub fn format_ref_advertisement(
    refs: &[(String, Oid)],
    capabilities: &UploadPackCapabilities,
) -> String {
    let mut result = String::new();

    if refs.is_empty() {
        let zero = Oid::ZERO;
        result.push_str(&format!("{} capabilities^{{}}\0{}\n", zero, capabilities));
    } else {
        for (i, (name, oid)) in refs.iter().enumerate() {
            if i == 0 {
                result.push_str(&format!("{} {}\0{}\n", oid, name, capabilities));
            } else {
                result.push_str(&format!("{} {}\n", oid, name));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::{Signature, TreeEntry};
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    struct MockMetadataClient {
        commits: RwLock<HashMap<Oid, Commit>>,
        trees: RwLock<HashMap<Oid, Tree>>,
        refs: RwLock<Vec<(String, Oid)>>,
    }

    impl MockMetadataClient {
        fn new() -> Self {
            Self {
                commits: RwLock::new(HashMap::new()),
                trees: RwLock::new(HashMap::new()),
                refs: RwLock::new(Vec::new()),
            }
        }

        async fn add_commit(&self, commit: Commit) {
            self.commits.write().await.insert(commit.oid, commit);
        }

        async fn add_tree(&self, tree: Tree) {
            self.trees.write().await.insert(tree.oid, tree);
        }

        async fn add_ref(&self, name: String, oid: Oid) {
            self.refs.write().await.push((name, oid));
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl MetadataClient for MockMetadataClient {
        async fn get_commit(&self, _repo_id: &str, oid: &Oid) -> Result<Option<Commit>> {
            Ok(self.commits.read().await.get(oid).cloned())
        }

        async fn get_tree(&self, _repo_id: &str, oid: &Oid) -> Result<Option<Tree>> {
            Ok(self.trees.read().await.get(oid).cloned())
        }

        async fn list_refs(&self, _repo_id: &str, _prefix: &str) -> Result<Vec<(String, Oid)>> {
            Ok(self.refs.read().await.clone())
        }

        async fn walk_commits(
            &self,
            _repo_id: &str,
            _from: Vec<Oid>,
            _until: Vec<Oid>,
            _limit: u32,
        ) -> Result<Vec<Commit>> {
            Ok(self.commits.read().await.values().cloned().collect())
        }
    }

    struct MockObjectClient {
        blobs: RwLock<HashMap<Oid, Blob>>,
    }

    impl MockObjectClient {
        fn new() -> Self {
            Self {
                blobs: RwLock::new(HashMap::new()),
            }
        }

        async fn add_blob(&self, blob: Blob) {
            self.blobs.write().await.insert(blob.oid, blob);
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl ObjectClient for MockObjectClient {
        async fn get_blob(&self, oid: &Oid) -> Result<Option<Blob>> {
            Ok(self.blobs.read().await.get(oid).cloned())
        }

        async fn get_blobs(&self, oids: Vec<Oid>) -> Result<Vec<Blob>> {
            let blobs = self.blobs.read().await;
            Ok(oids
                .iter()
                .filter_map(|oid| blobs.get(oid).cloned())
                .collect())
        }
    }

    fn create_test_commit(message: &str, tree_oid: Oid, parents: Vec<Oid>) -> Commit {
        let author = Signature::new("Author", "author@example.com", 1704067200, "+0000");
        let committer = author.clone();
        Commit::new(tree_oid, parents, author, committer, message)
    }

    #[tokio::test]
    async fn test_advertise_refs() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let oid = Oid::hash(b"test");
        metadata.add_ref("refs/heads/main".to_string(), oid).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let refs = upload_pack.advertise_refs().await.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].0, "refs/heads/main");
        assert_eq!(refs[0].1, oid);
    }

    #[tokio::test]
    async fn test_handle_request_simple() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let blob = Blob::new(b"hello world".to_vec());
        objects.add_blob(blob.clone()).await;

        let tree = Tree::new(vec![TreeEntry::file("README.md", blob.oid)]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("Initial commit", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit.oid);
        request.done = true;

        let pack_data = upload_pack.handle_request(request).await.unwrap();
        assert!(!pack_data.is_empty());
    }

    #[tokio::test]
    async fn test_handle_request_with_history() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let blob1 = Blob::new(b"content v1".to_vec());
        let blob2 = Blob::new(b"content v2".to_vec());
        objects.add_blob(blob1.clone()).await;
        objects.add_blob(blob2.clone()).await;

        let tree1 = Tree::new(vec![TreeEntry::file("file.txt", blob1.oid)]);
        let tree2 = Tree::new(vec![TreeEntry::file("file.txt", blob2.oid)]);
        metadata.add_tree(tree1.clone()).await;
        metadata.add_tree(tree2.clone()).await;

        let commit1 = create_test_commit("First commit", tree1.oid, vec![]);
        let commit2 = create_test_commit("Second commit", tree2.oid, vec![commit1.oid]);
        metadata.add_commit(commit1.clone()).await;
        metadata.add_commit(commit2.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit2.oid);
        request.done = true;

        let pack_data = upload_pack.handle_request(request).await.unwrap();
        assert!(!pack_data.is_empty());
    }

    #[tokio::test]
    async fn test_handle_request_incremental_fetch() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let blob1 = Blob::new(b"v1".to_vec());
        let blob2 = Blob::new(b"v2".to_vec());
        objects.add_blob(blob1.clone()).await;
        objects.add_blob(blob2.clone()).await;

        let tree1 = Tree::new(vec![TreeEntry::file("f.txt", blob1.oid)]);
        let tree2 = Tree::new(vec![TreeEntry::file("f.txt", blob2.oid)]);
        metadata.add_tree(tree1.clone()).await;
        metadata.add_tree(tree2.clone()).await;

        let commit1 = create_test_commit("c1", tree1.oid, vec![]);
        let commit2 = create_test_commit("c2", tree2.oid, vec![commit1.oid]);
        metadata.add_commit(commit1.clone()).await;
        metadata.add_commit(commit2.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit2.oid);
        request.add_have(commit1.oid);
        request.done = true;

        let pack_data = upload_pack.handle_request(request).await.unwrap();
        assert!(!pack_data.is_empty());
    }

    #[tokio::test]
    async fn test_negotiate() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let tree = Tree::new(vec![]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("test", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit.oid);
        request.add_have(commit.oid);
        request.done = true;

        let response = upload_pack.negotiate(&request).await.unwrap();
        assert!(response.ready);
    }

    #[tokio::test]
    async fn test_wants_are_valid() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let tree = Tree::new(vec![]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("test", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        assert!(upload_pack.wants_are_valid(&[commit.oid]).await.unwrap());
        assert!(!upload_pack
            .wants_are_valid(&[Oid::hash(b"nonexistent")])
            .await
            .unwrap());
    }

    #[test]
    fn test_upload_pack_capabilities_parse() {
        let caps = UploadPackCapabilities::parse("multi_ack side-band-64k ofs-delta");

        assert!(caps.multi_ack);
        assert!(caps.side_band_64k);
        assert!(caps.ofs_delta);
        assert!(!caps.thin_pack);
    }

    #[test]
    fn test_upload_pack_capabilities_to_string() {
        let mut caps = UploadPackCapabilities::new();
        caps.multi_ack = true;
        caps.side_band_64k = true;

        let s = caps.to_string();
        assert!(s.contains("multi_ack"));
        assert!(s.contains("side-band-64k"));
    }

    #[test]
    fn test_format_ref_advertisement() {
        let oid = Oid::hash(b"test");
        let refs = vec![("refs/heads/main".to_string(), oid)];
        let caps = UploadPackCapabilities::default_server();

        let adv = format_ref_advertisement(&refs, &caps);
        assert!(adv.contains(&oid.to_string()));
        assert!(adv.contains("refs/heads/main"));
        assert!(adv.contains("multi_ack"));
    }

    #[test]
    fn test_format_ref_advertisement_empty() {
        let refs = vec![];
        let caps = UploadPackCapabilities::default_server();

        let adv = format_ref_advertisement(&refs, &caps);
        assert!(adv.contains(&Oid::ZERO.to_string()));
        assert!(adv.contains("capabilities^{}"));
    }

    #[tokio::test]
    async fn test_nested_trees() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let blob = Blob::new(b"nested content".to_vec());
        objects.add_blob(blob.clone()).await;

        let inner_tree = Tree::new(vec![TreeEntry::file("inner.txt", blob.oid)]);
        metadata.add_tree(inner_tree.clone()).await;

        let outer_tree = Tree::new(vec![
            TreeEntry::directory("subdir", inner_tree.oid),
            TreeEntry::file("outer.txt", blob.oid),
        ]);
        metadata.add_tree(outer_tree.clone()).await;

        let commit = create_test_commit("nested", outer_tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit.oid);
        request.done = true;

        let pack_data = upload_pack.handle_request(request).await.unwrap();
        assert!(!pack_data.is_empty());
    }

    #[tokio::test]
    async fn test_with_options() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let upload_pack = GitUploadPack::new(metadata, objects, "repo".to_string())
            .with_max_pack_size(1024 * 1024)
            .with_blob_batch_size(50);

        assert_eq!(upload_pack.max_pack_size, 1024 * 1024);
        assert_eq!(upload_pack.blob_batch_size, 50);
    }

    #[test]
    fn test_upload_pack_capabilities_default() {
        let caps = UploadPackCapabilities::default();
        assert!(!caps.multi_ack);
        assert!(!caps.side_band_64k);
    }

    #[test]
    fn test_upload_pack_capabilities_server() {
        let caps = UploadPackCapabilities::default_server();
        assert!(caps.multi_ack);
        assert!(caps.multi_ack_detailed);
        assert!(caps.side_band);
        assert!(caps.side_band_64k);
        assert!(caps.ofs_delta);
        assert!(caps.shallow);
        assert!(caps.deepen_since);
        assert!(caps.deepen_not);
        assert!(caps.include_tag);
        assert!(caps.thin_pack);
        assert!(caps.filter);
    }

    #[test]
    fn test_upload_pack_capabilities_to_string_all() {
        let mut caps = UploadPackCapabilities::new();
        caps.multi_ack = true;
        caps.multi_ack_detailed = true;
        caps.side_band = true;
        caps.side_band_64k = true;
        caps.ofs_delta = true;
        caps.shallow = true;
        caps.deepen_since = true;
        caps.deepen_not = true;
        caps.no_progress = true;
        caps.include_tag = true;
        caps.thin_pack = true;
        caps.filter = true;

        let s = caps.to_string();
        assert!(s.contains("multi_ack"));
        assert!(s.contains("multi_ack_detailed"));
        assert!(s.contains("side-band"));
        assert!(s.contains("side-band-64k"));
        assert!(s.contains("ofs-delta"));
        assert!(s.contains("shallow"));
        assert!(s.contains("deepen-since"));
        assert!(s.contains("deepen-not"));
        assert!(s.contains("no-progress"));
        assert!(s.contains("include-tag"));
        assert!(s.contains("thin-pack"));
        assert!(s.contains("filter"));
    }

    #[test]
    fn test_upload_pack_capabilities_parse_all() {
        let caps = UploadPackCapabilities::parse(
            "multi_ack multi_ack_detailed side-band side-band-64k ofs-delta shallow deepen-since deepen-not no-progress include-tag thin-pack filter unknown"
        );
        assert!(caps.multi_ack);
        assert!(caps.multi_ack_detailed);
        assert!(caps.side_band);
        assert!(caps.side_band_64k);
        assert!(caps.ofs_delta);
        assert!(caps.shallow);
        assert!(caps.deepen_since);
        assert!(caps.deepen_not);
        assert!(caps.no_progress);
        assert!(caps.include_tag);
        assert!(caps.thin_pack);
        assert!(caps.filter);
    }

    #[tokio::test]
    async fn test_handle_request_streaming() {
        use futures::StreamExt;

        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let blob = Blob::new(b"hello world".to_vec());
        objects.add_blob(blob.clone()).await;

        let tree = Tree::new(vec![TreeEntry::file("README.md", blob.oid)]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("Initial commit", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let upload_pack = Arc::new(GitUploadPack::new(
            metadata,
            objects,
            "test-repo".to_string(),
        ));

        let mut request = NegotiationRequest::new();
        request.add_want(commit.oid);
        request.done = true;

        let mut stream = upload_pack.handle_request_streaming(request);

        let mut received = false;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    assert!(!data.is_empty());
                    received = true;
                }
                Err(_) => panic!("unexpected error"),
            }
        }

        assert!(received);
    }

    #[test]
    fn test_format_ref_advertisement_multiple_refs() {
        let oid1 = Oid::hash(b"test1");
        let oid2 = Oid::hash(b"test2");
        let refs = vec![
            ("refs/heads/main".to_string(), oid1),
            ("refs/heads/feature".to_string(), oid2),
        ];
        let caps = UploadPackCapabilities::default_server();

        let adv = format_ref_advertisement(&refs, &caps);
        assert!(adv.contains(&oid1.to_string()));
        assert!(adv.contains(&oid2.to_string()));
        assert!(adv.contains("refs/heads/main"));
        assert!(adv.contains("refs/heads/feature"));
        assert!(adv.contains('\0'));
        let lines: Vec<&str> = adv.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(!lines[1].contains('\0'));
    }

    #[tokio::test]
    async fn test_handle_request_streaming_pack_split() {
        use futures::StreamExt;

        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        for i in 0..10 {
            let data = format!("content {}", i).repeat(1000);
            let blob = Blob::new(data.into_bytes());
            objects.add_blob(blob.clone()).await;

            let tree = Tree::new(vec![TreeEntry::file(format!("file{}.txt", i), blob.oid)]);
            metadata.add_tree(tree.clone()).await;

            let parents = if i > 0 {
                let prev_oid = metadata.commits.read().await.keys().next().copied();
                prev_oid.into_iter().collect()
            } else {
                vec![]
            };

            let commit = create_test_commit(&format!("Commit {}", i), tree.oid, parents);
            metadata.add_commit(commit.clone()).await;
        }

        let first_commit_oid = metadata
            .commits
            .read()
            .await
            .keys()
            .copied()
            .next()
            .unwrap();

        let upload_pack = Arc::new(
            GitUploadPack::new(metadata, objects, "test-repo".to_string()).with_max_pack_size(1000),
        );

        let mut request = NegotiationRequest::new();
        request.add_want(first_commit_oid);
        request.done = true;

        let mut stream = upload_pack.handle_request_streaming(request);

        let mut pack_count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    assert!(!data.is_empty());
                    pack_count += 1;
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }

        assert!(pack_count >= 1);
    }

    #[tokio::test]
    async fn test_fetch_blobs_empty() {
        let metadata = Arc::new(MockMetadataClient::new());
        let objects = Arc::new(MockObjectClient::new());

        let tree = Tree::new(vec![]);
        metadata.add_tree(tree.clone()).await;

        let commit = create_test_commit("Empty tree commit", tree.oid, vec![]);
        metadata.add_commit(commit.clone()).await;

        let upload_pack = GitUploadPack::new(metadata, objects, "test-repo".to_string());

        let mut request = NegotiationRequest::new();
        request.add_want(commit.oid);
        request.done = true;

        let pack_data = upload_pack.handle_request(request).await.unwrap();
        assert!(!pack_data.is_empty());
    }
}
