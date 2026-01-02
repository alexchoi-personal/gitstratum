use async_trait::async_trait;
use bytes::Bytes;
use gitstratum_core::{Blob, Commit, Object, Oid, Signature, Tree, TreeEntry};
use gitstratum_frontend_cluster::{
    ClusterState, ControlPlaneClient, ControlPlaneConnection, FrontendBuilder, GitReceivePack,
    GitUploadPack, MetadataClient, MetadataConnection, MetadataWriter, NegotiationRequest,
    ObjectClient, ObjectConnection, ObjectWriter, PackReader, PackWriter, RefUpdate, Result,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

struct TestControlPlane {
    locks: Mutex<HashMap<String, String>>,
    next_id: Mutex<u64>,
}

impl TestControlPlane {
    fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
        }
    }
}

#[async_trait]
impl ControlPlaneConnection for TestControlPlane {
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

#[async_trait]
impl ControlPlaneClient for TestControlPlane {
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
}

struct TestMetadata {
    commits: Mutex<HashMap<Oid, Commit>>,
    trees: Mutex<HashMap<Oid, Tree>>,
    refs: Mutex<HashMap<String, Oid>>,
}

impl TestMetadata {
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
impl MetadataConnection for TestMetadata {
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

#[async_trait]
impl MetadataClient for TestMetadata {
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
}

#[async_trait]
impl MetadataWriter for TestMetadata {
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

struct TestObject {
    blobs: Mutex<HashMap<Oid, Blob>>,
}

impl TestObject {
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
impl ObjectConnection for TestObject {
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

#[async_trait]
impl ObjectClient for TestObject {
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
}

#[async_trait]
impl ObjectWriter for TestObject {
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

fn create_commit(message: &str, tree_oid: Oid, parents: Vec<Oid>) -> Commit {
    let author = Signature::new("Test Author", "test@example.com", 1704067200, "+0000");
    Commit::new(tree_oid, parents, author.clone(), author, message)
}

fn create_pack(objects: &[Object]) -> Bytes {
    let mut writer = PackWriter::new();
    for obj in objects {
        writer.add_object(obj).unwrap();
    }
    writer.build().unwrap()
}

#[tokio::test]
async fn test_full_clone_workflow() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let blob1 = Blob::new(b"file content 1".to_vec());
    let blob2 = Blob::new(b"file content 2".to_vec());
    object.add_blob(blob1.clone()).await;
    object.add_blob(blob2.clone()).await;

    let tree = Tree::new(vec![
        TreeEntry::file("file1.txt", blob1.oid),
        TreeEntry::file("file2.txt", blob2.oid),
    ]);
    metadata.add_tree(tree.clone()).await;

    let commit = create_commit("Initial commit", tree.oid, vec![]);
    metadata.add_commit(commit.clone()).await;
    metadata.set_ref("refs/heads/main", commit.oid).await;

    let frontend = FrontendBuilder::new("frontend-test")
        .control_plane(control)
        .metadata(metadata)
        .object(object)
        .build()
        .unwrap();

    let refs = frontend.advertise_refs("test-repo").await.unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].1, commit.oid);

    let mut request = NegotiationRequest::new();
    request.add_want(commit.oid);
    request.done = true;

    let pack_data = frontend.handle_fetch("test-repo", request).await.unwrap();

    let reader = PackReader::new(pack_data).unwrap();
    let entries = reader.read_all().unwrap();

    assert!(entries.iter().any(|e| e.oid == commit.oid));
    assert!(entries.iter().any(|e| e.oid == tree.oid));
    assert!(entries.iter().any(|e| e.oid == blob1.oid));
    assert!(entries.iter().any(|e| e.oid == blob2.oid));
}

#[tokio::test]
async fn test_incremental_fetch_workflow() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let blob1 = Blob::new(b"version 1".to_vec());
    object.add_blob(blob1.clone()).await;

    let tree1 = Tree::new(vec![TreeEntry::file("file.txt", blob1.oid)]);
    metadata.add_tree(tree1.clone()).await;

    let commit1 = create_commit("First commit", tree1.oid, vec![]);
    metadata.add_commit(commit1.clone()).await;

    let blob2 = Blob::new(b"version 2".to_vec());
    object.add_blob(blob2.clone()).await;

    let tree2 = Tree::new(vec![TreeEntry::file("file.txt", blob2.oid)]);
    metadata.add_tree(tree2.clone()).await;

    let commit2 = create_commit("Second commit", tree2.oid, vec![commit1.oid]);
    metadata.add_commit(commit2.clone()).await;
    metadata.set_ref("refs/heads/main", commit2.oid).await;

    let frontend = FrontendBuilder::new("frontend-test")
        .control_plane(control)
        .metadata(metadata)
        .object(object)
        .build()
        .unwrap();

    let mut request = NegotiationRequest::new();
    request.add_want(commit2.oid);
    request.add_have(commit1.oid);
    request.done = true;

    let pack_data = frontend.handle_fetch("test-repo", request).await.unwrap();

    let reader = PackReader::new(pack_data).unwrap();
    let entries = reader.read_all().unwrap();

    assert!(entries.iter().any(|e| e.oid == commit2.oid));
    assert!(!entries.iter().any(|e| e.oid == commit1.oid));
}

#[tokio::test]
async fn test_push_workflow() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let frontend = FrontendBuilder::new("frontend-test")
        .control_plane(control)
        .metadata(metadata.clone())
        .object(object)
        .build()
        .unwrap();

    let blob = Blob::new(b"new content".to_vec());
    let tree = Tree::new(vec![TreeEntry::file("new.txt", blob.oid)]);
    let commit = create_commit("New commit", tree.oid, vec![]);

    let pack_data = create_pack(&[
        Object::Blob(blob.clone()),
        Object::Tree(tree.clone()),
        Object::Commit(commit.clone()),
    ]);

    let updates = vec![RefUpdate::new(
        "refs/heads/main".to_string(),
        Oid::ZERO,
        commit.oid,
    )];

    let result = frontend
        .handle_push("test-repo", updates, pack_data)
        .await
        .unwrap();

    assert!(result.all_successful());
    assert!(result.objects_received > 0);

    let stored_ref = metadata.refs.lock().await.get("refs/heads/main").copied();
    assert_eq!(stored_ref, Some(commit.oid));
}

#[tokio::test]
async fn test_push_update_existing_ref() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let old_blob = Blob::new(b"old".to_vec());
    object.add_blob(old_blob.clone()).await;
    let old_tree = Tree::new(vec![TreeEntry::file("f.txt", old_blob.oid)]);
    metadata.add_tree(old_tree.clone()).await;
    let old_commit = create_commit("Old", old_tree.oid, vec![]);
    metadata.add_commit(old_commit.clone()).await;
    metadata.set_ref("refs/heads/main", old_commit.oid).await;

    let frontend = FrontendBuilder::new("frontend-test")
        .control_plane(control)
        .metadata(metadata.clone())
        .object(object)
        .build()
        .unwrap();

    let new_blob = Blob::new(b"new".to_vec());
    let new_tree = Tree::new(vec![TreeEntry::file("f.txt", new_blob.oid)]);
    let new_commit = create_commit("New", new_tree.oid, vec![old_commit.oid]);

    let pack_data = create_pack(&[
        Object::Blob(new_blob),
        Object::Tree(new_tree),
        Object::Commit(new_commit.clone()),
    ]);

    let updates = vec![RefUpdate::new(
        "refs/heads/main".to_string(),
        old_commit.oid,
        new_commit.oid,
    )];

    let result = frontend
        .handle_push("test-repo", updates, pack_data)
        .await
        .unwrap();
    assert!(result.all_successful());
}

#[tokio::test]
async fn test_upload_pack_with_nested_trees() {
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let blob = Blob::new(b"deep file".to_vec());
    object.add_blob(blob.clone()).await;

    let inner_tree = Tree::new(vec![TreeEntry::file("inner.txt", blob.oid)]);
    metadata.add_tree(inner_tree.clone()).await;

    let middle_tree = Tree::new(vec![TreeEntry::directory("inner", inner_tree.oid)]);
    metadata.add_tree(middle_tree.clone()).await;

    let outer_tree = Tree::new(vec![
        TreeEntry::directory("middle", middle_tree.oid),
        TreeEntry::file("root.txt", blob.oid),
    ]);
    metadata.add_tree(outer_tree.clone()).await;

    let commit = create_commit("Nested", outer_tree.oid, vec![]);
    metadata.add_commit(commit.clone()).await;

    let upload_pack = GitUploadPack::new(metadata, object, "test-repo".to_string());

    let mut request = NegotiationRequest::new();
    request.add_want(commit.oid);
    request.done = true;

    let pack_data = upload_pack.handle_request(request).await.unwrap();

    let reader = PackReader::new(pack_data).unwrap();
    let entries = reader.read_all().unwrap();

    assert!(entries.iter().any(|e| e.oid == inner_tree.oid));
    assert!(entries.iter().any(|e| e.oid == middle_tree.oid));
    assert!(entries.iter().any(|e| e.oid == outer_tree.oid));
}

#[tokio::test]
async fn test_receive_pack_multiple_refs() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let blob = Blob::new(b"content".to_vec());
    let tree = Tree::new(vec![TreeEntry::file("f.txt", blob.oid)]);
    let commit1 = create_commit("Feature 1", tree.oid, vec![]);
    let commit2 = create_commit("Feature 2", tree.oid, vec![]);

    let pack_data = create_pack(&[
        Object::Blob(blob),
        Object::Tree(tree),
        Object::Commit(commit1.clone()),
        Object::Commit(commit2.clone()),
    ]);

    let receive_pack = GitReceivePack::new(
        control,
        metadata.clone(),
        object,
        "test-repo".to_string(),
        "client-1".to_string(),
    );

    let updates = vec![
        RefUpdate::new("refs/heads/feature1".to_string(), Oid::ZERO, commit1.oid),
        RefUpdate::new("refs/heads/feature2".to_string(), Oid::ZERO, commit2.oid),
    ];

    let result = receive_pack.handle_push(updates, pack_data).await.unwrap();

    assert!(result.all_successful());
    assert_eq!(result.updates.len(), 2);

    let refs = metadata.refs.lock().await;
    assert_eq!(refs.get("refs/heads/feature1"), Some(&commit1.oid));
    assert_eq!(refs.get("refs/heads/feature2"), Some(&commit2.oid));
}

#[tokio::test]
async fn test_pack_roundtrip_all_object_types() {
    let blob = Blob::new(b"blob content".to_vec());
    let tree = Tree::new(vec![
        TreeEntry::file("file.txt", blob.oid),
        TreeEntry::new(
            gitstratum_core::TreeEntryMode::Executable,
            "script.sh",
            Oid::hash(b"script"),
        ),
    ]);
    let author = Signature::new("Author", "author@test.com", 1704067200, "+0530");
    let committer = Signature::new("Committer", "committer@test.com", 1704067300, "-0800");
    let commit = Commit::new(
        tree.oid,
        vec![Oid::hash(b"parent1"), Oid::hash(b"parent2")],
        author,
        committer,
        "Test commit with multiple parents",
    );

    let pack_data = create_pack(&[
        Object::Blob(blob.clone()),
        Object::Tree(tree.clone()),
        Object::Commit(commit.clone()),
    ]);

    let reader = PackReader::new(pack_data).unwrap();
    let entries = reader.read_all().unwrap();

    assert_eq!(entries.len(), 3);

    for entry in entries {
        let obj = entry.clone().into_object().unwrap();
        match obj {
            Object::Blob(b) => {
                assert_eq!(b.oid, blob.oid);
                assert_eq!(b.data, blob.data);
            }
            Object::Tree(t) => {
                assert_eq!(t.oid, tree.oid);
                assert_eq!(t.entries.len(), tree.entries.len());
            }
            Object::Commit(c) => {
                assert_eq!(c.oid, commit.oid);
                assert_eq!(c.tree, commit.tree);
                assert_eq!(c.parents.len(), 2);
                assert_eq!(c.message, commit.message);
            }
        }
    }
}

#[tokio::test]
async fn test_frontend_get_operations() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let blob = Blob::new(b"test".to_vec());
    object.add_blob(blob.clone()).await;

    let tree = Tree::new(vec![TreeEntry::file("t.txt", blob.oid)]);
    metadata.add_tree(tree.clone()).await;

    let commit = create_commit("Test", tree.oid, vec![]);
    metadata.add_commit(commit.clone()).await;
    metadata.set_ref("refs/heads/main", commit.oid).await;

    let frontend = FrontendBuilder::new("test")
        .control_plane(control)
        .metadata(metadata)
        .object(object)
        .build()
        .unwrap();

    let ref_result = frontend.get_ref("repo", "refs/heads/main").await.unwrap();
    assert_eq!(ref_result, Some(commit.oid));

    let commit_result = frontend.get_commit("repo", &commit.oid).await.unwrap();
    assert!(commit_result.is_some());

    let tree_result = frontend.get_tree("repo", &tree.oid).await.unwrap();
    assert!(tree_result.is_some());

    let blob_result = frontend.get_blob(&blob.oid).await.unwrap();
    assert!(blob_result.is_some());
}

#[tokio::test]
async fn test_negotiation_request_parsing() {
    let oid1 = Oid::hash(b"want1");
    let oid2 = Oid::hash(b"have1");

    let lines = vec![
        format!("want {}", oid1),
        format!("have {}", oid2),
        "deepen 5".to_string(),
        "filter blob:none".to_string(),
        "done".to_string(),
    ];

    let request = NegotiationRequest::parse_lines(&lines).unwrap();

    assert!(request.wants.contains(&oid1));
    assert!(request.haves.contains(&oid2));
    assert_eq!(request.depth, Some(5));
    assert_eq!(request.filter, Some("blob:none".to_string()));
    assert!(request.done);
}

#[tokio::test]
async fn test_ref_update_validation() {
    let control = Arc::new(TestControlPlane::new());
    let metadata = Arc::new(TestMetadata::new());
    let object = Arc::new(TestObject::new());

    let receive_pack = GitReceivePack::new(
        control,
        metadata,
        object,
        "test-repo".to_string(),
        "client".to_string(),
    );

    let updates = vec![
        RefUpdate::new("refs/heads/valid".to_string(), Oid::ZERO, Oid::hash(b"1")),
        RefUpdate::new("invalid".to_string(), Oid::ZERO, Oid::hash(b"2")),
        RefUpdate::new(
            "refs/heads/bad..name".to_string(),
            Oid::ZERO,
            Oid::hash(b"3"),
        ),
    ];

    let results = receive_pack.validate_updates(&updates).await.unwrap();

    assert!(results[0].success);
    assert!(!results[1].success);
    assert!(!results[2].success);
}
