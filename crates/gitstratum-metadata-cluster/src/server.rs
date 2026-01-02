use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use gitstratum_core::{Oid, RefName, RepoId, Signature, TreeEntry, TreeEntryMode};
use gitstratum_proto::metadata_service_server::MetadataService;
use gitstratum_proto::{
    Commit as ProtoCommit, CreateRepoRequest, CreateRepoResponse, DeleteRepoRequest,
    DeleteRepoResponse, FindMergeBaseRequest, FindMergeBaseResponse, GetCommitRequest,
    GetCommitResponse, GetCommitsRequest, GetRefRequest, GetRefResponse, GetTreeRequest,
    GetTreeResponse, ListRefsRequest, ListRefsResponse, ListReposRequest, ListReposResponse,
    Oid as ProtoOid, PutCommitRequest, PutCommitResponse, PutTreeRequest, PutTreeResponse,
    RefEntry, Signature as ProtoSignature, Tree as ProtoTree, TreeEntry as ProtoTreeEntry,
    UpdateRefRequest, UpdateRefResponse, WalkCommitsRequest,
};

use crate::graph::{find_merge_base, walk_commits};
use crate::store::MetadataStore;

pub struct MetadataServiceImpl {
    store: Arc<MetadataStore>,
}

impl MetadataServiceImpl {
    pub fn new(store: Arc<MetadataStore>) -> Self {
        Self { store }
    }

    #[allow(clippy::result_large_err)]
    fn proto_oid_to_oid(proto: &ProtoOid) -> Result<Oid, Status> {
        Oid::from_slice(&proto.bytes).map_err(|e| Status::invalid_argument(e.to_string()))
    }

    fn oid_to_proto_oid(oid: &Oid) -> ProtoOid {
        ProtoOid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    fn proto_signature_to_signature(proto: &ProtoSignature) -> Signature {
        Signature::new(&proto.name, &proto.email, proto.timestamp, &proto.timezone)
    }

    fn signature_to_proto_signature(sig: &Signature) -> ProtoSignature {
        ProtoSignature {
            name: sig.name.clone(),
            email: sig.email.clone(),
            timestamp: sig.timestamp,
            timezone: sig.timezone.clone(),
        }
    }

    #[allow(clippy::result_large_err)]
    fn proto_commit_to_commit(proto: &ProtoCommit) -> Result<gitstratum_core::Commit, Status> {
        let oid = proto
            .oid
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?
            .unwrap_or(Oid::ZERO);

        let tree = proto
            .tree
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?
            .unwrap_or(Oid::ZERO);

        let parents: Vec<Oid> = proto
            .parents
            .iter()
            .map(Self::proto_oid_to_oid)
            .collect::<Result<_, _>>()?;

        let author = proto
            .author
            .as_ref()
            .map(Self::proto_signature_to_signature)
            .unwrap_or_else(|| Signature::new("", "", 0, "+0000"));

        let committer = proto
            .committer
            .as_ref()
            .map(Self::proto_signature_to_signature)
            .unwrap_or_else(|| Signature::new("", "", 0, "+0000"));

        Ok(gitstratum_core::Commit::with_oid(
            oid,
            tree,
            parents,
            author,
            committer,
            &proto.message,
        ))
    }

    fn commit_to_proto_commit(commit: &gitstratum_core::Commit) -> ProtoCommit {
        ProtoCommit {
            oid: Some(Self::oid_to_proto_oid(&commit.oid)),
            tree: Some(Self::oid_to_proto_oid(&commit.tree)),
            parents: commit.parents.iter().map(Self::oid_to_proto_oid).collect(),
            author: Some(Self::signature_to_proto_signature(&commit.author)),
            committer: Some(Self::signature_to_proto_signature(&commit.committer)),
            message: commit.message.clone(),
        }
    }

    #[allow(clippy::result_large_err)]
    fn proto_tree_to_tree(proto: &ProtoTree) -> Result<gitstratum_core::Tree, Status> {
        let oid = proto
            .oid
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?
            .unwrap_or(Oid::ZERO);

        let entries: Vec<TreeEntry> = proto
            .entries
            .iter()
            .map(|e| {
                let mode = TreeEntryMode::from_str(&e.mode)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                let entry_oid = e
                    .oid
                    .as_ref()
                    .map(Self::proto_oid_to_oid)
                    .transpose()?
                    .unwrap_or(Oid::ZERO);
                Ok(TreeEntry::new(mode, &e.name, entry_oid))
            })
            .collect::<Result<_, Status>>()?;

        Ok(gitstratum_core::Tree::with_oid(oid, entries))
    }

    fn tree_to_proto_tree(tree: &gitstratum_core::Tree) -> ProtoTree {
        ProtoTree {
            oid: Some(Self::oid_to_proto_oid(&tree.oid)),
            entries: tree
                .entries
                .iter()
                .map(|e| ProtoTreeEntry {
                    mode: e.mode.as_str().to_string(),
                    name: e.name.clone(),
                    oid: Some(Self::oid_to_proto_oid(&e.oid)),
                })
                .collect(),
        }
    }
}

#[async_trait]
impl MetadataService for MetadataServiceImpl {
    async fn get_ref(
        &self,
        request: Request<GetRefRequest>,
    ) -> Result<Response<GetRefResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let ref_name =
            RefName::new(&req.ref_name).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let result = self
            .store
            .get_ref(&repo_id, &ref_name)
            .map_err(Status::from)?;

        Ok(Response::new(GetRefResponse {
            target: result.map(|oid| Self::oid_to_proto_oid(&oid)),
            found: result.is_some(),
        }))
    }

    async fn list_refs(
        &self,
        request: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let refs = self
            .store
            .list_refs(&repo_id, &req.prefix)
            .map_err(Status::from)?;

        let entries: Vec<RefEntry> = refs
            .into_iter()
            .map(|(name, oid)| RefEntry {
                name: name.as_str().to_string(),
                target: Some(Self::oid_to_proto_oid(&oid)),
            })
            .collect();

        Ok(Response::new(ListRefsResponse { refs: entries }))
    }

    async fn update_ref(
        &self,
        request: Request<UpdateRefRequest>,
    ) -> Result<Response<UpdateRefResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let ref_name =
            RefName::new(&req.ref_name).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let old_target = req
            .old_target
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?;
        let new_target = req
            .new_target
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("new_target is required"))?;

        match self.store.update_ref(
            &repo_id,
            &ref_name,
            old_target.as_ref(),
            &new_target,
            req.force,
        ) {
            Ok(()) => Ok(Response::new(UpdateRefResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(UpdateRefResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_commit(
        &self,
        request: Request<GetCommitRequest>,
    ) -> Result<Response<GetCommitResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let oid = req
            .oid
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("oid is required"))?;

        let result = self
            .store
            .get_commit(&repo_id, &oid)
            .map_err(Status::from)?;

        let found = result.is_some();
        Ok(Response::new(GetCommitResponse {
            commit: result.map(|c| Self::commit_to_proto_commit(&c)),
            found,
        }))
    }

    type GetCommitsStream = Pin<Box<dyn Stream<Item = Result<ProtoCommit, Status>> + Send>>;

    async fn get_commits(
        &self,
        request: Request<GetCommitsRequest>,
    ) -> Result<Response<Self::GetCommitsStream>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let oids: Vec<Oid> = req
            .oids
            .iter()
            .map(Self::proto_oid_to_oid)
            .collect::<Result<_, _>>()?;

        let store = self.store.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            for oid in oids {
                match store.get_commit(&repo_id, &oid) {
                    Ok(Some(commit)) => {
                        let proto = Self::commit_to_proto_commit(&commit);
                        if tx.send(Ok(proto)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let _ = tx.send(Err(e.into())).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn put_commit(
        &self,
        request: Request<PutCommitRequest>,
    ) -> Result<Response<PutCommitResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let commit = req
            .commit
            .as_ref()
            .map(Self::proto_commit_to_commit)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("commit is required"))?;

        match self.store.put_commit(&repo_id, &commit) {
            Ok(()) => Ok(Response::new(PutCommitResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(PutCommitResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_tree(
        &self,
        request: Request<GetTreeRequest>,
    ) -> Result<Response<GetTreeResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let oid = req
            .oid
            .as_ref()
            .map(Self::proto_oid_to_oid)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("oid is required"))?;

        let result = self.store.get_tree(&repo_id, &oid).map_err(Status::from)?;

        let found = result.is_some();
        Ok(Response::new(GetTreeResponse {
            tree: result.map(|t| Self::tree_to_proto_tree(&t)),
            found,
        }))
    }

    async fn put_tree(
        &self,
        request: Request<PutTreeRequest>,
    ) -> Result<Response<PutTreeResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let tree = req
            .tree
            .as_ref()
            .map(Self::proto_tree_to_tree)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("tree is required"))?;

        match self.store.put_tree(&repo_id, &tree) {
            Ok(()) => Ok(Response::new(PutTreeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(PutTreeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    type WalkCommitsStream = Pin<Box<dyn Stream<Item = Result<ProtoCommit, Status>> + Send>>;

    async fn walk_commits(
        &self,
        request: Request<WalkCommitsRequest>,
    ) -> Result<Response<Self::WalkCommitsStream>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let from: Vec<Oid> = req
            .from
            .iter()
            .map(Self::proto_oid_to_oid)
            .collect::<Result<_, _>>()?;

        let until: Vec<Oid> = req
            .until
            .iter()
            .map(Self::proto_oid_to_oid)
            .collect::<Result<_, _>>()?;

        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };

        let store = self.store.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            use futures::StreamExt;
            let mut walker = walk_commits(store, repo_id, from, until, limit);

            while let Some(result) = walker.next().await {
                match result {
                    Ok(commit) => {
                        let proto = Self::commit_to_proto_commit(&commit);
                        if tx.send(Ok(proto)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.into())).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn find_merge_base(
        &self,
        request: Request<FindMergeBaseRequest>,
    ) -> Result<Response<FindMergeBaseResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let commits: Vec<Oid> = req
            .commits
            .iter()
            .map(Self::proto_oid_to_oid)
            .collect::<Result<_, _>>()?;

        let result = find_merge_base(&self.store, &repo_id, &commits).map_err(Status::from)?;

        Ok(Response::new(FindMergeBaseResponse {
            merge_base: result.map(|oid| Self::oid_to_proto_oid(&oid)),
            found: result.is_some(),
        }))
    }

    async fn create_repo(
        &self,
        request: Request<CreateRepoRequest>,
    ) -> Result<Response<CreateRepoResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self.store.create_repo(&repo_id) {
            Ok(()) => Ok(Response::new(CreateRepoResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CreateRepoResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn delete_repo(
        &self,
        request: Request<DeleteRepoRequest>,
    ) -> Result<Response<DeleteRepoResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self.store.delete_repo(&repo_id) {
            Ok(()) => Ok(Response::new(DeleteRepoResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteRepoResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn list_repos(
        &self,
        request: Request<ListReposRequest>,
    ) -> Result<Response<ListReposResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            100
        };

        let (repos, next_cursor) = self
            .store
            .list_repos(&req.prefix, limit, &req.cursor)
            .map_err(Status::from)?;

        Ok(Response::new(ListReposResponse {
            repo_ids: repos.into_iter().map(|r| r.as_str().to_string()).collect(),
            next_cursor: next_cursor.unwrap_or_default(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::{Commit, Tree, TreeEntry, TreeEntryMode};

    fn create_test_oid(data: &[u8]) -> Oid {
        Oid::hash(data)
    }

    fn create_test_signature() -> Signature {
        Signature::new("Test Author", "test@example.com", 1704067200, "+0000")
    }

    #[test]
    fn test_proto_oid_to_oid_valid() {
        let oid = create_test_oid(b"test");
        let proto_oid = ProtoOid {
            bytes: oid.as_bytes().to_vec(),
        };
        let result = MetadataServiceImpl::proto_oid_to_oid(&proto_oid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), oid);
    }

    #[test]
    fn test_proto_oid_to_oid_invalid() {
        let proto_oid = ProtoOid {
            bytes: vec![1, 2, 3],
        };
        let result = MetadataServiceImpl::proto_oid_to_oid(&proto_oid);
        assert!(result.is_err());
    }

    #[test]
    fn test_oid_to_proto_oid() {
        let oid = create_test_oid(b"test");
        let proto_oid = MetadataServiceImpl::oid_to_proto_oid(&oid);
        assert_eq!(proto_oid.bytes, oid.as_bytes());
    }

    #[test]
    fn test_proto_signature_to_signature() {
        let proto_sig = ProtoSignature {
            name: "Test Name".to_string(),
            email: "test@example.com".to_string(),
            timestamp: 1704067200,
            timezone: "+0100".to_string(),
        };
        let sig = MetadataServiceImpl::proto_signature_to_signature(&proto_sig);
        assert_eq!(sig.name, "Test Name");
        assert_eq!(sig.email, "test@example.com");
        assert_eq!(sig.timestamp, 1704067200);
        assert_eq!(sig.timezone, "+0100");
    }

    #[test]
    fn test_signature_to_proto_signature() {
        let sig = create_test_signature();
        let proto_sig = MetadataServiceImpl::signature_to_proto_signature(&sig);
        assert_eq!(proto_sig.name, sig.name);
        assert_eq!(proto_sig.email, sig.email);
        assert_eq!(proto_sig.timestamp, sig.timestamp);
        assert_eq!(proto_sig.timezone, sig.timezone);
    }

    #[test]
    fn test_proto_commit_to_commit_full() {
        let tree_oid = create_test_oid(b"tree");
        let parent_oid = create_test_oid(b"parent");
        let sig = create_test_signature();
        let commit = Commit::new(
            tree_oid,
            vec![parent_oid],
            sig.clone(),
            sig.clone(),
            "Test message",
        );

        let proto_commit = ProtoCommit {
            oid: Some(MetadataServiceImpl::oid_to_proto_oid(&commit.oid)),
            tree: Some(MetadataServiceImpl::oid_to_proto_oid(&tree_oid)),
            parents: vec![MetadataServiceImpl::oid_to_proto_oid(&parent_oid)],
            author: Some(MetadataServiceImpl::signature_to_proto_signature(&sig)),
            committer: Some(MetadataServiceImpl::signature_to_proto_signature(&sig)),
            message: "Test message".to_string(),
        };

        let result = MetadataServiceImpl::proto_commit_to_commit(&proto_commit);
        assert!(result.is_ok());
        let converted = result.unwrap();
        assert_eq!(converted.tree, tree_oid);
        assert_eq!(converted.parents.len(), 1);
        assert_eq!(converted.message, "Test message");
    }

    #[test]
    fn test_proto_commit_to_commit_missing_fields() {
        let proto_commit = ProtoCommit {
            oid: None,
            tree: None,
            parents: vec![],
            author: None,
            committer: None,
            message: "Test".to_string(),
        };

        let result = MetadataServiceImpl::proto_commit_to_commit(&proto_commit);
        assert!(result.is_ok());
        let converted = result.unwrap();
        assert_eq!(converted.oid, Oid::ZERO);
        assert_eq!(converted.tree, Oid::ZERO);
        assert!(converted.parents.is_empty());
        assert_eq!(converted.author.name, "");
        assert_eq!(converted.author.email, "");
    }

    #[test]
    fn test_proto_commit_to_commit_invalid_oid() {
        let proto_commit = ProtoCommit {
            oid: Some(ProtoOid {
                bytes: vec![1, 2, 3],
            }),
            tree: None,
            parents: vec![],
            author: None,
            committer: None,
            message: "Test".to_string(),
        };

        let result = MetadataServiceImpl::proto_commit_to_commit(&proto_commit);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_commit_to_commit_invalid_tree_oid() {
        let proto_commit = ProtoCommit {
            oid: None,
            tree: Some(ProtoOid {
                bytes: vec![1, 2, 3],
            }),
            parents: vec![],
            author: None,
            committer: None,
            message: "Test".to_string(),
        };

        let result = MetadataServiceImpl::proto_commit_to_commit(&proto_commit);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_commit_to_commit_invalid_parent_oid() {
        let proto_commit = ProtoCommit {
            oid: None,
            tree: None,
            parents: vec![ProtoOid {
                bytes: vec![1, 2, 3],
            }],
            author: None,
            committer: None,
            message: "Test".to_string(),
        };

        let result = MetadataServiceImpl::proto_commit_to_commit(&proto_commit);
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_to_proto_commit() {
        let tree_oid = create_test_oid(b"tree");
        let parent_oid = create_test_oid(b"parent");
        let sig = create_test_signature();
        let commit = Commit::new(tree_oid, vec![parent_oid], sig.clone(), sig, "Test message");

        let proto = MetadataServiceImpl::commit_to_proto_commit(&commit);
        assert!(proto.oid.is_some());
        assert!(proto.tree.is_some());
        assert_eq!(proto.parents.len(), 1);
        assert!(proto.author.is_some());
        assert!(proto.committer.is_some());
        assert_eq!(proto.message, "Test message");
    }

    #[test]
    fn test_proto_tree_to_tree_full() {
        let entry_oid = create_test_oid(b"file");
        let entry = TreeEntry::new(TreeEntryMode::File, "file.txt", entry_oid);
        let tree = Tree::new(vec![entry]);

        let proto_tree = ProtoTree {
            oid: Some(MetadataServiceImpl::oid_to_proto_oid(&tree.oid)),
            entries: vec![ProtoTreeEntry {
                mode: "100644".to_string(),
                name: "file.txt".to_string(),
                oid: Some(MetadataServiceImpl::oid_to_proto_oid(&entry_oid)),
            }],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_ok());
        let converted = result.unwrap();
        assert_eq!(converted.entries.len(), 1);
        assert_eq!(converted.entries[0].name, "file.txt");
    }

    #[test]
    fn test_proto_tree_to_tree_missing_oid() {
        let entry_oid = create_test_oid(b"file");
        let proto_tree = ProtoTree {
            oid: None,
            entries: vec![ProtoTreeEntry {
                mode: "100644".to_string(),
                name: "file.txt".to_string(),
                oid: Some(MetadataServiceImpl::oid_to_proto_oid(&entry_oid)),
            }],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_ok());
        let converted = result.unwrap();
        assert_eq!(converted.oid, Oid::ZERO);
    }

    #[test]
    fn test_proto_tree_to_tree_invalid_oid() {
        let proto_tree = ProtoTree {
            oid: Some(ProtoOid {
                bytes: vec![1, 2, 3],
            }),
            entries: vec![],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_tree_to_tree_invalid_mode() {
        let entry_oid = create_test_oid(b"file");
        let proto_tree = ProtoTree {
            oid: None,
            entries: vec![ProtoTreeEntry {
                mode: "invalid".to_string(),
                name: "file.txt".to_string(),
                oid: Some(MetadataServiceImpl::oid_to_proto_oid(&entry_oid)),
            }],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_tree_to_tree_invalid_entry_oid() {
        let proto_tree = ProtoTree {
            oid: None,
            entries: vec![ProtoTreeEntry {
                mode: "100644".to_string(),
                name: "file.txt".to_string(),
                oid: Some(ProtoOid {
                    bytes: vec![1, 2, 3],
                }),
            }],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_tree_to_tree_missing_entry_oid() {
        let proto_tree = ProtoTree {
            oid: None,
            entries: vec![ProtoTreeEntry {
                mode: "100644".to_string(),
                name: "file.txt".to_string(),
                oid: None,
            }],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_ok());
        let converted = result.unwrap();
        assert_eq!(converted.entries[0].oid, Oid::ZERO);
    }

    #[test]
    fn test_tree_to_proto_tree() {
        let entry_oid = create_test_oid(b"file");
        let entry = TreeEntry::new(TreeEntryMode::File, "file.txt", entry_oid);
        let tree = Tree::new(vec![entry]);

        let proto = MetadataServiceImpl::tree_to_proto_tree(&tree);
        assert!(proto.oid.is_some());
        assert_eq!(proto.entries.len(), 1);
        assert_eq!(proto.entries[0].name, "file.txt");
        assert_eq!(proto.entries[0].mode, "100644");
    }

    #[test]
    fn test_tree_to_proto_tree_all_modes() {
        let entries = vec![
            TreeEntry::new(TreeEntryMode::File, "a_file.txt", create_test_oid(b"1")),
            TreeEntry::new(
                TreeEntryMode::Executable,
                "b_script.sh",
                create_test_oid(b"2"),
            ),
            TreeEntry::new(TreeEntryMode::Directory, "c_subdir", create_test_oid(b"3")),
            TreeEntry::new(TreeEntryMode::Symlink, "d_link", create_test_oid(b"4")),
            TreeEntry::new(TreeEntryMode::Submodule, "e_submod", create_test_oid(b"5")),
        ];
        let tree = Tree::new(entries);

        let proto = MetadataServiceImpl::tree_to_proto_tree(&tree);
        assert_eq!(proto.entries.len(), 5);
        assert_eq!(proto.entries[0].mode, "100644");
        assert_eq!(proto.entries[1].mode, "100755");
        assert_eq!(proto.entries[2].mode, "040000");
        assert_eq!(proto.entries[3].mode, "120000");
        assert_eq!(proto.entries[4].mode, "160000");
    }

    #[test]
    fn test_proto_tree_to_tree_all_modes() {
        let oid = create_test_oid(b"file");
        let proto_tree = ProtoTree {
            oid: None,
            entries: vec![
                ProtoTreeEntry {
                    mode: "100644".to_string(),
                    name: "file.txt".to_string(),
                    oid: Some(MetadataServiceImpl::oid_to_proto_oid(&oid)),
                },
                ProtoTreeEntry {
                    mode: "100755".to_string(),
                    name: "script.sh".to_string(),
                    oid: Some(MetadataServiceImpl::oid_to_proto_oid(&oid)),
                },
                ProtoTreeEntry {
                    mode: "040000".to_string(),
                    name: "subdir".to_string(),
                    oid: Some(MetadataServiceImpl::oid_to_proto_oid(&oid)),
                },
                ProtoTreeEntry {
                    mode: "120000".to_string(),
                    name: "link".to_string(),
                    oid: Some(MetadataServiceImpl::oid_to_proto_oid(&oid)),
                },
                ProtoTreeEntry {
                    mode: "160000".to_string(),
                    name: "submod".to_string(),
                    oid: Some(MetadataServiceImpl::oid_to_proto_oid(&oid)),
                },
            ],
        };

        let result = MetadataServiceImpl::proto_tree_to_tree(&proto_tree);
        assert!(result.is_ok());
        let tree = result.unwrap();
        assert_eq!(tree.entries.len(), 5);
        assert_eq!(tree.entries[0].mode, TreeEntryMode::File);
        assert_eq!(tree.entries[1].mode, TreeEntryMode::Executable);
        assert_eq!(tree.entries[2].mode, TreeEntryMode::Directory);
        assert_eq!(tree.entries[3].mode, TreeEntryMode::Symlink);
        assert_eq!(tree.entries[4].mode, TreeEntryMode::Submodule);
    }

    #[test]
    fn test_commit_to_proto_and_back() {
        let tree_oid = create_test_oid(b"tree");
        let parent_oid = create_test_oid(b"parent");
        let sig = create_test_signature();
        let original = Commit::new(
            tree_oid,
            vec![parent_oid],
            sig.clone(),
            sig,
            "Roundtrip test",
        );

        let proto = MetadataServiceImpl::commit_to_proto_commit(&original);
        let converted = MetadataServiceImpl::proto_commit_to_commit(&proto).unwrap();

        assert_eq!(converted.oid, original.oid);
        assert_eq!(converted.tree, original.tree);
        assert_eq!(converted.parents, original.parents);
        assert_eq!(converted.message, original.message);
        assert_eq!(converted.author.name, original.author.name);
        assert_eq!(converted.committer.email, original.committer.email);
    }

    #[test]
    fn test_tree_to_proto_and_back() {
        let entries = vec![
            TreeEntry::new(TreeEntryMode::File, "file1.txt", create_test_oid(b"1")),
            TreeEntry::new(TreeEntryMode::Executable, "run.sh", create_test_oid(b"2")),
        ];
        let original = Tree::new(entries);

        let proto = MetadataServiceImpl::tree_to_proto_tree(&original);
        let converted = MetadataServiceImpl::proto_tree_to_tree(&proto).unwrap();

        assert_eq!(converted.oid, original.oid);
        assert_eq!(converted.entries.len(), original.entries.len());
        for (orig, conv) in original.entries.iter().zip(converted.entries.iter()) {
            assert_eq!(conv.mode, orig.mode);
            assert_eq!(conv.name, orig.name);
            assert_eq!(conv.oid, orig.oid);
        }
    }

    #[test]
    fn test_commit_no_parents() {
        let tree_oid = create_test_oid(b"tree");
        let sig = create_test_signature();
        let commit = Commit::new(tree_oid, vec![], sig.clone(), sig, "Initial commit");

        let proto = MetadataServiceImpl::commit_to_proto_commit(&commit);
        assert!(proto.parents.is_empty());

        let converted = MetadataServiceImpl::proto_commit_to_commit(&proto).unwrap();
        assert!(converted.parents.is_empty());
    }

    #[test]
    fn test_commit_multiple_parents() {
        let tree_oid = create_test_oid(b"tree");
        let parent1 = create_test_oid(b"parent1");
        let parent2 = create_test_oid(b"parent2");
        let parent3 = create_test_oid(b"parent3");
        let sig = create_test_signature();
        let commit = Commit::new(
            tree_oid,
            vec![parent1, parent2, parent3],
            sig.clone(),
            sig,
            "Merge commit",
        );

        let proto = MetadataServiceImpl::commit_to_proto_commit(&commit);
        assert_eq!(proto.parents.len(), 3);

        let converted = MetadataServiceImpl::proto_commit_to_commit(&proto).unwrap();
        assert_eq!(converted.parents.len(), 3);
        assert_eq!(converted.parents[0], parent1);
        assert_eq!(converted.parents[1], parent2);
        assert_eq!(converted.parents[2], parent3);
    }

    #[test]
    fn test_tree_empty_entries() {
        let tree = Tree::new(vec![]);

        let proto = MetadataServiceImpl::tree_to_proto_tree(&tree);
        assert!(proto.entries.is_empty());

        let converted = MetadataServiceImpl::proto_tree_to_tree(&proto).unwrap();
        assert!(converted.entries.is_empty());
    }

    #[test]
    fn test_signature_with_negative_timezone() {
        let proto_sig = ProtoSignature {
            name: "Test".to_string(),
            email: "test@test.com".to_string(),
            timestamp: 1234567890,
            timezone: "-0500".to_string(),
        };

        let sig = MetadataServiceImpl::proto_signature_to_signature(&proto_sig);
        assert_eq!(sig.timezone, "-0500");

        let back = MetadataServiceImpl::signature_to_proto_signature(&sig);
        assert_eq!(back.timezone, "-0500");
    }

    #[test]
    fn test_signature_empty_fields() {
        let proto_sig = ProtoSignature {
            name: String::new(),
            email: String::new(),
            timestamp: 0,
            timezone: String::new(),
        };

        let sig = MetadataServiceImpl::proto_signature_to_signature(&proto_sig);
        assert!(sig.name.is_empty());
        assert!(sig.email.is_empty());
        assert_eq!(sig.timestamp, 0);
        assert!(sig.timezone.is_empty());
    }

    #[test]
    fn test_proto_commit_with_empty_message() {
        let proto_commit = ProtoCommit {
            oid: None,
            tree: None,
            parents: vec![],
            author: None,
            committer: None,
            message: String::new(),
        };

        let result = MetadataServiceImpl::proto_commit_to_commit(&proto_commit);
        assert!(result.is_ok());
        let commit = result.unwrap();
        assert!(commit.message.is_empty());
    }

    #[test]
    fn test_tree_entry_with_special_characters_in_name() {
        let entry_oid = create_test_oid(b"file");
        let entry = TreeEntry::new(TreeEntryMode::File, "file with spaces.txt", entry_oid);
        let tree = Tree::new(vec![entry]);

        let proto = MetadataServiceImpl::tree_to_proto_tree(&tree);
        assert_eq!(proto.entries[0].name, "file with spaces.txt");

        let converted = MetadataServiceImpl::proto_tree_to_tree(&proto).unwrap();
        assert_eq!(converted.entries[0].name, "file with spaces.txt");
    }

    #[test]
    fn test_tree_entry_with_unicode_name() {
        let entry_oid = create_test_oid(b"file");
        let entry = TreeEntry::new(TreeEntryMode::File, "文件.txt", entry_oid);
        let tree = Tree::new(vec![entry]);

        let proto = MetadataServiceImpl::tree_to_proto_tree(&tree);
        assert_eq!(proto.entries[0].name, "文件.txt");

        let converted = MetadataServiceImpl::proto_tree_to_tree(&proto).unwrap();
        assert_eq!(converted.entries[0].name, "文件.txt");
    }

    #[test]
    fn test_commit_message_with_newlines() {
        let tree_oid = create_test_oid(b"tree");
        let sig = create_test_signature();
        let message = "First line\n\nSecond paragraph\n- bullet 1\n- bullet 2";
        let commit = Commit::new(tree_oid, vec![], sig.clone(), sig, message);

        let proto = MetadataServiceImpl::commit_to_proto_commit(&commit);
        assert_eq!(proto.message, message);

        let converted = MetadataServiceImpl::proto_commit_to_commit(&proto).unwrap();
        assert_eq!(converted.message, message);
    }

    #[test]
    fn test_signature_with_unicode_name() {
        let sig = Signature::new("日本語の名前", "test@example.com", 1234567890, "+0900");
        let proto = MetadataServiceImpl::signature_to_proto_signature(&sig);
        assert_eq!(proto.name, "日本語の名前");

        let back = MetadataServiceImpl::proto_signature_to_signature(&proto);
        assert_eq!(back.name, "日本語の名前");
    }

    #[test]
    fn test_proto_oid_to_oid_empty_bytes() {
        let proto_oid = ProtoOid { bytes: vec![] };
        let result = MetadataServiceImpl::proto_oid_to_oid(&proto_oid);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_oid_to_oid_oversized_bytes() {
        let proto_oid = ProtoOid {
            bytes: vec![0u8; 100],
        };
        let result = MetadataServiceImpl::proto_oid_to_oid(&proto_oid);
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_service_impl_new() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(crate::store::MetadataStore::open(temp_dir.path()).unwrap());
        let service = MetadataServiceImpl::new(store.clone());
        assert!(Arc::ptr_eq(&service.store, &store));
    }
}

#[cfg(test)]
mod grpc_handler_tests {
    use super::*;
    use crate::store::MetadataStore;
    use gitstratum_proto::{
        CreateRepoRequest, DeleteRepoRequest, FindMergeBaseRequest, GetCommitRequest,
        GetCommitsRequest, GetRefRequest, GetTreeRequest, ListRefsRequest, ListReposRequest,
        PutCommitRequest, PutTreeRequest, UpdateRefRequest, WalkCommitsRequest,
    };
    use tempfile::TempDir;
    use tokio_stream::StreamExt;
    use tonic::Request;

    fn create_test_service() -> (MetadataServiceImpl, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(MetadataStore::open(temp_dir.path()).unwrap());
        let service = MetadataServiceImpl::new(store);
        (service, temp_dir)
    }

    fn create_valid_oid() -> ProtoOid {
        let oid = Oid::hash(b"test");
        ProtoOid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    fn create_invalid_oid() -> ProtoOid {
        ProtoOid {
            bytes: vec![1, 2, 3],
        }
    }

    #[tokio::test]
    async fn test_get_ref_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(GetRefRequest {
            repo_id: "".to_string(),
            ref_name: "refs/heads/main".to_string(),
        });
        let result = service.get_ref(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_ref_invalid_ref_name() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(GetRefRequest {
            repo_id: "test-repo".to_string(),
            ref_name: "invalid-ref".to_string(),
        });
        let result = service.get_ref(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_list_refs_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(ListRefsRequest {
            repo_id: "".to_string(),
            prefix: "".to_string(),
        });
        let result = service.list_refs(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_update_ref_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let oid = create_valid_oid();
        let request = Request::new(UpdateRefRequest {
            repo_id: "".to_string(),
            ref_name: "refs/heads/main".to_string(),
            old_target: None,
            new_target: Some(oid),
            force: false,
        });
        let result = service.update_ref(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_update_ref_missing_new_target() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(UpdateRefRequest {
            repo_id: "test-repo".to_string(),
            ref_name: "refs/heads/main".to_string(),
            old_target: None,
            new_target: None,
            force: false,
        });
        let result = service.update_ref(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("new_target is required"));
    }

    #[tokio::test]
    async fn test_update_ref_invalid_old_target_oid() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(UpdateRefRequest {
            repo_id: "test-repo".to_string(),
            ref_name: "refs/heads/main".to_string(),
            old_target: Some(create_invalid_oid()),
            new_target: Some(create_valid_oid()),
            force: false,
        });
        let result = service.update_ref(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_commit_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(GetCommitRequest {
            repo_id: "".to_string(),
            oid: Some(create_valid_oid()),
        });
        let result = service.get_commit(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_commit_missing_oid() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(GetCommitRequest {
            repo_id: "test-repo".to_string(),
            oid: None,
        });
        let result = service.get_commit(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("oid is required"));
    }

    #[tokio::test]
    async fn test_get_commits_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(GetCommitsRequest {
            repo_id: "".to_string(),
            oids: vec![create_valid_oid()],
        });
        let result = service.get_commits(request).await;
        assert!(result.is_err());
        let status = result.err().unwrap();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_commits_invalid_oid() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(GetCommitsRequest {
            repo_id: "test-repo".to_string(),
            oids: vec![create_invalid_oid()],
        });
        let result = service.get_commits(request).await;
        assert!(result.is_err());
        let status = result.err().unwrap();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_commits_streaming_nonexistent() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(GetCommitsRequest {
            repo_id: "test-repo".to_string(),
            oids: vec![create_valid_oid()],
        });
        let mut stream = service.get_commits(request).await.unwrap().into_inner();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item);
        }
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_get_commits_streaming_with_existing() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let tree_oid = Oid::hash(b"tree");
        let sig = Signature::new("Author", "author@test.com", 1234567890, "+0000");
        let commit = gitstratum_core::Commit::new(tree_oid, vec![], sig.clone(), sig, "Test");
        let proto_oid = ProtoOid {
            bytes: commit.oid.as_bytes().to_vec(),
        };

        let proto_commit = ProtoCommit {
            oid: Some(proto_oid.clone()),
            tree: Some(ProtoOid {
                bytes: tree_oid.as_bytes().to_vec(),
            }),
            parents: vec![],
            author: Some(ProtoSignature {
                name: "Author".to_string(),
                email: "author@test.com".to_string(),
                timestamp: 1234567890,
                timezone: "+0000".to_string(),
            }),
            committer: Some(ProtoSignature {
                name: "Author".to_string(),
                email: "author@test.com".to_string(),
                timestamp: 1234567890,
                timezone: "+0000".to_string(),
            }),
            message: "Test".to_string(),
        };

        let put_req = Request::new(PutCommitRequest {
            repo_id: "test-repo".to_string(),
            commit: Some(proto_commit),
        });
        service.put_commit(put_req).await.unwrap();

        let request = Request::new(GetCommitsRequest {
            repo_id: "test-repo".to_string(),
            oids: vec![proto_oid],
        });
        let mut stream = service.get_commits(request).await.unwrap().into_inner();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].message, "Test");
    }

    #[tokio::test]
    async fn test_put_commit_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let proto_commit = ProtoCommit {
            oid: None,
            tree: None,
            parents: vec![],
            author: None,
            committer: None,
            message: "Test".to_string(),
        };
        let request = Request::new(PutCommitRequest {
            repo_id: "".to_string(),
            commit: Some(proto_commit),
        });
        let result = service.put_commit(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_put_commit_missing_commit() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(PutCommitRequest {
            repo_id: "test-repo".to_string(),
            commit: None,
        });
        let result = service.put_commit(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("commit is required"));
    }

    #[tokio::test]
    async fn test_get_tree_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(GetTreeRequest {
            repo_id: "".to_string(),
            oid: Some(create_valid_oid()),
        });
        let result = service.get_tree(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_get_tree_missing_oid() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(GetTreeRequest {
            repo_id: "test-repo".to_string(),
            oid: None,
        });
        let result = service.get_tree(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("oid is required"));
    }

    #[tokio::test]
    async fn test_put_tree_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let proto_tree = ProtoTree {
            oid: None,
            entries: vec![],
        };
        let request = Request::new(PutTreeRequest {
            repo_id: "".to_string(),
            tree: Some(proto_tree),
        });
        let result = service.put_tree(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_put_tree_missing_tree() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(PutTreeRequest {
            repo_id: "test-repo".to_string(),
            tree: None,
        });
        let result = service.put_tree(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("tree is required"));
    }

    #[tokio::test]
    async fn test_walk_commits_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(WalkCommitsRequest {
            repo_id: "".to_string(),
            from: vec![create_valid_oid()],
            until: vec![],
            limit: 0,
        });
        let result = service.walk_commits(request).await;
        assert!(result.is_err());
        let status = result.err().unwrap();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_walk_commits_with_limit() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let tree_oid = Oid::hash(b"tree");
        let sig = Signature::new("Author", "author@test.com", 1234567890, "+0000");

        let commit1 =
            gitstratum_core::Commit::new(tree_oid, vec![], sig.clone(), sig.clone(), "Commit 1");
        let commit2 = gitstratum_core::Commit::new(
            tree_oid,
            vec![commit1.oid],
            sig.clone(),
            sig.clone(),
            "Commit 2",
        );
        let commit3 =
            gitstratum_core::Commit::new(tree_oid, vec![commit2.oid], sig.clone(), sig, "Commit 3");

        for commit in [&commit1, &commit2, &commit3] {
            let proto_commit = MetadataServiceImpl::commit_to_proto_commit(commit);
            let put_req = Request::new(PutCommitRequest {
                repo_id: "test-repo".to_string(),
                commit: Some(proto_commit),
            });
            service.put_commit(put_req).await.unwrap();
        }

        let request = Request::new(WalkCommitsRequest {
            repo_id: "test-repo".to_string(),
            from: vec![ProtoOid {
                bytes: commit3.oid.as_bytes().to_vec(),
            }],
            until: vec![],
            limit: 2,
        });
        let mut stream = service.walk_commits(request).await.unwrap().into_inner();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn test_find_merge_base_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(FindMergeBaseRequest {
            repo_id: "".to_string(),
            commits: vec![create_valid_oid()],
        });
        let result = service.find_merge_base(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_delete_repo_invalid_repo_id() {
        let (service, _temp) = create_test_service();
        let request = Request::new(DeleteRepoRequest {
            repo_id: "".to_string(),
        });
        let result = service.delete_repo(request).await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_delete_repo_nonexistent() {
        let (service, _temp) = create_test_service();
        let request = Request::new(DeleteRepoRequest {
            repo_id: "nonexistent-repo".to_string(),
        });
        let response = service.delete_repo(request).await.unwrap().into_inner();
        assert!(!response.success);
        assert!(!response.error.is_empty());
    }

    #[tokio::test]
    async fn test_list_repos_with_zero_limit() {
        let (service, _temp) = create_test_service();
        for i in 0..3 {
            let create_req = Request::new(CreateRepoRequest {
                repo_id: format!("repo{}", i),
            });
            service.create_repo(create_req).await.unwrap();
        }

        let request = Request::new(ListReposRequest {
            prefix: "".to_string(),
            limit: 0,
            cursor: "".to_string(),
        });
        let response = service.list_repos(request).await.unwrap().into_inner();
        assert_eq!(response.repo_ids.len(), 3);
    }

    #[tokio::test]
    async fn test_list_repos_invalid_repo_returns_error() {
        let (service, _temp) = create_test_service();

        let request = Request::new(ListReposRequest {
            prefix: "".to_string(),
            limit: 10,
            cursor: "".to_string(),
        });
        let response = service.list_repos(request).await.unwrap().into_inner();
        assert!(response.repo_ids.is_empty());
    }

    #[tokio::test]
    async fn test_update_ref_with_force() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");

        let update1 = Request::new(UpdateRefRequest {
            repo_id: "test-repo".to_string(),
            ref_name: "refs/heads/main".to_string(),
            old_target: None,
            new_target: Some(ProtoOid {
                bytes: oid1.as_bytes().to_vec(),
            }),
            force: false,
        });
        service.update_ref(update1).await.unwrap();

        let update2 = Request::new(UpdateRefRequest {
            repo_id: "test-repo".to_string(),
            ref_name: "refs/heads/main".to_string(),
            old_target: None,
            new_target: Some(ProtoOid {
                bytes: oid2.as_bytes().to_vec(),
            }),
            force: true,
        });
        let response = service.update_ref(update2).await.unwrap().into_inner();
        assert!(response.success);
    }

    #[tokio::test]
    async fn test_get_commits_empty_oids() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(GetCommitsRequest {
            repo_id: "test-repo".to_string(),
            oids: vec![],
        });
        let mut stream = service.get_commits(request).await.unwrap().into_inner();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item);
        }
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_walk_commits_nonexistent_from() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(WalkCommitsRequest {
            repo_id: "test-repo".to_string(),
            from: vec![create_valid_oid()],
            until: vec![],
            limit: 0,
        });
        let mut stream = service.walk_commits(request).await.unwrap().into_inner();

        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item);
        }
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_find_merge_base_empty_commits() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(FindMergeBaseRequest {
            repo_id: "test-repo".to_string(),
            commits: vec![],
        });
        let response = service.find_merge_base(request).await.unwrap().into_inner();
        assert!(!response.found);
    }

    #[tokio::test]
    async fn test_find_merge_base_single_commit() {
        let (service, _temp) = create_test_service();
        let create_req = Request::new(CreateRepoRequest {
            repo_id: "test-repo".to_string(),
        });
        service.create_repo(create_req).await.unwrap();

        let request = Request::new(FindMergeBaseRequest {
            repo_id: "test-repo".to_string(),
            commits: vec![create_valid_oid()],
        });
        let response = service.find_merge_base(request).await.unwrap().into_inner();
        assert!(response.found);
    }
}
