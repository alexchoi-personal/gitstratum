use std::pin::Pin;
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

    fn proto_oid_to_oid(proto: &ProtoOid) -> Result<Oid, Status> {
        Oid::from_slice(&proto.bytes).map_err(|e| Status::invalid_argument(e.to_string()))
    }

    fn oid_to_proto_oid(oid: &Oid) -> ProtoOid {
        ProtoOid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    fn proto_signature_to_signature(proto: &ProtoSignature) -> Signature {
        Signature::new(
            &proto.name,
            &proto.email,
            proto.timestamp,
            &proto.timezone,
        )
    }

    fn signature_to_proto_signature(sig: &Signature) -> ProtoSignature {
        ProtoSignature {
            name: sig.name.clone(),
            email: sig.email.clone(),
            timestamp: sig.timestamp,
            timezone: sig.timezone.clone(),
        }
    }

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
    async fn get_ref(&self, request: Request<GetRefRequest>) -> Result<Response<GetRefResponse>, Status> {
        let req = request.into_inner();
        let repo_id =
            RepoId::new(&req.repo_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let ref_name =
            RefName::new(&req.ref_name).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let result = self
            .store
            .get_ref(&repo_id, &ref_name)
            .map_err(|e| Status::from(e))?;

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
            .map_err(|e| Status::from(e))?;

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
            .map_err(|e| Status::from(e))?;

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

        let result = self
            .store
            .get_tree(&repo_id, &oid)
            .map_err(|e| Status::from(e))?;

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

        let result = find_merge_base(&self.store, &repo_id, &commits)
            .map_err(|e| Status::from(e))?;

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
            .map_err(|e| Status::from(e))?;

        Ok(Response::new(ListReposResponse {
            repo_ids: repos.into_iter().map(|r| r.as_str().to_string()).collect(),
            next_cursor: next_cursor.unwrap_or_default(),
        }))
    }
}
