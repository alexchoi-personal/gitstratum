
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use gitstratum_core::{Commit, Oid, RefName, RepoId, Signature, Tree, TreeEntry, TreeEntryMode};
use gitstratum_proto::metadata_service_client::MetadataServiceClient;
use gitstratum_proto::{
    Commit as ProtoCommit, CreateRepoRequest, DeleteRepoRequest, FindMergeBaseRequest,
    GetCommitRequest, GetCommitsRequest, GetRefRequest, GetTreeRequest, ListRefsRequest,
    ListReposRequest, Oid as ProtoOid, PutCommitRequest, PutTreeRequest, Signature as ProtoSignature,
    Tree as ProtoTree, TreeEntry as ProtoTreeEntry, UpdateRefRequest, WalkCommitsRequest,
};

use crate::error::{MetadataStoreError, Result};

pub struct MetadataClient {
    client: MetadataServiceClient<Channel>,
}

impl MetadataClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let endpoint = Endpoint::from_shared(addr.to_string())
            .map_err(|e| MetadataStoreError::Connection(e.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));

        let channel = endpoint.connect().await?;
        let client = MetadataServiceClient::new(channel);

        Ok(Self { client })
    }

    pub async fn connect_with_channel(channel: Channel) -> Self {
        let client = MetadataServiceClient::new(channel);
        Self { client }
    }

    fn oid_to_proto(oid: &Oid) -> ProtoOid {
        ProtoOid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    fn proto_to_oid(proto: &ProtoOid) -> Result<Oid> {
        Oid::from_slice(&proto.bytes)
            .map_err(|e| MetadataStoreError::Deserialization(e.to_string()))
    }

    fn signature_to_proto(sig: &Signature) -> ProtoSignature {
        ProtoSignature {
            name: sig.name.clone(),
            email: sig.email.clone(),
            timestamp: sig.timestamp,
            timezone: sig.timezone.clone(),
        }
    }

    fn proto_to_signature(proto: &ProtoSignature) -> Signature {
        Signature::new(&proto.name, &proto.email, proto.timestamp, &proto.timezone)
    }

    fn commit_to_proto(commit: &Commit) -> ProtoCommit {
        ProtoCommit {
            oid: Some(Self::oid_to_proto(&commit.oid)),
            tree: Some(Self::oid_to_proto(&commit.tree)),
            parents: commit.parents.iter().map(Self::oid_to_proto).collect(),
            author: Some(Self::signature_to_proto(&commit.author)),
            committer: Some(Self::signature_to_proto(&commit.committer)),
            message: commit.message.clone(),
        }
    }

    fn proto_to_commit(proto: &ProtoCommit) -> Result<Commit> {
        let oid = proto
            .oid
            .as_ref()
            .map(Self::proto_to_oid)
            .transpose()?
            .unwrap_or(Oid::ZERO);

        let tree = proto
            .tree
            .as_ref()
            .map(Self::proto_to_oid)
            .transpose()?
            .unwrap_or(Oid::ZERO);

        let parents: Vec<Oid> = proto
            .parents
            .iter()
            .map(Self::proto_to_oid)
            .collect::<Result<_>>()?;

        let author = proto
            .author
            .as_ref()
            .map(Self::proto_to_signature)
            .unwrap_or_else(|| Signature::new("", "", 0, "+0000"));

        let committer = proto
            .committer
            .as_ref()
            .map(Self::proto_to_signature)
            .unwrap_or_else(|| Signature::new("", "", 0, "+0000"));

        Ok(Commit::with_oid(
            oid,
            tree,
            parents,
            author,
            committer,
            &proto.message,
        ))
    }

    fn tree_to_proto(tree: &Tree) -> ProtoTree {
        ProtoTree {
            oid: Some(Self::oid_to_proto(&tree.oid)),
            entries: tree
                .entries
                .iter()
                .map(|e| ProtoTreeEntry {
                    mode: e.mode.as_str().to_string(),
                    name: e.name.clone(),
                    oid: Some(Self::oid_to_proto(&e.oid)),
                })
                .collect(),
        }
    }

    fn proto_to_tree(proto: &ProtoTree) -> Result<Tree> {
        let oid = proto
            .oid
            .as_ref()
            .map(Self::proto_to_oid)
            .transpose()?
            .unwrap_or(Oid::ZERO);

        let entries: Vec<TreeEntry> = proto
            .entries
            .iter()
            .map(|e| {
                let mode = TreeEntryMode::from_str(&e.mode)
                    .map_err(|err| MetadataStoreError::Deserialization(err.to_string()))?;
                let entry_oid = e
                    .oid
                    .as_ref()
                    .map(Self::proto_to_oid)
                    .transpose()?
                    .unwrap_or(Oid::ZERO);
                Ok(TreeEntry::new(mode, &e.name, entry_oid))
            })
            .collect::<Result<_>>()?;

        Ok(Tree::with_oid(oid, entries))
    }

    pub async fn get_ref(&mut self, repo_id: &RepoId, ref_name: &RefName) -> Result<Option<Oid>> {
        let request = GetRefRequest {
            repo_id: repo_id.as_str().to_string(),
            ref_name: ref_name.as_str().to_string(),
        };

        let response = self.client.get_ref(request).await?.into_inner();

        if response.found {
            let oid = response
                .target
                .as_ref()
                .map(Self::proto_to_oid)
                .transpose()?;
            Ok(oid)
        } else {
            Ok(None)
        }
    }

    pub async fn list_refs(
        &mut self,
        repo_id: &RepoId,
        prefix: &str,
    ) -> Result<Vec<(RefName, Oid)>> {
        let request = ListRefsRequest {
            repo_id: repo_id.as_str().to_string(),
            prefix: prefix.to_string(),
        };

        let response = self.client.list_refs(request).await?.into_inner();

        let mut refs = Vec::new();
        for entry in response.refs {
            if let Ok(ref_name) = RefName::new(&entry.name) {
                if let Some(target) = &entry.target {
                    if let Ok(oid) = Self::proto_to_oid(target) {
                        refs.push((ref_name, oid));
                    }
                }
            }
        }

        Ok(refs)
    }

    pub async fn update_ref(
        &mut self,
        repo_id: &RepoId,
        ref_name: &RefName,
        old_target: Option<&Oid>,
        new_target: &Oid,
        force: bool,
    ) -> Result<()> {
        let request = UpdateRefRequest {
            repo_id: repo_id.as_str().to_string(),
            ref_name: ref_name.as_str().to_string(),
            old_target: old_target.map(Self::oid_to_proto),
            new_target: Some(Self::oid_to_proto(new_target)),
            force,
        };

        let response = self.client.update_ref(request).await?.into_inner();

        if response.success {
            Ok(())
        } else {
            Err(MetadataStoreError::Internal(response.error))
        }
    }

    pub async fn get_commit(&mut self, repo_id: &RepoId, oid: &Oid) -> Result<Option<Commit>> {
        let request = GetCommitRequest {
            repo_id: repo_id.as_str().to_string(),
            oid: Some(Self::oid_to_proto(oid)),
        };

        let response = self.client.get_commit(request).await?.into_inner();

        if response.found {
            let commit = response
                .commit
                .as_ref()
                .map(Self::proto_to_commit)
                .transpose()?;
            Ok(commit)
        } else {
            Ok(None)
        }
    }

    pub async fn get_commits(
        &mut self,
        repo_id: &RepoId,
        oids: &[Oid],
    ) -> Result<Vec<Commit>> {
        let request = GetCommitsRequest {
            repo_id: repo_id.as_str().to_string(),
            oids: oids.iter().map(Self::oid_to_proto).collect(),
        };

        let mut stream = self.client.get_commits(request).await?.into_inner();

        let mut commits = Vec::new();
        while let Some(proto) = stream.message().await? {
            commits.push(Self::proto_to_commit(&proto)?);
        }

        Ok(commits)
    }

    pub async fn put_commit(&mut self, repo_id: &RepoId, commit: &Commit) -> Result<()> {
        let request = PutCommitRequest {
            repo_id: repo_id.as_str().to_string(),
            commit: Some(Self::commit_to_proto(commit)),
        };

        let response = self.client.put_commit(request).await?.into_inner();

        if response.success {
            Ok(())
        } else {
            Err(MetadataStoreError::Internal(response.error))
        }
    }

    pub async fn get_tree(&mut self, repo_id: &RepoId, oid: &Oid) -> Result<Option<Tree>> {
        let request = GetTreeRequest {
            repo_id: repo_id.as_str().to_string(),
            oid: Some(Self::oid_to_proto(oid)),
        };

        let response = self.client.get_tree(request).await?.into_inner();

        if response.found {
            let tree = response
                .tree
                .as_ref()
                .map(Self::proto_to_tree)
                .transpose()?;
            Ok(tree)
        } else {
            Ok(None)
        }
    }

    pub async fn put_tree(&mut self, repo_id: &RepoId, tree: &Tree) -> Result<()> {
        let request = PutTreeRequest {
            repo_id: repo_id.as_str().to_string(),
            tree: Some(Self::tree_to_proto(tree)),
        };

        let response = self.client.put_tree(request).await?.into_inner();

        if response.success {
            Ok(())
        } else {
            Err(MetadataStoreError::Internal(response.error))
        }
    }

    pub async fn walk_commits(
        &mut self,
        repo_id: &RepoId,
        from: &[Oid],
        until: &[Oid],
        limit: Option<u32>,
    ) -> Result<Vec<Commit>> {
        let request = WalkCommitsRequest {
            repo_id: repo_id.as_str().to_string(),
            from: from.iter().map(Self::oid_to_proto).collect(),
            until: until.iter().map(Self::oid_to_proto).collect(),
            limit: limit.unwrap_or(0),
        };

        let mut stream = self.client.walk_commits(request).await?.into_inner();

        let mut commits = Vec::new();
        while let Some(proto) = stream.message().await? {
            commits.push(Self::proto_to_commit(&proto)?);
        }

        Ok(commits)
    }

    pub async fn find_merge_base(
        &mut self,
        repo_id: &RepoId,
        commits: &[Oid],
    ) -> Result<Option<Oid>> {
        let request = FindMergeBaseRequest {
            repo_id: repo_id.as_str().to_string(),
            commits: commits.iter().map(Self::oid_to_proto).collect(),
        };

        let response = self.client.find_merge_base(request).await?.into_inner();

        if response.found {
            let oid = response
                .merge_base
                .as_ref()
                .map(Self::proto_to_oid)
                .transpose()?;
            Ok(oid)
        } else {
            Ok(None)
        }
    }

    pub async fn create_repo(&mut self, repo_id: &RepoId) -> Result<()> {
        let request = CreateRepoRequest {
            repo_id: repo_id.as_str().to_string(),
        };

        let response = self.client.create_repo(request).await?.into_inner();

        if response.success {
            Ok(())
        } else {
            Err(MetadataStoreError::Internal(response.error))
        }
    }

    pub async fn delete_repo(&mut self, repo_id: &RepoId) -> Result<()> {
        let request = DeleteRepoRequest {
            repo_id: repo_id.as_str().to_string(),
        };

        let response = self.client.delete_repo(request).await?.into_inner();

        if response.success {
            Ok(())
        } else {
            Err(MetadataStoreError::Internal(response.error))
        }
    }

    pub async fn list_repos(
        &mut self,
        prefix: &str,
        limit: u32,
        cursor: &str,
    ) -> Result<(Vec<RepoId>, Option<String>)> {
        let request = ListReposRequest {
            prefix: prefix.to_string(),
            limit,
            cursor: cursor.to_string(),
        };

        let response = self.client.list_repos(request).await?.into_inner();

        let repos: Vec<RepoId> = response
            .repo_ids
            .iter()
            .filter_map(|id| RepoId::new(id).ok())
            .collect();

        let next_cursor = if response.next_cursor.is_empty() {
            None
        } else {
            Some(response.next_cursor)
        };

        Ok((repos, next_cursor))
    }
}

impl Clone for MetadataClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
        }
    }
}
