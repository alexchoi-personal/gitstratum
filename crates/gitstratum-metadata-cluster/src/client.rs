use std::str::FromStr;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tracing::warn;

use gitstratum_core::{Commit, Oid, RefName, RepoId, Signature, Tree, TreeEntry, TreeEntryMode};
use gitstratum_proto::metadata_service_client::MetadataServiceClient;
use gitstratum_proto::{
    Commit as ProtoCommit, CreateRepoRequest, DeleteRepoRequest, FindMergeBaseRequest,
    GetCommitRequest, GetCommitsRequest, GetRefRequest, GetTreeRequest, ListRefsRequest,
    ListReposRequest, Oid as ProtoOid, PutCommitRequest, PutTreeRequest,
    Signature as ProtoSignature, Tree as ProtoTree, TreeEntry as ProtoTreeEntry, UpdateRefRequest,
    WalkCommitsRequest,
};

use crate::error::{MetadataStoreError, Result};

#[derive(Debug)]
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
            match RefName::new(&entry.name) {
                Ok(ref_name) => {
                    if let Some(target) = &entry.target {
                        match Self::proto_to_oid(target) {
                            Ok(oid) => refs.push((ref_name, oid)),
                            Err(e) => {
                                warn!(ref_name = %entry.name, error = %e, "skipping ref with invalid OID");
                            }
                        }
                    } else {
                        warn!(ref_name = %entry.name, "skipping ref with missing target");
                    }
                }
                Err(e) => {
                    warn!(ref_name = %entry.name, error = %e, "skipping ref with invalid name");
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

    pub async fn get_commits(&mut self, repo_id: &RepoId, oids: &[Oid]) -> Result<Vec<Commit>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_conversion_comprehensive() {
        let test_cases: Vec<([u8; 32], bool)> = vec![
            ([0x00; 32], true),
            ([0xff; 32], true),
            ([0xab; 32], true),
            (
                [
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                    0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a,
                    0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
                ],
                true,
            ),
            (
                [
                    0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                ],
                true,
            ),
        ];

        for (bytes, _) in test_cases {
            let oid = Oid::from_slice(&bytes).unwrap();
            let proto = MetadataClient::oid_to_proto(&oid);
            assert_eq!(proto.bytes.len(), 32);
            let restored = MetadataClient::proto_to_oid(&proto).unwrap();
            assert_eq!(restored.as_bytes(), &bytes);
        }

        let zero_oid = Oid::ZERO;
        let proto = MetadataClient::oid_to_proto(&zero_oid);
        assert_eq!(proto.bytes, vec![0u8; 32]);
        let restored = MetadataClient::proto_to_oid(&proto).unwrap();
        assert!(restored.is_zero());

        let hashed = Oid::hash(b"test data");
        let proto = MetadataClient::oid_to_proto(&hashed);
        assert_eq!(proto.bytes, hashed.as_bytes().to_vec());

        let invalid_lengths = vec![0, 10, 16, 31, 33, 64];
        for len in invalid_lengths {
            let proto = ProtoOid {
                bytes: vec![0xab; len],
            };
            let result = MetadataClient::proto_to_oid(&proto);
            assert!(result.is_err());
            match result.unwrap_err() {
                MetadataStoreError::Deserialization(_) => {}
                _ => panic!("Expected Deserialization error for length {}", len),
            }
        }
    }

    #[test]
    fn test_signature_conversion_comprehensive() {
        let long_name = "A".repeat(1000);
        let signatures: Vec<(&str, &str, i64, &str)> = vec![
            ("Test Author", "test@example.com", 1704067200, "+0530"),
            ("Proto Author", "proto@example.com", 1704067300, "-0800"),
            (
                "Test \u{00e9}\u{00e8}\u{00ea}",
                "test+special@example.com",
                1704067200,
                "-1200",
            ),
            (
                "\u{4e2d}\u{6587}\u{540d}\u{5b57}",
                "unicode@test.com",
                1704067200,
                "+0800",
            ),
            (&long_name, "test@test.com", 1704067200, "+0000"),
            ("", "", 0, ""),
            ("Test", "test@test.com", 0, "+0000"),
            ("Test", "test@test.com", i64::MAX, "+0000"),
            ("Test", "test@test.com", i64::MIN, "+0000"),
            ("Test", "test@test.com", -1000, "+0000"),
        ];

        for (name, email, timestamp, timezone) in signatures {
            let sig = Signature::new(name, email, timestamp, timezone);
            let proto = MetadataClient::signature_to_proto(&sig);
            assert_eq!(proto.name, name);
            assert_eq!(proto.email, email);
            assert_eq!(proto.timestamp, timestamp);
            assert_eq!(proto.timezone, timezone);

            let restored = MetadataClient::proto_to_signature(&proto);
            assert_eq!(restored.name, sig.name);
            assert_eq!(restored.email, sig.email);
            assert_eq!(restored.timestamp, sig.timestamp);
            assert_eq!(restored.timezone, sig.timezone);
        }

        let timezones = vec!["+0000", "-0000", "+1200", "-1200", "+0530", "-0930"];
        for tz in timezones {
            let sig = Signature::new("Test", "test@test.com", 1704067200, tz);
            let proto = MetadataClient::signature_to_proto(&sig);
            let restored = MetadataClient::proto_to_signature(&proto);
            assert_eq!(restored.timezone, tz);
        }
    }

    #[test]
    fn test_commit_conversion_comprehensive() {
        let tree_oid = Oid::hash(b"tree content");
        let parent1 = Oid::hash(b"parent1");
        let parent2 = Oid::hash(b"parent2");
        let author = Signature::new("Author Name", "author@test.com", 1704067200, "+0530");
        let committer = Signature::new("Committer Name", "committer@test.com", 1704067300, "-0800");
        let original = Commit::new(
            tree_oid,
            vec![parent1, parent2],
            author,
            committer,
            "Merge commit\n\nWith detailed message",
        );

        let proto = MetadataClient::commit_to_proto(&original);
        let restored = MetadataClient::proto_to_commit(&proto).unwrap();

        assert_eq!(restored.oid, original.oid);
        assert_eq!(restored.tree, original.tree);
        assert_eq!(restored.parents.len(), original.parents.len());
        for (orig, rest) in original.parents.iter().zip(restored.parents.iter()) {
            assert_eq!(orig, rest);
        }
        assert_eq!(restored.author.name, original.author.name);
        assert_eq!(restored.author.email, original.author.email);
        assert_eq!(restored.author.timestamp, original.author.timestamp);
        assert_eq!(restored.author.timezone, original.author.timezone);
        assert_eq!(restored.committer.name, original.committer.name);
        assert_eq!(restored.committer.email, original.committer.email);
        assert_eq!(restored.message, original.message);

        let parent_counts = vec![0, 1, 4, 10];
        for count in parent_counts {
            let parents: Vec<Oid> = (0..count)
                .map(|i| Oid::hash(format!("parent{}", i).as_bytes()))
                .collect();
            let tree_oid = Oid::hash(b"tree");
            let author = Signature::new("Author", "author@test.com", 1704067200, "+0000");
            let committer = Signature::new("Committer", "committer@test.com", 1704067300, "+0000");
            let commit = Commit::new(tree_oid, parents.clone(), author, committer, "Test");

            let proto = MetadataClient::commit_to_proto(&commit);
            assert_eq!(proto.parents.len(), count);

            let restored = MetadataClient::proto_to_commit(&proto).unwrap();
            assert_eq!(restored.parents.len(), count);
            for (original, restored) in parents.iter().zip(restored.parents.iter()) {
                assert_eq!(original, restored);
            }
        }

        let long_message = "A".repeat(100_000);
        let messages: Vec<&str> = vec![
            "",
            "Single line",
            "Line 1\nLine 2",
            "Line 1\n\nLine 3 with blank line",
            "\n\nStarts with newlines",
            "Ends with newlines\n\n",
            "\t\tTabbed content",
            "  Spaces  ",
            "Message with\0null byte",
            "Message with unicode: \u{1f600}\u{1f389}\u{1f680}",
            &long_message,
        ];

        for message in messages {
            let tree_oid = Oid::hash(b"tree");
            let author = Signature::new("Author", "author@test.com", 1704067200, "+0000");
            let committer = Signature::new("Committer", "committer@test.com", 1704067300, "+0000");
            let commit = Commit::new(tree_oid, vec![], author, committer, message);

            let proto = MetadataClient::commit_to_proto(&commit);
            let restored = MetadataClient::proto_to_commit(&proto).unwrap();
            assert_eq!(restored.message, message);
        }

        let proto_missing = ProtoCommit {
            oid: None,
            tree: None,
            parents: vec![],
            author: None,
            committer: None,
            message: "Empty commit".to_string(),
        };
        let commit = MetadataClient::proto_to_commit(&proto_missing).unwrap();
        assert!(commit.oid.is_zero());
        assert!(commit.tree.is_zero());
        assert!(commit.parents.is_empty());
        assert_eq!(commit.author.name, "");
        assert_eq!(commit.author.email, "");
        assert_eq!(commit.author.timestamp, 0);
        assert_eq!(commit.author.timezone, "+0000");
        assert_eq!(commit.committer.name, "");
        assert_eq!(commit.committer.email, "");
        assert_eq!(commit.committer.timezone, "+0000");

        let proto_author_only = ProtoCommit {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            tree: Some(ProtoOid {
                bytes: vec![0x22; 32],
            }),
            parents: vec![],
            author: Some(ProtoSignature {
                name: "Author Only".to_string(),
                email: "author@only.com".to_string(),
                timestamp: 1704067200,
                timezone: "+0000".to_string(),
            }),
            committer: None,
            message: "Only author set".to_string(),
        };
        let commit = MetadataClient::proto_to_commit(&proto_author_only).unwrap();
        assert_eq!(commit.author.name, "Author Only");
        assert_eq!(commit.committer.name, "");

        let proto_committer_only = ProtoCommit {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            tree: Some(ProtoOid {
                bytes: vec![0x22; 32],
            }),
            parents: vec![],
            author: None,
            committer: Some(ProtoSignature {
                name: "Committer Only".to_string(),
                email: "committer@only.com".to_string(),
                timestamp: 1704067300,
                timezone: "+0100".to_string(),
            }),
            message: "Only committer set".to_string(),
        };
        let commit = MetadataClient::proto_to_commit(&proto_committer_only).unwrap();
        assert_eq!(commit.committer.name, "Committer Only");
        assert_eq!(commit.author.name, "");
    }

    #[test]
    fn test_commit_conversion_error_cases() {
        let invalid_oid_cases = vec![
            (
                Some(ProtoOid { bytes: vec![0; 10] }),
                None,
                "invalid commit oid",
            ),
            (
                Some(ProtoOid {
                    bytes: vec![0x11; 32],
                }),
                Some(ProtoOid { bytes: vec![0; 5] }),
                "invalid tree oid",
            ),
        ];

        for (oid, tree, desc) in invalid_oid_cases {
            let proto = ProtoCommit {
                oid,
                tree,
                parents: vec![],
                author: None,
                committer: None,
                message: desc.to_string(),
            };
            assert!(
                MetadataClient::proto_to_commit(&proto).is_err(),
                "Should fail for {}",
                desc
            );
        }

        let proto = ProtoCommit {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            tree: Some(ProtoOid {
                bytes: vec![0x22; 32],
            }),
            parents: vec![ProtoOid { bytes: vec![0; 16] }],
            author: None,
            committer: None,
            message: "Invalid parent".to_string(),
        };
        assert!(MetadataClient::proto_to_commit(&proto).is_err());

        let proto = ProtoCommit {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            tree: Some(ProtoOid {
                bytes: vec![0x22; 32],
            }),
            parents: vec![
                ProtoOid {
                    bytes: vec![0x33; 32],
                },
                ProtoOid {
                    bytes: vec![0x44; 10],
                },
                ProtoOid {
                    bytes: vec![0x55; 32],
                },
            ],
            author: None,
            committer: None,
            message: "Invalid middle parent".to_string(),
        };
        assert!(MetadataClient::proto_to_commit(&proto).is_err());
    }

    #[test]
    fn test_tree_conversion_comprehensive() {
        let modes = vec![
            (TreeEntryMode::File, "100644"),
            (TreeEntryMode::Executable, "100755"),
            (TreeEntryMode::Symlink, "120000"),
            (TreeEntryMode::Directory, "040000"),
            (TreeEntryMode::Submodule, "160000"),
        ];

        for (mode, expected_str) in &modes {
            let entry = TreeEntry::new(*mode, "test", Oid::hash(b"test"));
            let tree = Tree::new(vec![entry]);
            let proto = MetadataClient::tree_to_proto(&tree);
            assert_eq!(proto.entries[0].mode, *expected_str);
        }

        for (expected_mode, mode_str) in &modes {
            let proto = ProtoTree {
                oid: Some(ProtoOid {
                    bytes: vec![0x00; 32],
                }),
                entries: vec![ProtoTreeEntry {
                    mode: mode_str.to_string(),
                    name: "entry".to_string(),
                    oid: Some(ProtoOid {
                        bytes: vec![0x11; 32],
                    }),
                }],
            };

            let result = MetadataClient::proto_to_tree(&proto);
            assert!(result.is_ok());
            let tree = result.unwrap();
            assert_eq!(tree.entries[0].mode, *expected_mode);
        }

        let entries = vec![
            TreeEntry::file("README.md", Oid::hash(b"readme content")),
            TreeEntry::new(TreeEntryMode::Executable, "script.sh", Oid::hash(b"script")),
            TreeEntry::directory("src", Oid::hash(b"src tree")),
        ];
        let original = Tree::new(entries);

        let proto = MetadataClient::tree_to_proto(&original);
        let restored = MetadataClient::proto_to_tree(&proto).unwrap();

        assert_eq!(restored.oid, original.oid);
        assert_eq!(restored.entries.len(), original.entries.len());
        for (orig_entry, rest_entry) in original.entries.iter().zip(restored.entries.iter()) {
            assert_eq!(rest_entry.mode, orig_entry.mode);
            assert_eq!(rest_entry.name, orig_entry.name);
            assert_eq!(rest_entry.oid, orig_entry.oid);
        }

        let tree = Tree::new(vec![]);
        let proto = MetadataClient::tree_to_proto(&tree);
        assert!(proto.entries.is_empty());
        let restored = MetadataClient::proto_to_tree(&proto).unwrap();
        assert!(restored.entries.is_empty());

        let entry_counts = vec![1, 3, 100, 1000];
        for count in entry_counts {
            let entries: Vec<TreeEntry> = (0..count)
                .map(|i| {
                    if i % 2 == 0 {
                        TreeEntry::file(
                            format!("file_{}.txt", i),
                            Oid::hash(format!("f{}", i).as_bytes()),
                        )
                    } else {
                        TreeEntry::directory(
                            format!("dir_{}", i),
                            Oid::hash(format!("d{}", i).as_bytes()),
                        )
                    }
                })
                .collect();

            let tree = Tree::new(entries);
            let proto = MetadataClient::tree_to_proto(&tree);
            assert_eq!(proto.entries.len(), count);

            let restored = MetadataClient::proto_to_tree(&proto).unwrap();
            assert_eq!(restored.entries.len(), count);
        }

        let long_filename = format!("{}.txt", "a".repeat(255));
        let filenames: Vec<&str> = vec![
            "\u{4e2d}\u{6587}.txt",
            "\u{0420}\u{0443}\u{0441}\u{0441}\u{043a}\u{0438}\u{0439}.md",
            "\u{1f600}.emoji",
            "file with spaces.txt",
            "path/to/file.txt",
            &long_filename,
            "",
        ];
        for name in filenames {
            let entry = TreeEntry::file(name, Oid::hash(name.as_bytes()));
            let tree = Tree::new(vec![entry]);
            let proto = MetadataClient::tree_to_proto(&tree);
            assert_eq!(proto.entries[0].name, name);
            let restored = MetadataClient::proto_to_tree(&proto).unwrap();
            assert_eq!(restored.entries[0].name, name);
        }

        let proto = ProtoTree {
            oid: None,
            entries: vec![ProtoTreeEntry {
                mode: "040000".to_string(),
                name: "subdir".to_string(),
                oid: None,
            }],
        };
        let result = MetadataClient::proto_to_tree(&proto);
        assert!(result.is_ok());
        let tree = result.unwrap();
        assert!(tree.oid.is_zero());
        assert!(tree.entries[0].oid.is_zero());

        let entry_names: Vec<String> = vec!["z.txt", "a.txt", "m.txt", "0.txt", "Z.txt"]
            .into_iter()
            .map(String::from)
            .collect();
        let entries: Vec<TreeEntry> = entry_names
            .iter()
            .map(|name| TreeEntry::file(name, Oid::hash(name.as_bytes())))
            .collect();
        let tree = Tree::new(entries);
        let proto = MetadataClient::tree_to_proto(&tree);
        let restored = MetadataClient::proto_to_tree(&proto).unwrap();

        let mut sorted_names = entry_names.clone();
        sorted_names.sort();
        for (i, name) in sorted_names.iter().enumerate() {
            assert_eq!(&restored.entries[i].name, name);
        }
    }

    #[test]
    fn test_tree_conversion_error_cases() {
        let invalid_modes = vec!["999999", "", "invalid", "badmode"];
        for mode in invalid_modes {
            let proto = ProtoTree {
                oid: Some(ProtoOid {
                    bytes: vec![0x11; 32],
                }),
                entries: vec![ProtoTreeEntry {
                    mode: mode.to_string(),
                    name: "file.txt".to_string(),
                    oid: Some(ProtoOid {
                        bytes: vec![0x22; 32],
                    }),
                }],
            };
            let result = MetadataClient::proto_to_tree(&proto);
            assert!(result.is_err(), "Mode '{}' should be invalid", mode);
        }

        let proto = ProtoTree {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            entries: vec![ProtoTreeEntry {
                mode: "100644".to_string(),
                name: "file.txt".to_string(),
                oid: Some(ProtoOid { bytes: vec![0; 8] }),
            }],
        };
        assert!(MetadataClient::proto_to_tree(&proto).is_err());

        let invalid_tree_oids = vec![10, 31, 33];
        for len in invalid_tree_oids {
            let proto = ProtoTree {
                oid: Some(ProtoOid {
                    bytes: vec![0; len],
                }),
                entries: vec![],
            };
            assert!(
                MetadataClient::proto_to_tree(&proto).is_err(),
                "Tree oid with {} bytes should be invalid",
                len
            );
        }

        let proto = ProtoTree {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            entries: vec![
                ProtoTreeEntry {
                    mode: "100644".to_string(),
                    name: "valid.txt".to_string(),
                    oid: Some(ProtoOid {
                        bytes: vec![0x22; 32],
                    }),
                },
                ProtoTreeEntry {
                    mode: "invalid".to_string(),
                    name: "invalid.txt".to_string(),
                    oid: Some(ProtoOid {
                        bytes: vec![0x33; 32],
                    }),
                },
            ],
        };
        assert!(MetadataClient::proto_to_tree(&proto).is_err());

        let proto = ProtoTree {
            oid: Some(ProtoOid {
                bytes: vec![0x11; 32],
            }),
            entries: vec![
                ProtoTreeEntry {
                    mode: "100644".to_string(),
                    name: "valid.txt".to_string(),
                    oid: Some(ProtoOid {
                        bytes: vec![0x22; 32],
                    }),
                },
                ProtoTreeEntry {
                    mode: "100644".to_string(),
                    name: "invalid_oid.txt".to_string(),
                    oid: Some(ProtoOid {
                        bytes: vec![0x33; 5],
                    }),
                },
            ],
        };
        assert!(MetadataClient::proto_to_tree(&proto).is_err());
    }

    #[tokio::test]
    async fn test_client_connection_and_lifecycle() {
        let invalid_addresses = vec!["not-a-valid-uri", "grpc://localhost:50051"];
        for addr in invalid_addresses {
            let result = MetadataClient::connect(addr).await;
            assert!(result.is_err(), "Address '{}' should fail", addr);
            if addr == "not-a-valid-uri" {
                match result.unwrap_err() {
                    MetadataStoreError::Connection(msg) => {
                        assert!(!msg.is_empty());
                    }
                    _ => panic!("Expected Connection error"),
                }
            }
        }

        let unreachable_addresses = vec![
            "http://192.0.2.1:12345",
            "http://127.0.0.1:0",
            "https://localhost:50051",
            "http://127.0.0.1:50051",
            "http://127.0.0.1:50052",
            "http://localhost:50053",
        ];
        for addr in unreachable_addresses {
            let result = MetadataClient::connect(addr).await;
            assert!(result.is_err());
        }

        use tonic::transport::Endpoint;
        let endpoint = Endpoint::from_static("http://127.0.0.1:50051");
        let channel = endpoint.connect_lazy();
        let client = MetadataClient::connect_with_channel(channel.clone()).await;
        let debug_str = format!("{:?}", client);
        assert!(debug_str.contains("MetadataClient"));
        assert!(debug_str.contains("client"));

        let cloned = client.clone();
        let debug_cloned = format!("{:?}", cloned);
        assert!(debug_cloned.contains("MetadataClient"));

        let client2 = MetadataClient::connect_with_channel(channel).await;
        assert!(format!("{:?}", client2).contains("MetadataClient"));
    }
}
