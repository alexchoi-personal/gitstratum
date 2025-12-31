use std::sync::Arc;

use gitstratum_core::{Commit, Oid, Signature, Tree, TreeEntry, TreeEntryMode};
use gitstratum_metadata::{MetadataServiceImpl, MetadataStore};
use gitstratum_proto::metadata_service_server::MetadataService;
use gitstratum_proto::{
    CreateRepoRequest, DeleteRepoRequest, FindMergeBaseRequest, GetCommitRequest,
    GetRefRequest, GetTreeRequest, ListRefsRequest, ListReposRequest, Oid as ProtoOid,
    PutCommitRequest, PutTreeRequest, UpdateRefRequest, WalkCommitsRequest,
    Commit as ProtoCommit, Tree as ProtoTree, TreeEntry as ProtoTreeEntry,
    Signature as ProtoSignature,
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

fn create_proto_oid(data: &[u8]) -> ProtoOid {
    let oid = Oid::hash(data);
    ProtoOid {
        bytes: oid.as_bytes().to_vec(),
    }
}

fn create_proto_signature() -> ProtoSignature {
    ProtoSignature {
        name: "Test Author".to_string(),
        email: "test@example.com".to_string(),
        timestamp: 1704067200,
        timezone: "+0000".to_string(),
    }
}

fn create_proto_commit(tree: &ProtoOid, parents: Vec<ProtoOid>, message: &str) -> ProtoCommit {
    let sig = create_proto_signature();
    let tree_oid = Oid::from_slice(&tree.bytes).unwrap();
    let parent_oids: Vec<Oid> = parents
        .iter()
        .map(|p| Oid::from_slice(&p.bytes).unwrap())
        .collect();
    let core_sig = Signature::new(&sig.name, &sig.email, sig.timestamp, &sig.timezone);
    let commit = Commit::new(tree_oid, parent_oids, core_sig.clone(), core_sig, message);

    ProtoCommit {
        oid: Some(ProtoOid {
            bytes: commit.oid.as_bytes().to_vec(),
        }),
        tree: Some(tree.clone()),
        parents,
        author: Some(sig.clone()),
        committer: Some(sig),
        message: message.to_string(),
    }
}

fn create_proto_tree(entries: Vec<(&str, &ProtoOid)>) -> ProtoTree {
    let tree_entries: Vec<TreeEntry> = entries
        .iter()
        .map(|(name, oid)| {
            let entry_oid = Oid::from_slice(&oid.bytes).unwrap();
            TreeEntry::new(TreeEntryMode::File, *name, entry_oid)
        })
        .collect();
    let tree = Tree::new(tree_entries);

    ProtoTree {
        oid: Some(ProtoOid {
            bytes: tree.oid.as_bytes().to_vec(),
        }),
        entries: entries
            .into_iter()
            .map(|(name, oid)| ProtoTreeEntry {
                mode: "100644".to_string(),
                name: name.to_string(),
                oid: Some(oid.clone()),
            })
            .collect(),
    }
}

#[tokio::test]
async fn test_create_repo() {
    let (service, _temp) = create_test_service();

    let request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });

    let response = service.create_repo(request).await.unwrap().into_inner();

    assert!(response.success);
    assert!(response.error.is_empty());
}

#[tokio::test]
async fn test_create_repo_duplicate() {
    let (service, _temp) = create_test_service();

    let request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(request).await.unwrap();

    let request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    let response = service.create_repo(request).await.unwrap().into_inner();

    assert!(!response.success);
    assert!(!response.error.is_empty());
}

#[tokio::test]
async fn test_delete_repo() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let delete_request = Request::new(DeleteRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    let response = service.delete_repo(delete_request).await.unwrap().into_inner();

    assert!(response.success);
}

#[tokio::test]
async fn test_list_repos() {
    let (service, _temp) = create_test_service();

    for i in 0..3 {
        let request = Request::new(CreateRepoRequest {
            repo_id: format!("repo{}", i),
        });
        service.create_repo(request).await.unwrap();
    }

    let request = Request::new(ListReposRequest {
        prefix: "".to_string(),
        limit: 10,
        cursor: "".to_string(),
    });
    let response = service.list_repos(request).await.unwrap().into_inner();

    assert_eq!(response.repo_ids.len(), 3);
}

#[tokio::test]
async fn test_get_ref_not_found() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let request = Request::new(GetRefRequest {
        repo_id: "test-repo".to_string(),
        ref_name: "refs/heads/main".to_string(),
    });
    let response = service.get_ref(request).await.unwrap().into_inner();

    assert!(!response.found);
}

#[tokio::test]
async fn test_update_and_get_ref() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let oid = create_proto_oid(b"commit");
    let update_request = Request::new(UpdateRefRequest {
        repo_id: "test-repo".to_string(),
        ref_name: "refs/heads/main".to_string(),
        old_target: None,
        new_target: Some(oid.clone()),
        force: false,
    });
    let update_response = service.update_ref(update_request).await.unwrap().into_inner();
    assert!(update_response.success);

    let get_request = Request::new(GetRefRequest {
        repo_id: "test-repo".to_string(),
        ref_name: "refs/heads/main".to_string(),
    });
    let get_response = service.get_ref(get_request).await.unwrap().into_inner();

    assert!(get_response.found);
    assert_eq!(get_response.target.unwrap().bytes, oid.bytes);
}

#[tokio::test]
async fn test_list_refs() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let oid = create_proto_oid(b"commit");

    for name in &["refs/heads/main", "refs/heads/feature", "refs/tags/v1.0"] {
        let request = Request::new(UpdateRefRequest {
            repo_id: "test-repo".to_string(),
            ref_name: name.to_string(),
            old_target: None,
            new_target: Some(oid.clone()),
            force: false,
        });
        service.update_ref(request).await.unwrap();
    }

    let request = Request::new(ListRefsRequest {
        repo_id: "test-repo".to_string(),
        prefix: "refs/heads/".to_string(),
    });
    let response = service.list_refs(request).await.unwrap().into_inner();

    assert_eq!(response.refs.len(), 2);
}

#[tokio::test]
async fn test_put_and_get_commit() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let tree = create_proto_oid(b"tree");
    let commit = create_proto_commit(&tree, vec![], "Test commit");

    let put_request = Request::new(PutCommitRequest {
        repo_id: "test-repo".to_string(),
        commit: Some(commit.clone()),
    });
    let put_response = service.put_commit(put_request).await.unwrap().into_inner();
    assert!(put_response.success);

    let get_request = Request::new(GetCommitRequest {
        repo_id: "test-repo".to_string(),
        oid: commit.oid.clone(),
    });
    let get_response = service.get_commit(get_request).await.unwrap().into_inner();

    assert!(get_response.found);
    let fetched = get_response.commit.unwrap();
    assert_eq!(fetched.oid, commit.oid);
    assert_eq!(fetched.message, commit.message);
}

#[tokio::test]
async fn test_get_commit_not_found() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let oid = create_proto_oid(b"nonexistent");
    let request = Request::new(GetCommitRequest {
        repo_id: "test-repo".to_string(),
        oid: Some(oid),
    });
    let response = service.get_commit(request).await.unwrap().into_inner();

    assert!(!response.found);
}

#[tokio::test]
async fn test_put_and_get_tree() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let file_oid = create_proto_oid(b"file");
    let tree = create_proto_tree(vec![("file.txt", &file_oid)]);

    let put_request = Request::new(PutTreeRequest {
        repo_id: "test-repo".to_string(),
        tree: Some(tree.clone()),
    });
    let put_response = service.put_tree(put_request).await.unwrap().into_inner();
    assert!(put_response.success);

    let get_request = Request::new(GetTreeRequest {
        repo_id: "test-repo".to_string(),
        oid: tree.oid.clone(),
    });
    let get_response = service.get_tree(get_request).await.unwrap().into_inner();

    assert!(get_response.found);
    let fetched = get_response.tree.unwrap();
    assert_eq!(fetched.oid, tree.oid);
    assert_eq!(fetched.entries.len(), 1);
}

#[tokio::test]
async fn test_walk_commits() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let tree = create_proto_oid(b"tree");
    let commit1 = create_proto_commit(&tree, vec![], "Commit 1");
    let commit2 = create_proto_commit(&tree, vec![commit1.oid.clone().unwrap()], "Commit 2");
    let commit3 = create_proto_commit(&tree, vec![commit2.oid.clone().unwrap()], "Commit 3");

    for commit in &[&commit1, &commit2, &commit3] {
        let request = Request::new(PutCommitRequest {
            repo_id: "test-repo".to_string(),
            commit: Some((*commit).clone()),
        });
        service.put_commit(request).await.unwrap();
    }

    let request = Request::new(WalkCommitsRequest {
        repo_id: "test-repo".to_string(),
        from: vec![commit3.oid.clone().unwrap()],
        until: vec![],
        limit: 0,
    });
    let mut stream = service.walk_commits(request).await.unwrap().into_inner();

    let mut walked = Vec::new();
    while let Some(result) = stream.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 3);
}

#[tokio::test]
async fn test_find_merge_base() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let tree = create_proto_oid(b"tree");
    let base = create_proto_commit(&tree, vec![], "Base");
    let branch1 = create_proto_commit(&tree, vec![base.oid.clone().unwrap()], "Branch 1");
    let branch2 = create_proto_commit(&tree, vec![base.oid.clone().unwrap()], "Branch 2");

    for commit in &[&base, &branch1, &branch2] {
        let request = Request::new(PutCommitRequest {
            repo_id: "test-repo".to_string(),
            commit: Some((*commit).clone()),
        });
        service.put_commit(request).await.unwrap();
    }

    let request = Request::new(FindMergeBaseRequest {
        repo_id: "test-repo".to_string(),
        commits: vec![branch1.oid.clone().unwrap(), branch2.oid.clone().unwrap()],
    });
    let response = service.find_merge_base(request).await.unwrap().into_inner();

    assert!(response.found);
    assert_eq!(response.merge_base.unwrap().bytes, base.oid.unwrap().bytes);
}

#[tokio::test]
async fn test_invalid_repo_id() {
    let (service, _temp) = create_test_service();

    let request = Request::new(CreateRepoRequest {
        repo_id: "".to_string(),
    });

    let result = service.create_repo(request).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_invalid_ref_name() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let oid = create_proto_oid(b"commit");
    let request = Request::new(UpdateRefRequest {
        repo_id: "test-repo".to_string(),
        ref_name: "invalid".to_string(),
        old_target: None,
        new_target: Some(oid),
        force: false,
    });

    let result = service.update_ref(request).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cas_failure() {
    let (service, _temp) = create_test_service();

    let create_request = Request::new(CreateRepoRequest {
        repo_id: "test-repo".to_string(),
    });
    service.create_repo(create_request).await.unwrap();

    let oid1 = create_proto_oid(b"commit1");
    let oid2 = create_proto_oid(b"commit2");
    let wrong_oid = create_proto_oid(b"wrong");

    let update1 = Request::new(UpdateRefRequest {
        repo_id: "test-repo".to_string(),
        ref_name: "refs/heads/main".to_string(),
        old_target: None,
        new_target: Some(oid1),
        force: false,
    });
    service.update_ref(update1).await.unwrap();

    let update2 = Request::new(UpdateRefRequest {
        repo_id: "test-repo".to_string(),
        ref_name: "refs/heads/main".to_string(),
        old_target: Some(wrong_oid),
        new_target: Some(oid2),
        force: false,
    });
    let response = service.update_ref(update2).await.unwrap().into_inner();

    assert!(!response.success);
    assert!(response.error.contains("compare-and-swap"));
}
