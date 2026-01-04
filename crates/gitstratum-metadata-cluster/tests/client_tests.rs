use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;

use gitstratum_core::{Commit, Oid, RefName, RepoId, Signature, Tree, TreeEntry, TreeEntryMode};
use gitstratum_metadata_cluster::{MetadataClient, MetadataServiceImpl, MetadataStore};
use gitstratum_proto::metadata_service_server::MetadataServiceServer;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::transport::Server;

fn get_available_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn create_test_server() -> (SocketAddr, TempDir, oneshot::Sender<()>) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(MetadataStore::open(temp_dir.path()).unwrap());
    let service = MetadataServiceImpl::new(store);

    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(MetadataServiceServer::new(service))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    (addr, temp_dir, shutdown_tx)
}

fn create_test_signature() -> Signature {
    Signature::new("Test Author", "test@example.com", 1704067200, "+0000")
}

fn create_test_commit(tree_oid: Oid, parents: Vec<Oid>, message: &str) -> Commit {
    let sig = create_test_signature();
    Commit::new(tree_oid, parents, sig.clone(), sig, message)
}

fn create_test_tree(entries: Vec<(&str, Oid)>) -> Tree {
    let tree_entries: Vec<TreeEntry> = entries
        .iter()
        .map(|(name, oid)| TreeEntry::new(TreeEntryMode::File, *name, *oid))
        .collect();
    Tree::new(tree_entries)
}

#[tokio::test]
async fn test_client_create_repo() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    let result = client.create_repo(&repo_id).await;
    assert!(result.is_ok());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_create_repo_duplicate() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let result = client.create_repo(&repo_id).await;
    assert!(result.is_err());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_delete_repo() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let result = client.delete_repo(&repo_id).await;
    assert!(result.is_ok());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_delete_repo_not_found() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("nonexistent-repo").unwrap();
    let result = client.delete_repo(&repo_id).await;
    assert!(result.is_err());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_repos() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    for i in 0..3 {
        let repo_id = RepoId::new(format!("repo-{}", i)).unwrap();
        client.create_repo(&repo_id).await.unwrap();
    }

    let (repos, next_cursor) = client.list_repos("", 10, "").await.unwrap();
    assert_eq!(repos.len(), 3);
    assert!(next_cursor.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_repos_with_prefix() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    client
        .create_repo(&RepoId::new("alpha-one").unwrap())
        .await
        .unwrap();
    client
        .create_repo(&RepoId::new("alpha-two").unwrap())
        .await
        .unwrap();
    client
        .create_repo(&RepoId::new("beta-one").unwrap())
        .await
        .unwrap();

    let (repos, _) = client.list_repos("alpha", 10, "").await.unwrap();
    assert_eq!(repos.len(), 2);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_repos_empty() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let (repos, next_cursor) = client.list_repos("", 10, "").await.unwrap();
    assert!(repos.is_empty());
    assert!(next_cursor.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_repos_with_cursor() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    for i in 0..5 {
        let repo_id = RepoId::new(format!("repo-{:02}", i)).unwrap();
        client.create_repo(&repo_id).await.unwrap();
    }

    let (repos, next_cursor) = client.list_repos("", 3, "").await.unwrap();
    assert_eq!(repos.len(), 3);
    assert!(next_cursor.is_some());

    let (more_repos, _) = client
        .list_repos("", 3, &next_cursor.unwrap())
        .await
        .unwrap();
    assert_eq!(more_repos.len(), 2);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_get_ref_not_found() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    let result = client.get_ref(&repo_id, &ref_name).await.unwrap();
    assert!(result.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_update_and_get_ref() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    let oid = Oid::hash(b"commit");

    client
        .update_ref(&repo_id, &ref_name, None, &oid, false)
        .await
        .unwrap();

    let result = client.get_ref(&repo_id, &ref_name).await.unwrap();
    assert_eq!(result, Some(oid));

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_update_ref_force() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");

    client
        .update_ref(&repo_id, &ref_name, None, &oid1, false)
        .await
        .unwrap();

    client
        .update_ref(&repo_id, &ref_name, None, &oid2, true)
        .await
        .unwrap();

    let result = client.get_ref(&repo_id, &ref_name).await.unwrap();
    assert_eq!(result, Some(oid2));

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_update_ref_cas_failure() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");
    let wrong_oid = Oid::hash(b"wrong");

    client
        .update_ref(&repo_id, &ref_name, None, &oid1, false)
        .await
        .unwrap();

    let result = client
        .update_ref(&repo_id, &ref_name, Some(&wrong_oid), &oid2, false)
        .await;
    assert!(result.is_err());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_update_ref_with_old_target() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");

    client
        .update_ref(&repo_id, &ref_name, None, &oid1, false)
        .await
        .unwrap();

    client
        .update_ref(&repo_id, &ref_name, Some(&oid1), &oid2, false)
        .await
        .unwrap();

    let result = client.get_ref(&repo_id, &ref_name).await.unwrap();
    assert_eq!(result, Some(oid2));

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_refs() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let oid = Oid::hash(b"commit");

    let ref_names = ["refs/heads/main", "refs/heads/feature", "refs/tags/v1.0"];
    for name in ref_names.iter() {
        let ref_name = RefName::new(*name).unwrap();
        client
            .update_ref(&repo_id, &ref_name, None, &oid, false)
            .await
            .unwrap();
    }

    let refs = client.list_refs(&repo_id, "refs/heads/").await.unwrap();
    assert_eq!(refs.len(), 2);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_refs_empty() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let refs = client.list_refs(&repo_id, "").await.unwrap();
    assert!(refs.is_empty());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_list_refs_all() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let oid = Oid::hash(b"commit");

    let ref_names = ["refs/heads/main", "refs/heads/feature", "refs/tags/v1.0"];
    for name in ref_names.iter() {
        let ref_name = RefName::new(*name).unwrap();
        client
            .update_ref(&repo_id, &ref_name, None, &oid, false)
            .await
            .unwrap();
    }

    let refs = client.list_refs(&repo_id, "").await.unwrap();
    assert_eq!(refs.len(), 3);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_put_and_get_commit() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit = create_test_commit(tree_oid, vec![], "Test commit");

    client.put_commit(&repo_id, &commit).await.unwrap();

    let result = client.get_commit(&repo_id, &commit.oid).await.unwrap();
    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.oid, commit.oid);
    assert_eq!(fetched.message, commit.message);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_get_commit_not_found() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let oid = Oid::hash(b"nonexistent");
    let result = client.get_commit(&repo_id, &oid).await.unwrap();
    assert!(result.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_put_commit_overwrite() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit = create_test_commit(tree_oid, vec![], "Test commit");

    client.put_commit(&repo_id, &commit).await.unwrap();
    let result = client.put_commit(&repo_id, &commit).await;
    assert!(result.is_ok());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_get_commits() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit1 = create_test_commit(tree_oid, vec![], "Commit 1");
    let commit2 = create_test_commit(tree_oid, vec![commit1.oid], "Commit 2");
    let commit3 = create_test_commit(tree_oid, vec![commit2.oid], "Commit 3");

    client.put_commit(&repo_id, &commit1).await.unwrap();
    client.put_commit(&repo_id, &commit2).await.unwrap();
    client.put_commit(&repo_id, &commit3).await.unwrap();

    let oids = vec![commit1.oid, commit2.oid, commit3.oid];
    let commits = client.get_commits(&repo_id, &oids).await.unwrap();
    assert_eq!(commits.len(), 3);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_get_commits_empty() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let commits = client.get_commits(&repo_id, &[]).await.unwrap();
    assert!(commits.is_empty());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_put_and_get_tree() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let file_oid = Oid::hash(b"file content");
    let tree = create_test_tree(vec![("file.txt", file_oid)]);

    client.put_tree(&repo_id, &tree).await.unwrap();

    let result = client.get_tree(&repo_id, &tree.oid).await.unwrap();
    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.oid, tree.oid);
    assert_eq!(fetched.entries.len(), 1);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_get_tree_not_found() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let oid = Oid::hash(b"nonexistent");
    let result = client.get_tree(&repo_id, &oid).await.unwrap();
    assert!(result.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_put_tree_overwrite() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let file_oid = Oid::hash(b"file content");
    let tree = create_test_tree(vec![("file.txt", file_oid)]);

    client.put_tree(&repo_id, &tree).await.unwrap();
    let result = client.put_tree(&repo_id, &tree).await;
    assert!(result.is_ok());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_walk_commits() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit1 = create_test_commit(tree_oid, vec![], "Commit 1");
    let commit2 = create_test_commit(tree_oid, vec![commit1.oid], "Commit 2");
    let commit3 = create_test_commit(tree_oid, vec![commit2.oid], "Commit 3");

    client.put_commit(&repo_id, &commit1).await.unwrap();
    client.put_commit(&repo_id, &commit2).await.unwrap();
    client.put_commit(&repo_id, &commit3).await.unwrap();

    let commits = client
        .walk_commits(&repo_id, &[commit3.oid], &[], None)
        .await
        .unwrap();
    assert_eq!(commits.len(), 3);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_walk_commits_with_limit() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit1 = create_test_commit(tree_oid, vec![], "Commit 1");
    let commit2 = create_test_commit(tree_oid, vec![commit1.oid], "Commit 2");
    let commit3 = create_test_commit(tree_oid, vec![commit2.oid], "Commit 3");

    client.put_commit(&repo_id, &commit1).await.unwrap();
    client.put_commit(&repo_id, &commit2).await.unwrap();
    client.put_commit(&repo_id, &commit3).await.unwrap();

    let commits = client
        .walk_commits(&repo_id, &[commit3.oid], &[], Some(2))
        .await
        .unwrap();
    assert_eq!(commits.len(), 2);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_walk_commits_with_until() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit1 = create_test_commit(tree_oid, vec![], "Commit 1");
    let commit2 = create_test_commit(tree_oid, vec![commit1.oid], "Commit 2");
    let commit3 = create_test_commit(tree_oid, vec![commit2.oid], "Commit 3");

    client.put_commit(&repo_id, &commit1).await.unwrap();
    client.put_commit(&repo_id, &commit2).await.unwrap();
    client.put_commit(&repo_id, &commit3).await.unwrap();

    let commits = client
        .walk_commits(&repo_id, &[commit3.oid], &[commit1.oid], None)
        .await
        .unwrap();
    assert_eq!(commits.len(), 2);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_walk_commits_empty() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let commits = client.walk_commits(&repo_id, &[], &[], None).await.unwrap();
    assert!(commits.is_empty());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_find_merge_base() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let base = create_test_commit(tree_oid, vec![], "Base");
    let branch1 = create_test_commit(tree_oid, vec![base.oid], "Branch 1");
    let branch2 = create_test_commit(tree_oid, vec![base.oid], "Branch 2");

    client.put_commit(&repo_id, &base).await.unwrap();
    client.put_commit(&repo_id, &branch1).await.unwrap();
    client.put_commit(&repo_id, &branch2).await.unwrap();

    let result = client
        .find_merge_base(&repo_id, &[branch1.oid, branch2.oid])
        .await
        .unwrap();
    assert_eq!(result, Some(base.oid));

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_find_merge_base_not_found() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit1 = create_test_commit(tree_oid, vec![], "Commit 1");
    let commit2 = create_test_commit(tree_oid, vec![], "Commit 2");

    client.put_commit(&repo_id, &commit1).await.unwrap();
    client.put_commit(&repo_id, &commit2).await.unwrap();

    let result = client
        .find_merge_base(&repo_id, &[commit1.oid, commit2.oid])
        .await
        .unwrap();
    assert!(result.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_find_merge_base_single_commit() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let commit = create_test_commit(tree_oid, vec![], "Single commit");

    client.put_commit(&repo_id, &commit).await.unwrap();

    let result = client
        .find_merge_base(&repo_id, &[commit.oid])
        .await
        .unwrap();
    assert!(result.is_some());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_find_merge_base_empty() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let result = client.find_merge_base(&repo_id, &[]).await.unwrap();
    assert!(result.is_none());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_put_tree_with_various_modes() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let entries = vec![
        TreeEntry::file("file.txt", Oid::hash(b"file")),
        TreeEntry::new(TreeEntryMode::Executable, "script.sh", Oid::hash(b"script")),
        TreeEntry::directory("subdir", Oid::hash(b"dir")),
        TreeEntry::new(TreeEntryMode::Symlink, "link", Oid::hash(b"target")),
        TreeEntry::new(TreeEntryMode::Submodule, "submodule", Oid::hash(b"commit")),
    ];
    let tree = Tree::new(entries);

    client.put_tree(&repo_id, &tree).await.unwrap();

    let result = client.get_tree(&repo_id, &tree.oid).await.unwrap();
    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.entries.len(), 5);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_commit_with_multiple_parents() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let parent1 = create_test_commit(tree_oid, vec![], "Parent 1");
    let parent2 = create_test_commit(tree_oid, vec![], "Parent 2");
    let merge = create_test_commit(tree_oid, vec![parent1.oid, parent2.oid], "Merge commit");

    client.put_commit(&repo_id, &parent1).await.unwrap();
    client.put_commit(&repo_id, &parent2).await.unwrap();
    client.put_commit(&repo_id, &merge).await.unwrap();

    let result = client.get_commit(&repo_id, &merge.oid).await.unwrap();
    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.parents.len(), 2);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_connect_with_channel() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;

    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr)).unwrap();
    let channel = endpoint.connect().await.unwrap();

    let mut client = MetadataClient::connect_with_channel(channel).await;

    let repo_id = RepoId::new("test-repo").unwrap();
    let result = client.create_repo(&repo_id).await;
    assert!(result.is_ok());

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_clone() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let _cloned = client.clone();

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_multiple_operations() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let tree_oid = Oid::hash(b"tree");
    let file_oid = Oid::hash(b"file");
    let tree = create_test_tree(vec![("file.txt", file_oid)]);
    client.put_tree(&repo_id, &tree).await.unwrap();

    let commit = create_test_commit(tree_oid, vec![], "Initial commit");
    client.put_commit(&repo_id, &commit).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    client
        .update_ref(&repo_id, &ref_name, None, &commit.oid, false)
        .await
        .unwrap();

    let result = client.get_ref(&repo_id, &ref_name).await.unwrap();
    assert_eq!(result, Some(commit.oid));

    let refs = client.list_refs(&repo_id, "").await.unwrap();
    assert_eq!(refs.len(), 1);

    let (repos, _) = client.list_repos("", 10, "").await.unwrap();
    assert_eq!(repos.len(), 1);

    drop(shutdown_tx);
}

#[tokio::test]
async fn test_client_get_ref_found_with_target() {
    let (addr, _temp, shutdown_tx) = create_test_server().await;
    let mut client = MetadataClient::connect(&format!("http://{}", addr))
        .await
        .unwrap();

    let repo_id = RepoId::new("test-repo").unwrap();
    client.create_repo(&repo_id).await.unwrap();

    let ref_name = RefName::new("refs/heads/main").unwrap();
    let oid = Oid::hash(b"commit-oid");

    client
        .update_ref(&repo_id, &ref_name, None, &oid, false)
        .await
        .unwrap();

    let result = client.get_ref(&repo_id, &ref_name).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), oid);

    drop(shutdown_tx);
}
