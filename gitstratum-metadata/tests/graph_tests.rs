use std::sync::Arc;

use futures::StreamExt;
use gitstratum_core::{Commit, Oid, RepoId, Signature};
use gitstratum_metadata::{
    collect_reachable_commits, find_merge_base, is_ancestor, walk_commits, MetadataStore,
};
use tempfile::TempDir;

fn create_test_store() -> (Arc<MetadataStore>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(MetadataStore::open(temp_dir.path()).unwrap());
    (store, temp_dir)
}

fn create_signature(timestamp: i64) -> Signature {
    Signature::new("Test", "test@example.com", timestamp, "+0000")
}

fn create_commit_with_timestamp(
    tree: Oid,
    parents: Vec<Oid>,
    message: &str,
    timestamp: i64,
) -> Commit {
    let sig = create_signature(timestamp);
    Commit::new(tree, parents, sig.clone(), sig, message)
}

fn setup_linear_history(store: &MetadataStore, repo_id: &RepoId, count: usize) -> Vec<Commit> {
    let tree = Oid::hash(b"tree");
    let mut commits: Vec<Commit> = Vec::new();

    for i in 0..count {
        let parents = if i == 0 {
            vec![]
        } else {
            vec![commits[i - 1].oid]
        };
        let commit = create_commit_with_timestamp(
            tree,
            parents,
            &format!("Commit {}", i),
            1000 + (i as i64),
        );
        store.put_commit(repo_id, &commit).unwrap();
        commits.push(commit);
    }

    commits
}

#[tokio::test]
async fn test_walk_commits_linear() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![commits[4].oid],
        vec![],
        None,
    );

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 5);
    assert_eq!(walked[0].oid, commits[4].oid);
    assert_eq!(walked[4].oid, commits[0].oid);
}

#[tokio::test]
async fn test_walk_commits_with_limit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![commits[9].oid],
        vec![],
        Some(3),
    );

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 3);
}

#[tokio::test]
async fn test_walk_commits_with_until() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![commits[9].oid],
        vec![commits[5].oid],
        None,
    );

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 4);
    for commit in &walked {
        assert_ne!(commit.oid, commits[5].oid);
    }
}

#[tokio::test]
async fn test_walk_commits_empty() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let mut walker = walk_commits(store.clone(), repo_id, vec![], vec![], None);

    let result = walker.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_walk_commits_nonexistent() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let nonexistent = Oid::hash(b"nonexistent");
    let mut walker = walk_commits(store.clone(), repo_id, vec![nonexistent], vec![], None);

    let result = walker.next().await;
    assert!(result.is_none());
}

#[test]
fn test_find_merge_base_empty() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let result = find_merge_base(&store, &repo_id, &[]).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_find_merge_base_single() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 3);

    let result = find_merge_base(&store, &repo_id, &[commits[2].oid]).unwrap();
    assert_eq!(result, Some(commits[2].oid));
}

#[test]
fn test_find_merge_base_linear() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let result = find_merge_base(&store, &repo_id, &[commits[4].oid, commits[2].oid]).unwrap();
    assert_eq!(result, Some(commits[2].oid));
}

#[test]
fn test_find_merge_base_fork() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let branch1 = create_commit_with_timestamp(tree, vec![base.oid], "Branch 1", 2000);
    store.put_commit(&repo_id, &branch1).unwrap();

    let branch2 = create_commit_with_timestamp(tree, vec![base.oid], "Branch 2", 2001);
    store.put_commit(&repo_id, &branch2).unwrap();

    let result = find_merge_base(&store, &repo_id, &[branch1.oid, branch2.oid]).unwrap();
    assert_eq!(result, Some(base.oid));
}

#[test]
fn test_find_merge_base_complex() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1001);
    store.put_commit(&repo_id, &c2).unwrap();

    let c3 = create_commit_with_timestamp(tree, vec![c2.oid], "C3", 1002);
    store.put_commit(&repo_id, &c3).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![c2.oid], "B1", 1003);
    store.put_commit(&repo_id, &b1).unwrap();

    let b2 = create_commit_with_timestamp(tree, vec![b1.oid], "B2", 1004);
    store.put_commit(&repo_id, &b2).unwrap();

    let result = find_merge_base(&store, &repo_id, &[c3.oid, b2.oid]).unwrap();
    assert_eq!(result, Some(c2.oid));
}

#[test]
fn test_find_merge_base_three_branches() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let a = create_commit_with_timestamp(tree, vec![base.oid], "A", 2000);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![base.oid], "B", 2001);
    store.put_commit(&repo_id, &b).unwrap();

    let c = create_commit_with_timestamp(tree, vec![base.oid], "C", 2002);
    store.put_commit(&repo_id, &c).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a.oid, b.oid, c.oid]).unwrap();
    assert_eq!(result, Some(base.oid));
}

#[test]
fn test_is_ancestor_same_commit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 3);

    let result = is_ancestor(&store, &repo_id, &commits[1].oid, &commits[1].oid).unwrap();
    assert!(result);
}

#[test]
fn test_is_ancestor_true() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let result = is_ancestor(&store, &repo_id, &commits[1].oid, &commits[4].oid).unwrap();
    assert!(result);
}

#[test]
fn test_is_ancestor_false() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let result = is_ancestor(&store, &repo_id, &commits[4].oid, &commits[1].oid).unwrap();
    assert!(!result);
}

#[test]
fn test_is_ancestor_unrelated() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![], "C2", 1001);
    store.put_commit(&repo_id, &c2).unwrap();

    let result = is_ancestor(&store, &repo_id, &c1.oid, &c2.oid).unwrap();
    assert!(!result);
}

#[test]
fn test_collect_reachable_commits() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let reachable = collect_reachable_commits(&store, &repo_id, &[commits[9].oid], 100).unwrap();

    assert_eq!(reachable.len(), 10);
    for commit in &commits {
        assert!(reachable.contains(&commit.oid));
    }
}

#[test]
fn test_collect_reachable_commits_with_depth() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let reachable = collect_reachable_commits(&store, &repo_id, &[commits[9].oid], 3).unwrap();

    assert!(reachable.len() <= 4);
    assert!(reachable.contains(&commits[9].oid));
}

#[test]
fn test_collect_reachable_from_multiple() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let a = create_commit_with_timestamp(tree, vec![base.oid], "A", 2000);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![base.oid], "B", 2001);
    store.put_commit(&repo_id, &b).unwrap();

    let reachable = collect_reachable_commits(&store, &repo_id, &[a.oid, b.oid], 100).unwrap();

    assert_eq!(reachable.len(), 3);
    assert!(reachable.contains(&base.oid));
    assert!(reachable.contains(&a.oid));
    assert!(reachable.contains(&b.oid));
}

#[tokio::test]
async fn test_walk_commits_branched() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![base.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let a2 = create_commit_with_timestamp(tree, vec![a1.oid], "A2", 3000);
    store.put_commit(&repo_id, &a2).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![base.oid], "B1", 2500);
    store.put_commit(&repo_id, &b1).unwrap();

    let merge = create_commit_with_timestamp(tree, vec![a2.oid, b1.oid], "Merge", 4000);
    store.put_commit(&repo_id, &merge).unwrap();

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![merge.oid],
        vec![],
        None,
    );

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 5);
    assert_eq!(walked[0].oid, merge.oid);
}

#[tokio::test]
async fn test_walk_commits_async_basic() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let walked = gitstratum_metadata::walk_commits_async(
        store.clone(),
        repo_id,
        vec![commits[4].oid],
        vec![],
        None,
    )
    .await
    .unwrap();

    assert_eq!(walked.len(), 5);
    assert_eq!(walked[0].oid, commits[4].oid);
    assert_eq!(walked[4].oid, commits[0].oid);
}

#[tokio::test]
async fn test_walk_commits_async_with_limit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let walked = gitstratum_metadata::walk_commits_async(
        store.clone(),
        repo_id,
        vec![commits[9].oid],
        vec![],
        Some(3),
    )
    .await
    .unwrap();

    assert_eq!(walked.len(), 3);
}

#[tokio::test]
async fn test_walk_commits_async_with_until() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let walked = gitstratum_metadata::walk_commits_async(
        store.clone(),
        repo_id,
        vec![commits[9].oid],
        vec![commits[5].oid],
        None,
    )
    .await
    .unwrap();

    assert_eq!(walked.len(), 4);
    for commit in &walked {
        assert_ne!(commit.oid, commits[5].oid);
    }
}

#[tokio::test]
async fn test_walk_commits_async_empty() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let walked = gitstratum_metadata::walk_commits_async(
        store.clone(),
        repo_id,
        vec![],
        vec![],
        None,
    )
    .await
    .unwrap();

    assert!(walked.is_empty());
}

#[test]
fn test_find_merge_base_no_common_ancestor() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![], "C2", 2000);
    store.put_commit(&repo_id, &c2).unwrap();

    let result = find_merge_base(&store, &repo_id, &[c1.oid, c2.oid]).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_find_merge_base_with_nonexistent_commit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");
    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let nonexistent = Oid::hash(b"nonexistent");
    let result = find_merge_base(&store, &repo_id, &[c1.oid, nonexistent]).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_find_merge_base_identical_commits() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");
    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let result = find_merge_base(&store, &repo_id, &[c1.oid, c1.oid]).unwrap();
    assert_eq!(result, Some(c1.oid));
}

#[test]
fn test_find_merge_base_deep_history() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 100);

    let tree = Oid::hash(b"tree");

    let branch1 = create_commit_with_timestamp(tree, vec![commits[50].oid], "Branch1", 2000);
    store.put_commit(&repo_id, &branch1).unwrap();

    let branch2 = create_commit_with_timestamp(tree, vec![commits[50].oid], "Branch2", 2001);
    store.put_commit(&repo_id, &branch2).unwrap();

    let result = find_merge_base(&store, &repo_id, &[branch1.oid, branch2.oid]).unwrap();
    assert_eq!(result, Some(commits[50].oid));
}

#[test]
fn test_is_ancestor_with_multiple_parents() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let a = create_commit_with_timestamp(tree, vec![base.oid], "A", 2000);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![base.oid], "B", 2001);
    store.put_commit(&repo_id, &b).unwrap();

    let merge = create_commit_with_timestamp(tree, vec![a.oid, b.oid], "Merge", 3000);
    store.put_commit(&repo_id, &merge).unwrap();

    assert!(is_ancestor(&store, &repo_id, &base.oid, &merge.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &a.oid, &merge.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &b.oid, &merge.oid).unwrap());
    assert!(!is_ancestor(&store, &repo_id, &merge.oid, &base.oid).unwrap());
}

#[test]
fn test_is_ancestor_nonexistent_commits() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let nonexistent1 = Oid::hash(b"nonexistent1");
    let nonexistent2 = Oid::hash(b"nonexistent2");

    let result = is_ancestor(&store, &repo_id, &nonexistent1, &nonexistent2).unwrap();
    assert!(!result);
}

#[test]
fn test_collect_reachable_empty_from() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let reachable = collect_reachable_commits(&store, &repo_id, &[], 100).unwrap();
    assert!(reachable.is_empty());
}

#[test]
fn test_collect_reachable_zero_depth() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let reachable = collect_reachable_commits(&store, &repo_id, &[commits[4].oid], 0).unwrap();
    assert_eq!(reachable.len(), 1);
    assert!(reachable.contains(&commits[4].oid));
}

#[test]
fn test_collect_reachable_nonexistent_commit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let nonexistent = Oid::hash(b"nonexistent");
    let reachable = collect_reachable_commits(&store, &repo_id, &[nonexistent], 100).unwrap();
    assert_eq!(reachable.len(), 1);
    assert!(reachable.contains(&nonexistent));
}

#[test]
fn test_collect_reachable_with_diamond() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let left = create_commit_with_timestamp(tree, vec![base.oid], "Left", 2000);
    store.put_commit(&repo_id, &left).unwrap();

    let right = create_commit_with_timestamp(tree, vec![base.oid], "Right", 2001);
    store.put_commit(&repo_id, &right).unwrap();

    let merge = create_commit_with_timestamp(tree, vec![left.oid, right.oid], "Merge", 3000);
    store.put_commit(&repo_id, &merge).unwrap();

    let reachable = collect_reachable_commits(&store, &repo_id, &[merge.oid], 100).unwrap();
    assert_eq!(reachable.len(), 4);
    assert!(reachable.contains(&base.oid));
    assert!(reachable.contains(&left.oid));
    assert!(reachable.contains(&right.oid));
    assert!(reachable.contains(&merge.oid));
}

#[tokio::test]
async fn test_walk_commits_from_in_until() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![commits[2].oid],
        vec![commits[2].oid],
        None,
    );

    let result = walker.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_walk_commits_duplicate_from() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 3);

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![commits[2].oid, commits[2].oid, commits[2].oid],
        vec![],
        None,
    );

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 3);
}

#[test]
fn test_find_merge_base_chooses_closest_among_multiple() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1001);
    store.put_commit(&repo_id, &c2).unwrap();

    let c3 = create_commit_with_timestamp(tree, vec![c2.oid], "C3", 1002);
    store.put_commit(&repo_id, &c3).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![c3.oid], "A1", 1003);
    store.put_commit(&repo_id, &a1).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![c3.oid], "B1", 1004);
    store.put_commit(&repo_id, &b1).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a1.oid, b1.oid]).unwrap();
    assert_eq!(result, Some(c3.oid));
}

#[test]
fn test_find_merge_base_with_missing_parent_commit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");
    let missing_parent = Oid::hash(b"missing_parent");

    let c1 = create_commit_with_timestamp(tree, vec![missing_parent], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![missing_parent], "C2", 1001);
    store.put_commit(&repo_id, &c2).unwrap();

    let result = find_merge_base(&store, &repo_id, &[c1.oid, c2.oid]).unwrap();
    assert!(result.is_none() || result == Some(missing_parent));
}

#[test]
fn test_is_ancestor_visiting_same_commit_twice() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let left = create_commit_with_timestamp(tree, vec![base.oid], "Left", 2000);
    store.put_commit(&repo_id, &left).unwrap();

    let right = create_commit_with_timestamp(tree, vec![base.oid], "Right", 2001);
    store.put_commit(&repo_id, &right).unwrap();

    let merge = create_commit_with_timestamp(tree, vec![left.oid, right.oid], "Merge", 3000);
    store.put_commit(&repo_id, &merge).unwrap();

    let result = is_ancestor(&store, &repo_id, &base.oid, &merge.oid).unwrap();
    assert!(result);
}

#[test]
fn test_find_merge_base_asymmetric_depths() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(&repo_id, &base).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![base.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let a2 = create_commit_with_timestamp(tree, vec![a1.oid], "A2", 2001);
    store.put_commit(&repo_id, &a2).unwrap();

    let a3 = create_commit_with_timestamp(tree, vec![a2.oid], "A3", 2002);
    store.put_commit(&repo_id, &a3).unwrap();

    let a4 = create_commit_with_timestamp(tree, vec![a3.oid], "A4", 2003);
    store.put_commit(&repo_id, &a4).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![base.oid], "B1", 2500);
    store.put_commit(&repo_id, &b1).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a4.oid, b1.oid]).unwrap();
    assert_eq!(result, Some(base.oid));
}

#[test]
fn test_find_merge_base_multiple_common_ancestors() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1001);
    store.put_commit(&repo_id, &c2).unwrap();

    let c3 = create_commit_with_timestamp(tree, vec![c2.oid], "C3", 1002);
    store.put_commit(&repo_id, &c3).unwrap();

    let branch_a = create_commit_with_timestamp(tree, vec![c3.oid], "BranchA", 2000);
    store.put_commit(&repo_id, &branch_a).unwrap();

    let branch_b = create_commit_with_timestamp(tree, vec![c3.oid], "BranchB", 2001);
    store.put_commit(&repo_id, &branch_b).unwrap();

    let result = find_merge_base(&store, &repo_id, &[branch_a.oid, branch_b.oid]).unwrap();
    assert_eq!(result, Some(c3.oid));
}

#[test]
fn test_find_merge_base_fallback_path_with_depth_comparison() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let c1 = create_commit_with_timestamp(tree, vec![], "C1", 1000);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1001);
    store.put_commit(&repo_id, &c2).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![c2.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let a2 = create_commit_with_timestamp(tree, vec![a1.oid], "A2", 2001);
    store.put_commit(&repo_id, &a2).unwrap();

    let a3 = create_commit_with_timestamp(tree, vec![a2.oid], "A3", 2002);
    store.put_commit(&repo_id, &a3).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![c2.oid], "B1", 2500);
    store.put_commit(&repo_id, &b1).unwrap();

    let b2 = create_commit_with_timestamp(tree, vec![b1.oid], "B2", 2501);
    store.put_commit(&repo_id, &b2).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a3.oid, b2.oid]).unwrap();
    assert_eq!(result, Some(c2.oid));
}

#[test]
fn test_is_ancestor_complex_diamond() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![root.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![root.oid], "B1", 2001);
    store.put_commit(&repo_id, &b1).unwrap();

    let merge1 = create_commit_with_timestamp(tree, vec![a1.oid, b1.oid], "Merge1", 3000);
    store.put_commit(&repo_id, &merge1).unwrap();

    let c1 = create_commit_with_timestamp(tree, vec![merge1.oid], "C1", 3500);
    store.put_commit(&repo_id, &c1).unwrap();

    let d1 = create_commit_with_timestamp(tree, vec![merge1.oid], "D1", 3501);
    store.put_commit(&repo_id, &d1).unwrap();

    let merge2 = create_commit_with_timestamp(tree, vec![c1.oid, d1.oid], "Merge2", 4000);
    store.put_commit(&repo_id, &merge2).unwrap();

    assert!(is_ancestor(&store, &repo_id, &root.oid, &merge2.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &a1.oid, &merge2.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &b1.oid, &merge2.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &merge1.oid, &merge2.oid).unwrap());
}

#[test]
fn test_collect_reachable_many_paths_to_same_commit() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let a = create_commit_with_timestamp(tree, vec![root.oid], "A", 2000);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![root.oid], "B", 2001);
    store.put_commit(&repo_id, &b).unwrap();

    let c = create_commit_with_timestamp(tree, vec![root.oid], "C", 2002);
    store.put_commit(&repo_id, &c).unwrap();

    let merge = create_commit_with_timestamp(tree, vec![a.oid, b.oid, c.oid], "Merge", 3000);
    store.put_commit(&repo_id, &merge).unwrap();

    let reachable = collect_reachable_commits(&store, &repo_id, &[merge.oid], 100).unwrap();
    assert_eq!(reachable.len(), 5);
}

#[tokio::test]
async fn test_walk_commits_large_history() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 50);

    let mut walker = walk_commits(
        store.clone(),
        repo_id,
        vec![commits[49].oid],
        vec![],
        None,
    );

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 50);
}

#[test]
fn test_find_merge_base_selects_best_depth() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let c1 = create_commit_with_timestamp(tree, vec![root.oid], "C1", 1001);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1002);
    store.put_commit(&repo_id, &c2).unwrap();

    let c3 = create_commit_with_timestamp(tree, vec![c2.oid], "C3", 1003);
    store.put_commit(&repo_id, &c3).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![c3.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let a2 = create_commit_with_timestamp(tree, vec![a1.oid], "A2", 2001);
    store.put_commit(&repo_id, &a2).unwrap();

    let a3 = create_commit_with_timestamp(tree, vec![a2.oid], "A3", 2002);
    store.put_commit(&repo_id, &a3).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![c3.oid], "B1", 2500);
    store.put_commit(&repo_id, &b1).unwrap();

    let b2 = create_commit_with_timestamp(tree, vec![b1.oid], "B2", 2501);
    store.put_commit(&repo_id, &b2).unwrap();

    let b3 = create_commit_with_timestamp(tree, vec![b2.oid], "B3", 2502);
    store.put_commit(&repo_id, &b3).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a3.oid, b3.oid]).unwrap();
    assert_eq!(result, Some(c3.oid));
}

#[test]
fn test_find_merge_base_fallback_chooses_lowest_depth() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let c1 = create_commit_with_timestamp(tree, vec![root.oid], "C1", 1001);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1002);
    store.put_commit(&repo_id, &c2).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![c2.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let a2 = create_commit_with_timestamp(tree, vec![a1.oid], "A2", 2001);
    store.put_commit(&repo_id, &a2).unwrap();

    let a3 = create_commit_with_timestamp(tree, vec![a2.oid], "A3", 2002);
    store.put_commit(&repo_id, &a3).unwrap();

    let a4 = create_commit_with_timestamp(tree, vec![a3.oid], "A4", 2003);
    store.put_commit(&repo_id, &a4).unwrap();

    let a5 = create_commit_with_timestamp(tree, vec![a4.oid], "A5", 2004);
    store.put_commit(&repo_id, &a5).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![c2.oid], "B1", 2500);
    store.put_commit(&repo_id, &b1).unwrap();

    let b2 = create_commit_with_timestamp(tree, vec![b1.oid], "B2", 2501);
    store.put_commit(&repo_id, &b2).unwrap();

    let b3 = create_commit_with_timestamp(tree, vec![b2.oid], "B3", 2502);
    store.put_commit(&repo_id, &b3).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a5.oid, b3.oid]).unwrap();
    assert_eq!(result, Some(c2.oid));
}

#[test]
fn test_find_merge_base_multiple_common_picks_shallower() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let c1 = create_commit_with_timestamp(tree, vec![root.oid], "C1", 1001);
    store.put_commit(&repo_id, &c1).unwrap();

    let c2 = create_commit_with_timestamp(tree, vec![c1.oid], "C2", 1002);
    store.put_commit(&repo_id, &c2).unwrap();

    let c3 = create_commit_with_timestamp(tree, vec![c2.oid], "C3", 1003);
    store.put_commit(&repo_id, &c3).unwrap();

    let a1 = create_commit_with_timestamp(tree, vec![c3.oid], "A1", 2000);
    store.put_commit(&repo_id, &a1).unwrap();

    let b1 = create_commit_with_timestamp(tree, vec![c3.oid], "B1", 2001);
    store.put_commit(&repo_id, &b1).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a1.oid, b1.oid]).unwrap();
    assert_eq!(result, Some(c3.oid));
}

#[test]
fn test_find_merge_base_parent_not_in_store() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");
    let missing_parent = Oid::hash(b"missing");

    let a = create_commit_with_timestamp(tree, vec![missing_parent], "A", 2000);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![missing_parent], "B", 2001);
    store.put_commit(&repo_id, &b).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a.oid, b.oid]).unwrap();
    assert!(result.is_none() || result == Some(missing_parent));
}

#[test]
fn test_find_merge_base_exercises_fallback_path() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let mut prev_a = root.oid;
    for i in 0..20 {
        let c = create_commit_with_timestamp(tree, vec![prev_a], &format!("A{}", i), 2000 + i as i64);
        store.put_commit(&repo_id, &c).unwrap();
        prev_a = c.oid;
    }

    let mut prev_b = root.oid;
    for i in 0..10 {
        let c = create_commit_with_timestamp(tree, vec![prev_b], &format!("B{}", i), 3000 + i as i64);
        store.put_commit(&repo_id, &c).unwrap();
        prev_b = c.oid;
    }

    let result = find_merge_base(&store, &repo_id, &[prev_a, prev_b]).unwrap();
    assert_eq!(result, Some(root.oid));
}

#[test]
fn test_find_merge_base_same_timestamp_ordering() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let tree = Oid::hash(b"tree");

    let root = create_commit_with_timestamp(tree, vec![], "Root", 1000);
    store.put_commit(&repo_id, &root).unwrap();

    let a = create_commit_with_timestamp(tree, vec![root.oid], "A", 2000);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![root.oid], "B", 2000);
    store.put_commit(&repo_id, &b).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a.oid, b.oid]).unwrap();
    assert_eq!(result, Some(root.oid));
}
