use std::sync::Arc;

use futures::StreamExt;
use gitstratum_core::{Commit, Oid, RepoId, Signature};
use gitstratum_metadata_cluster::{
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

fn setup_diamond_graph(store: &MetadataStore, repo_id: &RepoId) -> (Commit, Commit, Commit, Commit) {
    let tree = Oid::hash(b"tree");

    let base = create_commit_with_timestamp(tree, vec![], "Base", 1000);
    store.put_commit(repo_id, &base).unwrap();

    let left = create_commit_with_timestamp(tree, vec![base.oid], "Left", 2000);
    store.put_commit(repo_id, &left).unwrap();

    let right = create_commit_with_timestamp(tree, vec![base.oid], "Right", 2001);
    store.put_commit(repo_id, &right).unwrap();

    let merge = create_commit_with_timestamp(tree, vec![left.oid, right.oid], "Merge", 3000);
    store.put_commit(repo_id, &merge).unwrap();

    (base, left, right, merge)
}

#[tokio::test]
async fn test_walk_commits_comprehensive() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![commits[9].oid], vec![], None);
    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }
    assert_eq!(walked.len(), 10);
    assert_eq!(walked[0].oid, commits[9].oid);
    assert_eq!(walked[9].oid, commits[0].oid);

    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![commits[9].oid], vec![], Some(3));
    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }
    assert_eq!(walked.len(), 3);

    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![commits[9].oid], vec![commits[5].oid], None);
    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }
    assert_eq!(walked.len(), 4);
    for commit in &walked {
        assert_ne!(commit.oid, commits[5].oid);
    }

    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![], vec![], None);
    assert!(walker.next().await.is_none());

    let nonexistent = Oid::hash(b"nonexistent");
    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![nonexistent], vec![], None);
    assert!(walker.next().await.is_none());

    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![commits[2].oid], vec![commits[2].oid], None);
    assert!(walker.next().await.is_none());

    let mut walker = walk_commits(store.clone(), repo_id.clone(), vec![commits[2].oid, commits[2].oid, commits[2].oid], vec![], None);
    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }
    assert_eq!(walked.len(), 3);
}

#[tokio::test]
async fn test_walk_commits_branched_and_merged() {
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

    let mut walker = walk_commits(store.clone(), repo_id, vec![merge.oid], vec![], None);

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 5);
    assert_eq!(walked[0].oid, merge.oid);
}

#[tokio::test]
async fn test_walk_commits_async_api() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let walked = gitstratum_metadata_cluster::walk_commits_async(
        store.clone(), repo_id.clone(), vec![commits[9].oid], vec![], None,
    ).await.unwrap();
    assert_eq!(walked.len(), 10);
    assert_eq!(walked[0].oid, commits[9].oid);
    assert_eq!(walked[9].oid, commits[0].oid);

    let walked = gitstratum_metadata_cluster::walk_commits_async(
        store.clone(), repo_id.clone(), vec![commits[9].oid], vec![], Some(3),
    ).await.unwrap();
    assert_eq!(walked.len(), 3);

    let walked = gitstratum_metadata_cluster::walk_commits_async(
        store.clone(), repo_id.clone(), vec![commits[9].oid], vec![commits[5].oid], None,
    ).await.unwrap();
    assert_eq!(walked.len(), 4);
    for commit in &walked {
        assert_ne!(commit.oid, commits[5].oid);
    }

    let walked = gitstratum_metadata_cluster::walk_commits_async(
        store.clone(), repo_id, vec![], vec![], None,
    ).await.unwrap();
    assert!(walked.is_empty());
}

#[tokio::test]
async fn test_walk_commits_large_history() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 50);

    let mut walker = walk_commits(store.clone(), repo_id, vec![commits[49].oid], vec![], None);

    let mut walked = Vec::new();
    while let Some(result) = walker.next().await {
        walked.push(result.unwrap());
    }

    assert_eq!(walked.len(), 50);
}

#[test]
fn test_find_merge_base_comprehensive() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    assert!(find_merge_base(&store, &repo_id, &[]).unwrap().is_none());

    let commits = setup_linear_history(&store, &repo_id, 5);

    let result = find_merge_base(&store, &repo_id, &[commits[4].oid]).unwrap();
    assert_eq!(result, Some(commits[4].oid));

    let result = find_merge_base(&store, &repo_id, &[commits[4].oid, commits[2].oid]).unwrap();
    assert_eq!(result, Some(commits[2].oid));

    let result = find_merge_base(&store, &repo_id, &[commits[2].oid, commits[2].oid]).unwrap();
    assert_eq!(result, Some(commits[2].oid));
}

#[test]
fn test_find_merge_base_fork_scenarios() {
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

    let branch3 = create_commit_with_timestamp(tree, vec![base.oid], "Branch 3", 2002);
    store.put_commit(&repo_id, &branch3).unwrap();

    let result = find_merge_base(&store, &repo_id, &[branch1.oid, branch2.oid]).unwrap();
    assert_eq!(result, Some(base.oid));

    let result = find_merge_base(&store, &repo_id, &[branch1.oid, branch2.oid, branch3.oid]).unwrap();
    assert_eq!(result, Some(base.oid));
}

#[test]
fn test_find_merge_base_complex_graphs() {
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

    let nonexistent = Oid::hash(b"nonexistent");
    let result = find_merge_base(&store, &repo_id, &[c1.oid, nonexistent]).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_find_merge_base_deep_and_asymmetric() {
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

    let base = create_commit_with_timestamp(tree, vec![], "AsymBase", 3000);
    store.put_commit(&repo_id, &base).unwrap();

    let mut prev_a = base.oid;
    for i in 0..20 {
        let c = create_commit_with_timestamp(tree, vec![prev_a], &format!("A{}", i), 4000 + i as i64);
        store.put_commit(&repo_id, &c).unwrap();
        prev_a = c.oid;
    }

    let mut prev_b = base.oid;
    for i in 0..10 {
        let c = create_commit_with_timestamp(tree, vec![prev_b], &format!("B{}", i), 5000 + i as i64);
        store.put_commit(&repo_id, &c).unwrap();
        prev_b = c.oid;
    }

    let result = find_merge_base(&store, &repo_id, &[prev_a, prev_b]).unwrap();
    assert_eq!(result, Some(base.oid));
}

#[test]
fn test_find_merge_base_missing_parent_and_edge_cases() {
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

    let root = create_commit_with_timestamp(tree, vec![], "Root", 2000);
    store.put_commit(&repo_id, &root).unwrap();

    let a = create_commit_with_timestamp(tree, vec![root.oid], "A", 2001);
    store.put_commit(&repo_id, &a).unwrap();

    let b = create_commit_with_timestamp(tree, vec![root.oid], "B", 2001);
    store.put_commit(&repo_id, &b).unwrap();

    let result = find_merge_base(&store, &repo_id, &[a.oid, b.oid]).unwrap();
    assert_eq!(result, Some(root.oid));
}

#[test]
fn test_is_ancestor_comprehensive() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 5);

    assert!(is_ancestor(&store, &repo_id, &commits[1].oid, &commits[1].oid).unwrap());

    assert!(is_ancestor(&store, &repo_id, &commits[1].oid, &commits[4].oid).unwrap());

    assert!(!is_ancestor(&store, &repo_id, &commits[4].oid, &commits[1].oid).unwrap());

    let tree = Oid::hash(b"tree");
    let unrelated = create_commit_with_timestamp(tree, vec![], "Unrelated", 9000);
    store.put_commit(&repo_id, &unrelated).unwrap();

    assert!(!is_ancestor(&store, &repo_id, &commits[0].oid, &unrelated.oid).unwrap());

    let nonexistent1 = Oid::hash(b"nonexistent1");
    let nonexistent2 = Oid::hash(b"nonexistent2");
    assert!(!is_ancestor(&store, &repo_id, &nonexistent1, &nonexistent2).unwrap());
}

#[test]
fn test_is_ancestor_with_merges() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let (base, left, right, merge) = setup_diamond_graph(&store, &repo_id);

    assert!(is_ancestor(&store, &repo_id, &base.oid, &merge.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &left.oid, &merge.oid).unwrap());
    assert!(is_ancestor(&store, &repo_id, &right.oid, &merge.oid).unwrap());
    assert!(!is_ancestor(&store, &repo_id, &merge.oid, &base.oid).unwrap());
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
fn test_collect_reachable_commits_comprehensive() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let commits = setup_linear_history(&store, &repo_id, 10);

    let reachable = collect_reachable_commits(&store, &repo_id, &[commits[9].oid], 100).unwrap();
    assert_eq!(reachable.len(), 10);
    for commit in &commits {
        assert!(reachable.contains(&commit.oid));
    }

    let reachable = collect_reachable_commits(&store, &repo_id, &[commits[9].oid], 3).unwrap();
    assert!(reachable.len() <= 4);
    assert!(reachable.contains(&commits[9].oid));

    let reachable = collect_reachable_commits(&store, &repo_id, &[commits[4].oid], 0).unwrap();
    assert_eq!(reachable.len(), 1);
    assert!(reachable.contains(&commits[4].oid));

    let reachable = collect_reachable_commits(&store, &repo_id, &[], 100).unwrap();
    assert!(reachable.is_empty());

    let nonexistent = Oid::hash(b"nonexistent");
    let reachable = collect_reachable_commits(&store, &repo_id, &[nonexistent], 100).unwrap();
    assert_eq!(reachable.len(), 1);
    assert!(reachable.contains(&nonexistent));
}

#[test]
fn test_collect_reachable_from_multiple_starting_points() {
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

    let reachable = collect_reachable_commits(&store, &repo_id, &[a.oid, b.oid], 100).unwrap();
    assert_eq!(reachable.len(), 3);
    assert!(reachable.contains(&base.oid));
    assert!(reachable.contains(&a.oid));
    assert!(reachable.contains(&b.oid));

    let merge = create_commit_with_timestamp(tree, vec![a.oid, b.oid, c.oid], "Merge", 3000);
    store.put_commit(&repo_id, &merge).unwrap();

    let reachable = collect_reachable_commits(&store, &repo_id, &[merge.oid], 100).unwrap();
    assert_eq!(reachable.len(), 5);
}

#[test]
fn test_collect_reachable_diamond_graph() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let (base, left, right, merge) = setup_diamond_graph(&store, &repo_id);

    let reachable = collect_reachable_commits(&store, &repo_id, &[merge.oid], 100).unwrap();
    assert_eq!(reachable.len(), 4);
    assert!(reachable.contains(&base.oid));
    assert!(reachable.contains(&left.oid));
    assert!(reachable.contains(&right.oid));
    assert!(reachable.contains(&merge.oid));
}
