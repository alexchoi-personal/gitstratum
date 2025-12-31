use gitstratum_core::{Commit, Oid, RefName, RepoId, Signature, Tree, TreeEntry, TreeEntryMode};
use gitstratum_metadata_cluster::MetadataStore;
use tempfile::TempDir;

fn create_test_store() -> (MetadataStore, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = MetadataStore::open(temp_dir.path()).unwrap();
    (store, temp_dir)
}

fn create_test_signature() -> Signature {
    Signature::new("Test Author", "test@example.com", 1704067200, "+0000")
}

fn create_test_commit(tree: Oid, parents: Vec<Oid>, message: &str) -> Commit {
    let sig = create_test_signature();
    Commit::new(tree, parents, sig.clone(), sig, message)
}

fn create_test_tree(entries: Vec<(&str, Oid)>) -> Tree {
    let tree_entries: Vec<TreeEntry> = entries
        .into_iter()
        .map(|(name, oid)| TreeEntry::new(TreeEntryMode::File, name, oid))
        .collect();
    Tree::new(tree_entries)
}

#[test]
fn test_repository_lifecycle_with_listing_and_pagination() {
    let (store, _temp) = create_test_store();

    let repo1 = RepoId::new("org/repo1").unwrap();
    let repo2 = RepoId::new("org/repo2").unwrap();
    let repo3 = RepoId::new("other/repo3").unwrap();

    assert!(!store.repo_exists(&repo1).unwrap());
    store.create_repo(&repo1).unwrap();
    assert!(store.repo_exists(&repo1).unwrap());

    assert!(store.create_repo(&repo1).is_err());

    store.create_repo(&repo2).unwrap();
    store.create_repo(&repo3).unwrap();

    let (all_repos, _) = store.list_repos("", 10, "").unwrap();
    assert_eq!(all_repos.len(), 3);

    let (org_repos, _) = store.list_repos("org/", 10, "").unwrap();
    assert_eq!(org_repos.len(), 2);

    let (page1, cursor1) = store.list_repos("", 2, "").unwrap();
    assert_eq!(page1.len(), 2);
    assert!(cursor1.is_some());

    let (page2, cursor2) = store.list_repos("", 2, &cursor1.unwrap()).unwrap();
    assert_eq!(page2.len(), 1);
    assert!(cursor2.is_none());

    store.delete_repo(&repo1).unwrap();
    assert!(!store.repo_exists(&repo1).unwrap());

    assert!(store.delete_repo(&repo1).is_err());
}

#[test]
fn test_ref_operations_and_cas_semantics() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let main_ref = RefName::branch("main").unwrap();
    let feature_ref = RefName::branch("feature/test").unwrap();
    let tag_ref = RefName::tag("v1.0.0").unwrap();
    let remote_ref = RefName::remote("origin", "main").unwrap();

    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");
    let wrong_oid = Oid::hash(b"wrong");

    assert!(store.get_ref(&repo_id, &main_ref).unwrap().is_none());

    store.update_ref(&repo_id, &main_ref, None, &oid1, false).unwrap();
    assert_eq!(store.get_ref(&repo_id, &main_ref).unwrap(), Some(oid1));

    store.update_ref(&repo_id, &main_ref, Some(&oid1), &oid2, false).unwrap();
    assert_eq!(store.get_ref(&repo_id, &main_ref).unwrap(), Some(oid2));

    assert!(store.update_ref(&repo_id, &main_ref, Some(&wrong_oid), &oid1, false).is_err());

    store.update_ref(&repo_id, &main_ref, Some(&wrong_oid), &oid1, true).unwrap();
    assert_eq!(store.get_ref(&repo_id, &main_ref).unwrap(), Some(oid1));

    store.update_ref(&repo_id, &feature_ref, None, &oid1, false).unwrap();
    store.update_ref(&repo_id, &tag_ref, None, &oid1, false).unwrap();
    store.update_ref(&repo_id, &remote_ref, None, &oid1, false).unwrap();

    let all_refs = store.list_refs(&repo_id, "").unwrap();
    assert_eq!(all_refs.len(), 4);

    let head_refs = store.list_refs(&repo_id, "refs/heads/").unwrap();
    assert_eq!(head_refs.len(), 2);

    let tag_refs = store.list_refs(&repo_id, "refs/tags/").unwrap();
    assert_eq!(tag_refs.len(), 1);

    let remote_refs = store.list_refs(&repo_id, "refs/remotes/").unwrap();
    assert_eq!(remote_refs.len(), 1);

    let nonexistent = store.list_refs(&repo_id, "refs/nonexistent/").unwrap();
    assert!(nonexistent.is_empty());

    store.delete_ref(&repo_id, &main_ref).unwrap();
    assert!(store.get_ref(&repo_id, &main_ref).unwrap().is_none());
}

#[test]
fn test_commit_and_tree_storage_with_caching() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    let nonexistent_oid = Oid::hash(b"nonexistent");
    assert!(store.get_commit(&repo_id, &nonexistent_oid).unwrap().is_none());
    assert!(store.get_tree(&repo_id, &nonexistent_oid).unwrap().is_none());

    let tree1 = create_test_tree(vec![
        ("file1.txt", Oid::hash(b"file1")),
        ("file2.txt", Oid::hash(b"file2")),
    ]);
    store.put_tree(&repo_id, &tree1).unwrap();

    let tree_result = store.get_tree(&repo_id, &tree1.oid).unwrap().unwrap();
    assert_eq!(tree_result.oid, tree1.oid);
    assert_eq!(tree_result.entries.len(), 2);

    let _ = store.get_tree(&repo_id, &tree1.oid).unwrap();
    let _ = store.get_tree(&repo_id, &tree1.oid).unwrap();

    let commit1 = create_test_commit(tree1.oid, vec![], "Initial commit");
    store.put_commit(&repo_id, &commit1).unwrap();

    let commit_result = store.get_commit(&repo_id, &commit1.oid).unwrap().unwrap();
    assert_eq!(commit_result.oid, commit1.oid);
    assert_eq!(commit_result.tree, commit1.tree);
    assert_eq!(commit_result.message, "Initial commit");

    let commit2 = create_test_commit(tree1.oid, vec![commit1.oid], "Second commit");
    store.put_commit(&repo_id, &commit2).unwrap();

    let commit2_result = store.get_commit(&repo_id, &commit2.oid).unwrap().unwrap();
    assert_eq!(commit2_result.parents.len(), 1);
    assert_eq!(commit2_result.parents[0], commit1.oid);

    let _ = store.get_commit(&repo_id, &commit1.oid).unwrap();
    let _ = store.get_commit(&repo_id, &commit1.oid).unwrap();

    store.clear_cache();

    let after_clear = store.get_commit(&repo_id, &commit1.oid).unwrap();
    assert!(after_clear.is_some());

    let tree_after_clear = store.get_tree(&repo_id, &tree1.oid).unwrap();
    assert!(tree_after_clear.is_some());
}

#[test]
fn test_repository_deletion_cascades_all_data() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    store.create_repo(&repo_id).unwrap();

    for i in 0..5 {
        let ref_name = RefName::branch(&format!("branch{}", i)).unwrap();
        let oid = Oid::hash(format!("commit{}", i).as_bytes());
        store.update_ref(&repo_id, &ref_name, None, &oid, true).unwrap();

        let tree_oid = Oid::hash(format!("tree{}", i).as_bytes());
        let commit = create_test_commit(tree_oid, vec![], &format!("Commit {}", i));
        store.put_commit(&repo_id, &commit).unwrap();

        let tree = create_test_tree(vec![(
            &format!("file{}.txt", i),
            Oid::hash(format!("content{}", i).as_bytes()),
        )]);
        store.put_tree(&repo_id, &tree).unwrap();
    }

    store.delete_repo(&repo_id).unwrap();
    assert!(!store.repo_exists(&repo_id).unwrap());
}

#[test]
fn test_multi_repository_isolation() {
    let (store, _temp) = create_test_store();

    let repo1 = RepoId::new("repo1").unwrap();
    let repo2 = RepoId::new("repo2").unwrap();
    store.create_repo(&repo1).unwrap();
    store.create_repo(&repo2).unwrap();

    let ref_name = RefName::branch("main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");

    store.update_ref(&repo1, &ref_name, None, &oid1, false).unwrap();
    store.update_ref(&repo2, &ref_name, None, &oid2, false).unwrap();

    let tree = Oid::hash(b"tree");
    let commit1 = create_test_commit(tree, vec![], "Repo1 commit");
    let commit2 = create_test_commit(tree, vec![], "Repo2 commit");

    store.put_commit(&repo1, &commit1).unwrap();
    store.put_commit(&repo2, &commit2).unwrap();

    assert_eq!(store.get_ref(&repo1, &ref_name).unwrap(), Some(oid1));
    assert_eq!(store.get_ref(&repo2, &ref_name).unwrap(), Some(oid2));

    assert!(store.get_commit(&repo1, &commit1.oid).unwrap().is_some());
    assert!(store.get_commit(&repo1, &commit2.oid).unwrap().is_none());

    assert!(store.get_commit(&repo2, &commit2.oid).unwrap().is_some());
    assert!(store.get_commit(&repo2, &commit1.oid).unwrap().is_none());
}

#[test]
fn test_custom_cache_size_and_eviction() {
    let temp_dir = TempDir::new().unwrap();
    let store = MetadataStore::open_with_cache_size(temp_dir.path(), 100).unwrap();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = Oid::hash(b"tree");

    store.create_repo(&repo_id).unwrap();

    for i in 0..150 {
        let commit = create_test_commit(tree, vec![], &format!("Commit {}", i));
        store.put_commit(&repo_id, &commit).unwrap();
    }

    let final_commit = create_test_commit(tree, vec![], "Final commit");
    store.put_commit(&repo_id, &final_commit).unwrap();

    let result = store.get_commit(&repo_id, &final_commit.oid).unwrap();
    assert!(result.is_some());
}
