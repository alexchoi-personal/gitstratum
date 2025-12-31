use gitstratum_core::{Commit, Oid, RefName, RepoId, Signature, Tree, TreeEntry, TreeEntryMode};
use gitstratum_metadata::MetadataStore;
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
fn test_repo_create_and_exists() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();

    assert!(!store.repo_exists(&repo_id).unwrap());

    store.create_repo(&repo_id).unwrap();

    assert!(store.repo_exists(&repo_id).unwrap());
}

#[test]
fn test_repo_create_duplicate() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();

    store.create_repo(&repo_id).unwrap();
    let result = store.create_repo(&repo_id);

    assert!(result.is_err());
}

#[test]
fn test_repo_delete() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();

    store.create_repo(&repo_id).unwrap();
    store.delete_repo(&repo_id).unwrap();

    assert!(!store.repo_exists(&repo_id).unwrap());
}

#[test]
fn test_repo_delete_not_found() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("nonexistent").unwrap();

    let result = store.delete_repo(&repo_id);

    assert!(result.is_err());
}

#[test]
fn test_repo_list() {
    let (store, _temp) = create_test_store();

    store.create_repo(&RepoId::new("alpha").unwrap()).unwrap();
    store.create_repo(&RepoId::new("beta").unwrap()).unwrap();
    store.create_repo(&RepoId::new("gamma").unwrap()).unwrap();

    let (repos, cursor) = store.list_repos("", 10, "").unwrap();

    assert_eq!(repos.len(), 3);
    assert!(cursor.is_none());
}

#[test]
fn test_repo_list_with_prefix() {
    let (store, _temp) = create_test_store();

    store.create_repo(&RepoId::new("org/repo1").unwrap()).unwrap();
    store.create_repo(&RepoId::new("org/repo2").unwrap()).unwrap();
    store.create_repo(&RepoId::new("other/repo3").unwrap()).unwrap();

    let (repos, _) = store.list_repos("org/", 10, "").unwrap();

    assert_eq!(repos.len(), 2);
}

#[test]
fn test_repo_list_with_pagination() {
    let (store, _temp) = create_test_store();

    store.create_repo(&RepoId::new("repo1").unwrap()).unwrap();
    store.create_repo(&RepoId::new("repo2").unwrap()).unwrap();
    store.create_repo(&RepoId::new("repo3").unwrap()).unwrap();

    let (repos, cursor) = store.list_repos("", 2, "").unwrap();

    assert_eq!(repos.len(), 2);
    assert!(cursor.is_some());

    let (repos2, cursor2) = store.list_repos("", 2, &cursor.unwrap()).unwrap();

    assert_eq!(repos2.len(), 1);
    assert!(cursor2.is_none());
}

#[test]
fn test_ref_get_set() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let oid = Oid::hash(b"test commit");

    store.create_repo(&repo_id).unwrap();

    assert!(store.get_ref(&repo_id, &ref_name).unwrap().is_none());

    store.update_ref(&repo_id, &ref_name, None, &oid, false).unwrap();

    let result = store.get_ref(&repo_id, &ref_name).unwrap();
    assert_eq!(result, Some(oid));
}

#[test]
fn test_ref_cas_success() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &ref_name, None, &oid1, false).unwrap();
    store.update_ref(&repo_id, &ref_name, Some(&oid1), &oid2, false).unwrap();

    let result = store.get_ref(&repo_id, &ref_name).unwrap();
    assert_eq!(result, Some(oid2));
}

#[test]
fn test_ref_cas_failure() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");
    let wrong_oid = Oid::hash(b"wrong");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &ref_name, None, &oid1, false).unwrap();

    let result = store.update_ref(&repo_id, &ref_name, Some(&wrong_oid), &oid2, false);

    assert!(result.is_err());
}

#[test]
fn test_ref_force_update() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");
    let wrong_oid = Oid::hash(b"wrong");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &ref_name, None, &oid1, false).unwrap();
    store.update_ref(&repo_id, &ref_name, Some(&wrong_oid), &oid2, true).unwrap();

    let result = store.get_ref(&repo_id, &ref_name).unwrap();
    assert_eq!(result, Some(oid2));
}

#[test]
fn test_ref_list() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let oid = Oid::hash(b"commit");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &RefName::branch("main").unwrap(), None, &oid, false).unwrap();
    store.update_ref(&repo_id, &RefName::branch("feature").unwrap(), None, &oid, false).unwrap();
    store.update_ref(&repo_id, &RefName::tag("v1.0").unwrap(), None, &oid, false).unwrap();

    let refs = store.list_refs(&repo_id, "refs/heads/").unwrap();

    assert_eq!(refs.len(), 2);
}

#[test]
fn test_ref_delete() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let oid = Oid::hash(b"commit");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &ref_name, None, &oid, false).unwrap();
    store.delete_ref(&repo_id, &ref_name).unwrap();

    assert!(store.get_ref(&repo_id, &ref_name).unwrap().is_none());
}

#[test]
fn test_commit_put_get() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = Oid::hash(b"tree");
    let commit = create_test_commit(tree, vec![], "Initial commit");

    store.create_repo(&repo_id).unwrap();
    store.put_commit(&repo_id, &commit).unwrap();

    let result = store.get_commit(&repo_id, &commit.oid).unwrap();

    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.oid, commit.oid);
    assert_eq!(fetched.tree, commit.tree);
    assert_eq!(fetched.message, commit.message);
}

#[test]
fn test_commit_not_found() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let oid = Oid::hash(b"nonexistent");

    store.create_repo(&repo_id).unwrap();

    let result = store.get_commit(&repo_id, &oid).unwrap();

    assert!(result.is_none());
}

#[test]
fn test_commit_with_parents() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = Oid::hash(b"tree");
    let parent = create_test_commit(tree, vec![], "Parent commit");
    let child = create_test_commit(tree, vec![parent.oid], "Child commit");

    store.create_repo(&repo_id).unwrap();
    store.put_commit(&repo_id, &parent).unwrap();
    store.put_commit(&repo_id, &child).unwrap();

    let result = store.get_commit(&repo_id, &child.oid).unwrap().unwrap();

    assert_eq!(result.parents.len(), 1);
    assert_eq!(result.parents[0], parent.oid);
}

#[test]
fn test_commit_cache() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = Oid::hash(b"tree");
    let commit = create_test_commit(tree, vec![], "Test commit");

    store.create_repo(&repo_id).unwrap();
    store.put_commit(&repo_id, &commit).unwrap();

    let _ = store.get_commit(&repo_id, &commit.oid).unwrap();
    let _ = store.get_commit(&repo_id, &commit.oid).unwrap();

    store.clear_cache();

    let result = store.get_commit(&repo_id, &commit.oid).unwrap();
    assert!(result.is_some());
}

#[test]
fn test_tree_put_get() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = create_test_tree(vec![
        ("file1.txt", Oid::hash(b"file1")),
        ("file2.txt", Oid::hash(b"file2")),
    ]);

    store.create_repo(&repo_id).unwrap();
    store.put_tree(&repo_id, &tree).unwrap();

    let result = store.get_tree(&repo_id, &tree.oid).unwrap();

    assert!(result.is_some());
    let fetched = result.unwrap();
    assert_eq!(fetched.oid, tree.oid);
    assert_eq!(fetched.entries.len(), 2);
}

#[test]
fn test_tree_not_found() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let oid = Oid::hash(b"nonexistent");

    store.create_repo(&repo_id).unwrap();

    let result = store.get_tree(&repo_id, &oid).unwrap();

    assert!(result.is_none());
}

#[test]
fn test_tree_cache() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = create_test_tree(vec![("file.txt", Oid::hash(b"file"))]);

    store.create_repo(&repo_id).unwrap();
    store.put_tree(&repo_id, &tree).unwrap();

    let _ = store.get_tree(&repo_id, &tree.oid).unwrap();
    let _ = store.get_tree(&repo_id, &tree.oid).unwrap();

    store.clear_cache();

    let result = store.get_tree(&repo_id, &tree.oid).unwrap();
    assert!(result.is_some());
}

#[test]
fn test_delete_repo_cleans_up_data() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let tree = Oid::hash(b"tree");
    let commit = create_test_commit(tree, vec![], "Test commit");
    let tree_obj = create_test_tree(vec![("file.txt", Oid::hash(b"file"))]);

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &ref_name, None, &commit.oid, false).unwrap();
    store.put_commit(&repo_id, &commit).unwrap();
    store.put_tree(&repo_id, &tree_obj).unwrap();

    store.delete_repo(&repo_id).unwrap();

    assert!(!store.repo_exists(&repo_id).unwrap());
}

#[test]
fn test_multiple_repos_isolation() {
    let (store, _temp) = create_test_store();
    let repo1 = RepoId::new("repo1").unwrap();
    let repo2 = RepoId::new("repo2").unwrap();
    let ref_name = RefName::branch("main").unwrap();
    let oid1 = Oid::hash(b"commit1");
    let oid2 = Oid::hash(b"commit2");

    store.create_repo(&repo1).unwrap();
    store.create_repo(&repo2).unwrap();
    store.update_ref(&repo1, &ref_name, None, &oid1, false).unwrap();
    store.update_ref(&repo2, &ref_name, None, &oid2, false).unwrap();

    let result1 = store.get_ref(&repo1, &ref_name).unwrap();
    let result2 = store.get_ref(&repo2, &ref_name).unwrap();

    assert_eq!(result1, Some(oid1));
    assert_eq!(result2, Some(oid2));
}

#[test]
fn test_custom_cache_size() {
    let temp_dir = TempDir::new().unwrap();
    let store = MetadataStore::open_with_cache_size(temp_dir.path(), 100).unwrap();
    let repo_id = RepoId::new("test-repo").unwrap();
    let tree = Oid::hash(b"tree");

    store.create_repo(&repo_id).unwrap();

    for i in 0..150 {
        let commit = create_test_commit(tree, vec![], &format!("Commit {}", i));
        store.put_commit(&repo_id, &commit).unwrap();
    }

    let commit = create_test_commit(tree, vec![], "Final commit");
    store.put_commit(&repo_id, &commit).unwrap();
    let result = store.get_commit(&repo_id, &commit.oid).unwrap();
    assert!(result.is_some());
}

#[test]
fn test_list_repos_skips_invalid_repo_ids() {
    let temp_dir = TempDir::new().unwrap();
    let store = MetadataStore::open(temp_dir.path()).unwrap();

    store.create_repo(&RepoId::new("valid-repo").unwrap()).unwrap();
    store.create_repo(&RepoId::new("another-valid").unwrap()).unwrap();

    let (repos, _) = store.list_repos("", 10, "").unwrap();
    assert_eq!(repos.len(), 2);
}

#[test]
fn test_list_refs_handles_various_ref_formats() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let oid = Oid::hash(b"commit");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &RefName::branch("main").unwrap(), None, &oid, true).unwrap();
    store.update_ref(&repo_id, &RefName::branch("feature/test").unwrap(), None, &oid, true).unwrap();
    store.update_ref(&repo_id, &RefName::tag("v1.0.0").unwrap(), None, &oid, true).unwrap();
    store.update_ref(&repo_id, &RefName::remote("origin", "main").unwrap(), None, &oid, true).unwrap();

    let all_refs = store.list_refs(&repo_id, "").unwrap();
    assert_eq!(all_refs.len(), 4);

    let head_refs = store.list_refs(&repo_id, "refs/heads/").unwrap();
    assert_eq!(head_refs.len(), 2);

    let tag_refs = store.list_refs(&repo_id, "refs/tags/").unwrap();
    assert_eq!(tag_refs.len(), 1);

    let remote_refs = store.list_refs(&repo_id, "refs/remotes/").unwrap();
    assert_eq!(remote_refs.len(), 1);
}

#[test]
fn test_list_repos_with_empty_prefix_and_cursor() {
    let (store, _temp) = create_test_store();

    store.create_repo(&RepoId::new("aaa").unwrap()).unwrap();
    store.create_repo(&RepoId::new("bbb").unwrap()).unwrap();
    store.create_repo(&RepoId::new("ccc").unwrap()).unwrap();

    let (repos1, cursor1) = store.list_repos("", 1, "").unwrap();
    assert_eq!(repos1.len(), 1);
    assert!(cursor1.is_some());

    let (repos2, cursor2) = store.list_repos("", 1, &cursor1.unwrap()).unwrap();
    assert_eq!(repos2.len(), 1);
    assert!(cursor2.is_some());

    let (repos3, _cursor3) = store.list_repos("", 1, &cursor2.unwrap()).unwrap();
    assert_eq!(repos3.len(), 1);
}

#[test]
fn test_delete_repo_with_many_refs_commits_trees() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();

    store.create_repo(&repo_id).unwrap();

    for i in 0..5 {
        let ref_name = RefName::branch(&format!("branch{}", i)).unwrap();
        let oid = Oid::hash(format!("commit{}", i).as_bytes());
        store.update_ref(&repo_id, &ref_name, None, &oid, true).unwrap();
    }

    for i in 0..5 {
        let tree_oid = Oid::hash(format!("tree{}", i).as_bytes());
        let commit = create_test_commit(tree_oid, vec![], &format!("Commit {}", i));
        store.put_commit(&repo_id, &commit).unwrap();

        let tree = create_test_tree(vec![
            (&format!("file{}.txt", i), Oid::hash(format!("content{}", i).as_bytes())),
        ]);
        store.put_tree(&repo_id, &tree).unwrap();
    }

    store.delete_repo(&repo_id).unwrap();
    assert!(!store.repo_exists(&repo_id).unwrap());
}

#[test]
fn test_list_refs_empty_result() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();

    store.create_repo(&repo_id).unwrap();

    let refs = store.list_refs(&repo_id, "refs/heads/").unwrap();
    assert!(refs.is_empty());
}

#[test]
fn test_list_refs_with_nonmatching_prefix() {
    let (store, _temp) = create_test_store();
    let repo_id = RepoId::new("test-repo").unwrap();
    let oid = Oid::hash(b"commit");

    store.create_repo(&repo_id).unwrap();
    store.update_ref(&repo_id, &RefName::branch("main").unwrap(), None, &oid, true).unwrap();

    let refs = store.list_refs(&repo_id, "refs/nonexistent/").unwrap();
    assert!(refs.is_empty());
}
