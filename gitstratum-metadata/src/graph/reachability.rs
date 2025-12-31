use std::collections::{HashSet, VecDeque};

use gitstratum_core::{Oid, RepoId};

use crate::error::Result;
use crate::store::MetadataStore;

pub fn collect_reachable_commits(
    store: &MetadataStore,
    repo_id: &RepoId,
    from: &[Oid],
    max_depth: usize,
) -> Result<HashSet<Oid>> {
    let mut reachable = HashSet::new();
    let mut queue: VecDeque<(Oid, usize)> = from.iter().map(|oid| (*oid, 0)).collect();

    while let Some((oid, depth)) = queue.pop_front() {
        if depth > max_depth {
            continue;
        }

        if !reachable.insert(oid) {
            continue;
        }

        if let Some(commit) = store.get_commit(repo_id, &oid)? {
            for parent in &commit.parents {
                if !reachable.contains(parent) {
                    queue.push_back((*parent, depth + 1));
                }
            }
        }
    }

    Ok(reachable)
}

pub fn is_ancestor(
    store: &MetadataStore,
    repo_id: &RepoId,
    potential_ancestor: &Oid,
    descendant: &Oid,
) -> Result<bool> {
    if potential_ancestor == descendant {
        return Ok(true);
    }

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(*descendant);

    while let Some(oid) = queue.pop_front() {
        if oid == *potential_ancestor {
            return Ok(true);
        }

        if !visited.insert(oid) {
            continue;
        }

        if let Some(commit) = store.get_commit(repo_id, &oid)? {
            for parent in &commit.parents {
                if !visited.contains(parent) {
                    queue.push_back(*parent);
                }
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::{Commit, Signature};
    use tempfile::TempDir;

    fn create_test_store() -> (MetadataStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::open(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    fn create_commit(oid_seed: &[u8], parents: Vec<Oid>) -> Commit {
        Commit {
            oid: Oid::hash(oid_seed),
            tree: Oid::hash(b"tree"),
            parents,
            message: "Test commit".to_string(),
            author: Signature::new("Test", "test@example.com", 0, "+0000"),
            committer: Signature::new("Test", "test@example.com", 0, "+0000"),
        }
    }

    #[test]
    fn test_is_ancestor_same_commit() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let oid = Oid::hash(b"commit1");
        assert!(is_ancestor(&store, &repo_id, &oid, &oid).unwrap());
    }

    #[test]
    fn test_is_ancestor_direct_parent() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let parent_commit = create_commit(b"parent", vec![]);
        let child_commit = create_commit(b"child", vec![parent_commit.oid]);

        store.put_commit(&repo_id, &parent_commit).unwrap();
        store.put_commit(&repo_id, &child_commit).unwrap();

        assert!(is_ancestor(&store, &repo_id, &parent_commit.oid, &child_commit.oid).unwrap());
        assert!(!is_ancestor(&store, &repo_id, &child_commit.oid, &parent_commit.oid).unwrap());
    }

    #[test]
    fn test_is_ancestor_not_related() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let commit1 = create_commit(b"commit1", vec![]);
        let commit2 = create_commit(b"commit2", vec![]);

        store.put_commit(&repo_id, &commit1).unwrap();
        store.put_commit(&repo_id, &commit2).unwrap();

        assert!(!is_ancestor(&store, &repo_id, &commit1.oid, &commit2.oid).unwrap());
        assert!(!is_ancestor(&store, &repo_id, &commit2.oid, &commit1.oid).unwrap());
    }

    #[test]
    fn test_collect_reachable_commits() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let commit1 = create_commit(b"commit1", vec![]);
        let commit2 = create_commit(b"commit2", vec![commit1.oid]);
        let commit3 = create_commit(b"commit3", vec![commit2.oid]);

        store.put_commit(&repo_id, &commit1).unwrap();
        store.put_commit(&repo_id, &commit2).unwrap();
        store.put_commit(&repo_id, &commit3).unwrap();

        let reachable = collect_reachable_commits(&store, &repo_id, &[commit3.oid], 10).unwrap();
        assert_eq!(reachable.len(), 3);
        assert!(reachable.contains(&commit1.oid));
        assert!(reachable.contains(&commit2.oid));
        assert!(reachable.contains(&commit3.oid));
    }

    #[test]
    fn test_collect_reachable_commits_with_depth_limit() {
        let (store, _dir) = create_test_store();
        let repo_id = RepoId::new("test/repo").unwrap();
        store.create_repo(&repo_id).unwrap();

        let commit1 = create_commit(b"commit1", vec![]);
        let commit2 = create_commit(b"commit2", vec![commit1.oid]);
        let commit3 = create_commit(b"commit3", vec![commit2.oid]);

        store.put_commit(&repo_id, &commit1).unwrap();
        store.put_commit(&repo_id, &commit2).unwrap();
        store.put_commit(&repo_id, &commit3).unwrap();

        let reachable = collect_reachable_commits(&store, &repo_id, &[commit3.oid], 1).unwrap();
        assert_eq!(reachable.len(), 2);
        assert!(!reachable.contains(&commit1.oid));
        assert!(reachable.contains(&commit2.oid));
        assert!(reachable.contains(&commit3.oid));
    }
}
