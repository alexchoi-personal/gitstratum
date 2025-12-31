use gitstratum_core::Oid;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct ShallowInfo {
    pub shallow_commits: HashSet<Oid>,
    pub depth: Option<u32>,
    pub deepen_since: Option<i64>,
    pub deepen_not: Vec<String>,
}

impl ShallowInfo {
    pub fn new() -> Self {
        Self {
            shallow_commits: HashSet::new(),
            depth: None,
            deepen_since: None,
            deepen_not: Vec::new(),
        }
    }

    pub fn with_depth(mut self, depth: u32) -> Self {
        self.depth = Some(depth);
        self
    }

    pub fn with_deepen_since(mut self, timestamp: i64) -> Self {
        self.deepen_since = Some(timestamp);
        self
    }

    pub fn with_deepen_not(mut self, ref_name: String) -> Self {
        self.deepen_not.push(ref_name);
        self
    }

    pub fn add_shallow(&mut self, oid: Oid) {
        self.shallow_commits.insert(oid);
    }

    pub fn is_shallow(&self, oid: &Oid) -> bool {
        self.shallow_commits.contains(oid)
    }

    pub fn has_shallow_constraints(&self) -> bool {
        self.depth.is_some() || self.deepen_since.is_some() || !self.deepen_not.is_empty()
    }

    pub fn shallow_count(&self) -> usize {
        self.shallow_commits.len()
    }
}

impl Default for ShallowInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShallowUpdate {
    Shallow(Oid),
    Unshallow(Oid),
}

impl ShallowUpdate {
    pub fn shallow(oid: Oid) -> Self {
        ShallowUpdate::Shallow(oid)
    }

    pub fn unshallow(oid: Oid) -> Self {
        ShallowUpdate::Unshallow(oid)
    }

    pub fn to_line(&self) -> String {
        match self {
            ShallowUpdate::Shallow(oid) => format!("shallow {}", oid),
            ShallowUpdate::Unshallow(oid) => format!("unshallow {}", oid),
        }
    }
}

pub fn compute_shallow_updates(
    current_shallow: &HashSet<Oid>,
    new_shallow: &HashSet<Oid>,
) -> Vec<ShallowUpdate> {
    let mut updates = Vec::new();

    for oid in new_shallow {
        if !current_shallow.contains(oid) {
            updates.push(ShallowUpdate::Shallow(*oid));
        }
    }

    for oid in current_shallow {
        if !new_shallow.contains(oid) {
            updates.push(ShallowUpdate::Unshallow(*oid));
        }
    }

    updates
}

pub fn should_include_commit(
    _commit_oid: &Oid,
    commit_depth: u32,
    commit_timestamp: i64,
    info: &ShallowInfo,
) -> bool {
    if let Some(max_depth) = info.depth {
        if commit_depth > max_depth {
            return false;
        }
    }

    if let Some(since) = info.deepen_since {
        if commit_timestamp < since {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(s: &str) -> Oid {
        Oid::hash(s.as_bytes())
    }

    #[test]
    fn test_shallow_info_new() {
        let info = ShallowInfo::new();
        assert!(info.shallow_commits.is_empty());
        assert!(info.depth.is_none());
        assert!(info.deepen_since.is_none());
        assert!(info.deepen_not.is_empty());
    }

    #[test]
    fn test_shallow_info_default() {
        let info = ShallowInfo::default();
        assert!(info.shallow_commits.is_empty());
    }

    #[test]
    fn test_shallow_info_with_depth() {
        let info = ShallowInfo::new().with_depth(1);
        assert_eq!(info.depth, Some(1));
    }

    #[test]
    fn test_shallow_info_with_deepen_since() {
        let info = ShallowInfo::new().with_deepen_since(1704067200);
        assert_eq!(info.deepen_since, Some(1704067200));
    }

    #[test]
    fn test_shallow_info_with_deepen_not() {
        let info = ShallowInfo::new().with_deepen_not("refs/heads/main".to_string());
        assert_eq!(info.deepen_not, vec!["refs/heads/main"]);
    }

    #[test]
    fn test_shallow_info_add_shallow() {
        let mut info = ShallowInfo::new();
        let oid = make_oid("shallow");
        info.add_shallow(oid);
        assert!(info.is_shallow(&oid));
        assert_eq!(info.shallow_count(), 1);
    }

    #[test]
    fn test_shallow_info_has_constraints() {
        let info1 = ShallowInfo::new();
        assert!(!info1.has_shallow_constraints());

        let info2 = ShallowInfo::new().with_depth(1);
        assert!(info2.has_shallow_constraints());

        let info3 = ShallowInfo::new().with_deepen_since(1234);
        assert!(info3.has_shallow_constraints());

        let info4 = ShallowInfo::new().with_deepen_not("refs/heads/main".to_string());
        assert!(info4.has_shallow_constraints());
    }

    #[test]
    fn test_shallow_update_shallow() {
        let oid = make_oid("commit");
        let update = ShallowUpdate::shallow(oid);
        assert_eq!(update, ShallowUpdate::Shallow(oid));
        assert!(update.to_line().starts_with("shallow "));
    }

    #[test]
    fn test_shallow_update_unshallow() {
        let oid = make_oid("commit");
        let update = ShallowUpdate::unshallow(oid);
        assert_eq!(update, ShallowUpdate::Unshallow(oid));
        assert!(update.to_line().starts_with("unshallow "));
    }

    #[test]
    fn test_compute_shallow_updates() {
        let oid1 = make_oid("commit1");
        let oid2 = make_oid("commit2");
        let oid3 = make_oid("commit3");

        let current: HashSet<_> = [oid1, oid2].into_iter().collect();
        let new: HashSet<_> = [oid2, oid3].into_iter().collect();

        let updates = compute_shallow_updates(&current, &new);

        assert!(updates.contains(&ShallowUpdate::Shallow(oid3)));
        assert!(updates.contains(&ShallowUpdate::Unshallow(oid1)));
        assert_eq!(updates.len(), 2);
    }

    #[test]
    fn test_should_include_commit_no_constraints() {
        let info = ShallowInfo::new();
        let oid = make_oid("commit");
        assert!(should_include_commit(&oid, 10, 1234567890, &info));
    }

    #[test]
    fn test_should_include_commit_depth() {
        let info = ShallowInfo::new().with_depth(5);
        let oid = make_oid("commit");

        assert!(should_include_commit(&oid, 3, 1234567890, &info));
        assert!(should_include_commit(&oid, 5, 1234567890, &info));
        assert!(!should_include_commit(&oid, 6, 1234567890, &info));
    }

    #[test]
    fn test_should_include_commit_deepen_since() {
        let info = ShallowInfo::new().with_deepen_since(1000);
        let oid = make_oid("commit");

        assert!(should_include_commit(&oid, 1, 2000, &info));
        assert!(should_include_commit(&oid, 1, 1000, &info));
        assert!(!should_include_commit(&oid, 1, 500, &info));
    }
}
