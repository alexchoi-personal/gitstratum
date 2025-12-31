use rocksdb::{ColumnFamilyDescriptor, Options};

pub const CF_REPOS: &str = "repos";
pub const CF_REFS: &str = "refs";
pub const CF_COMMITS: &str = "commits";
pub const CF_TREES: &str = "trees";
pub const CF_REPO_CONFIG: &str = "repo_config";
pub const CF_REPO_STATS: &str = "repo_stats";
pub const CF_COMMIT_GRAPH: &str = "commit_graph";
pub const CF_PACK_CACHE: &str = "pack_cache";
pub const CF_OBJECT_INDEX: &str = "object_index";

pub fn create_cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    vec![
        ColumnFamilyDescriptor::new(CF_REPOS, Options::default()),
        ColumnFamilyDescriptor::new(CF_REFS, Options::default()),
        ColumnFamilyDescriptor::new(CF_COMMITS, Options::default()),
        ColumnFamilyDescriptor::new(CF_TREES, Options::default()),
        ColumnFamilyDescriptor::new(CF_REPO_CONFIG, Options::default()),
        ColumnFamilyDescriptor::new(CF_REPO_STATS, Options::default()),
        ColumnFamilyDescriptor::new(CF_COMMIT_GRAPH, Options::default()),
        ColumnFamilyDescriptor::new(CF_PACK_CACHE, Options::default()),
        ColumnFamilyDescriptor::new(CF_OBJECT_INDEX, Options::default()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cf_constants() {
        assert_eq!(CF_REPOS, "repos");
        assert_eq!(CF_REFS, "refs");
        assert_eq!(CF_COMMITS, "commits");
        assert_eq!(CF_TREES, "trees");
        assert_eq!(CF_REPO_CONFIG, "repo_config");
        assert_eq!(CF_REPO_STATS, "repo_stats");
        assert_eq!(CF_COMMIT_GRAPH, "commit_graph");
        assert_eq!(CF_PACK_CACHE, "pack_cache");
        assert_eq!(CF_OBJECT_INDEX, "object_index");
    }

    #[test]
    fn test_create_cf_descriptors() {
        let descriptors = create_cf_descriptors();
        assert_eq!(descriptors.len(), 9);
    }
}
