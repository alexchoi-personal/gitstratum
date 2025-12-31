use async_trait::async_trait;
use gitstratum_core::Oid;
use std::sync::Arc;

use crate::error::Result;

#[async_trait]
pub trait RefSource: Send + Sync {
    async fn list_refs(&self, repo_id: &str, prefix: &str) -> Result<Vec<(String, Oid)>>;
    async fn get_ref(&self, repo_id: &str, ref_name: &str) -> Result<Option<Oid>>;
}

#[derive(Debug, Clone)]
pub struct LsRefsOptions {
    pub ref_prefix: Vec<String>,
    pub symrefs: bool,
    pub peel: bool,
}

impl LsRefsOptions {
    pub fn new() -> Self {
        Self {
            ref_prefix: Vec::new(),
            symrefs: false,
            peel: false,
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.ref_prefix.push(prefix.into());
        self
    }

    pub fn with_symrefs(mut self) -> Self {
        self.symrefs = true;
        self
    }

    pub fn with_peel(mut self) -> Self {
        self.peel = true;
        self
    }
}

impl Default for LsRefsOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LsRefs<R> {
    ref_source: Arc<R>,
    repo_id: String,
}

impl<R> LsRefs<R>
where
    R: RefSource + 'static,
{
    pub fn new(ref_source: Arc<R>, repo_id: String) -> Self {
        Self {
            ref_source,
            repo_id,
        }
    }

    pub async fn list(&self, options: &LsRefsOptions) -> Result<Vec<(String, Oid)>> {
        let mut all_refs = Vec::new();

        if options.ref_prefix.is_empty() {
            all_refs = self.ref_source.list_refs(&self.repo_id, "").await?;
        } else {
            for prefix in &options.ref_prefix {
                let refs = self.ref_source.list_refs(&self.repo_id, prefix).await?;
                all_refs.extend(refs);
            }
        }

        Ok(all_refs)
    }

    pub fn format_output(&self, refs: &[(String, Oid)]) -> Vec<String> {
        refs.iter()
            .map(|(name, oid)| format!("{} {}", oid, name))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    struct MockRefSource {
        refs: RwLock<HashMap<String, Oid>>,
    }

    impl MockRefSource {
        fn new() -> Self {
            Self {
                refs: RwLock::new(HashMap::new()),
            }
        }

        async fn add_ref(&self, name: &str, oid: Oid) {
            self.refs.write().await.insert(name.to_string(), oid);
        }
    }

    #[async_trait]
    impl RefSource for MockRefSource {
        async fn list_refs(&self, _repo_id: &str, prefix: &str) -> Result<Vec<(String, Oid)>> {
            let refs = self.refs.read().await;
            Ok(refs
                .iter()
                .filter(|(name, _)| name.starts_with(prefix))
                .map(|(name, oid)| (name.clone(), *oid))
                .collect())
        }

        async fn get_ref(&self, _repo_id: &str, ref_name: &str) -> Result<Option<Oid>> {
            Ok(self.refs.read().await.get(ref_name).copied())
        }
    }

    #[tokio::test]
    async fn test_ls_refs_list_all() {
        let source = Arc::new(MockRefSource::new());
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");
        source.add_ref("refs/heads/main", oid1).await;
        source.add_ref("refs/heads/feature", oid2).await;

        let ls_refs = LsRefs::new(source, "test-repo".to_string());
        let options = LsRefsOptions::new();

        let refs = ls_refs.list(&options).await.unwrap();
        assert_eq!(refs.len(), 2);
    }

    #[tokio::test]
    async fn test_ls_refs_list_with_prefix() {
        let source = Arc::new(MockRefSource::new());
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");
        source.add_ref("refs/heads/main", oid1).await;
        source.add_ref("refs/tags/v1.0", oid2).await;

        let ls_refs = LsRefs::new(source, "test-repo".to_string());
        let options = LsRefsOptions::new().with_prefix("refs/heads/");

        let refs = ls_refs.list(&options).await.unwrap();
        assert_eq!(refs.len(), 1);
        assert!(refs[0].0.starts_with("refs/heads/"));
    }

    #[tokio::test]
    async fn test_ls_refs_format_output() {
        let source = Arc::new(MockRefSource::new());
        let oid = Oid::hash(b"commit");
        source.add_ref("refs/heads/main", oid).await;

        let ls_refs = LsRefs::new(source, "test-repo".to_string());
        let refs = vec![("refs/heads/main".to_string(), oid)];
        let output = ls_refs.format_output(&refs);

        assert_eq!(output.len(), 1);
        assert!(output[0].contains(&oid.to_string()));
        assert!(output[0].contains("refs/heads/main"));
    }

    #[test]
    fn test_ls_refs_options_new() {
        let options = LsRefsOptions::new();
        assert!(options.ref_prefix.is_empty());
        assert!(!options.symrefs);
        assert!(!options.peel);
    }

    #[test]
    fn test_ls_refs_options_default() {
        let options = LsRefsOptions::default();
        assert!(options.ref_prefix.is_empty());
    }

    #[test]
    fn test_ls_refs_options_with_symrefs() {
        let options = LsRefsOptions::new().with_symrefs();
        assert!(options.symrefs);
    }

    #[test]
    fn test_ls_refs_options_with_peel() {
        let options = LsRefsOptions::new().with_peel();
        assert!(options.peel);
    }

    #[tokio::test]
    async fn test_ref_source_get_ref() {
        let source = Arc::new(MockRefSource::new());
        let oid = Oid::hash(b"commit");
        source.add_ref("refs/heads/main", oid).await;

        let result = source
            .get_ref("test-repo", "refs/heads/main")
            .await
            .unwrap();
        assert_eq!(result, Some(oid));

        let missing = source
            .get_ref("test-repo", "refs/heads/missing")
            .await
            .unwrap();
        assert!(missing.is_none());
    }
}
