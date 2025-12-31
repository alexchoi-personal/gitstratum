use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepoId(String);

impl RepoId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        Self::validate(&id)?;
        Ok(Self(id))
    }

    fn validate(id: &str) -> Result<()> {
        if id.is_empty() {
            return Err(Error::InvalidRefName("repo id cannot be empty".to_string()));
        }
        if id.len() > 256 {
            return Err(Error::InvalidRefName("repo id too long".to_string()));
        }
        if !id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '/') {
            return Err(Error::InvalidRefName(
                "repo id contains invalid characters".to_string(),
            ));
        }
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn owner(&self) -> Option<&str> {
        self.0.split('/').next()
    }

    pub fn name(&self) -> &str {
        self.0.split('/').last().unwrap_or(&self.0)
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RepoId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl AsRef<str> for RepoId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefName(String);

impl RefName {
    pub const HEAD: &'static str = "HEAD";

    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(Self(name))
    }

    pub fn head() -> Self {
        Self(Self::HEAD.to_string())
    }

    pub fn branch(name: &str) -> Result<Self> {
        Self::new(format!("refs/heads/{}", name))
    }

    pub fn tag(name: &str) -> Result<Self> {
        Self::new(format!("refs/tags/{}", name))
    }

    pub fn remote(remote: &str, branch: &str) -> Result<Self> {
        Self::new(format!("refs/remotes/{}/{}", remote, branch))
    }

    fn validate(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidRefName("ref name cannot be empty".to_string()));
        }
        if name == "HEAD" {
            return Ok(());
        }
        if !name.starts_with("refs/") {
            return Err(Error::InvalidRefName(
                "ref name must start with 'refs/'".to_string(),
            ));
        }
        if name.contains("..") {
            return Err(Error::InvalidRefName(
                "ref name cannot contain '..'".to_string(),
            ));
        }
        if name.ends_with('/') {
            return Err(Error::InvalidRefName(
                "ref name cannot end with '/'".to_string(),
            ));
        }
        if name.ends_with(".lock") {
            return Err(Error::InvalidRefName(
                "ref name cannot end with '.lock'".to_string(),
            ));
        }
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_head(&self) -> bool {
        self.0 == Self::HEAD
    }

    pub fn is_branch(&self) -> bool {
        self.0.starts_with("refs/heads/")
    }

    pub fn is_tag(&self) -> bool {
        self.0.starts_with("refs/tags/")
    }

    pub fn is_remote(&self) -> bool {
        self.0.starts_with("refs/remotes/")
    }

    pub fn short_name(&self) -> &str {
        if self.is_head() {
            return "HEAD";
        }
        if let Some(name) = self.0.strip_prefix("refs/heads/") {
            return name;
        }
        if let Some(name) = self.0.strip_prefix("refs/tags/") {
            return name;
        }
        if let Some(name) = self.0.strip_prefix("refs/remotes/") {
            return name;
        }
        &self.0
    }
}

impl fmt::Display for RefName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RefName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl AsRef<str> for RefName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repo_id_valid() {
        assert!(RepoId::new("owner/repo").is_ok());
        assert!(RepoId::new("my-repo").is_ok());
        assert!(RepoId::new("my_repo").is_ok());
        assert!(RepoId::new("repo123").is_ok());
    }

    #[test]
    fn test_repo_id_invalid() {
        assert!(RepoId::new("").is_err());
        assert!(RepoId::new("repo with spaces").is_err());
        assert!(RepoId::new("repo@name").is_err());
    }

    #[test]
    fn test_repo_id_owner_name() {
        let repo = RepoId::new("owner/repo").unwrap();
        assert_eq!(repo.owner(), Some("owner"));
        assert_eq!(repo.name(), "repo");

        let repo = RepoId::new("single").unwrap();
        assert_eq!(repo.owner(), Some("single"));
        assert_eq!(repo.name(), "single");
    }

    #[test]
    fn test_ref_name_head() {
        let head = RefName::head();
        assert!(head.is_head());
        assert_eq!(head.short_name(), "HEAD");

        let head_via_new = RefName::new("HEAD").unwrap();
        assert!(head_via_new.is_head());
    }

    #[test]
    fn test_ref_name_branch() {
        let branch = RefName::branch("main").unwrap();
        assert!(branch.is_branch());
        assert!(!branch.is_tag());
        assert_eq!(branch.short_name(), "main");
        assert_eq!(branch.as_str(), "refs/heads/main");
    }

    #[test]
    fn test_ref_name_tag() {
        let tag = RefName::tag("v1.0.0").unwrap();
        assert!(tag.is_tag());
        assert!(!tag.is_branch());
        assert_eq!(tag.short_name(), "v1.0.0");
    }

    #[test]
    fn test_ref_name_remote() {
        let remote = RefName::remote("origin", "main").unwrap();
        assert!(remote.is_remote());
        assert_eq!(remote.short_name(), "origin/main");
    }

    #[test]
    fn test_ref_name_invalid() {
        assert!(RefName::new("").is_err());
        assert!(RefName::new("invalid").is_err());
        assert!(RefName::new("refs/heads/").is_err());
        assert!(RefName::new("refs/heads/test..test").is_err());
        assert!(RefName::new("refs/heads/test.lock").is_err());
    }

    #[test]
    fn test_repo_id_too_long() {
        let long_id = "a".repeat(257);
        assert!(RepoId::new(long_id).is_err());
    }

    #[test]
    fn test_repo_id_display() {
        let repo = RepoId::new("owner/repo").unwrap();
        let display = format!("{}", repo);
        assert_eq!(display, "owner/repo");
    }

    #[test]
    fn test_repo_id_from_str() {
        let repo: RepoId = "owner/repo".parse().unwrap();
        assert_eq!(repo.as_str(), "owner/repo");

        let invalid: std::result::Result<RepoId, _> = "".parse();
        assert!(invalid.is_err());
    }

    #[test]
    fn test_repo_id_as_ref() {
        let repo = RepoId::new("owner/repo").unwrap();
        let str_ref: &str = repo.as_ref();
        assert_eq!(str_ref, "owner/repo");
    }

    #[test]
    fn test_ref_name_short_name_fallback() {
        let ref_name = RefName::new("refs/notes/commits").unwrap();
        assert_eq!(ref_name.short_name(), "refs/notes/commits");
    }

    #[test]
    fn test_ref_name_display() {
        let ref_name = RefName::branch("main").unwrap();
        let display = format!("{}", ref_name);
        assert_eq!(display, "refs/heads/main");
    }

    #[test]
    fn test_ref_name_from_str() {
        let ref_name: RefName = "refs/heads/main".parse().unwrap();
        assert_eq!(ref_name.as_str(), "refs/heads/main");

        let invalid: std::result::Result<RefName, _> = "invalid".parse();
        assert!(invalid.is_err());
    }

    #[test]
    fn test_ref_name_as_ref() {
        let ref_name = RefName::branch("main").unwrap();
        let str_ref: &str = ref_name.as_ref();
        assert_eq!(str_ref, "refs/heads/main");
    }
}
