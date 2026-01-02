use gitstratum_core::Oid;
use siphasher::sip::SipHasher24;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PackKey {
    repo_id: String,
    wants_hash: u64,
    haves_hash: u64,
    depth: Option<u32>,
}

impl PackKey {
    pub fn new(
        repo_id: impl Into<String>,
        wants: &[Oid],
        haves: &[Oid],
        depth: Option<u32>,
    ) -> Self {
        let wants_hash = Self::hash_oids(wants);
        let haves_hash = Self::hash_oids(haves);

        Self {
            repo_id: repo_id.into(),
            wants_hash,
            haves_hash,
            depth,
        }
    }

    fn hash_oids(oids: &[Oid]) -> u64 {
        let mut sorted: Vec<_> = oids.iter().collect();
        sorted.sort();

        let mut hasher = SipHasher24::new();
        for oid in sorted {
            oid.as_bytes().hash(&mut hasher);
        }
        hasher.finish()
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn wants_hash(&self) -> u64 {
        self.wants_hash
    }

    pub fn haves_hash(&self) -> u64 {
        self.haves_hash
    }

    pub fn depth(&self) -> Option<u32> {
        self.depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_key_new() {
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");
        let wants = vec![oid1, oid2];
        let haves = vec![Oid::hash(b"have1")];

        let key = PackKey::new("repo/test", &wants, &haves, Some(10));

        assert_eq!(key.repo_id(), "repo/test");
        assert_eq!(key.depth(), Some(10));
    }

    #[test]
    fn test_pack_key_order_independent() {
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");

        let key1 = PackKey::new("repo", &[oid1, oid2], &[], None);
        let key2 = PackKey::new("repo", &[oid2, oid1], &[], None);

        assert_eq!(key1.wants_hash(), key2.wants_hash());
    }

    #[test]
    fn test_pack_key_equality() {
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");

        let key1 = PackKey::new("repo", &[oid1, oid2], &[], Some(5));
        let key2 = PackKey::new("repo", &[oid1, oid2], &[], Some(5));

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_pack_key_different_repos() {
        let oid = Oid::hash(b"commit");

        let key1 = PackKey::new("repo1", &[oid], &[], None);
        let key2 = PackKey::new("repo2", &[oid], &[], None);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_pack_key_different_depth() {
        let oid = Oid::hash(b"commit");

        let key1 = PackKey::new("repo", &[oid], &[], Some(5));
        let key2 = PackKey::new("repo", &[oid], &[], Some(10));

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_pack_key_different_wants() {
        let oid1 = Oid::hash(b"commit1");
        let oid2 = Oid::hash(b"commit2");

        let key1 = PackKey::new("repo", &[oid1], &[], None);
        let key2 = PackKey::new("repo", &[oid2], &[], None);

        assert_ne!(key1.wants_hash(), key2.wants_hash());
    }

    #[test]
    fn test_pack_key_different_haves() {
        let want = Oid::hash(b"want");
        let have1 = Oid::hash(b"have1");
        let have2 = Oid::hash(b"have2");

        let key1 = PackKey::new("repo", &[want], &[have1], None);
        let key2 = PackKey::new("repo", &[want], &[have2], None);

        assert_ne!(key1.haves_hash(), key2.haves_hash());
    }

    #[test]
    fn test_pack_key_empty_oids() {
        let key = PackKey::new("repo", &[], &[], None);

        assert_eq!(key.repo_id(), "repo");
        assert!(key.wants_hash() != 0 || key.haves_hash() == key.wants_hash());
    }

    #[test]
    fn test_pack_key_hash_trait() {
        use std::collections::HashMap;

        let oid = Oid::hash(b"commit");
        let key = PackKey::new("repo", &[oid], &[], None);

        let mut map = HashMap::new();
        map.insert(key.clone(), 42);

        assert_eq!(map.get(&key), Some(&42));
    }

    #[test]
    fn test_pack_key_clone() {
        let oid = Oid::hash(b"commit");
        let key1 = PackKey::new("repo", &[oid], &[], Some(5));
        let key2 = key1.clone();

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_pack_key_debug() {
        let oid = Oid::hash(b"commit");
        let key = PackKey::new("repo", &[oid], &[], None);

        let debug = format!("{:?}", key);
        assert!(debug.contains("PackKey"));
        assert!(debug.contains("repo"));
    }
}
