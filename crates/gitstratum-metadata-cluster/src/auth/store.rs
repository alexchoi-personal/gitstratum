use std::sync::Arc;

use rocksdb::{ColumnFamily, DB};

pub struct AuthStore {
    db: Arc<DB>,
}

impl AuthStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    fn cf_auth(&self) -> &ColumnFamily {
        self.db.cf_handle("auth").expect("auth CF not found")
    }

    fn cf_acl(&self) -> &ColumnFamily {
        self.db.cf_handle("acl").expect("acl CF not found")
    }

    pub fn create_user(&self, user_id: &str, data: &[u8]) -> Result<(), rocksdb::Error> {
        let key = format!("user/{}", user_id);
        self.db.put_cf(self.cf_auth(), key.as_bytes(), data)
    }

    pub fn get_user(&self, user_id: &str) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        let key = format!("user/{}", user_id);
        self.db.get_cf(self.cf_auth(), key.as_bytes())
    }

    pub fn get_user_by_email(&self, email: &str) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        let key = format!("email/{}", email);
        if let Some(user_id) = self.db.get_cf(self.cf_auth(), key.as_bytes())? {
            let user_id = String::from_utf8_lossy(&user_id);
            self.get_user(&user_id)
        } else {
            Ok(None)
        }
    }

    pub fn add_ssh_key(&self, fingerprint: &str, data: &[u8]) -> Result<(), rocksdb::Error> {
        let key = format!("sshkey/{}", fingerprint);
        self.db.put_cf(self.cf_auth(), key.as_bytes(), data)
    }

    pub fn get_ssh_key(&self, fingerprint: &str) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        let key = format!("sshkey/{}", fingerprint);
        self.db.get_cf(self.cf_auth(), key.as_bytes())
    }

    pub fn delete_ssh_key(&self, fingerprint: &str) -> Result<(), rocksdb::Error> {
        let key = format!("sshkey/{}", fingerprint);
        self.db.delete_cf(self.cf_auth(), key.as_bytes())
    }

    pub fn store_token(&self, hash: &str, data: &[u8]) -> Result<(), rocksdb::Error> {
        let key = format!("pat/{}", hash);
        self.db.put_cf(self.cf_auth(), key.as_bytes(), data)
    }

    pub fn get_token(&self, hash: &str) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        let key = format!("pat/{}", hash);
        self.db.get_cf(self.cf_auth(), key.as_bytes())
    }

    pub fn delete_token(&self, hash: &str) -> Result<(), rocksdb::Error> {
        let key = format!("pat/{}", hash);
        self.db.delete_cf(self.cf_auth(), key.as_bytes())
    }

    pub fn set_permission(
        &self,
        repo_id: &str,
        user_id: &str,
        perm: u8,
    ) -> Result<(), rocksdb::Error> {
        let key = format!("{}/acl/{}", repo_id, user_id);
        self.db.put_cf(self.cf_acl(), key.as_bytes(), &[perm])
    }

    pub fn get_permission(
        &self,
        repo_id: &str,
        user_id: &str,
    ) -> Result<Option<u8>, rocksdb::Error> {
        let key = format!("{}/acl/{}", repo_id, user_id);
        self.db
            .get_cf(self.cf_acl(), key.as_bytes())
            .map(|opt| opt.and_then(|v| v.first().copied()))
    }

    pub fn delete_permission(&self, repo_id: &str, user_id: &str) -> Result<(), rocksdb::Error> {
        let key = format!("{}/acl/{}", repo_id, user_id);
        self.db.delete_cf(self.cf_acl(), key.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::column_families::create_cf_descriptors;
    use rocksdb::Options;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, Arc<DB>) {
        let tmp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = create_cf_descriptors();
        let db = DB::open_cf_descriptors(&opts, tmp_dir.path(), cf_descriptors).unwrap();
        (tmp_dir, Arc::new(db))
    }

    #[test]
    fn test_user_crud() {
        let (_tmp, db) = create_test_db();
        let store = AuthStore::new(db);

        let user_data = b"user json data";
        store.create_user("user123", user_data).unwrap();

        let result = store.get_user("user123").unwrap();
        assert_eq!(result, Some(user_data.to_vec()));

        let missing = store.get_user("nonexistent").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_ssh_key_crud() {
        let (_tmp, db) = create_test_db();
        let store = AuthStore::new(db);

        let key_data = b"ssh key data";
        let fingerprint = "SHA256:abc123";

        store.add_ssh_key(fingerprint, key_data).unwrap();

        let result = store.get_ssh_key(fingerprint).unwrap();
        assert_eq!(result, Some(key_data.to_vec()));

        store.delete_ssh_key(fingerprint).unwrap();
        let deleted = store.get_ssh_key(fingerprint).unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    fn test_token_crud() {
        let (_tmp, db) = create_test_db();
        let store = AuthStore::new(db);

        let token_data = b"token json";
        let hash = "sha256hash123";

        store.store_token(hash, token_data).unwrap();

        let result = store.get_token(hash).unwrap();
        assert_eq!(result, Some(token_data.to_vec()));

        store.delete_token(hash).unwrap();
        let deleted = store.get_token(hash).unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    fn test_permission_crud() {
        let (_tmp, db) = create_test_db();
        let store = AuthStore::new(db);

        let repo_id = "org/repo";
        let user_id = "user123";
        let perm: u8 = 0x07;

        store.set_permission(repo_id, user_id, perm).unwrap();

        let result = store.get_permission(repo_id, user_id).unwrap();
        assert_eq!(result, Some(0x07));

        store.delete_permission(repo_id, user_id).unwrap();
        let deleted = store.get_permission(repo_id, user_id).unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    fn test_permission_values() {
        let (_tmp, db) = create_test_db();
        let store = AuthStore::new(db);

        store.set_permission("repo", "user1", 0x01).unwrap();
        store.set_permission("repo", "user2", 0x03).unwrap();
        store.set_permission("repo", "user3", 0x07).unwrap();

        assert_eq!(store.get_permission("repo", "user1").unwrap(), Some(0x01));
        assert_eq!(store.get_permission("repo", "user2").unwrap(), Some(0x03));
        assert_eq!(store.get_permission("repo", "user3").unwrap(), Some(0x07));
    }

    #[test]
    fn test_multiple_repos_permissions() {
        let (_tmp, db) = create_test_db();
        let store = AuthStore::new(db);

        store.set_permission("repo1", "user1", 0x01).unwrap();
        store.set_permission("repo2", "user1", 0x07).unwrap();

        assert_eq!(store.get_permission("repo1", "user1").unwrap(), Some(0x01));
        assert_eq!(store.get_permission("repo2", "user1").unwrap(), Some(0x07));
    }
}
