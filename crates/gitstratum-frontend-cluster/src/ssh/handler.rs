use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use russh::keys::ssh_key;
use russh::server::{Auth, Msg, Session};
use russh::{Channel, ChannelId};

use crate::auth::{AuthResult, AuthStore, LocalValidator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitOperation {
    UploadPack,
    ReceivePack,
}

impl GitOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            GitOperation::UploadPack => "upload-pack",
            GitOperation::ReceivePack => "receive-pack",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommand {
    pub operation: GitOperation,
    pub repo_id: String,
}

pub fn parse_git_command(cmd: &str) -> Option<GitCommand> {
    let cmd = cmd.trim();

    let (git_op, rest) = if let Some(rest) = cmd.strip_prefix("git-upload-pack ") {
        (GitOperation::UploadPack, rest)
    } else if let Some(rest) = cmd.strip_prefix("git-receive-pack ") {
        (GitOperation::ReceivePack, rest)
    } else if let Some(rest) = cmd.strip_prefix("git upload-pack ") {
        (GitOperation::UploadPack, rest)
    } else if let Some(rest) = cmd.strip_prefix("git receive-pack ") {
        (GitOperation::ReceivePack, rest)
    } else {
        return None;
    };

    let rest = rest.trim();

    let repo_path = if ((rest.starts_with('\'') && rest.ends_with('\''))
        || (rest.starts_with('"') && rest.ends_with('"')))
        && rest.len() >= 2
    {
        &rest[1..rest.len() - 1]
    } else {
        rest
    };

    if repo_path.is_empty() {
        return None;
    }

    let repo_id = repo_path.trim_start_matches('/').to_string();
    if repo_id.is_empty() {
        return None;
    }

    Some(GitCommand {
        operation: git_op,
        repo_id,
    })
}

pub struct GitSshHandler<S: AuthStore> {
    peer_addr: Option<SocketAddr>,
    validator: Arc<LocalValidator<S>>,
    auth_result: Option<AuthResult>,
}

impl<S: AuthStore> GitSshHandler<S> {
    pub(crate) fn new(peer_addr: Option<SocketAddr>, validator: Arc<LocalValidator<S>>) -> Self {
        Self {
            peer_addr,
            validator,
            auth_result: None,
        }
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    pub fn auth_result(&self) -> Option<&AuthResult> {
        self.auth_result.as_ref()
    }
}

#[async_trait]
impl<S: AuthStore + 'static> russh::server::Handler for GitSshHandler<S> {
    type Error = russh::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        public_key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        let fingerprint = public_key.fingerprint(ssh_key::HashAlg::Sha256).to_string();

        tracing::debug!(
            peer_addr = ?self.peer_addr,
            fingerprint = %fingerprint,
            "authenticating ssh public key"
        );

        match self.validator.validate_ssh_key(&fingerprint) {
            Ok(result) => {
                tracing::info!(
                    peer_addr = ?self.peer_addr,
                    user_id = %result.user_id,
                    "ssh authentication successful"
                );
                self.auth_result = Some(result);
                Ok(Auth::Accept)
            }
            Err(e) => {
                tracing::warn!(
                    peer_addr = ?self.peer_addr,
                    fingerprint = %fingerprint,
                    error = %e,
                    "ssh authentication failed"
                );
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                })
            }
        }
    }

    async fn channel_open_session(
        &mut self,
        _channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        if self.auth_result.is_some() {
            tracing::debug!(peer_addr = ?self.peer_addr, "session channel opened");
            Ok(true)
        } else {
            tracing::warn!(peer_addr = ?self.peer_addr, "session channel denied - not authenticated");
            Ok(false)
        }
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let cmd_str = match std::str::from_utf8(data) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    peer_addr = ?self.peer_addr,
                    channel = ?channel,
                    error = %e,
                    "invalid UTF-8 in exec request"
                );
                session.channel_failure(channel)?;
                return Ok(());
            }
        };

        tracing::debug!(
            peer_addr = ?self.peer_addr,
            channel = ?channel,
            command = %cmd_str,
            "exec request received"
        );

        match parse_git_command(cmd_str) {
            Some(git_cmd) => {
                tracing::info!(
                    peer_addr = ?self.peer_addr,
                    channel = ?channel,
                    operation = %git_cmd.operation.as_str(),
                    repo_id = %git_cmd.repo_id,
                    "parsed git command"
                );
                session.channel_success(channel)?;
            }
            None => {
                tracing::warn!(
                    peer_addr = ?self.peer_addr,
                    channel = ?channel,
                    command = %cmd_str,
                    "invalid or unsupported command"
                );
                session.channel_failure(channel)?;
            }
        }

        Ok(())
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        _data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        tracing::debug!(peer_addr = ?self.peer_addr, channel = ?channel, "channel eof");
        session.eof(channel)?;
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        tracing::debug!(peer_addr = ?self.peer_addr, channel = ?channel, "channel close");
        session.close(channel)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::rate_limit::AuthRateLimiter;
    use crate::auth::types::{SshKey, StoredToken, TokenScopes, User, UserStatus};
    use crate::auth::AuthError;
    use std::collections::HashMap;
    use std::sync::RwLock;

    struct MockAuthStore {
        users: RwLock<HashMap<String, User>>,
        ssh_keys: RwLock<HashMap<String, SshKey>>,
    }

    impl MockAuthStore {
        fn new() -> Self {
            Self {
                users: RwLock::new(HashMap::new()),
                ssh_keys: RwLock::new(HashMap::new()),
            }
        }

        fn add_user(&self, user: User) {
            self.users
                .write()
                .unwrap()
                .insert(user.user_id.clone(), user);
        }

        fn add_ssh_key(&self, fingerprint: &str, user_id: &str) {
            let key = SshKey {
                key_id: format!("key_{}", fingerprint),
                user_id: user_id.to_string(),
                fingerprint: fingerprint.to_string(),
                public_key: "ssh-ed25519 AAAA...".to_string(),
                title: "Test Key".to_string(),
                scopes: TokenScopes {
                    read: true,
                    write: true,
                    admin: false,
                },
                created_at: 0,
            };
            self.ssh_keys
                .write()
                .unwrap()
                .insert(fingerprint.to_string(), key);
        }
    }

    impl AuthStore for MockAuthStore {
        fn get_token_by_hash(&self, _hash: &str) -> Result<Option<StoredToken>, AuthError> {
            Ok(None)
        }

        fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError> {
            Ok(self.users.read().unwrap().get(user_id).cloned())
        }

        fn get_ssh_key(&self, fingerprint: &str) -> Result<Option<SshKey>, AuthError> {
            Ok(self.ssh_keys.read().unwrap().get(fingerprint).cloned())
        }
    }

    fn create_test_user(user_id: &str) -> User {
        User {
            user_id: user_id.to_string(),
            name: "Test User".to_string(),
            email: "test@example.com".to_string(),
            password_hash: "hash".to_string(),
            status: UserStatus::Active,
            created_at: 0,
        }
    }

    #[test]
    fn test_handler_new() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let addr: SocketAddr = "127.0.0.1:22".parse().unwrap();
        let handler: GitSshHandler<MockAuthStore> = GitSshHandler::new(Some(addr), validator);

        assert_eq!(handler.peer_addr(), Some(addr));
        assert!(handler.auth_result().is_none());
    }

    #[test]
    fn test_handler_no_peer_addr() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let handler: GitSshHandler<MockAuthStore> = GitSshHandler::new(None, validator);

        assert!(handler.peer_addr().is_none());
    }

    #[tokio::test]
    async fn test_auth_publickey_success() {
        let store = Arc::new(MockAuthStore::new());
        store.add_user(create_test_user("user1"));
        store.add_ssh_key("SHA256:test_fingerprint", "user1");

        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let mut handler: GitSshHandler<MockAuthStore> = GitSshHandler::new(None, validator);

        let validation_result = handler
            .validator
            .validate_ssh_key("SHA256:test_fingerprint");
        assert!(validation_result.is_ok());

        let result = validation_result.unwrap();
        handler.auth_result = Some(result);

        assert!(handler.auth_result().is_some());
        assert_eq!(handler.auth_result().unwrap().user_id, "user1");
    }

    #[tokio::test]
    async fn test_auth_publickey_failure() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let handler: GitSshHandler<MockAuthStore> = GitSshHandler::new(None, validator);

        let validation_result = handler
            .validator
            .validate_ssh_key("SHA256:unknown_fingerprint");
        assert!(validation_result.is_err());
    }

    #[test]
    fn test_handler_with_socket_addr() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let addr: SocketAddr = "192.168.1.100:12345".parse().unwrap();
        let handler: GitSshHandler<MockAuthStore> = GitSshHandler::new(Some(addr), validator);

        assert_eq!(
            handler.peer_addr().unwrap().ip().to_string(),
            "192.168.1.100"
        );
        assert_eq!(handler.peer_addr().unwrap().port(), 12345);
    }

    #[test]
    fn test_git_operation_as_str() {
        assert_eq!(GitOperation::UploadPack.as_str(), "upload-pack");
        assert_eq!(GitOperation::ReceivePack.as_str(), "receive-pack");
    }

    #[test]
    fn test_git_operation_debug() {
        let op = GitOperation::UploadPack;
        let debug_str = format!("{:?}", op);
        assert!(debug_str.contains("UploadPack"));
    }

    #[test]
    fn test_git_command_equality() {
        let cmd1 = GitCommand {
            operation: GitOperation::UploadPack,
            repo_id: "test/repo".to_string(),
        };
        let cmd2 = GitCommand {
            operation: GitOperation::UploadPack,
            repo_id: "test/repo".to_string(),
        };
        assert_eq!(cmd1, cmd2);
    }

    #[test]
    fn test_git_command_inequality() {
        let cmd1 = GitCommand {
            operation: GitOperation::UploadPack,
            repo_id: "test/repo".to_string(),
        };
        let cmd2 = GitCommand {
            operation: GitOperation::ReceivePack,
            repo_id: "test/repo".to_string(),
        };
        assert_ne!(cmd1, cmd2);
    }

    #[test]
    fn test_parse_git_upload_pack_single_quotes() {
        let result = parse_git_command("git-upload-pack '/repo/path'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.operation, GitOperation::UploadPack);
        assert_eq!(cmd.repo_id, "repo/path");
    }

    #[test]
    fn test_parse_git_receive_pack_single_quotes() {
        let result = parse_git_command("git-receive-pack '/my/repo'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.operation, GitOperation::ReceivePack);
        assert_eq!(cmd.repo_id, "my/repo");
    }

    #[test]
    fn test_parse_git_upload_pack_double_quotes() {
        let result = parse_git_command("git-upload-pack \"/repo/path\"");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.operation, GitOperation::UploadPack);
        assert_eq!(cmd.repo_id, "repo/path");
    }

    #[test]
    fn test_parse_git_upload_pack_no_quotes() {
        let result = parse_git_command("git-upload-pack /repo/path");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.operation, GitOperation::UploadPack);
        assert_eq!(cmd.repo_id, "repo/path");
    }

    #[test]
    fn test_parse_git_space_form_upload_pack() {
        let result = parse_git_command("git upload-pack '/repo/path'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.operation, GitOperation::UploadPack);
        assert_eq!(cmd.repo_id, "repo/path");
    }

    #[test]
    fn test_parse_git_space_form_receive_pack() {
        let result = parse_git_command("git receive-pack '/repo/path'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.operation, GitOperation::ReceivePack);
        assert_eq!(cmd.repo_id, "repo/path");
    }

    #[test]
    fn test_parse_simple_repo_name() {
        let result = parse_git_command("git-upload-pack 'myrepo'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "myrepo");
    }

    #[test]
    fn test_parse_repo_with_git_suffix() {
        let result = parse_git_command("git-upload-pack '/user/repo.git'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "user/repo.git");
    }

    #[test]
    fn test_parse_command_with_whitespace() {
        let result = parse_git_command("  git-upload-pack '/repo'  ");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "repo");
    }

    #[test]
    fn test_parse_invalid_command() {
        assert!(parse_git_command("ls -la").is_none());
    }

    #[test]
    fn test_parse_empty_command() {
        assert!(parse_git_command("").is_none());
    }

    #[test]
    fn test_parse_git_only() {
        assert!(parse_git_command("git").is_none());
    }

    #[test]
    fn test_parse_empty_path() {
        assert!(parse_git_command("git-upload-pack ''").is_none());
    }

    #[test]
    fn test_parse_only_slash() {
        assert!(parse_git_command("git-upload-pack '/'").is_none());
    }

    #[test]
    fn test_parse_partial_command() {
        assert!(parse_git_command("git-upload").is_none());
    }

    #[test]
    fn test_parse_deeply_nested_path() {
        let result = parse_git_command("git-upload-pack '/a/b/c/d/e/f/repo'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "a/b/c/d/e/f/repo");
    }

    #[test]
    fn test_parse_path_with_dots() {
        let result = parse_git_command("git-upload-pack '/user/my.repo.git'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "user/my.repo.git");
    }

    #[test]
    fn test_parse_path_with_dashes() {
        let result = parse_git_command("git-upload-pack '/user-name/my-repo'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "user-name/my-repo");
    }

    #[test]
    fn test_parse_multiple_leading_slashes() {
        let result = parse_git_command("git-upload-pack '///repo'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "repo");
    }

    #[test]
    fn test_parse_numeric_repo_name() {
        let result = parse_git_command("git-upload-pack '/12345/67890'");
        assert!(result.is_some());
        let cmd = result.unwrap();
        assert_eq!(cmd.repo_id, "12345/67890");
    }
}
