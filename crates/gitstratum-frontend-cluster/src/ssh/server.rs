use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use russh::keys::ssh_key::PrivateKey;
use russh::server::Config;
use russh::MethodSet;

use crate::auth::{AuthStore, LocalValidator};

use super::handler::GitSshHandler;

#[derive(Debug, Clone)]
pub struct SshServerConfig {
    pub listen_addr: SocketAddr,
    pub host_key_path: PathBuf,
    pub inactivity_timeout: Option<Duration>,
    pub auth_rejection_time: Duration,
}

impl SshServerConfig {
    pub fn new(listen_addr: SocketAddr, host_key_path: PathBuf) -> Self {
        Self {
            listen_addr,
            host_key_path,
            inactivity_timeout: Some(Duration::from_secs(600)),
            auth_rejection_time: Duration::from_secs(1),
        }
    }

    pub fn with_inactivity_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.inactivity_timeout = timeout;
        self
    }

    pub fn with_auth_rejection_time(mut self, time: Duration) -> Self {
        self.auth_rejection_time = time;
        self
    }
}

impl Default for SshServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:22".parse().unwrap(),
            host_key_path: PathBuf::from("/etc/ssh/ssh_host_ed25519_key"),
            inactivity_timeout: Some(Duration::from_secs(600)),
            auth_rejection_time: Duration::from_secs(1),
        }
    }
}

pub struct SshServer<S: AuthStore> {
    config: SshServerConfig,
    validator: Arc<LocalValidator<S>>,
}

impl<S: AuthStore + 'static> SshServer<S> {
    pub fn new(config: SshServerConfig, validator: Arc<LocalValidator<S>>) -> Self {
        Self { config, validator }
    }

    pub fn config(&self) -> &SshServerConfig {
        &self.config
    }

    pub async fn run(&mut self) -> Result<(), std::io::Error> {
        let host_key = load_host_key(&self.config.host_key_path)?;

        let russh_config = Config {
            server_id: russh::SshId::Standard(format!(
                "SSH-2.0-GitStratum_{}",
                env!("CARGO_PKG_VERSION")
            )),
            methods: MethodSet::PUBLICKEY,
            auth_banner: None,
            auth_rejection_time: self.config.auth_rejection_time,
            auth_rejection_time_initial: Some(Duration::from_millis(0)),
            keys: vec![host_key],
            inactivity_timeout: self.config.inactivity_timeout,
            ..Default::default()
        };

        let russh_config = Arc::new(russh_config);

        tracing::info!(
            listen_addr = %self.config.listen_addr,
            "starting ssh server"
        );

        russh::server::Server::run_on_address(self, russh_config, self.config.listen_addr).await
    }
}

impl<S: AuthStore + 'static> russh::server::Server for SshServer<S> {
    type Handler = GitSshHandler<S>;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        tracing::debug!(peer_addr = ?peer_addr, "new ssh client connection");
        GitSshHandler::new(peer_addr, Arc::clone(&self.validator))
    }

    fn handle_session_error(&mut self, error: <Self::Handler as russh::server::Handler>::Error) {
        tracing::error!(error = %error, "ssh session error");
    }
}

pub fn load_host_key(path: &std::path::Path) -> Result<PrivateKey, std::io::Error> {
    tracing::debug!(path = %path.display(), "loading ssh host key");

    PrivateKey::read_openssh_file(path).map_err(|e| {
        tracing::error!(path = %path.display(), error = %e, "failed to load ssh host key");
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::rate_limit::AuthRateLimiter;
    use crate::auth::types::{StoredToken, User};
    use crate::auth::AuthError;
    use russh::keys::ssh_key;
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::RwLock;
    use tempfile::NamedTempFile;

    struct MockAuthStore {
        users: RwLock<HashMap<String, User>>,
        ssh_keys: RwLock<HashMap<String, String>>,
    }

    impl MockAuthStore {
        fn new() -> Self {
            Self {
                users: RwLock::new(HashMap::new()),
                ssh_keys: RwLock::new(HashMap::new()),
            }
        }
    }

    impl AuthStore for MockAuthStore {
        fn get_token_by_hash(&self, _hash: &str) -> Result<Option<StoredToken>, AuthError> {
            Ok(None)
        }

        fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError> {
            Ok(self.users.read().unwrap().get(user_id).cloned())
        }

        fn get_ssh_key_user(&self, fingerprint: &str) -> Result<Option<String>, AuthError> {
            Ok(self.ssh_keys.read().unwrap().get(fingerprint).cloned())
        }
    }

    #[test]
    fn test_ssh_server_config_new() {
        let addr: SocketAddr = "127.0.0.1:2222".parse().unwrap();
        let path = PathBuf::from("/path/to/key");
        let config = SshServerConfig::new(addr, path.clone());

        assert_eq!(config.listen_addr, addr);
        assert_eq!(config.host_key_path, path);
        assert_eq!(config.inactivity_timeout, Some(Duration::from_secs(600)));
        assert_eq!(config.auth_rejection_time, Duration::from_secs(1));
    }

    #[test]
    fn test_ssh_server_config_default() {
        let config = SshServerConfig::default();

        assert_eq!(config.listen_addr.port(), 22);
        assert_eq!(
            config.host_key_path,
            PathBuf::from("/etc/ssh/ssh_host_ed25519_key")
        );
        assert_eq!(config.inactivity_timeout, Some(Duration::from_secs(600)));
        assert_eq!(config.auth_rejection_time, Duration::from_secs(1));
    }

    #[test]
    fn test_ssh_server_config_with_inactivity_timeout() {
        let addr: SocketAddr = "127.0.0.1:2222".parse().unwrap();
        let path = PathBuf::from("/path/to/key");
        let config = SshServerConfig::new(addr, path)
            .with_inactivity_timeout(Some(Duration::from_secs(300)));

        assert_eq!(config.inactivity_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_ssh_server_config_with_no_inactivity_timeout() {
        let addr: SocketAddr = "127.0.0.1:2222".parse().unwrap();
        let path = PathBuf::from("/path/to/key");
        let config = SshServerConfig::new(addr, path).with_inactivity_timeout(None);

        assert!(config.inactivity_timeout.is_none());
    }

    #[test]
    fn test_ssh_server_config_with_auth_rejection_time() {
        let addr: SocketAddr = "127.0.0.1:2222".parse().unwrap();
        let path = PathBuf::from("/path/to/key");
        let config =
            SshServerConfig::new(addr, path).with_auth_rejection_time(Duration::from_secs(3));

        assert_eq!(config.auth_rejection_time, Duration::from_secs(3));
    }

    #[test]
    fn test_ssh_server_config_debug() {
        let config = SshServerConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SshServerConfig"));
        assert!(debug_str.contains("listen_addr"));
    }

    #[test]
    fn test_ssh_server_config_clone() {
        let config = SshServerConfig::default();
        let cloned = config.clone();
        assert_eq!(config.listen_addr, cloned.listen_addr);
        assert_eq!(config.host_key_path, cloned.host_key_path);
    }

    #[test]
    fn test_ssh_server_new() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let config = SshServerConfig::default();
        let server: SshServer<MockAuthStore> = SshServer::new(config.clone(), validator);

        assert_eq!(server.config().listen_addr, config.listen_addr);
    }

    #[test]
    fn test_ssh_server_config_accessor() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let addr: SocketAddr = "192.168.1.1:2222".parse().unwrap();
        let path = PathBuf::from("/custom/path");
        let config = SshServerConfig::new(addr, path.clone());

        let server: SshServer<MockAuthStore> = SshServer::new(config, validator);

        assert_eq!(server.config().listen_addr, addr);
        assert_eq!(server.config().host_key_path, path);
    }

    #[test]
    fn test_load_host_key_success() {
        let ed25519_key = concat!(
            "-----BEGIN OPENSSH PRIVATE KEY-----\n",
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n",
            "QyNTUxOQAAACAYKpHwiE1Gl+khe8XRKD65C+/NANANLvag74IHPxMxUgAAAKAKvKGFCryh\n",
            "hQAAAAtzc2gtZWQyNTUxOQAAACAYKpHwiE1Gl+khe8XRKD65C+/NANANLvag74IHPxMxUg\n",
            "AAAECMO/bzcNLGTGWvb8/dtmfSgWD+KXqBnE8lAyPgrqvtzxgqkfCITUaX6SF7xdEoPrkL\n",
            "780A0A0u9qDvggc/EzFSAAAAHGFsZXhAQWxleHMtTWFjQm9vay1Qcm8ubG9jYWwB\n",
            "-----END OPENSSH PRIVATE KEY-----\n",
        );

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(ed25519_key.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = load_host_key(temp_file.path());
        assert!(result.is_ok());

        let key = result.unwrap();
        assert_eq!(key.algorithm(), ssh_key::Algorithm::Ed25519);
    }

    #[test]
    fn test_load_host_key_file_not_found() {
        let result = load_host_key(std::path::Path::new("/nonexistent/path/to/key"));
        assert!(result.is_err());
    }

    #[test]
    fn test_load_host_key_invalid_format() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"invalid key data").unwrap();
        temp_file.flush().unwrap();

        let result = load_host_key(temp_file.path());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_load_host_key_rsa() {
        let rsa_key = concat!(
            "-----BEGIN OPENSSH PRIVATE KEY-----\n",
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABFwAAAAdzc2gtcn\n",
            "NhAAAAAwEAAQAAAQEA481TW+4BChlwNXeQEys4tG0Sz98itjTKMPyXSkA9dwhHtEQRGfvm\n",
            "48XE34+Itaz4pYppekYv+4qYQrTA5wOQpRH0NiHTbH8D/vHSoP65TiOomYw+p6F3XhSszy\n",
            "t1IUaD1gtrTpla1V8W9WFsERM4AWzKDbIh6uED/O4KBvghhypcWmAkkHTICbLdVlnnx1QV\n",
            "T4DfV6tGV9tiEwCmC/NhjuxVhx7z1lowiBpMYVHyhFIFcFZeQGcelL+M+HKI29l9AWq00s\n",
            "chYkKJ4NOOlPGvysTGrMy0cIFTbP1o3GuxTALVamJ/JdzjJOU3oAXRFgaCh1qRAdaebU7t\n",
            "WghXECL6swAAA9gpKUULKSlFCwAAAAdzc2gtcnNhAAABAQDjzVNb7gEKGXA1d5ATKzi0bR\n",
            "LP3yK2NMow/JdKQD13CEe0RBEZ++bjxcTfj4i1rPiliml6Ri/7iphCtMDnA5ClEfQ2IdNs\n",
            "fwP+8dKg/rlOI6iZjD6noXdeFKzPK3UhRoPWC2tOmVrVXxb1YWwREzgBbMoNsiHq4QP87g\n",
            "oG+CGHKlxaYCSQdMgJst1WWefHVBVPgN9Xq0ZX22ITAKYL82GO7FWHHvPWWjCIGkxhUfKE\n",
            "UgVwVl5AZx6Uv4z4cojb2X0BarTSxyFiQong046U8a/KxMaszLRwgVNs/Wjca7FMAtVqYn\n",
            "8l3OMk5TegBdEWBoKHWpEB1p5tTu1aCFcQIvqzAAAAAwEAAQAAAQA686fe4njiZDLlo0tl\n",
            "qonCJ3f204foH0Ez7Co6zOUbKMllnTfPwaC+0S9hq4N1gI3YSTmCqyc/sV415REGt6V3Em\n",
            "5gk+Bi83vVPj+D6meKETBQjDqqpt59Olx+QIDPW7BCdDIQ5R7cmP8YAV42DQMxWzXu1Wpx\n",
            "nkmp6vAtXYlPsOm3fgyStcdp/85k8z1/TWqgoRMmRU94HyArhv3Xkc+0CXzDukra2qVfjD\n",
            "Ooe4wP2+lnJqU9kX85Jg3Xm/Fgwja2Z0pp/HUFBI5CxNeXydcjj367Xy2d6ZrzmodTfB2F\n",
            "2IeanAOO7QmgfW2uC+b7E7sNs6ggMzzHUroqciAi+eABAAAAgQCTR4sghrV333iU86e0//\n",
            "CnuDw7IRQzqj2SCYbTj7XrVSKcTY5v/NA0TNnStBlf62KzteVmLfhCZaQMDi8sAnfp0w0V\n",
            "sBzc2xr2vnrRTXBdcsSrP7ymqq6ixV/dCP1aiL2W2vb/AeroDnJt9Y/bEXNhGUg55eslDE\n",
            "88HZPM40f+qgAAAIEA8vP2eCIxwwSu1/B4PKq9dqNkJ8YiaaNGdhKrQiK7kEWiroTgc2BY\n",
            "RdCRxpnGoWdk951Ju1dqPFKm3TCaSS7ta+RjnyJa/VaOYhurN0zT1NlBx7gO44zmkGhI2X\n",
            "/uUDrNjpCi0xXGRQjGi4QayGWmZDSDhkX7eahv/WKlLcBPVcEAAACBAPAJEqs2MjPgMoF2\n",
            "R4xmzvuu1Wosso5+vw+VenD5Z4bQJxq7277vKYVe4CRhZdB1MrXeZkp0PLDUXxoUfzzTwD\n",
            "2pncY2mROZzunVSrhrWvkr21qQycF59LYE3TXKkO0c82IYAOhGa0Ihzz5KYoFWnSC3kaQI\n",
            "udxl9xDatCYYXrVzAAAAHGFsZXhAQWxleHMtTWFjQm9vay1Qcm8ubG9jYWwBAgMEBQY=\n",
            "-----END OPENSSH PRIVATE KEY-----\n",
        );

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(rsa_key.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = load_host_key(temp_file.path());
        assert!(result.is_ok());

        let key = result.unwrap();
        assert!(key.algorithm().is_rsa());
    }

    #[test]
    fn test_ssh_server_new_client() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let config = SshServerConfig::default();
        let mut server: SshServer<MockAuthStore> = SshServer::new(config, validator);

        let addr: SocketAddr = "192.168.1.100:12345".parse().unwrap();
        let handler = russh::server::Server::new_client(&mut server, Some(addr));

        assert_eq!(handler.peer_addr(), Some(addr));
    }

    #[test]
    fn test_ssh_server_new_client_no_addr() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let config = SshServerConfig::default();
        let mut server: SshServer<MockAuthStore> = SshServer::new(config, validator);

        let handler = russh::server::Server::new_client(&mut server, None);

        assert!(handler.peer_addr().is_none());
    }

    #[test]
    fn test_ssh_server_handle_session_error() {
        let store = Arc::new(MockAuthStore::new());
        let rate_limiter = Arc::new(AuthRateLimiter::new(5, 300, 900));
        let validator = Arc::new(LocalValidator::new(store, rate_limiter));

        let config = SshServerConfig::default();
        let mut server: SshServer<MockAuthStore> = SshServer::new(config, validator);

        let error = russh::Error::Disconnect;
        russh::server::Server::handle_session_error(&mut server, error);
    }

    #[test]
    fn test_ssh_server_config_builder_pattern() {
        let addr: SocketAddr = "127.0.0.1:2222".parse().unwrap();
        let path = PathBuf::from("/path/to/key");

        let config = SshServerConfig::new(addr, path)
            .with_inactivity_timeout(Some(Duration::from_secs(1200)))
            .with_auth_rejection_time(Duration::from_secs(2));

        assert_eq!(config.inactivity_timeout, Some(Duration::from_secs(1200)));
        assert_eq!(config.auth_rejection_time, Duration::from_secs(2));
    }
}
