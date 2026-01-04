use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::RwLock;
use tonic::Status;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("failed to fetch CRL from {url}")]
    CrlFetchFailed {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("CRL fetch returned HTTP error from {url}: {status}")]
    CrlHttpError { url: String, status: String },

    #[error("Failed to parse CRL: {0}")]
    CrlParseFailed(String),

    #[error("CRL is stale: last fetched {elapsed:?} ago, max age is {max_age:?}")]
    StaleCrl {
        elapsed: Duration,
        max_age: Duration,
    },

    #[error("CRL cache is empty after refresh")]
    CrlCacheEmpty,
}

#[derive(Debug, Clone)]
pub struct CachedCrl {
    pub revoked_serials: Vec<String>,
    pub fetched_at: Instant,
}

pub struct CrlChecker {
    crl_cache: Arc<RwLock<Option<CachedCrl>>>,
    crl_url: String,
    refresh_interval: Duration,
    max_age: Duration,
}

impl CrlChecker {
    pub fn new(url: String, refresh_interval: Duration, max_age: Duration) -> Self {
        Self {
            crl_cache: Arc::new(RwLock::new(None)),
            crl_url: url,
            refresh_interval,
            max_age,
        }
    }

    pub async fn is_certificate_revoked(&self, serial: &str) -> Result<bool, TlsError> {
        let cache = self.crl_cache.read().await;

        if let Some(cached) = cache.as_ref() {
            let elapsed = cached.fetched_at.elapsed();

            if elapsed < self.refresh_interval {
                return Ok(cached.revoked_serials.iter().any(|s| s == serial));
            }

            if elapsed > self.max_age {
                return Err(TlsError::StaleCrl {
                    elapsed,
                    max_age: self.max_age,
                });
            }
        }
        drop(cache);

        self.refresh_crl().await?;

        let cache = self.crl_cache.read().await;
        if let Some(cached) = cache.as_ref() {
            Ok(cached.revoked_serials.iter().any(|s| s == serial))
        } else {
            Err(TlsError::CrlCacheEmpty)
        }
    }

    pub async fn refresh_crl(&self) -> Result<(), TlsError> {
        let mut cache = self.crl_cache.write().await;

        if let Some(cached) = cache.as_ref() {
            if cached.fetched_at.elapsed() < self.refresh_interval {
                return Ok(());
            }
        }

        let revoked_serials = self.fetch_and_parse_crl().await?;

        *cache = Some(CachedCrl {
            revoked_serials,
            fetched_at: Instant::now(),
        });

        Ok(())
    }

    async fn fetch_and_parse_crl(&self) -> Result<Vec<String>, TlsError> {
        let response = reqwest::get(&self.crl_url)
            .await
            .map_err(|e| TlsError::CrlFetchFailed {
                url: self.crl_url.clone(),
                source: e,
            })?;

        if !response.status().is_success() {
            return Err(TlsError::CrlHttpError {
                url: self.crl_url.clone(),
                status: response.status().to_string(),
            });
        }

        let body = response
            .text()
            .await
            .map_err(|e| TlsError::CrlFetchFailed {
                url: self.crl_url.clone(),
                source: e,
            })?;

        parse_crl_serials(&body)
    }

    pub fn crl_url(&self) -> &str {
        &self.crl_url
    }

    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    pub fn max_age(&self) -> Duration {
        self.max_age
    }
}

fn parse_crl_serials(pem_content: &str) -> Result<Vec<String>, TlsError> {
    let mut serials = Vec::new();

    for line in pem_content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("Serial Number:") {
            if let Some(serial) = trimmed.strip_prefix("Serial Number:") {
                serials.push(serial.trim().to_string());
            }
        }
    }

    Ok(serials)
}

pub fn validate_node_identity(
    cert_cn: &str,
    cert_sans: &[String],
    node: &gitstratum_proto::NodeInfo,
) -> Result<(), Status> {
    if node.id != cert_cn {
        return Err(Status::permission_denied(format!(
            "Node ID '{}' does not match certificate CN '{}'",
            node.id, cert_cn
        )));
    }

    if !cert_sans.contains(&node.address) {
        return Err(Status::permission_denied(format!(
            "Node address '{}' not in certificate SANs",
            node.address
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_proto::{NodeInfo, NodeState, NodeType};

    fn create_test_node(id: &str, address: &str) -> NodeInfo {
        NodeInfo {
            id: id.to_string(),
            address: address.to_string(),
            port: 8080,
            state: NodeState::Active as i32,
            r#type: NodeType::Object as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "gen-1".to_string(),
            registered_at: 0,
        }
    }

    #[test]
    fn test_validate_node_identity_success() {
        let node = create_test_node("node-1", "192.168.1.10");
        let cert_cn = "node-1";
        let cert_sans = vec![
            "192.168.1.10".to_string(),
            "node-1.cluster.local".to_string(),
        ];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_node_identity_cn_mismatch() {
        let node = create_test_node("node-1", "192.168.1.10");
        let cert_cn = "node-2";
        let cert_sans = vec!["192.168.1.10".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().contains("does not match certificate CN"));
    }

    #[test]
    fn test_validate_node_identity_san_mismatch() {
        let node = create_test_node("node-1", "192.168.1.10");
        let cert_cn = "node-1";
        let cert_sans = vec!["192.168.1.99".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().contains("not in certificate SANs"));
    }

    #[test]
    fn test_validate_node_identity_empty_sans() {
        let node = create_test_node("node-1", "192.168.1.10");
        let cert_cn = "node-1";
        let cert_sans: Vec<String> = vec![];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn test_validate_node_identity_empty_node_id() {
        let node = create_test_node("", "192.168.1.10");
        let cert_cn = "";
        let cert_sans = vec!["192.168.1.10".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_node_identity_special_characters() {
        let node = create_test_node("node-with-dashes_and_underscores.123", "10.0.0.1");
        let cert_cn = "node-with-dashes_and_underscores.123";
        let cert_sans = vec!["10.0.0.1".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_crl_serials() {
        let crl_content = r#"
Certificate Revocation List (CRL):
    Version 2 (0x1)
    Signature Algorithm: sha256WithRSAEncryption
Revoked Certificates:
    Serial Number: 1234567890ABCDEF
        Revocation Date: Jan  1 00:00:00 2024 GMT
    Serial Number: FEDCBA0987654321
        Revocation Date: Jan  2 00:00:00 2024 GMT
"#;

        let serials = parse_crl_serials(crl_content).unwrap();
        assert_eq!(serials.len(), 2);
        assert_eq!(serials[0], "1234567890ABCDEF");
        assert_eq!(serials[1], "FEDCBA0987654321");
    }

    #[test]
    fn test_parse_crl_serials_empty() {
        let crl_content = r#"
Certificate Revocation List (CRL):
    Version 2 (0x1)
    Signature Algorithm: sha256WithRSAEncryption
No Revoked Certificates.
"#;

        let serials = parse_crl_serials(crl_content).unwrap();
        assert!(serials.is_empty());
    }

    #[test]
    fn test_parse_crl_serials_empty_string() {
        let serials = parse_crl_serials("").unwrap();
        assert!(serials.is_empty());
    }

    #[test]
    fn test_parse_crl_serials_whitespace_only() {
        let serials = parse_crl_serials("   \n\t\n   ").unwrap();
        assert!(serials.is_empty());
    }

    #[test]
    fn test_parse_crl_serials_with_extra_whitespace() {
        let crl_content = "    Serial Number:    ABC123    \n  Serial Number:DEF456";
        let serials = parse_crl_serials(crl_content).unwrap();
        assert_eq!(serials.len(), 2);
        assert_eq!(serials[0], "ABC123");
        assert_eq!(serials[1], "DEF456");
    }

    #[test]
    fn test_parse_crl_serials_single_serial() {
        let crl_content = "Serial Number: SINGLE123";
        let serials = parse_crl_serials(crl_content).unwrap();
        assert_eq!(serials.len(), 1);
        assert_eq!(serials[0], "SINGLE123");
    }

    #[test]
    fn test_parse_crl_serials_no_serial_number_prefix() {
        let crl_content = "This is not a CRL\nJust random text\n123456";
        let serials = parse_crl_serials(crl_content).unwrap();
        assert!(serials.is_empty());
    }

    #[test]
    fn test_parse_crl_serials_partial_match() {
        let crl_content = "Serial Number without colon\nSerial: Not correct format";
        let serials = parse_crl_serials(crl_content).unwrap();
        assert!(serials.is_empty());
    }

    #[test]
    fn test_parse_crl_serials_lowercase_prefix() {
        let crl_content = "serial number: lowercase123";
        let serials = parse_crl_serials(crl_content).unwrap();
        assert!(serials.is_empty());
    }

    #[tokio::test]
    async fn test_crl_checker_new() {
        let checker = CrlChecker::new(
            "https://ca.example.com/crl.pem".to_string(),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );

        assert_eq!(checker.crl_url(), "https://ca.example.com/crl.pem");
        assert_eq!(checker.refresh_interval(), Duration::from_secs(300));
        assert_eq!(checker.max_age(), Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn test_crl_checker_accessors() {
        let url = "https://test.example.com/crl";
        let refresh = Duration::from_secs(60);
        let max_age = Duration::from_secs(120);

        let checker = CrlChecker::new(url.to_string(), refresh, max_age);

        assert_eq!(checker.crl_url(), url);
        assert_eq!(checker.refresh_interval(), refresh);
        assert_eq!(checker.max_age(), max_age);
    }

    #[tokio::test]
    async fn test_crl_checker_with_zero_durations() {
        let checker = CrlChecker::new(
            "https://ca.example.com/crl".to_string(),
            Duration::ZERO,
            Duration::ZERO,
        );

        assert_eq!(checker.refresh_interval(), Duration::ZERO);
        assert_eq!(checker.max_age(), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_crl_checker_is_certificate_revoked_with_prepopulated_cache() {
        let checker = CrlChecker::new(
            "https://ca.example.com/crl".to_string(),
            Duration::from_secs(3600),
            Duration::from_secs(7200),
        );

        {
            let mut cache = checker.crl_cache.write().await;
            *cache = Some(CachedCrl {
                revoked_serials: vec!["REVOKED001".to_string(), "REVOKED002".to_string()],
                fetched_at: Instant::now(),
            });
        }

        let result = checker.is_certificate_revoked("REVOKED001").await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = checker.is_certificate_revoked("REVOKED002").await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = checker.is_certificate_revoked("VALID001").await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_crl_checker_is_certificate_revoked_empty_cache_triggers_refresh() {
        let checker = CrlChecker::new(
            "http://127.0.0.1:1/nonexistent".to_string(),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );

        let result = checker.is_certificate_revoked("TEST123").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            TlsError::CrlFetchFailed { url, source: _ } => {
                assert_eq!(url, "http://127.0.0.1:1/nonexistent");
            }
            _ => panic!("Expected CrlFetchFailed error"),
        }
    }

    #[tokio::test]
    async fn test_crl_checker_stale_cache_returns_error() {
        let checker = CrlChecker::new(
            "http://127.0.0.1:1/nonexistent".to_string(),
            Duration::from_millis(1),
            Duration::from_millis(2),
        );

        {
            let mut cache = checker.crl_cache.write().await;
            *cache = Some(CachedCrl {
                revoked_serials: vec!["SERIAL123".to_string()],
                fetched_at: Instant::now() - Duration::from_secs(10),
            });
        }

        let result = checker.is_certificate_revoked("SERIAL123").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            TlsError::StaleCrl {
                elapsed: _,
                max_age,
            } => {
                assert_eq!(max_age, Duration::from_millis(2));
            }
            _ => panic!("Expected StaleCrl error"),
        }
    }

    #[tokio::test]
    async fn test_crl_checker_refresh_needed_but_within_max_age() {
        let checker = CrlChecker::new(
            "http://127.0.0.1:1/nonexistent".to_string(),
            Duration::from_millis(1),
            Duration::from_secs(3600),
        );

        {
            let mut cache = checker.crl_cache.write().await;
            *cache = Some(CachedCrl {
                revoked_serials: vec!["SERIAL123".to_string()],
                fetched_at: Instant::now() - Duration::from_millis(100),
            });
        }

        let result = checker.is_certificate_revoked("SERIAL123").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            TlsError::CrlFetchFailed { .. } => {}
            e => panic!("Expected CrlFetchFailed, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_crl_checker_refresh_crl_skips_if_recent() {
        let checker = CrlChecker::new(
            "http://127.0.0.1:1/nonexistent".to_string(),
            Duration::from_secs(3600),
            Duration::from_secs(7200),
        );

        {
            let mut cache = checker.crl_cache.write().await;
            *cache = Some(CachedCrl {
                revoked_serials: vec!["SERIAL999".to_string()],
                fetched_at: Instant::now(),
            });
        }

        let result = checker.refresh_crl().await;
        assert!(result.is_ok());

        let cache = checker.crl_cache.read().await;
        let cached = cache.as_ref().unwrap();
        assert_eq!(cached.revoked_serials, vec!["SERIAL999".to_string()]);
    }

    #[tokio::test]
    async fn test_crl_checker_refresh_crl_fails_on_network_error() {
        let checker = CrlChecker::new(
            "http://127.0.0.1:1/nonexistent".to_string(),
            Duration::from_millis(1),
            Duration::from_secs(3600),
        );

        {
            let mut cache = checker.crl_cache.write().await;
            *cache = Some(CachedCrl {
                revoked_serials: vec![],
                fetched_at: Instant::now() - Duration::from_secs(10),
            });
        }

        let result = checker.refresh_crl().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_error_display() {
        let http_error = TlsError::CrlHttpError {
            url: "https://example.com/crl".to_string(),
            status: "404 Not Found".to_string(),
        };
        assert!(http_error.to_string().contains("https://example.com/crl"));
        assert!(http_error.to_string().contains("404 Not Found"));

        let parse_error = TlsError::CrlParseFailed("invalid format".to_string());
        assert!(parse_error.to_string().contains("Failed to parse CRL"));
        assert!(parse_error.to_string().contains("invalid format"));

        let stale_error = TlsError::StaleCrl {
            elapsed: Duration::from_secs(7200),
            max_age: Duration::from_secs(3600),
        };
        assert!(stale_error.to_string().contains("CRL is stale"));

        let cache_empty = TlsError::CrlCacheEmpty;
        assert!(cache_empty.to_string().contains("empty after refresh"));
    }

    #[test]
    fn test_tls_error_debug() {
        let http_error = TlsError::CrlHttpError {
            url: "https://example.com/crl".to_string(),
            status: "500".to_string(),
        };
        let debug_str = format!("{:?}", http_error);
        assert!(debug_str.contains("CrlHttpError"));
        assert!(debug_str.contains("https://example.com/crl"));

        let parse_error = TlsError::CrlParseFailed("bad pem".to_string());
        let debug_str = format!("{:?}", parse_error);
        assert!(debug_str.contains("CrlParseFailed"));

        let stale_error = TlsError::StaleCrl {
            elapsed: Duration::from_secs(100),
            max_age: Duration::from_secs(50),
        };
        let debug_str = format!("{:?}", stale_error);
        assert!(debug_str.contains("StaleCrl"));
    }

    #[test]
    fn test_cached_crl_clone() {
        let cached = CachedCrl {
            revoked_serials: vec!["serial1".to_string(), "serial2".to_string()],
            fetched_at: Instant::now(),
        };

        let cloned = cached.clone();
        assert_eq!(cloned.revoked_serials, cached.revoked_serials);
    }

    #[test]
    fn test_cached_crl_debug() {
        let cached = CachedCrl {
            revoked_serials: vec!["ABC123".to_string()],
            fetched_at: Instant::now(),
        };
        let debug_str = format!("{:?}", cached);
        assert!(debug_str.contains("CachedCrl"));
        assert!(debug_str.contains("ABC123"));
    }

    #[test]
    fn test_cached_crl_empty_serials() {
        let cached = CachedCrl {
            revoked_serials: vec![],
            fetched_at: Instant::now(),
        };
        assert!(cached.revoked_serials.is_empty());
    }

    #[test]
    fn test_cached_crl_many_serials() {
        let serials: Vec<String> = (0..1000).map(|i| format!("SERIAL{:06}", i)).collect();
        let cached = CachedCrl {
            revoked_serials: serials.clone(),
            fetched_at: Instant::now(),
        };
        assert_eq!(cached.revoked_serials.len(), 1000);
        assert_eq!(cached.revoked_serials[0], "SERIAL000000");
        assert_eq!(cached.revoked_serials[999], "SERIAL000999");
    }

    #[test]
    fn test_tls_error_crl_http_error_contains_details() {
        let error = TlsError::CrlHttpError {
            url: "https://my-ca.internal/revoked.pem".to_string(),
            status: "503 Service Unavailable".to_string(),
        };
        let display = error.to_string();
        assert!(display.contains("my-ca.internal"));
        assert!(display.contains("503 Service Unavailable"));
    }

    #[test]
    fn test_tls_error_stale_crl_shows_durations() {
        let error = TlsError::StaleCrl {
            elapsed: Duration::from_secs(7200),
            max_age: Duration::from_secs(3600),
        };
        let display = error.to_string();
        assert!(display.contains("7200"));
        assert!(display.contains("3600"));
    }

    #[test]
    fn test_validate_node_identity_error_message_format() {
        let node = create_test_node("actual-node", "192.168.1.1");

        let result = validate_node_identity("expected-node", &["192.168.1.1".to_string()], &node);
        let err = result.unwrap_err();
        assert!(err.message().contains("actual-node"));
        assert!(err.message().contains("expected-node"));

        let node2 = create_test_node("node-1", "192.168.1.1");
        let result2 = validate_node_identity("node-1", &["10.0.0.1".to_string()], &node2);
        let err2 = result2.unwrap_err();
        assert!(err2.message().contains("192.168.1.1"));
    }
}
