use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::RwLock;
use tonic::Status;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("Failed to fetch CRL from {url}: {message}")]
    CrlFetchFailed { url: String, message: String },

    #[error("Failed to parse CRL: {0}")]
    CrlParseFailed(String),

    #[error("CRL is stale: last fetched {elapsed:?} ago, max age is {max_age:?}")]
    StaleCrl {
        elapsed: Duration,
        max_age: Duration,
    },
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
            Err(TlsError::CrlFetchFailed {
                url: self.crl_url.clone(),
                message: "CRL cache is empty after refresh".to_string(),
            })
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
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(TlsError::CrlFetchFailed {
                url: self.crl_url.clone(),
                message: format!("HTTP status: {}", response.status()),
            });
        }

        let body = response
            .text()
            .await
            .map_err(|e| TlsError::CrlFetchFailed {
                url: self.crl_url.clone(),
                message: e.to_string(),
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

    #[test]
    fn test_validate_node_identity_success() {
        let node = NodeInfo {
            id: "node-1".to_string(),
            address: "192.168.1.10".to_string(),
            port: 8080,
            state: NodeState::Active as i32,
            r#type: NodeType::Object as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "gen-1".to_string(),
            registered_at: 0,
        };

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
        let node = NodeInfo {
            id: "node-1".to_string(),
            address: "192.168.1.10".to_string(),
            port: 8080,
            state: NodeState::Active as i32,
            r#type: NodeType::Object as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "gen-1".to_string(),
            registered_at: 0,
        };

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
        let node = NodeInfo {
            id: "node-1".to_string(),
            address: "192.168.1.10".to_string(),
            port: 8080,
            state: NodeState::Active as i32,
            r#type: NodeType::Object as i32,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "gen-1".to_string(),
            registered_at: 0,
        };

        let cert_cn = "node-1";
        let cert_sans = vec!["192.168.1.99".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());

        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().contains("not in certificate SANs"));
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

    #[test]
    fn test_tls_error_display() {
        let fetch_error = TlsError::CrlFetchFailed {
            url: "https://example.com/crl".to_string(),
            message: "connection refused".to_string(),
        };
        assert!(fetch_error.to_string().contains("Failed to fetch CRL"));

        let parse_error = TlsError::CrlParseFailed("invalid format".to_string());
        assert!(parse_error.to_string().contains("Failed to parse CRL"));

        let stale_error = TlsError::StaleCrl {
            elapsed: Duration::from_secs(7200),
            max_age: Duration::from_secs(3600),
        };
        assert!(stale_error.to_string().contains("CRL is stale"));
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
}
