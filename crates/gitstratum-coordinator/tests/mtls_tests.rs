use std::time::{Duration, Instant};

use gitstratum_coordinator::{validate_node_identity, CachedCrl, CrlChecker, TlsError};
use gitstratum_proto::{NodeInfo, NodeState, NodeType};

fn create_node_info(id: &str, address: &str) -> NodeInfo {
    NodeInfo {
        id: id.to_string(),
        address: address.to_string(),
        port: 8080,
        state: NodeState::Active as i32,
        r#type: NodeType::Object as i32,
        last_heartbeat_at: 0,
        suspect_count: 0,
        generation_id: format!("gen-{}", id),
        registered_at: 0,
    }
}

mod node_identity_validation_tests {
    use super::*;

    #[test]
    fn test_valid_node_identity_with_ip() {
        let node = create_node_info("worker-1", "10.0.0.50");
        let cert_cn = "worker-1";
        let cert_sans = vec![
            "10.0.0.50".to_string(),
            "worker-1.default.svc.cluster.local".to_string(),
        ];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_node_identity_with_hostname() {
        let node = create_node_info("coordinator-0", "coordinator-0.cluster.local");
        let cert_cn = "coordinator-0";
        let cert_sans = vec![
            "coordinator-0.cluster.local".to_string(),
            "192.168.1.100".to_string(),
        ];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_cn_mismatch() {
        let node = create_node_info("worker-1", "10.0.0.50");
        let cert_cn = "worker-2";
        let cert_sans = vec!["10.0.0.50".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("does not match certificate CN"));
    }

    #[test]
    fn test_reject_san_mismatch() {
        let node = create_node_info("worker-1", "10.0.0.50");
        let cert_cn = "worker-1";
        let cert_sans = vec!["10.0.0.99".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("not in certificate SANs"));
    }

    #[test]
    fn test_multiple_sans_one_matches() {
        let node = create_node_info("db-primary", "db-primary.prod.local");
        let cert_cn = "db-primary";
        let cert_sans = vec![
            "db-primary.dev.local".to_string(),
            "db-primary.staging.local".to_string(),
            "db-primary.prod.local".to_string(),
            "192.168.50.1".to_string(),
        ];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ipv6_address_in_san() {
        let node = create_node_info("node-v6", "::1");
        let cert_cn = "node-v6";
        let cert_sans = vec!["::1".to_string(), "localhost".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_sans_list() {
        let node = create_node_info("worker-1", "10.0.0.50");
        let cert_cn = "worker-1";
        let cert_sans: Vec<String> = vec![];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());
    }

    #[test]
    fn test_case_sensitive_node_id() {
        let node = create_node_info("Worker-1", "10.0.0.50");
        let cert_cn = "worker-1";
        let cert_sans = vec!["10.0.0.50".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(result.is_err());
    }
}

mod crl_checker_tests {
    use super::*;

    #[test]
    fn test_cached_crl_creation() {
        let cached = CachedCrl {
            revoked_serials: vec![
                "ABCD1234".to_string(),
                "EFGH5678".to_string(),
                "IJKL9012".to_string(),
            ],
            fetched_at: Instant::now(),
        };

        assert_eq!(cached.revoked_serials.len(), 3);
        assert!(cached.revoked_serials.contains(&"ABCD1234".to_string()));
    }

    #[test]
    fn test_cached_crl_empty() {
        let cached = CachedCrl {
            revoked_serials: vec![],
            fetched_at: Instant::now(),
        };

        assert!(cached.revoked_serials.is_empty());
    }

    #[tokio::test]
    async fn test_crl_checker_configuration() {
        let checker = CrlChecker::new(
            "https://pki.example.com/crl/root.crl".to_string(),
            Duration::from_secs(600),
            Duration::from_secs(7200),
        );

        assert_eq!(checker.crl_url(), "https://pki.example.com/crl/root.crl");
        assert_eq!(checker.refresh_interval(), Duration::from_secs(600));
        assert_eq!(checker.max_age(), Duration::from_secs(7200));
    }

    #[tokio::test]
    async fn test_crl_checker_short_refresh() {
        let checker = CrlChecker::new(
            "https://ca.local/crl.pem".to_string(),
            Duration::from_secs(30),
            Duration::from_secs(120),
        );

        assert_eq!(checker.refresh_interval(), Duration::from_secs(30));
        assert_eq!(checker.max_age(), Duration::from_secs(120));
    }
}

mod tls_error_tests {
    use super::*;

    #[test]
    fn test_crl_fetch_failed_error() {
        let error = TlsError::CrlFetchFailed {
            url: "https://ca.example.com/crl".to_string(),
            message: "connection timeout".to_string(),
        };

        let display = error.to_string();
        assert!(display.contains("Failed to fetch CRL"));
        assert!(display.contains("ca.example.com"));
        assert!(display.contains("connection timeout"));
    }

    #[test]
    fn test_crl_parse_failed_error() {
        let error = TlsError::CrlParseFailed("invalid ASN.1 structure".to_string());

        let display = error.to_string();
        assert!(display.contains("Failed to parse CRL"));
        assert!(display.contains("invalid ASN.1"));
    }

    #[test]
    fn test_stale_crl_error() {
        let error = TlsError::StaleCrl {
            elapsed: Duration::from_secs(14400),
            max_age: Duration::from_secs(7200),
        };

        let display = error.to_string();
        assert!(display.contains("CRL is stale"));
    }

    #[test]
    fn test_error_debug_formatting() {
        let error = TlsError::CrlFetchFailed {
            url: "https://test.com/crl".to_string(),
            message: "DNS resolution failed".to_string(),
        };

        let debug = format!("{:?}", error);
        assert!(debug.contains("CrlFetchFailed"));
        assert!(debug.contains("test.com"));
    }
}

mod certificate_serial_tests {
    use super::*;

    #[test]
    fn test_serial_format_hex() {
        let cached = CachedCrl {
            revoked_serials: vec![
                "01:23:45:67:89:AB:CD:EF".to_string(),
                "1234567890ABCDEF".to_string(),
            ],
            fetched_at: Instant::now(),
        };

        assert!(cached.revoked_serials.iter().any(|s| s.contains(':')));
        assert!(cached.revoked_serials.iter().any(|s| !s.contains(':')));
    }

    #[test]
    fn test_serial_exact_match() {
        let cached = CachedCrl {
            revoked_serials: vec!["ABCD1234".to_string()],
            fetched_at: Instant::now(),
        };

        assert!(cached.revoked_serials.contains(&"ABCD1234".to_string()));
        assert!(!cached.revoked_serials.contains(&"abcd1234".to_string()));
        assert!(!cached.revoked_serials.contains(&"ABCD12345".to_string()));
    }
}

mod integration_scenario_tests {
    use super::*;

    #[test]
    fn test_node_registration_with_valid_cert() {
        let nodes = [
            ("coordinator-0", "192.168.1.100"),
            ("coordinator-1", "192.168.1.101"),
            ("coordinator-2", "192.168.1.102"),
        ];

        for (id, addr) in nodes {
            let node = create_node_info(id, addr);
            let cert_cn = id;
            let cert_sans = vec![addr.to_string(), format!("{}.cluster.local", id)];

            let result = validate_node_identity(cert_cn, &cert_sans, &node);
            assert!(result.is_ok(), "Failed for node {}", id);
        }
    }

    #[test]
    fn test_reject_stolen_identity() {
        let attacker_node = create_node_info("legitimate-node", "10.0.0.99");
        let stolen_cert_cn = "legitimate-node";
        let attacker_sans = vec!["10.0.0.1".to_string()];

        let result = validate_node_identity(stolen_cert_cn, &attacker_sans, &attacker_node);
        assert!(
            result.is_err(),
            "Should reject when SAN doesn't match node address"
        );
    }

    #[test]
    fn test_reject_spoofed_node_id() {
        let attacker_node = create_node_info("attacker-node", "10.0.0.1");
        let legit_cert_cn = "legitimate-node";
        let legit_sans = vec!["10.0.0.1".to_string()];

        let result = validate_node_identity(legit_cert_cn, &legit_sans, &attacker_node);
        assert!(
            result.is_err(),
            "Should reject when CN doesn't match node ID"
        );
    }

    #[test]
    fn test_wildcard_not_in_san() {
        let node = create_node_info("app-1", "app-1.prod.example.com");
        let cert_cn = "app-1";
        let cert_sans = vec!["*.prod.example.com".to_string()];

        let result = validate_node_identity(cert_cn, &cert_sans, &node);
        assert!(
            result.is_err(),
            "Exact match required, wildcards not supported"
        );
    }
}

mod cache_freshness_tests {
    use super::*;

    #[test]
    fn test_cache_age_tracking() {
        let before = Instant::now();
        let cached = CachedCrl {
            revoked_serials: vec!["SERIAL123".to_string()],
            fetched_at: Instant::now(),
        };
        let after = Instant::now();

        assert!(cached.fetched_at >= before);
        assert!(cached.fetched_at <= after);
    }

    #[test]
    fn test_multiple_caches_independent() {
        let cache1 = CachedCrl {
            revoked_serials: vec!["SERIAL1".to_string()],
            fetched_at: Instant::now(),
        };

        std::thread::sleep(Duration::from_millis(10));

        let cache2 = CachedCrl {
            revoked_serials: vec!["SERIAL2".to_string()],
            fetched_at: Instant::now(),
        };

        assert!(cache2.fetched_at > cache1.fetched_at);
        assert_ne!(cache1.revoked_serials, cache2.revoked_serials);
    }
}

mod node_type_tests {
    use super::*;

    #[test]
    fn test_object_node_validation() {
        let mut node = create_node_info("object-store-1", "10.0.1.50");
        node.r#type = NodeType::Object as i32;

        let result = validate_node_identity("object-store-1", &["10.0.1.50".to_string()], &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_metadata_node_validation() {
        let mut node = create_node_info("metadata-1", "10.0.2.50");
        node.r#type = NodeType::Metadata as i32;

        let result = validate_node_identity("metadata-1", &["10.0.2.50".to_string()], &node);
        assert!(result.is_ok());
    }

    #[test]
    fn test_frontend_node_validation() {
        let mut node = create_node_info("frontend-1", "10.0.3.50");
        node.r#type = NodeType::Frontend as i32;

        let result = validate_node_identity("frontend-1", &["10.0.3.50".to_string()], &node);
        assert!(result.is_ok());
    }
}
