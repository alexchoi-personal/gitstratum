use gitstratum_proto::gitstratum::NodeInfo;
use std::net::{Ipv4Addr, Ipv6Addr};
use tonic::Status;

pub fn validate_node_info(node: &NodeInfo) -> Result<(), Status> {
    if !is_valid_node_id(&node.id) {
        return Err(Status::invalid_argument(
            "Node ID must be 1-63 alphanumeric characters or hyphens, without leading/trailing hyphens",
        ));
    }

    if !is_valid_address(&node.address) {
        return Err(Status::invalid_argument(
            "Address must be valid IPv4, IPv6, or hostname (max 253 chars)",
        ));
    }

    if node.port == 0 || node.port > 65535 {
        return Err(Status::invalid_argument("Port must be 1-65535"));
    }

    if !is_valid_uuid(&node.generation_id) {
        return Err(Status::invalid_argument(
            "Generation ID must be valid UUID (36 chars with hyphens)",
        ));
    }

    #[cfg(not(debug_assertions))]
    if is_loopback(&node.address) {
        return Err(Status::invalid_argument(
            "Loopback addresses not allowed in production",
        ));
    }

    Ok(())
}

pub fn is_valid_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 63
        && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !id.starts_with('-')
        && !id.ends_with('-')
}

pub fn is_valid_address(address: &str) -> bool {
    if address.is_empty() || address.len() > 253 {
        return false;
    }

    if address.parse::<Ipv4Addr>().is_ok() {
        return true;
    }

    if address.parse::<Ipv6Addr>().is_ok() {
        return true;
    }

    let trimmed = address.strip_prefix('[').and_then(|s| s.strip_suffix(']'));
    if let Some(inner) = trimmed {
        if inner.parse::<Ipv6Addr>().is_ok() {
            return true;
        }
    }

    is_valid_hostname(address)
}

fn is_valid_hostname(hostname: &str) -> bool {
    if hostname.is_empty() || hostname.len() > 253 {
        return false;
    }

    let labels: Vec<&str> = hostname.split('.').collect();
    if labels.is_empty() {
        return false;
    }

    for label in labels {
        if label.is_empty() || label.len() > 63 {
            return false;
        }

        if label.starts_with('-') || label.ends_with('-') {
            return false;
        }

        if !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return false;
        }
    }

    true
}

pub fn is_valid_uuid(uuid: &str) -> bool {
    if uuid.len() != 36 {
        return false;
    }

    let parts: Vec<&str> = uuid.split('-').collect();
    if parts.len() != 5 {
        return false;
    }

    let expected_lengths = [8, 4, 4, 4, 12];
    for (part, &expected_len) in parts.iter().zip(expected_lengths.iter()) {
        if part.len() != expected_len {
            return false;
        }
        if !part.chars().all(|c| c.is_ascii_hexdigit()) {
            return false;
        }
    }

    true
}

#[allow(dead_code)]
pub(crate) fn is_loopback(address: &str) -> bool {
    if let Ok(ipv4) = address.parse::<Ipv4Addr>() {
        return ipv4.is_loopback();
    }

    if let Ok(ipv6) = address.parse::<Ipv6Addr>() {
        return ipv6.is_loopback();
    }

    let trimmed = address.strip_prefix('[').and_then(|s| s.strip_suffix(']'));
    if let Some(inner) = trimmed {
        if let Ok(ipv6) = inner.parse::<Ipv6Addr>() {
            return ipv6.is_loopback();
        }
    }

    let lower = address.to_lowercase();
    lower == "localhost" || lower.ends_with(".localhost")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_node_ids() {
        assert!(is_valid_node_id("node-1"));
        assert!(is_valid_node_id("a"));
        assert!(is_valid_node_id("abc123"));
        assert!(is_valid_node_id("node-abc-123"));
        assert!(is_valid_node_id(&"a".repeat(63)));
    }

    #[test]
    fn test_invalid_node_ids() {
        assert!(!is_valid_node_id(""));
        assert!(!is_valid_node_id("-node"));
        assert!(!is_valid_node_id("node-"));
        assert!(!is_valid_node_id("-"));
        assert!(!is_valid_node_id("node_1"));
        assert!(!is_valid_node_id("node.1"));
        assert!(!is_valid_node_id(&"a".repeat(64)));
    }

    #[test]
    fn test_valid_addresses() {
        assert!(is_valid_address("192.168.1.1"));
        assert!(is_valid_address("10.0.0.1"));
        assert!(is_valid_address("::1"));
        assert!(is_valid_address("2001:db8::1"));
        assert!(is_valid_address("[::1]"));
        assert!(is_valid_address("[2001:db8::1]"));
        assert!(is_valid_address("example.com"));
        assert!(is_valid_address("node-1.cluster.local"));
        assert!(is_valid_address("a"));
    }

    #[test]
    fn test_invalid_addresses() {
        assert!(!is_valid_address(""));
        assert!(!is_valid_address(&"a".repeat(254)));
        assert!(!is_valid_address("node..local"));
        assert!(!is_valid_address(".example.com"));
        assert!(!is_valid_address("example.com."));
        assert!(!is_valid_address("-example.com"));
        assert!(!is_valid_address("example-.com"));
    }

    #[test]
    fn test_valid_uuids() {
        assert!(is_valid_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(is_valid_uuid("00000000-0000-0000-0000-000000000000"));
        assert!(is_valid_uuid("ffffffff-ffff-ffff-ffff-ffffffffffff"));
        assert!(is_valid_uuid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"));
    }

    #[test]
    fn test_invalid_uuids() {
        assert!(!is_valid_uuid(""));
        assert!(!is_valid_uuid("550e8400e29b41d4a716446655440000"));
        assert!(!is_valid_uuid("550e8400-e29b-41d4-a716-44665544000"));
        assert!(!is_valid_uuid("550e8400-e29b-41d4-a716-4466554400000"));
        assert!(!is_valid_uuid("gggggggg-gggg-gggg-gggg-gggggggggggg"));
        assert!(!is_valid_uuid("550e8400-e29b-41d4-a716"));
    }

    #[test]
    fn test_loopback_addresses() {
        assert!(is_loopback("127.0.0.1"));
        assert!(is_loopback("127.0.0.2"));
        assert!(is_loopback("::1"));
        assert!(is_loopback("[::1]"));
        assert!(is_loopback("localhost"));
        assert!(is_loopback("LOCALHOST"));
        assert!(is_loopback("sub.localhost"));

        assert!(!is_loopback("192.168.1.1"));
        assert!(!is_loopback("10.0.0.1"));
        assert!(!is_loopback("::2"));
        assert!(!is_loopback("example.com"));
    }

    fn make_valid_node() -> NodeInfo {
        NodeInfo {
            id: "node-1".to_string(),
            address: "192.168.1.1".to_string(),
            port: 8080,
            state: 0,
            r#type: 0,
            last_heartbeat_at: 0,
            suspect_count: 0,
            generation_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            registered_at: 0,
        }
    }

    #[test]
    fn test_validate_node_info_success() {
        let node = make_valid_node();
        assert!(validate_node_info(&node).is_ok());
    }

    #[test]
    fn test_validate_node_info_invalid_id() {
        let mut node = make_valid_node();
        node.id = "-invalid".to_string();
        let err = validate_node_info(&node).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_validate_node_info_invalid_port() {
        let mut node = make_valid_node();
        node.port = 0;
        let err = validate_node_info(&node).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_validate_node_info_invalid_address() {
        let mut node = make_valid_node();
        node.address = "".to_string();
        let err = validate_node_info(&node).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_validate_node_info_invalid_generation() {
        let mut node = make_valid_node();
        node.generation_id = "not-a-uuid".to_string();
        let err = validate_node_info(&node).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_validate_node_info_port_out_of_range() {
        let mut node = make_valid_node();
        node.port = 70000;
        let err = validate_node_info(&node).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
