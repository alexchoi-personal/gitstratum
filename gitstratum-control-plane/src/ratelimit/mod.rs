mod bucket;
mod enforcement;
mod state;

pub use bucket::{TokenBucket, TokenBucketConfig};
pub use enforcement::{RateLimitDecision, RateLimitEnforcer};
pub use state::{RateLimitState, RateLimitStateEntry};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_module_exports_token_bucket() {
        let config = TokenBucketConfig::default();
        let bucket = TokenBucket::new(config);
        assert_eq!(bucket.capacity(), 100);
    }

    #[test]
    fn test_module_exports_token_bucket_config() {
        let config = TokenBucketConfig {
            capacity: 50,
            refill_rate: 5.0,
            refill_interval: Duration::from_secs(1),
        };
        assert_eq!(config.capacity, 50);
    }

    #[test]
    fn test_module_exports_rate_limit_decision() {
        let decision = RateLimitDecision::Allow;
        assert_eq!(decision, RateLimitDecision::default());
    }

    #[test]
    fn test_module_exports_rate_limit_enforcer() {
        let enforcer = RateLimitEnforcer::new();
        assert!(enforcer.state().global.is_none());
    }

    #[test]
    fn test_module_exports_rate_limit_state() {
        let state = RateLimitState::new();
        assert_eq!(state.version, 0);
    }

    #[test]
    fn test_module_exports_rate_limit_state_entry() {
        let entry = RateLimitStateEntry::new(100, 10.0);
        assert_eq!(entry.capacity, 100);
    }

    #[test]
    fn test_integration_enforcer_with_state() {
        let mut enforcer = RateLimitEnforcer::new()
            .with_client_limits(100, 10.0)
            .with_repo_limits(200, 20.0)
            .with_global_limits(1000, 100.0);

        let decision = enforcer.consume(Some("client1"), Some("repo1"), 50);
        assert_eq!(decision, RateLimitDecision::Allow);

        let state = enforcer.state();
        assert!(state.global.is_some());
        assert!(state.per_client.contains_key("client1"));
        assert!(state.per_repo.contains_key("repo1"));
    }

    #[test]
    fn test_integration_bucket_behavior() {
        let config = TokenBucketConfig {
            capacity: 10,
            refill_rate: 10.0,
            refill_interval: Duration::from_secs(1),
        };
        let mut bucket = TokenBucket::new(config);

        assert!(bucket.try_consume(10));
        assert!(!bucket.try_consume(1));
        assert_eq!(bucket.available_tokens(), 0);

        std::thread::sleep(Duration::from_millis(200));
        assert!(bucket.try_consume(1));
    }

    #[test]
    fn test_integration_state_entry_lifecycle() {
        let mut entry = RateLimitStateEntry::new(100, 100.0);

        assert!(entry.try_consume(50));
        assert_eq!(entry.available_tokens(), 50);

        std::thread::sleep(Duration::from_millis(100));
        let available = entry.available_tokens();
        assert!(available >= 50);
    }

    #[test]
    fn test_integration_full_rate_limiting_flow() {
        let mut enforcer = RateLimitEnforcer::new()
            .with_client_limits(10, 10.0)
            .with_global_limits(100, 10.0);

        for _ in 0..10 {
            let decision = enforcer.consume(Some("client1"), None, 1);
            assert_eq!(decision, RateLimitDecision::Allow);
        }

        let decision = enforcer.check(Some("client1"), None, 1);
        match decision {
            RateLimitDecision::Throttle { .. } => {}
            _ => panic!("Expected Throttle after exhausting limit"),
        }
    }

    #[test]
    fn test_integration_multiple_clients_isolated() {
        let mut enforcer = RateLimitEnforcer::new().with_client_limits(10, 1.0);

        enforcer.consume(Some("client1"), None, 10);
        let decision_client1 = enforcer.check(Some("client1"), None, 1);
        let decision_client2 = enforcer.check(Some("client2"), None, 1);

        match decision_client1 {
            RateLimitDecision::Throttle { .. } => {}
            _ => panic!("client1 should be throttled"),
        }
        assert_eq!(decision_client2, RateLimitDecision::Allow);
    }

    #[test]
    fn test_integration_state_serialization_roundtrip() {
        let mut state = RateLimitState::new();
        state.set_global_limit(1000, 100.0);
        state.get_or_create_client_entry("client1", 100, 10.0);
        state.version = 42;

        let serialized = serde_json::to_string(&state).unwrap();
        let deserialized: RateLimitState = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.version, 42);
        assert!(deserialized.global.is_some());
        assert!(deserialized.per_client.contains_key("client1"));
    }
}
