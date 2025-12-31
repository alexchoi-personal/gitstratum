use std::time::Duration;

use super::state::RateLimitState;

#[derive(Debug, Clone, PartialEq)]
pub enum RateLimitDecision {
    Allow,
    Throttle { retry_after: Duration },
    Reject { reason: String },
}

impl Default for RateLimitDecision {
    fn default() -> Self {
        Self::Allow
    }
}

pub struct RateLimitEnforcer {
    state: RateLimitState,
    default_client_capacity: u64,
    default_client_refill_rate: f64,
    default_repo_capacity: u64,
    default_repo_refill_rate: f64,
    global_capacity: u64,
    global_refill_rate: f64,
}

impl RateLimitEnforcer {
    pub fn new() -> Self {
        Self {
            state: RateLimitState::new(),
            default_client_capacity: 100,
            default_client_refill_rate: 10.0,
            default_repo_capacity: 1000,
            default_repo_refill_rate: 100.0,
            global_capacity: 10000,
            global_refill_rate: 1000.0,
        }
    }

    pub fn with_client_limits(mut self, capacity: u64, refill_rate: f64) -> Self {
        self.default_client_capacity = capacity;
        self.default_client_refill_rate = refill_rate;
        self
    }

    pub fn with_repo_limits(mut self, capacity: u64, refill_rate: f64) -> Self {
        self.default_repo_capacity = capacity;
        self.default_repo_refill_rate = refill_rate;
        self
    }

    pub fn with_global_limits(mut self, capacity: u64, refill_rate: f64) -> Self {
        self.global_capacity = capacity;
        self.global_refill_rate = refill_rate;
        self.state
            .set_global_limit(self.global_capacity, self.global_refill_rate);
        self
    }

    pub fn check(
        &mut self,
        client_id: Option<&str>,
        repo_id: Option<&str>,
        tokens: u64,
    ) -> RateLimitDecision {
        if let Some(global) = &mut self.state.global {
            let available = global.available_tokens();
            if available < tokens {
                let deficit = tokens - available;
                let wait_secs = deficit as f64 / self.global_refill_rate;
                return RateLimitDecision::Throttle {
                    retry_after: Duration::from_secs_f64(wait_secs),
                };
            }
        }

        if let Some(client_id) = client_id {
            let entry = self.state.get_or_create_client_entry(
                client_id,
                self.default_client_capacity,
                self.default_client_refill_rate,
            );
            let available = entry.available_tokens();
            if available < tokens {
                let deficit = tokens - available;
                let wait_secs = deficit as f64 / self.default_client_refill_rate;
                return RateLimitDecision::Throttle {
                    retry_after: Duration::from_secs_f64(wait_secs),
                };
            }
        }

        if let Some(repo_id) = repo_id {
            let entry = self.state.get_or_create_repo_entry(
                repo_id,
                self.default_repo_capacity,
                self.default_repo_refill_rate,
            );
            let available = entry.available_tokens();
            if available < tokens {
                let deficit = tokens - available;
                let wait_secs = deficit as f64 / self.default_repo_refill_rate;
                return RateLimitDecision::Throttle {
                    retry_after: Duration::from_secs_f64(wait_secs),
                };
            }
        }

        RateLimitDecision::Allow
    }

    pub fn consume(
        &mut self,
        client_id: Option<&str>,
        repo_id: Option<&str>,
        tokens: u64,
    ) -> RateLimitDecision {
        let allowed = self.state.check_and_consume(
            client_id,
            repo_id,
            tokens,
            self.default_client_capacity,
            self.default_client_refill_rate,
        );

        if allowed {
            RateLimitDecision::Allow
        } else {
            RateLimitDecision::Throttle {
                retry_after: Duration::from_secs(1),
            }
        }
    }

    pub fn state(&self) -> &RateLimitState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut RateLimitState {
        &mut self.state
    }

    pub fn cleanup_stale(&mut self, max_age: Duration) {
        self.state.cleanup_stale_entries(max_age.as_millis() as u64);
    }
}

impl Default for RateLimitEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_decision_variants_and_traits() {
        let allow = RateLimitDecision::default();
        assert_eq!(allow, RateLimitDecision::Allow);

        let throttle = RateLimitDecision::Throttle {
            retry_after: Duration::from_secs(5),
        };
        match &throttle {
            RateLimitDecision::Throttle { retry_after } => {
                assert_eq!(*retry_after, Duration::from_secs(5));
            }
            _ => panic!("Expected Throttle variant"),
        }

        let reject = RateLimitDecision::Reject {
            reason: "Too many requests".to_string(),
        };
        match &reject {
            RateLimitDecision::Reject { reason } => {
                assert_eq!(reason, "Too many requests");
            }
            _ => panic!("Expected Reject variant"),
        }

        let throttle_cloned = throttle.clone();
        assert_eq!(throttle, throttle_cloned);

        let reject_cloned = reject.clone();
        assert_eq!(reject, reject_cloned);

        let debug_allow = format!("{:?}", allow);
        assert!(debug_allow.contains("Allow"));

        let debug_throttle = format!("{:?}", throttle);
        assert!(debug_throttle.contains("Throttle"));
        assert!(debug_throttle.contains("retry_after"));

        let debug_reject = format!("{:?}", reject);
        assert!(debug_reject.contains("Reject"));
        assert!(debug_reject.contains("Too many requests"));

        let t1 = RateLimitDecision::Throttle {
            retry_after: Duration::from_secs(5),
        };
        let t2 = RateLimitDecision::Throttle {
            retry_after: Duration::from_secs(5),
        };
        let t3 = RateLimitDecision::Throttle {
            retry_after: Duration::from_secs(10),
        };
        assert_eq!(t1, t2);
        assert_ne!(t1, t3);
        assert_ne!(t1, allow);

        let r1 = RateLimitDecision::Reject {
            reason: "test".to_string(),
        };
        let r2 = RateLimitDecision::Reject {
            reason: "test".to_string(),
        };
        let r3 = RateLimitDecision::Reject {
            reason: "other".to_string(),
        };
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn test_enforcer_construction_and_builder_pattern() {
        let enforcer = RateLimitEnforcer::new();
        assert!(enforcer.state().global.is_none());
        assert!(enforcer.state().per_client.is_empty());
        assert!(enforcer.state().per_repo.is_empty());
        assert_eq!(enforcer.state().version, 0);

        let enforcer_default = RateLimitEnforcer::default();
        assert!(enforcer_default.state().global.is_none());

        let enforcer_client = RateLimitEnforcer::new().with_client_limits(50, 5.0);
        assert!(enforcer_client.state().global.is_none());
        assert!(enforcer_client.state().per_client.is_empty());

        let enforcer_repo = RateLimitEnforcer::new().with_repo_limits(500, 50.0);
        assert!(enforcer_repo.state().global.is_none());
        assert!(enforcer_repo.state().per_repo.is_empty());

        let enforcer_global = RateLimitEnforcer::new().with_global_limits(5000, 500.0);
        assert!(enforcer_global.state().global.is_some());

        let enforcer_chain = RateLimitEnforcer::new()
            .with_client_limits(50, 5.0)
            .with_repo_limits(500, 50.0)
            .with_global_limits(5000, 500.0);
        assert!(enforcer_chain.state().global.is_some());
        assert!(enforcer_chain.state().per_client.is_empty());
        assert!(enforcer_chain.state().per_repo.is_empty());

        let mut enforcer_mut = RateLimitEnforcer::new();
        enforcer_mut.state_mut().version = 42;
        assert_eq!(enforcer_mut.state().version, 42);
    }

    #[test]
    fn test_check_and_throttle_behavior() {
        let mut enforcer = RateLimitEnforcer::new();
        assert_eq!(
            enforcer.check(Some("client1"), Some("repo1"), 10),
            RateLimitDecision::Allow
        );
        assert_eq!(enforcer.check(None, None, 10), RateLimitDecision::Allow);

        let mut enforcer_global = RateLimitEnforcer::new().with_global_limits(5, 1.0);
        match enforcer_global.check(None, None, 10) {
            RateLimitDecision::Throttle { retry_after } => {
                assert!(retry_after > Duration::ZERO);
            }
            _ => panic!("Expected Throttle decision for global limit"),
        }
        assert_eq!(
            enforcer_global.check(None, None, 5),
            RateLimitDecision::Allow
        );

        let mut enforcer_client = RateLimitEnforcer::new().with_client_limits(5, 1.0);
        match enforcer_client.check(Some("client1"), None, 10) {
            RateLimitDecision::Throttle { retry_after } => {
                assert!(retry_after > Duration::ZERO);
            }
            _ => panic!("Expected Throttle decision for client limit"),
        }
        assert_eq!(
            enforcer_client.check(Some("client1"), None, 5),
            RateLimitDecision::Allow
        );

        let mut enforcer_repo = RateLimitEnforcer::new().with_repo_limits(5, 1.0);
        match enforcer_repo.check(None, Some("repo1"), 10) {
            RateLimitDecision::Throttle { retry_after } => {
                assert!(retry_after > Duration::ZERO);
            }
            _ => panic!("Expected Throttle decision for repo limit"),
        }
        assert_eq!(
            enforcer_repo.check(None, Some("repo1"), 5),
            RateLimitDecision::Allow
        );

        let mut enforcer_priority = RateLimitEnforcer::new()
            .with_client_limits(100, 10.0)
            .with_global_limits(5, 1.0);
        match enforcer_priority.check(Some("client1"), None, 10) {
            RateLimitDecision::Throttle { .. } => {}
            _ => panic!("Global limit should have priority"),
        }

        let mut enforcer_client_repo = RateLimitEnforcer::new()
            .with_client_limits(5, 1.0)
            .with_repo_limits(100, 10.0);
        match enforcer_client_repo.check(Some("client1"), Some("repo1"), 10) {
            RateLimitDecision::Throttle { .. } => {}
            _ => panic!("Client limit should have priority over repo"),
        }

        let mut enforcer_zero = RateLimitEnforcer::new()
            .with_client_limits(1, 0.1)
            .with_global_limits(1, 0.1);
        assert_eq!(
            enforcer_zero.check(Some("client1"), None, 0),
            RateLimitDecision::Allow
        );

        let mut enforcer_zero_cap = RateLimitEnforcer::new().with_client_limits(0, 1.0);
        match enforcer_zero_cap.check(Some("client1"), None, 1) {
            RateLimitDecision::Throttle { .. } => {}
            _ => panic!("Expected Throttle for zero capacity"),
        }

        let mut enforcer_retry = RateLimitEnforcer::new().with_global_limits(5, 5.0);
        match enforcer_retry.check(None, None, 10) {
            RateLimitDecision::Throttle { retry_after } => {
                assert_eq!(retry_after, Duration::from_secs(1));
            }
            _ => panic!("Expected Throttle with calculated retry_after"),
        }

        let mut enforcer_all = RateLimitEnforcer::new()
            .with_client_limits(100, 10.0)
            .with_repo_limits(200, 20.0)
            .with_global_limits(1000, 100.0);
        assert_eq!(
            enforcer_all.check(Some("client1"), Some("repo1"), 10),
            RateLimitDecision::Allow
        );
    }

    #[test]
    fn test_consume_and_state_management() {
        let mut enforcer = RateLimitEnforcer::new();
        assert_eq!(
            enforcer.consume(Some("client1"), Some("repo1"), 10),
            RateLimitDecision::Allow
        );
        assert_eq!(enforcer.consume(None, None, 10), RateLimitDecision::Allow);
        assert_eq!(
            enforcer.consume(Some("client1"), None, 0),
            RateLimitDecision::Allow
        );

        let mut enforcer_global = RateLimitEnforcer::new().with_global_limits(5, 1.0);
        match enforcer_global.consume(None, None, 10) {
            RateLimitDecision::Throttle { retry_after } => {
                assert_eq!(retry_after, Duration::from_secs(1));
            }
            _ => panic!("Expected Throttle decision"),
        }

        let mut enforcer_client = RateLimitEnforcer::new().with_client_limits(100, 10.0);
        assert_eq!(
            enforcer_client.consume(Some("client1"), None, 50),
            RateLimitDecision::Allow
        );
        assert!(!enforcer_client.state().per_client.is_empty());
        assert!(enforcer_client.state().per_repo.is_empty());

        let mut enforcer_repo = RateLimitEnforcer::new().with_repo_limits(100, 10.0);
        assert_eq!(
            enforcer_repo.consume(None, Some("repo1"), 50),
            RateLimitDecision::Allow
        );
        assert!(enforcer_repo.state().per_client.is_empty());
        assert!(!enforcer_repo.state().per_repo.is_empty());

        let mut enforcer_multi = RateLimitEnforcer::new();
        enforcer_multi.consume(Some("client1"), None, 1);
        enforcer_multi.consume(Some("client2"), None, 1);
        enforcer_multi.consume(Some("client3"), None, 1);
        assert_eq!(enforcer_multi.state().per_client.len(), 3);

        enforcer_multi.consume(None, Some("repo1"), 1);
        enforcer_multi.consume(None, Some("repo2"), 1);
        assert_eq!(enforcer_multi.state().per_repo.len(), 2);

        let mut enforcer_version = RateLimitEnforcer::new();
        enforcer_version.consume(None, None, 1);
        enforcer_version.consume(None, None, 1);
        assert_eq!(enforcer_version.state().version, 2);

        let mut enforcer_exhaust = RateLimitEnforcer::new().with_client_limits(10, 1.0);
        enforcer_exhaust.consume(Some("client1"), None, 10);
        match enforcer_exhaust.check(Some("client1"), None, 5) {
            RateLimitDecision::Throttle { .. } => {}
            _ => panic!("Expected Throttle after exhausting tokens"),
        }

        let mut enforcer_partial = RateLimitEnforcer::new().with_client_limits(20, 1.0);
        enforcer_partial.consume(Some("client1"), None, 15);
        match enforcer_partial.check(Some("client1"), None, 10) {
            RateLimitDecision::Throttle { retry_after } => {
                assert!(retry_after.as_secs_f64() > 0.0);
            }
            _ => panic!("Expected Throttle after partial consumption"),
        }

        let mut enforcer_buckets = RateLimitEnforcer::new()
            .with_client_limits(100, 10.0)
            .with_repo_limits(100, 10.0)
            .with_global_limits(100, 10.0);
        enforcer_buckets.consume(Some("client1"), Some("repo1"), 50);
        assert_eq!(
            enforcer_buckets.check(Some("client1"), Some("repo1"), 60),
            RateLimitDecision::Throttle {
                retry_after: Duration::from_secs(1)
            }
        );

        let mut enforcer_all = RateLimitEnforcer::new()
            .with_client_limits(100, 10.0)
            .with_repo_limits(200, 20.0)
            .with_global_limits(1000, 100.0);
        assert_eq!(
            enforcer_all.consume(Some("client1"), Some("repo1"), 10),
            RateLimitDecision::Allow
        );
    }

    #[test]
    fn test_cleanup_and_refill_behavior() {
        let mut enforcer = RateLimitEnforcer::new();
        enforcer.consume(Some("client1"), Some("repo1"), 1);
        std::thread::sleep(Duration::from_millis(50));
        enforcer.cleanup_stale(Duration::from_millis(10));
        assert!(enforcer.state().per_client.is_empty());
        assert!(enforcer.state().per_repo.is_empty());

        let mut enforcer_keep = RateLimitEnforcer::new();
        enforcer_keep.consume(Some("client1"), Some("repo1"), 1);
        enforcer_keep.cleanup_stale(Duration::from_secs(10));
        assert!(!enforcer_keep.state().per_client.is_empty());
        assert!(!enforcer_keep.state().per_repo.is_empty());

        let mut enforcer_selective = RateLimitEnforcer::new();
        enforcer_selective.consume(Some("client1"), None, 1);
        std::thread::sleep(Duration::from_millis(50));
        enforcer_selective.consume(Some("client2"), None, 1);
        enforcer_selective.cleanup_stale(Duration::from_millis(30));
        assert!(!enforcer_selective.state().per_client.contains_key("client1"));
        assert!(enforcer_selective.state().per_client.contains_key("client2"));

        let mut enforcer_refill = RateLimitEnforcer::new().with_client_limits(10, 1000.0);
        enforcer_refill.consume(Some("client1"), None, 10);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            enforcer_refill.check(Some("client1"), None, 5),
            RateLimitDecision::Allow
        );
    }
}
