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
