use crate::time::current_timestamp_millis;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitStateEntry {
    pub tokens: f64,
    pub last_update_epoch_ms: u64,
    pub capacity: u64,
    pub refill_rate: f64,
}

impl RateLimitStateEntry {
    pub fn new(capacity: u64, refill_rate: f64) -> Self {
        let now = current_timestamp_millis();

        Self {
            tokens: capacity as f64,
            last_update_epoch_ms: now,
            capacity,
            refill_rate,
        }
    }

    pub fn refill(&mut self) -> f64 {
        let now = current_timestamp_millis();

        let elapsed_ms = now.saturating_sub(self.last_update_epoch_ms);
        let elapsed_secs = elapsed_ms as f64 / 1000.0;

        let new_tokens = elapsed_secs * self.refill_rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity as f64);
        self.last_update_epoch_ms = now;

        self.tokens
    }

    pub fn try_consume(&mut self, tokens: u64) -> bool {
        self.refill();

        if self.tokens >= tokens as f64 {
            self.tokens -= tokens as f64;
            true
        } else {
            false
        }
    }

    pub fn available_tokens(&mut self) -> u64 {
        self.refill();
        self.tokens as u64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RateLimitState {
    pub per_client: HashMap<String, RateLimitStateEntry>,
    pub per_repo: HashMap<String, RateLimitStateEntry>,
    pub global: Option<RateLimitStateEntry>,
    pub version: u64,
}

impl RateLimitState {
    pub fn new() -> Self {
        Self {
            per_client: HashMap::new(),
            per_repo: HashMap::new(),
            global: None,
            version: 0,
        }
    }

    pub fn get_or_create_client_entry(
        &mut self,
        client_id: &str,
        capacity: u64,
        refill_rate: f64,
    ) -> &mut RateLimitStateEntry {
        self.per_client
            .entry(client_id.to_string())
            .or_insert_with(|| RateLimitStateEntry::new(capacity, refill_rate))
    }

    pub fn get_or_create_repo_entry(
        &mut self,
        repo_id: &str,
        capacity: u64,
        refill_rate: f64,
    ) -> &mut RateLimitStateEntry {
        self.per_repo
            .entry(repo_id.to_string())
            .or_insert_with(|| RateLimitStateEntry::new(capacity, refill_rate))
    }

    pub fn set_global_limit(&mut self, capacity: u64, refill_rate: f64) {
        self.global = Some(RateLimitStateEntry::new(capacity, refill_rate));
    }

    pub fn check_and_consume(
        &mut self,
        client_id: Option<&str>,
        repo_id: Option<&str>,
        tokens: u64,
        default_capacity: u64,
        default_refill_rate: f64,
    ) -> bool {
        if let Some(global) = &mut self.global {
            if !global.try_consume(tokens) {
                return false;
            }
        }

        if let Some(client_id) = client_id {
            let entry =
                self.get_or_create_client_entry(client_id, default_capacity, default_refill_rate);
            if !entry.try_consume(tokens) {
                return false;
            }
        }

        if let Some(repo_id) = repo_id {
            let entry =
                self.get_or_create_repo_entry(repo_id, default_capacity, default_refill_rate);
            if !entry.try_consume(tokens) {
                return false;
            }
        }

        self.version += 1;
        true
    }

    pub fn cleanup_stale_entries(&mut self, max_age_ms: u64) {
        let now = current_timestamp_millis();

        self.per_client
            .retain(|_, entry| now - entry.last_update_epoch_ms < max_age_ms);
        self.per_repo
            .retain(|_, entry| now - entry.last_update_epoch_ms < max_age_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_state_entry_token_bucket_operations() {
        let mut entry = RateLimitStateEntry::new(100, 10.0);
        assert_eq!(entry.capacity, 100);
        assert_eq!(entry.refill_rate, 10.0);
        assert_eq!(entry.tokens, 100.0);
        assert!(entry.last_update_epoch_ms > 0);

        assert_eq!(entry.available_tokens(), 100);
        assert!(entry.try_consume(50));
        assert!(entry.tokens < 51.0);
        assert!(entry.available_tokens() <= 50);

        assert!(entry.try_consume(30));
        assert!(entry.try_consume(10));
        assert!(!entry.try_consume(30));

        let mut entry2 = RateLimitStateEntry::new(10, 1.0);
        assert!(!entry2.try_consume(20));
        assert!(entry2.try_consume(10));
        assert!(entry2.tokens < 1.0);

        let mut entry3 = RateLimitStateEntry::new(100, 10.0);
        assert!(!entry3.try_consume(u64::MAX));

        let mut zero_cap = RateLimitStateEntry::new(0, 10.0);
        assert!(!zero_cap.try_consume(1));
        assert!(zero_cap.try_consume(0));
        assert_eq!(zero_cap.available_tokens(), 0);

        let mut entry4 = RateLimitStateEntry::new(100, 1000.0);
        entry4.tokens = 50.0;
        let initial_ts = entry4.last_update_epoch_ms;
        std::thread::sleep(std::time::Duration::from_millis(10));
        let tokens = entry4.refill();
        assert!(tokens >= 50.0);
        assert!(entry4.last_update_epoch_ms > initial_ts);

        let mut entry5 = RateLimitStateEntry::new(100, 10000.0);
        entry5.tokens = 99.0;
        std::thread::sleep(std::time::Duration::from_millis(100));
        let tokens = entry5.refill();
        assert_eq!(tokens, 100.0);

        let mut zero_refill = RateLimitStateEntry::new(10, 0.0);
        zero_refill.tokens = 5.0;
        std::thread::sleep(std::time::Duration::from_millis(10));
        zero_refill.refill();
        assert!(zero_refill.tokens <= 6.0);
    }

    #[test]
    fn test_rate_limit_state_management() {
        let state = RateLimitState::new();
        assert!(state.per_client.is_empty());
        assert!(state.per_repo.is_empty());
        assert!(state.global.is_none());
        assert_eq!(state.version, 0);

        let state_default = RateLimitState::default();
        assert!(state_default.per_client.is_empty());
        assert!(state_default.per_repo.is_empty());
        assert!(state_default.global.is_none());
        assert_eq!(state_default.version, 0);

        let mut state = RateLimitState::new();
        let entry = state.get_or_create_client_entry("client1", 100, 10.0);
        assert_eq!(entry.capacity, 100);
        assert_eq!(entry.refill_rate, 10.0);

        {
            let entry = state.get_or_create_client_entry("client1", 100, 10.0);
            entry.tokens = 50.0;
        }
        let entry = state.get_or_create_client_entry("client1", 200, 20.0);
        assert_eq!(entry.capacity, 100);
        assert!(entry.tokens <= 51.0);

        state.get_or_create_client_entry("client2", 200, 20.0);
        state.get_or_create_client_entry("client3", 300, 30.0);
        assert_eq!(state.per_client.len(), 3);

        let entry = state.get_or_create_repo_entry("repo1", 500, 50.0);
        assert_eq!(entry.capacity, 500);
        assert_eq!(entry.refill_rate, 50.0);

        {
            let entry = state.get_or_create_repo_entry("repo1", 500, 50.0);
            entry.tokens = 250.0;
        }
        let entry = state.get_or_create_repo_entry("repo1", 1000, 100.0);
        assert_eq!(entry.capacity, 500);
        assert!(entry.tokens <= 251.0);

        state.get_or_create_repo_entry("repo2", 200, 20.0);
        assert_eq!(state.per_repo.len(), 2);

        state.set_global_limit(10000, 1000.0);
        assert!(state.global.is_some());
        let global = state.global.as_ref().unwrap();
        assert_eq!(global.capacity, 10000);
        assert_eq!(global.refill_rate, 1000.0);

        state.set_global_limit(20000, 2000.0);
        let global = state.global.as_ref().unwrap();
        assert_eq!(global.capacity, 20000);
        assert_eq!(global.refill_rate, 2000.0);
    }

    #[test]
    fn test_check_and_consume_scenarios() {
        let mut state = RateLimitState::new();
        let result = state.check_and_consume(None, None, 10, 100, 10.0);
        assert!(result);
        assert_eq!(state.version, 1);

        let result = state.check_and_consume(Some("client1"), None, 10, 100, 10.0);
        assert!(result);
        assert_eq!(state.version, 2);

        let result = state.check_and_consume(None, Some("repo1"), 10, 100, 10.0);
        assert!(result);
        assert_eq!(state.version, 3);

        let result = state.check_and_consume(Some("client2"), Some("repo2"), 10, 100, 10.0);
        assert!(result);
        assert_eq!(state.version, 4);

        state.check_and_consume(None, None, 1, 100, 10.0);
        state.check_and_consume(None, None, 1, 100, 10.0);
        assert_eq!(state.version, 6);

        let mut state2 = RateLimitState::new();
        state2.set_global_limit(5, 1.0);
        let result = state2.check_and_consume(None, None, 10, 100, 10.0);
        assert!(!result);
        assert_eq!(state2.version, 0);

        let mut state3 = RateLimitState::new();
        let result = state3.check_and_consume(Some("client1"), None, 200, 100, 10.0);
        assert!(!result);
        assert_eq!(state3.version, 0);

        let mut state4 = RateLimitState::new();
        let result = state4.check_and_consume(None, Some("repo1"), 200, 100, 10.0);
        assert!(!result);
        assert_eq!(state4.version, 0);

        let mut state5 = RateLimitState::new();
        state5.set_global_limit(100, 10.0);
        let result = state5.check_and_consume(Some("client1"), Some("repo1"), 10, 100, 10.0);
        assert!(result);

        let mut state6 = RateLimitState::new();
        state6.set_global_limit(5, 1.0);
        let result = state6.check_and_consume(Some("client1"), Some("repo1"), 10, 100, 10.0);
        assert!(!result);
        assert!(state6.per_client.is_empty());
        assert!(state6.per_repo.is_empty());

        let mut state7 = RateLimitState::new();
        state7.get_or_create_client_entry("client1", 5, 1.0);
        let result = state7.check_and_consume(Some("client1"), Some("repo1"), 10, 5, 1.0);
        assert!(!result);
        assert!(state7.per_repo.is_empty());

        let mut state8 = RateLimitState::new();
        state8.check_and_consume(Some("client1"), None, 50, 100, 10.0);
        state8.check_and_consume(Some("client2"), None, 50, 100, 10.0);
        let client1 = state8.per_client.get("client1").unwrap();
        let client2 = state8.per_client.get("client2").unwrap();
        assert!(client1.tokens <= 51.0);
        assert!(client2.tokens <= 51.0);

        let mut state9 = RateLimitState::new();
        state9.check_and_consume(None, Some("repo1"), 50, 100, 10.0);
        state9.check_and_consume(None, Some("repo2"), 50, 100, 10.0);
        let repo1 = state9.per_repo.get("repo1").unwrap();
        let repo2 = state9.per_repo.get("repo2").unwrap();
        assert!(repo1.tokens <= 51.0);
        assert!(repo2.tokens <= 51.0);
    }

    #[test]
    fn test_cleanup_and_stale_entry_management() {
        let mut state = RateLimitState::new();
        state.cleanup_stale_entries(1000);
        assert!(state.per_client.is_empty());
        assert!(state.per_repo.is_empty());

        state.get_or_create_client_entry("client1", 100, 10.0);
        state.get_or_create_repo_entry("repo1", 100, 10.0);
        state.cleanup_stale_entries(10000);
        assert_eq!(state.per_client.len(), 1);
        assert_eq!(state.per_repo.len(), 1);

        let mut state2 = RateLimitState::new();
        state2.get_or_create_client_entry("client1", 100, 10.0);
        state2.get_or_create_repo_entry("repo1", 100, 10.0);
        std::thread::sleep(std::time::Duration::from_millis(50));
        state2.cleanup_stale_entries(10);
        assert!(state2.per_client.is_empty());
        assert!(state2.per_repo.is_empty());

        let mut state3 = RateLimitState::new();
        state3.get_or_create_client_entry("client1", 100, 10.0);
        std::thread::sleep(std::time::Duration::from_millis(50));
        state3.get_or_create_client_entry("client2", 100, 10.0);
        state3.cleanup_stale_entries(30);
        assert_eq!(state3.per_client.len(), 1);
        assert!(state3.per_client.contains_key("client2"));
    }

    #[test]
    fn test_serialization_and_trait_implementations() {
        let entry = RateLimitStateEntry::new(100, 10.0);
        let serialized = serde_json::to_string(&entry).unwrap();
        let deserialized: RateLimitStateEntry = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.capacity, 100);
        assert_eq!(deserialized.refill_rate, 10.0);

        let cloned_entry = entry.clone();
        assert_eq!(cloned_entry.capacity, 100);
        assert_eq!(cloned_entry.refill_rate, 10.0);

        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("RateLimitStateEntry"));

        let mut state = RateLimitState::new();
        state.get_or_create_client_entry("client1", 100, 10.0);
        state.set_global_limit(1000, 100.0);
        state.version = 42;
        let serialized = serde_json::to_string(&state).unwrap();
        let deserialized: RateLimitState = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.version, 42);
        assert!(deserialized.global.is_some());
        assert_eq!(deserialized.per_client.len(), 1);

        let cloned_state = state.clone();
        assert_eq!(cloned_state.per_client.len(), 1);
        assert!(cloned_state.global.is_some());

        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("RateLimitState"));
    }
}
