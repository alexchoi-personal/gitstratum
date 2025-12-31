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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            tokens: capacity as f64,
            last_update_epoch_ms: now,
            capacity,
            refill_rate,
        }
    }

    pub fn refill(&mut self) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.per_client
            .retain(|_, entry| now - entry.last_update_epoch_ms < max_age_ms);
        self.per_repo
            .retain(|_, entry| now - entry.last_update_epoch_ms < max_age_ms);
    }
}
