use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny { reason: String },
    Unknown,
}

impl Default for AuthDecision {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone)]
struct CacheEntry {
    decision: AuthDecision,
    created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct AuthCacheConfig {
    pub ttl: Duration,
    pub max_entries: usize,
    pub negative_ttl: Duration,
}

impl Default for AuthCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            max_entries: 10000,
            negative_ttl: Duration::from_secs(60),
        }
    }
}

pub struct AuthCache {
    config: AuthCacheConfig,
    entries: HashMap<String, CacheEntry>,
}

impl AuthCache {
    pub fn new(config: AuthCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&AuthDecision> {
        let entry = self.entries.get(key)?;

        let ttl = match &entry.decision {
            AuthDecision::Deny { .. } => self.config.negative_ttl,
            _ => self.config.ttl,
        };

        if entry.created_at.elapsed() > ttl {
            return None;
        }

        Some(&entry.decision)
    }

    pub fn insert(&mut self, key: String, decision: AuthDecision) {
        if self.entries.len() >= self.config.max_entries {
            self.evict_expired();

            if self.entries.len() >= self.config.max_entries {
                if let Some(oldest_key) = self.find_oldest_entry() {
                    self.entries.remove(&oldest_key);
                }
            }
        }

        self.entries.insert(
            key,
            CacheEntry {
                decision,
                created_at: Instant::now(),
            },
        );
    }

    pub fn invalidate(&mut self, key: &str) {
        self.entries.remove(key);
    }

    pub fn invalidate_prefix(&mut self, prefix: &str) {
        self.entries.retain(|k, _| !k.starts_with(prefix));
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn evict_expired(&mut self) {
        let ttl = self.config.ttl;
        let negative_ttl = self.config.negative_ttl;

        self.entries.retain(|_, entry| {
            let entry_ttl = match &entry.decision {
                AuthDecision::Deny { .. } => negative_ttl,
                _ => ttl,
            };
            entry.created_at.elapsed() <= entry_ttl
        });
    }

    fn find_oldest_entry(&self) -> Option<String> {
        self.entries
            .iter()
            .min_by_key(|(_, entry)| entry.created_at)
            .map(|(key, _)| key.clone())
    }

    pub fn config(&self) -> &AuthCacheConfig {
        &self.config
    }
}

impl Default for AuthCache {
    fn default() -> Self {
        Self::new(AuthCacheConfig::default())
    }
}
