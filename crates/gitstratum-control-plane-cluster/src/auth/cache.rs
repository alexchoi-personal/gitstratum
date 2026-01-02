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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_decision_allow() {
        let decision = AuthDecision::Allow;
        assert_eq!(decision, AuthDecision::Allow);
    }

    #[test]
    fn test_auth_decision_deny() {
        let decision = AuthDecision::Deny {
            reason: "access denied".to_string(),
        };
        match &decision {
            AuthDecision::Deny { reason } => assert_eq!(reason, "access denied"),
            _ => panic!("expected Deny variant"),
        }
    }

    #[test]
    fn test_auth_decision_unknown() {
        let decision = AuthDecision::Unknown;
        assert_eq!(decision, AuthDecision::Unknown);
    }

    #[test]
    fn test_auth_decision_default() {
        let decision = AuthDecision::default();
        assert_eq!(decision, AuthDecision::Unknown);
    }

    #[test]
    fn test_auth_decision_eq() {
        assert_eq!(AuthDecision::Allow, AuthDecision::Allow);
        assert_eq!(AuthDecision::Unknown, AuthDecision::Unknown);
        assert_ne!(AuthDecision::Allow, AuthDecision::Unknown);
        assert_ne!(
            AuthDecision::Allow,
            AuthDecision::Deny {
                reason: "test".to_string()
            }
        );

        let deny1 = AuthDecision::Deny {
            reason: "a".to_string(),
        };
        let deny2 = AuthDecision::Deny {
            reason: "a".to_string(),
        };
        let deny3 = AuthDecision::Deny {
            reason: "b".to_string(),
        };
        assert_eq!(deny1, deny2);
        assert_ne!(deny1, deny3);
    }

    #[test]
    fn test_auth_decision_clone() {
        let allow = AuthDecision::Allow;
        let cloned = allow.clone();
        assert_eq!(allow, cloned);

        let deny = AuthDecision::Deny {
            reason: "test".to_string(),
        };
        let deny_cloned = deny.clone();
        assert_eq!(deny, deny_cloned);
    }

    #[test]
    fn test_auth_decision_debug() {
        let allow = AuthDecision::Allow;
        let debug_str = format!("{:?}", allow);
        assert!(debug_str.contains("Allow"));

        let deny = AuthDecision::Deny {
            reason: "test".to_string(),
        };
        let debug_str = format!("{:?}", deny);
        assert!(debug_str.contains("Deny"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_auth_cache_config_default() {
        let config = AuthCacheConfig::default();
        assert_eq!(config.ttl, Duration::from_secs(300));
        assert_eq!(config.max_entries, 10000);
        assert_eq!(config.negative_ttl, Duration::from_secs(60));
    }

    #[test]
    fn test_auth_cache_config_debug() {
        let config = AuthCacheConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("AuthCacheConfig"));
    }

    #[test]
    fn test_auth_cache_config_clone() {
        let config = AuthCacheConfig {
            ttl: Duration::from_secs(600),
            max_entries: 5000,
            negative_ttl: Duration::from_secs(30),
        };
        let cloned = config.clone();
        assert_eq!(config.ttl, cloned.ttl);
        assert_eq!(config.max_entries, cloned.max_entries);
        assert_eq!(config.negative_ttl, cloned.negative_ttl);
    }

    #[test]
    fn test_auth_cache_new() {
        let config = AuthCacheConfig {
            ttl: Duration::from_secs(100),
            max_entries: 50,
            negative_ttl: Duration::from_secs(10),
        };
        let cache = AuthCache::new(config);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_auth_cache_default() {
        let cache = AuthCache::default();
        assert!(cache.is_empty());
        assert_eq!(cache.config().ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_auth_cache_insert_and_get() {
        let mut cache = AuthCache::default();
        cache.insert("key1".to_string(), AuthDecision::Allow);

        let result = cache.get("key1");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), &AuthDecision::Allow);
    }

    #[test]
    fn test_auth_cache_get_not_found() {
        let cache = AuthCache::default();
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_auth_cache_insert_deny() {
        let mut cache = AuthCache::default();
        cache.insert(
            "key1".to_string(),
            AuthDecision::Deny {
                reason: "forbidden".to_string(),
            },
        );

        let result = cache.get("key1");
        assert!(result.is_some());
        match result.unwrap() {
            AuthDecision::Deny { reason } => assert_eq!(reason, "forbidden"),
            _ => panic!("expected Deny variant"),
        }
    }

    #[test]
    fn test_auth_cache_insert_unknown() {
        let mut cache = AuthCache::default();
        cache.insert("key1".to_string(), AuthDecision::Unknown);

        let result = cache.get("key1");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), &AuthDecision::Unknown);
    }

    #[test]
    fn test_auth_cache_invalidate() {
        let mut cache = AuthCache::default();
        cache.insert("key1".to_string(), AuthDecision::Allow);
        assert!(cache.get("key1").is_some());

        cache.invalidate("key1");
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_auth_cache_invalidate_nonexistent() {
        let mut cache = AuthCache::default();
        cache.invalidate("nonexistent");
        assert!(cache.is_empty());
    }

    #[test]
    fn test_auth_cache_invalidate_prefix() {
        let mut cache = AuthCache::default();
        cache.insert("user:1:read".to_string(), AuthDecision::Allow);
        cache.insert("user:1:write".to_string(), AuthDecision::Allow);
        cache.insert("user:2:read".to_string(), AuthDecision::Allow);
        cache.insert("repo:1:read".to_string(), AuthDecision::Allow);

        assert_eq!(cache.len(), 4);

        cache.invalidate_prefix("user:1:");
        assert_eq!(cache.len(), 2);
        assert!(cache.get("user:1:read").is_none());
        assert!(cache.get("user:1:write").is_none());
        assert!(cache.get("user:2:read").is_some());
        assert!(cache.get("repo:1:read").is_some());
    }

    #[test]
    fn test_auth_cache_invalidate_prefix_no_match() {
        let mut cache = AuthCache::default();
        cache.insert("key1".to_string(), AuthDecision::Allow);
        cache.insert("key2".to_string(), AuthDecision::Allow);

        cache.invalidate_prefix("nonexistent:");
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_auth_cache_clear() {
        let mut cache = AuthCache::default();
        cache.insert("key1".to_string(), AuthDecision::Allow);
        cache.insert("key2".to_string(), AuthDecision::Allow);
        cache.insert("key3".to_string(), AuthDecision::Allow);

        assert_eq!(cache.len(), 3);

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_auth_cache_len() {
        let mut cache = AuthCache::default();
        assert_eq!(cache.len(), 0);

        cache.insert("key1".to_string(), AuthDecision::Allow);
        assert_eq!(cache.len(), 1);

        cache.insert("key2".to_string(), AuthDecision::Allow);
        assert_eq!(cache.len(), 2);

        cache.insert(
            "key1".to_string(),
            AuthDecision::Deny {
                reason: "test".to_string(),
            },
        );
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_auth_cache_is_empty() {
        let mut cache = AuthCache::default();
        assert!(cache.is_empty());

        cache.insert("key1".to_string(), AuthDecision::Allow);
        assert!(!cache.is_empty());

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_auth_cache_config_accessor() {
        let config = AuthCacheConfig {
            ttl: Duration::from_secs(999),
            max_entries: 123,
            negative_ttl: Duration::from_secs(45),
        };
        let cache = AuthCache::new(config);

        assert_eq!(cache.config().ttl, Duration::from_secs(999));
        assert_eq!(cache.config().max_entries, 123);
        assert_eq!(cache.config().negative_ttl, Duration::from_secs(45));
    }

    #[test]
    fn test_auth_cache_expired_entry_allow() {
        let config = AuthCacheConfig {
            ttl: Duration::from_millis(1),
            max_entries: 100,
            negative_ttl: Duration::from_millis(1),
        };
        let mut cache = AuthCache::new(config);
        cache.insert("key1".to_string(), AuthDecision::Allow);

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_auth_cache_expired_entry_deny() {
        let config = AuthCacheConfig {
            ttl: Duration::from_secs(300),
            max_entries: 100,
            negative_ttl: Duration::from_millis(1),
        };
        let mut cache = AuthCache::new(config);
        cache.insert(
            "key1".to_string(),
            AuthDecision::Deny {
                reason: "test".to_string(),
            },
        );

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_auth_cache_expired_entry_unknown() {
        let config = AuthCacheConfig {
            ttl: Duration::from_millis(1),
            max_entries: 100,
            negative_ttl: Duration::from_secs(300),
        };
        let mut cache = AuthCache::new(config);
        cache.insert("key1".to_string(), AuthDecision::Unknown);

        std::thread::sleep(Duration::from_millis(10));

        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_auth_cache_max_entries_eviction() {
        let config = AuthCacheConfig {
            ttl: Duration::from_secs(300),
            max_entries: 3,
            negative_ttl: Duration::from_secs(60),
        };
        let mut cache = AuthCache::new(config);

        cache.insert("key1".to_string(), AuthDecision::Allow);
        std::thread::sleep(Duration::from_millis(5));
        cache.insert("key2".to_string(), AuthDecision::Allow);
        std::thread::sleep(Duration::from_millis(5));
        cache.insert("key3".to_string(), AuthDecision::Allow);
        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(cache.len(), 3);

        cache.insert("key4".to_string(), AuthDecision::Allow);
        assert_eq!(cache.len(), 3);
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_auth_cache_evict_expired_on_insert() {
        let config = AuthCacheConfig {
            ttl: Duration::from_millis(5),
            max_entries: 2,
            negative_ttl: Duration::from_millis(5),
        };
        let mut cache = AuthCache::new(config);

        cache.insert("key1".to_string(), AuthDecision::Allow);
        cache.insert("key2".to_string(), AuthDecision::Allow);

        std::thread::sleep(Duration::from_millis(20));

        cache.insert("key3".to_string(), AuthDecision::Allow);
        assert_eq!(cache.len(), 1);
        assert!(cache.get("key3").is_some());
    }

    #[test]
    fn test_auth_cache_overwrite_existing_key() {
        let mut cache = AuthCache::default();
        cache.insert("key1".to_string(), AuthDecision::Allow);

        let result = cache.get("key1");
        assert_eq!(result.unwrap(), &AuthDecision::Allow);

        cache.insert(
            "key1".to_string(),
            AuthDecision::Deny {
                reason: "updated".to_string(),
            },
        );

        let result = cache.get("key1");
        match result.unwrap() {
            AuthDecision::Deny { reason } => assert_eq!(reason, "updated"),
            _ => panic!("expected Deny variant"),
        }
    }

    #[test]
    fn test_cache_entry_debug() {
        let entry = CacheEntry {
            decision: AuthDecision::Allow,
            created_at: Instant::now(),
        };
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("CacheEntry"));
        assert!(debug_str.contains("Allow"));
    }

    #[test]
    fn test_cache_entry_clone() {
        let entry = CacheEntry {
            decision: AuthDecision::Allow,
            created_at: Instant::now(),
        };
        let cloned = entry.clone();
        assert_eq!(entry.decision, cloned.decision);
    }
}
