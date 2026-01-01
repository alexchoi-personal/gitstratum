use crate::time::current_timestamp_millis;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: String,
    pub timestamp: u64,
    pub event_type: String,
    pub actor_id: String,
    pub actor_type: ActorType,
    pub resource_type: String,
    pub resource_id: String,
    pub action: String,
    pub outcome: AuditOutcome,
    pub metadata: std::collections::HashMap<String, String>,
    pub client_ip: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorType {
    User,
    Service,
    System,
    Anonymous,
}

impl Default for ActorType {
    fn default() -> Self {
        Self::Anonymous
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditOutcome {
    Success,
    Failure,
    Denied,
    Error,
}

impl Default for AuditOutcome {
    fn default() -> Self {
        Self::Success
    }
}

impl AuditEntry {
    pub fn new(
        event_type: impl Into<String>,
        actor_id: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: impl Into<String>,
        action: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: current_timestamp_millis(),
            event_type: event_type.into(),
            actor_id: actor_id.into(),
            actor_type: ActorType::default(),
            resource_type: resource_type.into(),
            resource_id: resource_id.into(),
            action: action.into(),
            outcome: AuditOutcome::default(),
            metadata: std::collections::HashMap::new(),
            client_ip: None,
            request_id: None,
        }
    }

    pub fn with_actor_type(mut self, actor_type: ActorType) -> Self {
        self.actor_type = actor_type;
        self
    }

    pub fn with_outcome(mut self, outcome: AuditOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct AuditLoggerConfig {
    pub buffer_size: usize,
    pub flush_interval: Duration,
    pub max_entries_per_flush: usize,
    pub enabled: bool,
}

impl Default for AuditLoggerConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10000,
            flush_interval: Duration::from_secs(5),
            max_entries_per_flush: 1000,
            enabled: true,
        }
    }
}

pub struct AuditLogger {
    config: AuditLoggerConfig,
    buffer: VecDeque<AuditEntry>,
    flushed_entries: Vec<AuditEntry>,
    total_entries: u64,
}

impl AuditLogger {
    pub fn new(config: AuditLoggerConfig) -> Self {
        Self {
            config,
            buffer: VecDeque::new(),
            flushed_entries: Vec::new(),
            total_entries: 0,
        }
    }

    pub fn log(&mut self, entry: AuditEntry) {
        if !self.config.enabled {
            return;
        }

        if self.buffer.len() >= self.config.buffer_size {
            self.buffer.pop_front();
        }

        self.buffer.push_back(entry);
        self.total_entries += 1;
    }

    pub fn log_action(
        &mut self,
        actor_id: &str,
        resource_type: &str,
        resource_id: &str,
        action: &str,
        outcome: AuditOutcome,
    ) {
        let entry = AuditEntry::new("action", actor_id, resource_type, resource_id, action)
            .with_outcome(outcome);
        self.log(entry);
    }

    pub fn flush(&mut self) -> Vec<AuditEntry> {
        let count = self.buffer.len().min(self.config.max_entries_per_flush);
        let flushed: Vec<_> = self.buffer.drain(..count).collect();

        self.flushed_entries.extend(flushed.clone());
        flushed
    }

    pub fn pending_count(&self) -> usize {
        self.buffer.len()
    }

    pub fn total_entries(&self) -> u64 {
        self.total_entries
    }

    pub fn flushed_entries(&self) -> &[AuditEntry] {
        &self.flushed_entries
    }

    pub fn config(&self) -> &AuditLoggerConfig {
        &self.config
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn clear_buffer(&mut self) {
        self.buffer.clear();
    }
}

impl Default for AuditLogger {
    fn default() -> Self {
        Self::new(AuditLoggerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_creation_and_builder_workflow() {
        assert_eq!(ActorType::default(), ActorType::Anonymous);
        assert_eq!(AuditOutcome::default(), AuditOutcome::Success);

        let entry = AuditEntry::new("test_event", "user1", "repo", "repo123", "read");
        assert_eq!(entry.event_type, "test_event");
        assert_eq!(entry.actor_id, "user1");
        assert_eq!(entry.resource_type, "repo");
        assert_eq!(entry.resource_id, "repo123");
        assert_eq!(entry.action, "read");
        assert_eq!(entry.actor_type, ActorType::Anonymous);
        assert_eq!(entry.outcome, AuditOutcome::Success);
        assert!(entry.metadata.is_empty());
        assert!(entry.client_ip.is_none());
        assert!(entry.request_id.is_none());
        assert!(!entry.id.is_empty());
        assert!(entry.timestamp > 0);

        for actor_type in [
            ActorType::User,
            ActorType::Service,
            ActorType::System,
            ActorType::Anonymous,
        ] {
            let e = AuditEntry::new("e", "a", "r", "i", "act").with_actor_type(actor_type);
            assert_eq!(e.actor_type, actor_type);
        }

        for outcome in [
            AuditOutcome::Success,
            AuditOutcome::Failure,
            AuditOutcome::Denied,
            AuditOutcome::Error,
        ] {
            let e = AuditEntry::new("e", "a", "r", "i", "act").with_outcome(outcome);
            assert_eq!(e.outcome, outcome);
        }

        let entry_meta = AuditEntry::new("e", "a", "r", "i", "act")
            .with_metadata("key1", "value1")
            .with_metadata("key2", "value2");
        assert_eq!(entry_meta.metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(entry_meta.metadata.get("key2"), Some(&"value2".to_string()));
        assert_eq!(entry_meta.metadata.len(), 2);

        let entry_ip = AuditEntry::new("e", "a", "r", "i", "act").with_client_ip("192.168.1.1");
        assert_eq!(entry_ip.client_ip, Some("192.168.1.1".to_string()));

        let entry_req = AuditEntry::new("e", "a", "r", "i", "act").with_request_id("req-12345");
        assert_eq!(entry_req.request_id, Some("req-12345".to_string()));

        let full_entry = AuditEntry::new("event", "actor", "resource", "id", "action")
            .with_actor_type(ActorType::User)
            .with_outcome(AuditOutcome::Success)
            .with_metadata("key", "value")
            .with_client_ip("10.0.0.1")
            .with_request_id("req-abc");
        assert_eq!(full_entry.actor_type, ActorType::User);
        assert_eq!(full_entry.outcome, AuditOutcome::Success);
        assert_eq!(full_entry.metadata.get("key"), Some(&"value".to_string()));
        assert_eq!(full_entry.client_ip, Some("10.0.0.1".to_string()));
        assert_eq!(full_entry.request_id, Some("req-abc".to_string()));

        let entry1 = AuditEntry::new("e", "a", "r", "i", "act");
        let entry2 = AuditEntry::new("e", "a", "r", "i", "act");
        assert_ne!(entry1.id, entry2.id);

        let ts_entry1 = AuditEntry::new("e", "a", "r", "i", "act");
        std::thread::sleep(Duration::from_millis(1));
        let ts_entry2 = AuditEntry::new("e", "a", "r", "i", "act");
        assert!(ts_entry2.timestamp >= ts_entry1.timestamp);
    }

    #[test]
    fn test_audit_logger_lifecycle_and_buffering_workflow() {
        let default_config = AuditLoggerConfig::default();
        assert_eq!(default_config.buffer_size, 10000);
        assert_eq!(default_config.flush_interval, Duration::from_secs(5));
        assert_eq!(default_config.max_entries_per_flush, 1000);
        assert!(default_config.enabled);

        let custom_config = AuditLoggerConfig {
            buffer_size: 100,
            flush_interval: Duration::from_secs(10),
            max_entries_per_flush: 50,
            enabled: true,
        };
        let logger = AuditLogger::new(custom_config.clone());
        assert_eq!(logger.config().buffer_size, 100);
        assert_eq!(logger.config().flush_interval, Duration::from_secs(10));
        assert_eq!(logger.config().max_entries_per_flush, 50);
        assert!(logger.is_enabled());
        assert_eq!(logger.pending_count(), 0);
        assert_eq!(logger.total_entries(), 0);

        let default_logger = AuditLogger::default();
        assert_eq!(default_logger.config().buffer_size, 10000);
        assert!(default_logger.is_enabled());
        assert_eq!(default_logger.pending_count(), 0);

        let mut logger = AuditLogger::default();
        logger.log(AuditEntry::new("e", "a", "r", "i", "act"));
        assert_eq!(logger.pending_count(), 1);
        assert_eq!(logger.total_entries(), 1);

        let mut disabled_logger = AuditLogger::new(AuditLoggerConfig {
            enabled: false,
            ..Default::default()
        });
        disabled_logger.log(AuditEntry::new("e", "a", "r", "i", "act"));
        assert_eq!(disabled_logger.pending_count(), 0);
        assert_eq!(disabled_logger.total_entries(), 0);

        let mut overflow_logger = AuditLogger::new(AuditLoggerConfig {
            buffer_size: 3,
            ..Default::default()
        });
        for i in 0..5 {
            overflow_logger.log(AuditEntry::new("e", format!("actor{}", i), "r", "i", "act"));
        }
        assert_eq!(overflow_logger.pending_count(), 3);
        assert_eq!(overflow_logger.total_entries(), 5);

        let mut action_logger = AuditLogger::default();
        action_logger.log_action(
            "user1",
            "repository",
            "repo123",
            "clone",
            AuditOutcome::Success,
        );
        assert_eq!(action_logger.pending_count(), 1);
        assert_eq!(action_logger.total_entries(), 1);

        let mut flush_logger = AuditLogger::default();
        for i in 0..5 {
            flush_logger.log(AuditEntry::new("e", format!("actor{}", i), "r", "i", "act"));
        }
        let flushed = flush_logger.flush();
        assert_eq!(flushed.len(), 5);
        assert_eq!(flush_logger.pending_count(), 0);
        assert_eq!(flush_logger.flushed_entries().len(), 5);

        let mut partial_logger = AuditLogger::new(AuditLoggerConfig {
            max_entries_per_flush: 3,
            ..Default::default()
        });
        for i in 0..5 {
            partial_logger.log(AuditEntry::new("e", format!("actor{}", i), "r", "i", "act"));
        }
        let flushed1 = partial_logger.flush();
        assert_eq!(flushed1.len(), 3);
        assert_eq!(partial_logger.pending_count(), 2);
        assert_eq!(partial_logger.flushed_entries().len(), 3);
        let flushed2 = partial_logger.flush();
        assert_eq!(flushed2.len(), 2);
        assert_eq!(partial_logger.pending_count(), 0);
        assert_eq!(partial_logger.flushed_entries().len(), 5);

        let mut empty_logger = AuditLogger::default();
        let empty_flushed = empty_logger.flush();
        assert!(empty_flushed.is_empty());
        assert_eq!(empty_logger.flushed_entries().len(), 0);

        let mut multi_flush_logger = AuditLogger::new(AuditLoggerConfig {
            max_entries_per_flush: 2,
            ..Default::default()
        });
        for i in 0..6 {
            multi_flush_logger.log(AuditEntry::new("e", format!("actor{}", i), "r", "i", "act"));
        }
        multi_flush_logger.flush();
        assert_eq!(multi_flush_logger.flushed_entries().len(), 2);
        assert_eq!(multi_flush_logger.pending_count(), 4);
        multi_flush_logger.flush();
        assert_eq!(multi_flush_logger.flushed_entries().len(), 4);
        assert_eq!(multi_flush_logger.pending_count(), 2);
        multi_flush_logger.flush();
        assert_eq!(multi_flush_logger.flushed_entries().len(), 6);
        assert_eq!(multi_flush_logger.pending_count(), 0);

        let mut toggle_logger = AuditLogger::default();
        assert!(toggle_logger.is_enabled());
        toggle_logger.set_enabled(false);
        assert!(!toggle_logger.is_enabled());
        toggle_logger.set_enabled(true);
        assert!(toggle_logger.is_enabled());

        let mut enable_disable_logger = AuditLogger::default();
        enable_disable_logger.log(AuditEntry::new("e", "actor1", "r", "i", "act"));
        assert_eq!(enable_disable_logger.pending_count(), 1);
        enable_disable_logger.set_enabled(false);
        enable_disable_logger.log(AuditEntry::new("e", "actor2", "r", "i", "act"));
        assert_eq!(enable_disable_logger.pending_count(), 1);
        enable_disable_logger.set_enabled(true);
        enable_disable_logger.log(AuditEntry::new("e", "actor3", "r", "i", "act"));
        assert_eq!(enable_disable_logger.pending_count(), 2);

        let mut clear_logger = AuditLogger::default();
        for i in 0..5 {
            clear_logger.log(AuditEntry::new("e", format!("actor{}", i), "r", "i", "act"));
        }
        assert_eq!(clear_logger.pending_count(), 5);
        clear_logger.clear_buffer();
        assert_eq!(clear_logger.pending_count(), 0);
        assert_eq!(clear_logger.total_entries(), 5);
    }

    #[test]
    fn test_serialization_workflow() {
        let entry = AuditEntry::new("event", "actor", "resource", "id", "action")
            .with_actor_type(ActorType::User)
            .with_outcome(AuditOutcome::Success)
            .with_metadata("key", "value")
            .with_client_ip("127.0.0.1")
            .with_request_id("req-123");
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: AuditEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_type, entry.event_type);
        assert_eq!(deserialized.actor_id, entry.actor_id);
        assert_eq!(deserialized.actor_type, entry.actor_type);
        assert_eq!(deserialized.outcome, entry.outcome);
        assert_eq!(deserialized.client_ip, entry.client_ip);
        assert_eq!(deserialized.request_id, entry.request_id);

        for actor_type in [
            ActorType::User,
            ActorType::Service,
            ActorType::System,
            ActorType::Anonymous,
        ] {
            let json = serde_json::to_string(&actor_type).unwrap();
            let deser: ActorType = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, actor_type);
        }

        for outcome in [
            AuditOutcome::Success,
            AuditOutcome::Failure,
            AuditOutcome::Denied,
            AuditOutcome::Error,
        ] {
            let json = serde_json::to_string(&outcome).unwrap();
            let deser: AuditOutcome = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, outcome);
        }
    }

    #[test]
    fn test_trait_implementations() {
        let user = ActorType::User;
        let service = ActorType::Service;
        let system = ActorType::System;
        let anonymous = ActorType::Anonymous;
        assert_ne!(user, service);
        assert_ne!(service, system);
        assert_ne!(system, anonymous);
        assert_eq!(user, ActorType::User);

        let success = AuditOutcome::Success;
        let failure = AuditOutcome::Failure;
        let denied = AuditOutcome::Denied;
        let error = AuditOutcome::Error;
        assert_ne!(success, failure);
        assert_ne!(failure, denied);
        assert_ne!(denied, error);
        assert_eq!(success, AuditOutcome::Success);

        let config = AuditLoggerConfig {
            buffer_size: 500,
            flush_interval: Duration::from_secs(30),
            max_entries_per_flush: 100,
            enabled: false,
        };
        let cloned_config = config.clone();
        assert_eq!(cloned_config.buffer_size, config.buffer_size);
        assert_eq!(cloned_config.flush_interval, config.flush_interval);
        assert_eq!(
            cloned_config.max_entries_per_flush,
            config.max_entries_per_flush
        );
        assert_eq!(cloned_config.enabled, config.enabled);

        let entry = AuditEntry::new("event", "actor", "resource", "id", "action")
            .with_actor_type(ActorType::User)
            .with_outcome(AuditOutcome::Failure)
            .with_metadata("key", "value")
            .with_client_ip("10.0.0.1")
            .with_request_id("req-456");
        let cloned_entry = entry.clone();
        assert_eq!(cloned_entry.id, entry.id);
        assert_eq!(cloned_entry.event_type, entry.event_type);
        assert_eq!(cloned_entry.actor_id, entry.actor_id);
        assert_eq!(cloned_entry.actor_type, entry.actor_type);
        assert_eq!(cloned_entry.outcome, entry.outcome);
        assert_eq!(cloned_entry.metadata, entry.metadata);
        assert_eq!(cloned_entry.client_ip, entry.client_ip);
        assert_eq!(cloned_entry.request_id, entry.request_id);

        let actor_copy = ActorType::Service;
        let copied_actor = actor_copy;
        assert_eq!(copied_actor, ActorType::Service);
        assert_eq!(actor_copy, ActorType::Service);

        let outcome_copy = AuditOutcome::Denied;
        let copied_outcome = outcome_copy;
        assert_eq!(copied_outcome, AuditOutcome::Denied);
        assert_eq!(outcome_copy, AuditOutcome::Denied);

        let debug_entry = AuditEntry::new("event", "actor", "resource", "id", "action");
        let debug_str = format!("{:?}", debug_entry);
        assert!(debug_str.contains("AuditEntry"));
        assert!(debug_str.contains("event"));

        let debug_actor = format!("{:?}", ActorType::User);
        assert!(debug_actor.contains("User"));

        let debug_outcome = format!("{:?}", AuditOutcome::Success);
        assert!(debug_outcome.contains("Success"));

        let debug_config = format!("{:?}", AuditLoggerConfig::default());
        assert!(debug_config.contains("AuditLoggerConfig"));
    }
}
