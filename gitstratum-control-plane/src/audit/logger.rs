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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: now,
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
