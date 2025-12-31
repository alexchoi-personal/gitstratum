use gitstratum_core::Oid;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct SessionState {
    pub session_id: String,
    pub repo_id: String,
    pub wants: HashSet<Oid>,
    pub haves: HashSet<Oid>,
    pub shallow: HashSet<Oid>,
    pub common_commits: HashSet<Oid>,
    pub round: u32,
    pub ready: bool,
    pub created_at: Instant,
    pub last_activity: Instant,
}

impl SessionState {
    pub fn new(session_id: String, repo_id: String) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            repo_id,
            wants: HashSet::new(),
            haves: HashSet::new(),
            shallow: HashSet::new(),
            common_commits: HashSet::new(),
            round: 0,
            ready: false,
            created_at: now,
            last_activity: now,
        }
    }

    pub fn add_want(&mut self, oid: Oid) {
        self.wants.insert(oid);
        self.touch();
    }

    pub fn add_have(&mut self, oid: Oid) {
        self.haves.insert(oid);
        self.touch();
    }

    pub fn add_shallow(&mut self, oid: Oid) {
        self.shallow.insert(oid);
        self.touch();
    }

    pub fn add_common(&mut self, oid: Oid) {
        self.common_commits.insert(oid);
        self.touch();
    }

    pub fn increment_round(&mut self) {
        self.round += 1;
        self.touch();
    }

    pub fn set_ready(&mut self) {
        self.ready = true;
        self.touch();
    }

    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn idle_time(&self) -> Duration {
        self.last_activity.elapsed()
    }
}

pub struct SessionCache {
    sessions: Arc<RwLock<HashMap<String, SessionState>>>,
    max_sessions: usize,
    session_ttl: Duration,
    idle_timeout: Duration,
}

impl SessionCache {
    pub fn new(max_sessions: usize, session_ttl: Duration, idle_timeout: Duration) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions,
            session_ttl,
            idle_timeout,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            10000,
            Duration::from_secs(300),
            Duration::from_secs(60),
        )
    }

    pub async fn create(&self, session_id: String, repo_id: String) -> SessionState {
        let state = SessionState::new(session_id.clone(), repo_id);

        let mut sessions = self.sessions.write().await;

        if sessions.len() >= self.max_sessions {
            self.evict_oldest(&mut sessions);
        }

        sessions.insert(session_id, state.clone());
        state
    }

    pub async fn get(&self, session_id: &str) -> Option<SessionState> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    pub async fn update(&self, state: SessionState) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(state.session_id.clone(), state);
    }

    pub async fn remove(&self, session_id: &str) -> Option<SessionState> {
        let mut sessions = self.sessions.write().await;
        sessions.remove(session_id)
    }

    pub async fn cleanup_expired(&self) {
        let mut sessions = self.sessions.write().await;
        let session_ttl = self.session_ttl;
        let idle_timeout = self.idle_timeout;

        sessions.retain(|_, state| {
            state.age() < session_ttl && state.idle_time() < idle_timeout
        });
    }

    pub async fn len(&self) -> usize {
        self.sessions.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.sessions.read().await.is_empty()
    }

    fn evict_oldest(&self, sessions: &mut HashMap<String, SessionState>) {
        if let Some((oldest_key, _)) = sessions
            .iter()
            .min_by_key(|(_, v)| v.last_activity)
            .map(|(k, v)| (k.clone(), v.last_activity))
        {
            sessions.remove(&oldest_key);
        }
    }
}

impl Default for SessionCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_state_new() {
        let state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        assert_eq!(state.session_id, "session-1");
        assert_eq!(state.repo_id, "repo-1");
        assert!(state.wants.is_empty());
        assert!(state.haves.is_empty());
        assert_eq!(state.round, 0);
        assert!(!state.ready);
    }

    #[test]
    fn test_session_state_add_want() {
        let mut state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        let oid = Oid::hash(b"commit");
        state.add_want(oid);
        assert!(state.wants.contains(&oid));
    }

    #[test]
    fn test_session_state_add_have() {
        let mut state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        let oid = Oid::hash(b"commit");
        state.add_have(oid);
        assert!(state.haves.contains(&oid));
    }

    #[test]
    fn test_session_state_add_shallow() {
        let mut state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        let oid = Oid::hash(b"commit");
        state.add_shallow(oid);
        assert!(state.shallow.contains(&oid));
    }

    #[test]
    fn test_session_state_add_common() {
        let mut state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        let oid = Oid::hash(b"commit");
        state.add_common(oid);
        assert!(state.common_commits.contains(&oid));
    }

    #[test]
    fn test_session_state_increment_round() {
        let mut state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        state.increment_round();
        state.increment_round();
        assert_eq!(state.round, 2);
    }

    #[test]
    fn test_session_state_set_ready() {
        let mut state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        state.set_ready();
        assert!(state.ready);
    }

    #[test]
    fn test_session_state_age() {
        let state = SessionState::new("session-1".to_string(), "repo-1".to_string());
        std::thread::sleep(Duration::from_millis(1));
        assert!(state.age() >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_session_cache_create() {
        let cache = SessionCache::with_defaults();
        let state = cache.create("session-1".to_string(), "repo-1".to_string()).await;
        assert_eq!(state.session_id, "session-1");
    }

    #[tokio::test]
    async fn test_session_cache_get() {
        let cache = SessionCache::with_defaults();
        cache.create("session-1".to_string(), "repo-1".to_string()).await;

        let state = cache.get("session-1").await.unwrap();
        assert_eq!(state.session_id, "session-1");
    }

    #[tokio::test]
    async fn test_session_cache_get_missing() {
        let cache = SessionCache::with_defaults();
        let result = cache.get("nonexistent").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_session_cache_update() {
        let cache = SessionCache::with_defaults();
        let mut state = cache.create("session-1".to_string(), "repo-1".to_string()).await;

        state.set_ready();
        cache.update(state).await;

        let updated = cache.get("session-1").await.unwrap();
        assert!(updated.ready);
    }

    #[tokio::test]
    async fn test_session_cache_remove() {
        let cache = SessionCache::with_defaults();
        cache.create("session-1".to_string(), "repo-1".to_string()).await;

        let removed = cache.remove("session-1").await;
        assert!(removed.is_some());
        assert!(cache.get("session-1").await.is_none());
    }

    #[tokio::test]
    async fn test_session_cache_cleanup_expired() {
        let cache = SessionCache::new(
            100,
            Duration::from_millis(1),
            Duration::from_millis(1),
        );

        cache.create("session-1".to_string(), "repo-1".to_string()).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        cache.cleanup_expired().await;
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_session_cache_max_sessions() {
        let cache = SessionCache::new(
            2,
            Duration::from_secs(300),
            Duration::from_secs(60),
        );

        cache.create("session-1".to_string(), "repo-1".to_string()).await;
        cache.create("session-2".to_string(), "repo-1".to_string()).await;
        cache.create("session-3".to_string(), "repo-1".to_string()).await;

        assert_eq!(cache.len().await, 2);
    }

    #[tokio::test]
    async fn test_session_cache_default() {
        let cache = SessionCache::default();
        assert!(cache.is_empty().await);
    }
}
