use std::fmt;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

impl SessionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SessionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SessionId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for SessionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for SessionId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for NodeId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_session_id_new() {
        let id = SessionId::new("session-1");
        assert_eq!(id.as_str(), "session-1");
    }

    #[test]
    fn test_session_id_new_string() {
        let id = SessionId::new("session-2".to_string());
        assert_eq!(id.as_str(), "session-2");
    }

    #[test]
    fn test_session_id_display() {
        let id = SessionId::new("session-3");
        assert_eq!(format!("{}", id), "session-3");
    }

    #[test]
    fn test_session_id_from_string() {
        let id: SessionId = "session-4".to_string().into();
        assert_eq!(id.as_str(), "session-4");
    }

    #[test]
    fn test_session_id_from_str() {
        let id: SessionId = "session-5".into();
        assert_eq!(id.as_str(), "session-5");
    }

    #[test]
    fn test_session_id_as_ref() {
        let id = SessionId::new("session-6");
        let s: &str = id.as_ref();
        assert_eq!(s, "session-6");
    }

    #[test]
    fn test_session_id_clone() {
        let id = SessionId::new("session-7");
        let cloned = id.clone();
        assert_eq!(id, cloned);
    }

    #[test]
    fn test_session_id_eq() {
        let id1 = SessionId::new("session-8");
        let id2 = SessionId::new("session-8");
        let id3 = SessionId::new("session-9");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_session_id_hash() {
        let mut set = HashSet::new();
        set.insert(SessionId::new("session-10"));
        set.insert(SessionId::new("session-10"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_session_id_debug() {
        let id = SessionId::new("session-11");
        let debug = format!("{:?}", id);
        assert!(debug.contains("SessionId"));
        assert!(debug.contains("session-11"));
    }

    #[test]
    fn test_node_id_new() {
        let id = NodeId::new("node-1");
        assert_eq!(id.as_str(), "node-1");
    }

    #[test]
    fn test_node_id_new_string() {
        let id = NodeId::new("node-2".to_string());
        assert_eq!(id.as_str(), "node-2");
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId::new("node-3");
        assert_eq!(format!("{}", id), "node-3");
    }

    #[test]
    fn test_node_id_from_string() {
        let id: NodeId = "node-4".to_string().into();
        assert_eq!(id.as_str(), "node-4");
    }

    #[test]
    fn test_node_id_from_str() {
        let id: NodeId = "node-5".into();
        assert_eq!(id.as_str(), "node-5");
    }

    #[test]
    fn test_node_id_as_ref() {
        let id = NodeId::new("node-6");
        let s: &str = id.as_ref();
        assert_eq!(s, "node-6");
    }

    #[test]
    fn test_node_id_clone() {
        let id = NodeId::new("node-7");
        let cloned = id.clone();
        assert_eq!(id, cloned);
    }

    #[test]
    fn test_node_id_eq() {
        let id1 = NodeId::new("node-8");
        let id2 = NodeId::new("node-8");
        let id3 = NodeId::new("node-9");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_node_id_hash() {
        let mut set = HashSet::new();
        set.insert(NodeId::new("node-10"));
        set.insert(NodeId::new("node-10"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_node_id_debug() {
        let id = NodeId::new("node-11");
        let debug = format!("{:?}", id);
        assert!(debug.contains("NodeId"));
        assert!(debug.contains("node-11"));
    }

    #[test]
    fn test_session_id_empty() {
        let id = SessionId::new("");
        assert_eq!(id.as_str(), "");
    }

    #[test]
    fn test_node_id_empty() {
        let id = NodeId::new("");
        assert_eq!(id.as_str(), "");
    }

    #[test]
    fn test_session_id_with_special_chars() {
        let id = SessionId::new("session-abc-123_456");
        assert_eq!(id.as_str(), "session-abc-123_456");
    }

    #[test]
    fn test_node_id_with_special_chars() {
        let id = NodeId::new("node-abc-123_456");
        assert_eq!(id.as_str(), "node-abc-123_456");
    }
}
