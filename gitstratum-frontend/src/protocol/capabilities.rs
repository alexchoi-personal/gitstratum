use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    caps: HashSet<String>,
}

impl Capabilities {
    pub fn new() -> Self {
        Self {
            caps: HashSet::new(),
        }
    }

    pub fn add(&mut self, cap: impl Into<String>) {
        self.caps.insert(cap.into());
    }

    pub fn has(&self, cap: &str) -> bool {
        self.caps.contains(cap)
    }

    pub fn parse(s: &str) -> Self {
        let mut caps = Self::new();
        for cap in s.split_whitespace() {
            caps.add(cap);
        }
        caps
    }

    pub fn to_string(&self) -> String {
        let mut sorted: Vec<_> = self.caps.iter().collect();
        sorted.sort();
        sorted.into_iter().cloned().collect::<Vec<_>>().join(" ")
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.caps.iter()
    }
}

#[derive(Debug, Clone)]
pub struct ServerCapabilities {
    pub ls_refs: bool,
    pub fetch: bool,
    pub push: bool,
    pub object_format: String,
    pub agent: String,
}

impl ServerCapabilities {
    pub fn new() -> Self {
        Self {
            ls_refs: true,
            fetch: true,
            push: true,
            object_format: "sha256".to_string(),
            agent: "gitstratum/1.0".to_string(),
        }
    }

    pub fn to_capability_advertisement(&self) -> Vec<String> {
        let mut caps = Vec::new();
        caps.push("version 2".to_string());
        caps.push(format!("agent={}", self.agent));
        if self.ls_refs {
            caps.push("ls-refs".to_string());
        }
        if self.fetch {
            caps.push("fetch".to_string());
        }
        if self.push {
            caps.push("push".to_string());
        }
        caps.push(format!("object-format={}", self.object_format));
        caps
    }
}

impl Default for ServerCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities_new() {
        let caps = Capabilities::new();
        assert!(!caps.has("anything"));
    }

    #[test]
    fn test_capabilities_add_and_has() {
        let mut caps = Capabilities::new();
        caps.add("multi_ack");
        assert!(caps.has("multi_ack"));
        assert!(!caps.has("side-band"));
    }

    #[test]
    fn test_capabilities_parse() {
        let caps = Capabilities::parse("multi_ack side-band-64k ofs-delta");
        assert!(caps.has("multi_ack"));
        assert!(caps.has("side-band-64k"));
        assert!(caps.has("ofs-delta"));
        assert!(!caps.has("thin-pack"));
    }

    #[test]
    fn test_capabilities_to_string() {
        let mut caps = Capabilities::new();
        caps.add("b_cap");
        caps.add("a_cap");
        let s = caps.to_string();
        assert!(s.contains("a_cap"));
        assert!(s.contains("b_cap"));
    }

    #[test]
    fn test_capabilities_iter() {
        let mut caps = Capabilities::new();
        caps.add("cap1");
        caps.add("cap2");
        let count = caps.iter().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_capabilities_default() {
        let caps = Capabilities::default();
        assert!(!caps.has("anything"));
    }

    #[test]
    fn test_server_capabilities_new() {
        let caps = ServerCapabilities::new();
        assert!(caps.ls_refs);
        assert!(caps.fetch);
        assert!(caps.push);
        assert_eq!(caps.object_format, "sha256");
    }

    #[test]
    fn test_server_capabilities_default() {
        let caps = ServerCapabilities::default();
        assert!(caps.ls_refs);
    }

    #[test]
    fn test_server_capabilities_advertisement() {
        let caps = ServerCapabilities::new();
        let adv = caps.to_capability_advertisement();
        assert!(adv.contains(&"version 2".to_string()));
        assert!(adv.iter().any(|s| s.starts_with("agent=")));
        assert!(adv.contains(&"ls-refs".to_string()));
        assert!(adv.contains(&"fetch".to_string()));
        assert!(adv.contains(&"push".to_string()));
    }
}
