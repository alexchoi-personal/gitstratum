use crate::error::{FrontendError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    LsRefs,
    Fetch,
    Push,
}

impl Command {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim() {
            "ls-refs" => Ok(Command::LsRefs),
            "fetch" => Ok(Command::Fetch),
            "push" | "receive-pack" => Ok(Command::Push),
            _ => Err(FrontendError::InvalidProtocol(format!(
                "unknown command: {}",
                s
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Command::LsRefs => "ls-refs",
            Command::Fetch => "fetch",
            Command::Push => "push",
        }
    }
}

pub struct ProtocolV2 {
    version: u8,
}

impl ProtocolV2 {
    pub fn new() -> Self {
        Self { version: 2 }
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn parse_command(&self, line: &str) -> Result<Command> {
        Command::parse(line)
    }
}

impl Default for ProtocolV2 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_parse() {
        assert_eq!(Command::parse("ls-refs").unwrap(), Command::LsRefs);
        assert_eq!(Command::parse("fetch").unwrap(), Command::Fetch);
        assert_eq!(Command::parse("push").unwrap(), Command::Push);
        assert_eq!(Command::parse("receive-pack").unwrap(), Command::Push);
    }

    #[test]
    fn test_command_parse_unknown() {
        assert!(Command::parse("unknown").is_err());
    }

    #[test]
    fn test_command_as_str() {
        assert_eq!(Command::LsRefs.as_str(), "ls-refs");
        assert_eq!(Command::Fetch.as_str(), "fetch");
        assert_eq!(Command::Push.as_str(), "push");
    }

    #[test]
    fn test_protocol_v2() {
        let proto = ProtocolV2::new();
        assert_eq!(proto.version(), 2);
    }

    #[test]
    fn test_protocol_v2_default() {
        let proto = ProtocolV2::default();
        assert_eq!(proto.version(), 2);
    }

    #[test]
    fn test_protocol_v2_parse_command() {
        let proto = ProtocolV2::new();
        assert_eq!(proto.parse_command("ls-refs").unwrap(), Command::LsRefs);
    }
}
