use std::collections::HashSet;
use std::fmt;

use gitstratum_core::Oid;

use crate::error::{FrontendError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationLine {
    Want(Oid),
    Have(Oid),
    Done,
    Shallow(Oid),
    Deepen(u32),
    DeepenSince(i64),
    DeepenNot(String),
    Filter(String),
}

impl NegotiationLine {
    pub fn parse(line: &str) -> Result<Self> {
        let line = line.trim();

        if line == "done" {
            return Ok(NegotiationLine::Done);
        }

        if let Some(rest) = line.strip_prefix("want ") {
            let oid = parse_oid_from_line(rest)?;
            return Ok(NegotiationLine::Want(oid));
        }

        if let Some(rest) = line.strip_prefix("have ") {
            let oid = parse_oid_from_line(rest)?;
            return Ok(NegotiationLine::Have(oid));
        }

        if let Some(rest) = line.strip_prefix("shallow ") {
            let oid = parse_oid_from_line(rest)?;
            return Ok(NegotiationLine::Shallow(oid));
        }

        if let Some(rest) = line.strip_prefix("deepen ") {
            let depth: u32 = rest
                .trim()
                .parse()
                .map_err(|_| FrontendError::InvalidProtocol("invalid deepen value".to_string()))?;
            return Ok(NegotiationLine::Deepen(depth));
        }

        if let Some(rest) = line.strip_prefix("deepen-since ") {
            let timestamp: i64 = rest.trim().parse().map_err(|_| {
                FrontendError::InvalidProtocol("invalid deepen-since value".to_string())
            })?;
            return Ok(NegotiationLine::DeepenSince(timestamp));
        }

        if let Some(rest) = line.strip_prefix("deepen-not ") {
            return Ok(NegotiationLine::DeepenNot(rest.trim().to_string()));
        }

        if let Some(rest) = line.strip_prefix("filter ") {
            return Ok(NegotiationLine::Filter(rest.trim().to_string()));
        }

        Err(FrontendError::InvalidProtocol(format!(
            "unknown negotiation line: {}",
            line
        )))
    }
}

impl fmt::Display for NegotiationLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NegotiationLine::Want(oid) => write!(f, "want {}", oid),
            NegotiationLine::Have(oid) => write!(f, "have {}", oid),
            NegotiationLine::Done => write!(f, "done"),
            NegotiationLine::Shallow(oid) => write!(f, "shallow {}", oid),
            NegotiationLine::Deepen(depth) => write!(f, "deepen {}", depth),
            NegotiationLine::DeepenSince(ts) => write!(f, "deepen-since {}", ts),
            NegotiationLine::DeepenNot(ref_name) => write!(f, "deepen-not {}", ref_name),
            NegotiationLine::Filter(spec) => write!(f, "filter {}", spec),
        }
    }
}

fn parse_oid_from_line(s: &str) -> Result<Oid> {
    let hex = s.split_whitespace().next().unwrap_or(s);
    Oid::from_hex(hex).map_err(|e| FrontendError::InvalidProtocol(format!("invalid oid: {}", e)))
}

#[derive(Debug, Clone)]
pub struct NegotiationRequest {
    pub wants: HashSet<Oid>,
    pub haves: HashSet<Oid>,
    pub shallow: HashSet<Oid>,
    pub depth: Option<u32>,
    pub deepen_since: Option<i64>,
    pub deepen_not: Vec<String>,
    pub filter: Option<String>,
    pub done: bool,
}

impl NegotiationRequest {
    pub fn new() -> Self {
        Self {
            wants: HashSet::new(),
            haves: HashSet::new(),
            shallow: HashSet::new(),
            depth: None,
            deepen_since: None,
            deepen_not: Vec::new(),
            filter: None,
            done: false,
        }
    }

    pub fn parse_lines(lines: &[String]) -> Result<Self> {
        let mut request = Self::new();

        for line in lines {
            if line.is_empty() {
                continue;
            }

            let parsed = NegotiationLine::parse(line)?;
            match parsed {
                NegotiationLine::Want(oid) => {
                    request.wants.insert(oid);
                }
                NegotiationLine::Have(oid) => {
                    request.haves.insert(oid);
                }
                NegotiationLine::Done => {
                    request.done = true;
                }
                NegotiationLine::Shallow(oid) => {
                    request.shallow.insert(oid);
                }
                NegotiationLine::Deepen(depth) => {
                    request.depth = Some(depth);
                }
                NegotiationLine::DeepenSince(ts) => {
                    request.deepen_since = Some(ts);
                }
                NegotiationLine::DeepenNot(ref_name) => {
                    request.deepen_not.push(ref_name);
                }
                NegotiationLine::Filter(spec) => {
                    request.filter = Some(spec);
                }
            }
        }

        Ok(request)
    }

    pub fn is_clone(&self) -> bool {
        self.haves.is_empty()
    }

    pub fn has_common_commits(&self, available: &HashSet<Oid>) -> bool {
        self.haves.iter().any(|h| available.contains(h))
    }

    pub fn add_want(&mut self, oid: Oid) {
        self.wants.insert(oid);
    }

    pub fn add_have(&mut self, oid: Oid) {
        self.haves.insert(oid);
    }
}

impl Default for NegotiationRequest {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct NegotiationResponse {
    pub acks: Vec<(Oid, AckType)>,
    pub naks: Vec<Oid>,
    pub ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckType {
    Ack,
    AckContinue,
    AckCommon,
    AckReady,
}

impl NegotiationResponse {
    pub fn new() -> Self {
        Self {
            acks: Vec::new(),
            naks: Vec::new(),
            ready: false,
        }
    }

    pub fn nak() -> Self {
        Self::new()
    }

    pub fn add_ack(&mut self, oid: Oid, ack_type: AckType) {
        self.acks.push((oid, ack_type));
    }

    pub fn add_nak(&mut self, oid: Oid) {
        self.naks.push(oid);
    }

    pub fn set_ready(&mut self) {
        self.ready = true;
    }

    pub fn to_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();

        for (oid, ack_type) in &self.acks {
            let line = match ack_type {
                AckType::Ack => format!("ACK {}", oid),
                AckType::AckContinue => format!("ACK {} continue", oid),
                AckType::AckCommon => format!("ACK {} common", oid),
                AckType::AckReady => format!("ACK {} ready", oid),
            };
            lines.push(line);
        }

        if self.acks.is_empty() && !self.naks.is_empty() {
            lines.push("NAK".to_string());
        }

        if self.ready {
            lines.push("ready".to_string());
        }

        lines
    }
}

impl Default for NegotiationResponse {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ObjectWalker {
    wanted: HashSet<Oid>,
    known: HashSet<Oid>,
    to_send: Vec<Oid>,
    visited: HashSet<Oid>,
}

impl ObjectWalker {
    pub fn new(wants: HashSet<Oid>, haves: HashSet<Oid>) -> Self {
        Self {
            wanted: wants,
            known: haves,
            to_send: Vec::new(),
            visited: HashSet::new(),
        }
    }

    pub fn from_request(request: &NegotiationRequest) -> Self {
        Self::new(request.wants.clone(), request.haves.clone())
    }

    pub fn needs_object(&self, oid: &Oid) -> bool {
        !self.known.contains(oid) && !self.visited.contains(oid)
    }

    pub fn mark_visited(&mut self, oid: Oid) {
        self.visited.insert(oid);
    }

    pub fn mark_to_send(&mut self, oid: Oid) {
        if !self.visited.contains(&oid) && !self.known.contains(&oid) {
            self.to_send.push(oid);
            self.visited.insert(oid);
        }
    }

    pub fn is_boundary(&self, oid: &Oid) -> bool {
        self.known.contains(oid)
    }

    pub fn objects_to_send(&self) -> &[Oid] {
        &self.to_send
    }

    pub fn add_known(&mut self, oid: Oid) {
        self.known.insert(oid);
    }

    pub fn wanted(&self) -> &HashSet<Oid> {
        &self.wanted
    }

    pub fn known(&self) -> &HashSet<Oid> {
        &self.known
    }
}

pub fn compute_common_commits(
    client_haves: &HashSet<Oid>,
    server_commits: &HashSet<Oid>,
) -> HashSet<Oid> {
    client_haves.intersection(server_commits).copied().collect()
}

pub fn negotiate_refs(
    request: &NegotiationRequest,
    available_commits: &HashSet<Oid>,
) -> NegotiationResponse {
    let mut response = NegotiationResponse::new();

    let common = compute_common_commits(&request.haves, available_commits);

    if common.is_empty() {
        if !request.wants.is_empty() {
            response.add_nak(Oid::ZERO);
        }
    } else {
        for oid in common.iter().take(32) {
            response.add_ack(*oid, AckType::AckCommon);
        }
    }

    if request.done {
        response.set_ready();
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(s: &str) -> Oid {
        Oid::hash(s.as_bytes())
    }

    #[test]
    fn test_parse_want() {
        let oid = make_oid("test");
        let line = format!("want {}", oid);
        let parsed = NegotiationLine::parse(&line).unwrap();
        assert_eq!(parsed, NegotiationLine::Want(oid));
    }

    #[test]
    fn test_parse_have() {
        let oid = make_oid("test");
        let line = format!("have {}", oid);
        let parsed = NegotiationLine::parse(&line).unwrap();
        assert_eq!(parsed, NegotiationLine::Have(oid));
    }

    #[test]
    fn test_parse_done() {
        let parsed = NegotiationLine::parse("done").unwrap();
        assert_eq!(parsed, NegotiationLine::Done);
    }

    #[test]
    fn test_parse_shallow() {
        let oid = make_oid("shallow");
        let line = format!("shallow {}", oid);
        let parsed = NegotiationLine::parse(&line).unwrap();
        assert_eq!(parsed, NegotiationLine::Shallow(oid));
    }

    #[test]
    fn test_parse_deepen() {
        let parsed = NegotiationLine::parse("deepen 10").unwrap();
        assert_eq!(parsed, NegotiationLine::Deepen(10));
    }

    #[test]
    fn test_parse_deepen_since() {
        let parsed = NegotiationLine::parse("deepen-since 1704067200").unwrap();
        assert_eq!(parsed, NegotiationLine::DeepenSince(1704067200));
    }

    #[test]
    fn test_parse_deepen_not() {
        let parsed = NegotiationLine::parse("deepen-not refs/heads/main").unwrap();
        assert_eq!(
            parsed,
            NegotiationLine::DeepenNot("refs/heads/main".to_string())
        );
    }

    #[test]
    fn test_parse_filter() {
        let parsed = NegotiationLine::parse("filter blob:none").unwrap();
        assert_eq!(parsed, NegotiationLine::Filter("blob:none".to_string()));
    }

    #[test]
    fn test_negotiation_line_to_string() {
        let oid = make_oid("test");

        assert_eq!(
            NegotiationLine::Want(oid).to_string(),
            format!("want {}", oid)
        );
        assert_eq!(
            NegotiationLine::Have(oid).to_string(),
            format!("have {}", oid)
        );
        assert_eq!(NegotiationLine::Done.to_string(), "done");
        assert_eq!(NegotiationLine::Deepen(5).to_string(), "deepen 5");
    }

    #[test]
    fn test_negotiation_request_parse() {
        let oid1 = make_oid("want1");
        let oid2 = make_oid("have1");

        let lines = vec![
            format!("want {}", oid1),
            format!("have {}", oid2),
            "deepen 10".to_string(),
            "done".to_string(),
        ];

        let request = NegotiationRequest::parse_lines(&lines).unwrap();

        assert!(request.wants.contains(&oid1));
        assert!(request.haves.contains(&oid2));
        assert_eq!(request.depth, Some(10));
        assert!(request.done);
    }

    #[test]
    fn test_negotiation_request_is_clone() {
        let mut request = NegotiationRequest::new();
        assert!(request.is_clone());

        request.add_have(make_oid("have"));
        assert!(!request.is_clone());
    }

    #[test]
    fn test_negotiation_response_nak() {
        let response = NegotiationResponse::nak();
        assert!(response.acks.is_empty());
        assert!(!response.ready);
    }

    #[test]
    fn test_negotiation_response_to_lines() {
        let mut response = NegotiationResponse::new();
        let oid = make_oid("ack");
        response.add_ack(oid, AckType::AckCommon);
        response.set_ready();

        let lines = response.to_lines();
        assert!(lines.iter().any(|l| l.contains("ACK")));
        assert!(lines.contains(&"ready".to_string()));
    }

    #[test]
    fn test_object_walker_needs_object() {
        let want = make_oid("want");
        let have = make_oid("have");

        let wants = [want].into_iter().collect();
        let haves = [have].into_iter().collect();

        let walker = ObjectWalker::new(wants, haves);

        assert!(walker.needs_object(&want));
        assert!(!walker.needs_object(&have));
    }

    #[test]
    fn test_object_walker_mark_to_send() {
        let want = make_oid("want");
        let wants = [want].into_iter().collect();
        let haves = HashSet::new();

        let mut walker = ObjectWalker::new(wants, haves);

        let new_oid = make_oid("new");
        walker.mark_to_send(new_oid);

        assert!(walker.objects_to_send().contains(&new_oid));
        assert!(!walker.needs_object(&new_oid));
    }

    #[test]
    fn test_object_walker_boundary() {
        let have = make_oid("have");
        let wants = HashSet::new();
        let haves = [have].into_iter().collect();

        let walker = ObjectWalker::new(wants, haves);
        assert!(walker.is_boundary(&have));
    }

    #[test]
    fn test_compute_common_commits() {
        let commit1 = make_oid("commit1");
        let commit2 = make_oid("commit2");
        let commit3 = make_oid("commit3");

        let client_haves: HashSet<_> = [commit1, commit2].into_iter().collect();
        let server_commits: HashSet<_> = [commit2, commit3].into_iter().collect();

        let common = compute_common_commits(&client_haves, &server_commits);

        assert_eq!(common.len(), 1);
        assert!(common.contains(&commit2));
    }

    #[test]
    fn test_negotiate_refs_with_common() {
        let want = make_oid("want");
        let common_commit = make_oid("common");

        let mut request = NegotiationRequest::new();
        request.add_want(want);
        request.add_have(common_commit);
        request.done = true;

        let available: HashSet<_> = [common_commit, want].into_iter().collect();

        let response = negotiate_refs(&request, &available);

        assert!(!response.acks.is_empty());
        assert!(response.ready);
    }

    #[test]
    fn test_negotiate_refs_no_common() {
        let want = make_oid("want");
        let client_have = make_oid("client_have");

        let mut request = NegotiationRequest::new();
        request.add_want(want);
        request.add_have(client_have);

        let available: HashSet<_> = [want].into_iter().collect();

        let response = negotiate_refs(&request, &available);

        assert!(response.acks.is_empty());
    }

    #[test]
    fn test_parse_want_with_capabilities() {
        let oid = make_oid("test");
        let line = format!("want {} multi_ack_detailed side-band-64k", oid);
        let parsed = NegotiationLine::parse(&line).unwrap();
        assert_eq!(parsed, NegotiationLine::Want(oid));
    }

    #[test]
    fn test_object_walker_from_request() {
        let mut request = NegotiationRequest::new();
        request.add_want(make_oid("want1"));
        request.add_want(make_oid("want2"));
        request.add_have(make_oid("have1"));

        let walker = ObjectWalker::from_request(&request);

        assert_eq!(walker.wanted().len(), 2);
        assert_eq!(walker.known().len(), 1);
    }

    #[test]
    fn test_request_has_common_commits() {
        let common = make_oid("common");

        let mut request = NegotiationRequest::new();
        request.add_have(common);
        request.add_have(make_oid("other"));

        let available: HashSet<_> = [common].into_iter().collect();

        assert!(request.has_common_commits(&available));

        let no_match: HashSet<_> = [make_oid("different")].into_iter().collect();
        assert!(!request.has_common_commits(&no_match));
    }

    #[test]
    fn test_ack_types() {
        let oid = make_oid("test");

        let mut response = NegotiationResponse::new();
        response.add_ack(oid, AckType::Ack);
        response.add_ack(oid, AckType::AckContinue);
        response.add_ack(oid, AckType::AckCommon);
        response.add_ack(oid, AckType::AckReady);

        let lines = response.to_lines();
        assert!(lines.iter().any(|l| l.ends_with(&oid.to_string())));
        assert!(lines.iter().any(|l| l.contains("continue")));
        assert!(lines.iter().any(|l| l.contains("common")));
        assert!(lines.iter().any(|l| l.contains("ready")));
    }

    #[test]
    fn test_parse_unknown_line() {
        let result = NegotiationLine::parse("unknown command");
        assert!(result.is_err());
    }

    #[test]
    fn test_negotiation_line_to_string_all() {
        let oid = make_oid("test");

        assert_eq!(
            NegotiationLine::Shallow(oid).to_string(),
            format!("shallow {}", oid)
        );
        assert_eq!(
            NegotiationLine::DeepenSince(1704067200).to_string(),
            "deepen-since 1704067200"
        );
        assert_eq!(
            NegotiationLine::DeepenNot("refs/heads/main".to_string()).to_string(),
            "deepen-not refs/heads/main"
        );
        assert_eq!(
            NegotiationLine::Filter("blob:none".to_string()).to_string(),
            "filter blob:none"
        );
    }

    #[test]
    fn test_negotiation_request_parse_all_types() {
        let oid1 = make_oid("want1");
        let oid2 = make_oid("have1");
        let oid3 = make_oid("shallow1");

        let lines = vec![
            format!("want {}", oid1),
            format!("have {}", oid2),
            format!("shallow {}", oid3),
            "deepen 5".to_string(),
            "deepen-since 1704067200".to_string(),
            "deepen-not refs/heads/main".to_string(),
            "filter blob:none".to_string(),
            "done".to_string(),
            "".to_string(),
        ];

        let request = NegotiationRequest::parse_lines(&lines).unwrap();

        assert!(request.wants.contains(&oid1));
        assert!(request.haves.contains(&oid2));
        assert!(request.shallow.contains(&oid3));
        assert_eq!(request.depth, Some(5));
        assert_eq!(request.deepen_since, Some(1704067200));
        assert_eq!(request.deepen_not, vec!["refs/heads/main".to_string()]);
        assert_eq!(request.filter, Some("blob:none".to_string()));
        assert!(request.done);
    }

    #[test]
    fn test_negotiation_request_default() {
        let request = NegotiationRequest::default();
        assert!(request.wants.is_empty());
        assert!(request.haves.is_empty());
        assert!(!request.done);
    }

    #[test]
    fn test_negotiation_response_nak_with_naks() {
        let mut response = NegotiationResponse::new();
        response.add_nak(Oid::ZERO);

        let lines = response.to_lines();
        assert!(lines.contains(&"NAK".to_string()));
    }

    #[test]
    fn test_negotiation_response_default() {
        let response = NegotiationResponse::default();
        assert!(response.acks.is_empty());
        assert!(response.naks.is_empty());
        assert!(!response.ready);
    }

    #[test]
    fn test_object_walker_add_known() {
        let want = make_oid("want");
        let wants = [want].into_iter().collect();
        let haves = HashSet::new();

        let mut walker = ObjectWalker::new(wants, haves);

        let new_known = make_oid("new_known");
        walker.add_known(new_known);

        assert!(!walker.needs_object(&new_known));
        assert!(walker.is_boundary(&new_known));
    }
}
