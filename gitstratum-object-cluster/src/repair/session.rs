use std::cmp::Ordering;

use gitstratum_core::Oid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PositionRange {
    pub start: u64,
    pub end: u64,
}

impl PositionRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub fn contains(&self, position: u64) -> bool {
        position >= self.start && position < self.end
    }

    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepairSessionStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl RepairSessionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RepairSessionStatus::Completed
                | RepairSessionStatus::Failed
                | RepairSessionStatus::Cancelled
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, RepairSessionStatus::InProgress)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RebalanceDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepairType {
    CrashRecovery {
        downtime_start: u64,
        downtime_end: u64,
    },
    AntiEntropy,
    Rebalance {
        direction: RebalanceDirection,
    },
}

impl RepairType {
    pub fn crash_recovery(downtime_start: u64, downtime_end: u64) -> Self {
        Self::CrashRecovery {
            downtime_start,
            downtime_end,
        }
    }

    pub fn rebalance_incoming() -> Self {
        Self::Rebalance {
            direction: RebalanceDirection::Incoming,
        }
    }

    pub fn rebalance_outgoing() -> Self {
        Self::Rebalance {
            direction: RebalanceDirection::Outgoing,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairCheckpoint {
    pub position: u64,
    pub oid: Oid,
    pub timestamp: u64,
}

impl RepairCheckpoint {
    pub fn new(position: u64, oid: Oid, timestamp: u64) -> Self {
        Self {
            position,
            oid,
            timestamp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepairPriority {
    FailedWrite = 1,
    CrashRecovery = 2,
    Rebalance = 3,
    AntiEntropy = 4,
}

impl RepairPriority {
    pub fn value(&self) -> u8 {
        *self as u8
    }
}

impl PartialOrd for RepairPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RepairPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RepairProgress {
    pub objects_compared: u64,
    pub objects_missing: u64,
    pub objects_transferred: u64,
    pub bytes_transferred: u64,
    pub last_checkpoint: Option<RepairCheckpoint>,
}

impl RepairProgress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_compared(&mut self, count: u64) {
        self.objects_compared = self.objects_compared.saturating_add(count);
    }

    pub fn add_missing(&mut self, count: u64) {
        self.objects_missing = self.objects_missing.saturating_add(count);
    }

    pub fn add_transferred(&mut self, count: u64, bytes: u64) {
        self.objects_transferred = self.objects_transferred.saturating_add(count);
        self.bytes_transferred = self.bytes_transferred.saturating_add(bytes);
    }

    pub fn set_checkpoint(&mut self, checkpoint: RepairCheckpoint) {
        self.last_checkpoint = Some(checkpoint);
    }

    pub fn clear_checkpoint(&mut self) {
        self.last_checkpoint = None;
    }

    pub fn missing_rate(&self) -> f64 {
        if self.objects_compared == 0 {
            return 0.0;
        }
        self.objects_missing as f64 / self.objects_compared as f64
    }

    pub fn transfer_rate(&self) -> f64 {
        if self.objects_missing == 0 {
            return 0.0;
        }
        self.objects_transferred as f64 / self.objects_missing as f64
    }
}

#[derive(Debug, Clone)]
pub struct RepairSession {
    id: String,
    session_type: RepairType,
    status: RepairSessionStatus,
    ring_version: u64,
    ranges: Vec<PositionRange>,
    progress: RepairProgress,
    peer_nodes: Vec<String>,
}

impl RepairSession {
    pub fn builder() -> RepairSessionBuilder {
        RepairSessionBuilder::default()
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn session_type(&self) -> RepairType {
        self.session_type
    }

    pub fn status(&self) -> RepairSessionStatus {
        self.status
    }

    pub fn ring_version(&self) -> u64 {
        self.ring_version
    }

    pub fn ranges(&self) -> &[PositionRange] {
        &self.ranges
    }

    pub fn progress(&self) -> &RepairProgress {
        &self.progress
    }

    pub fn progress_mut(&mut self) -> &mut RepairProgress {
        &mut self.progress
    }

    pub fn peer_nodes(&self) -> &[String] {
        &self.peer_nodes
    }

    pub fn set_status(&mut self, status: RepairSessionStatus) {
        self.status = status;
    }

    pub fn start(&mut self) {
        self.status = RepairSessionStatus::InProgress;
    }

    pub fn complete(&mut self) {
        self.status = RepairSessionStatus::Completed;
    }

    pub fn fail(&mut self) {
        self.status = RepairSessionStatus::Failed;
    }

    pub fn cancel(&mut self) {
        self.status = RepairSessionStatus::Cancelled;
    }

    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }

    pub fn add_compared(&mut self, count: u64) {
        self.progress.add_compared(count);
    }

    pub fn add_missing(&mut self, count: u64) {
        self.progress.add_missing(count);
    }

    pub fn add_transferred(&mut self, count: u64, bytes: u64) {
        self.progress.add_transferred(count, bytes);
    }

    pub fn set_checkpoint(&mut self, checkpoint: RepairCheckpoint) {
        self.progress.set_checkpoint(checkpoint);
    }
}

#[derive(Debug, Clone, Default)]
pub struct RepairSessionBuilder {
    id: Option<String>,
    session_type: Option<RepairType>,
    status: Option<RepairSessionStatus>,
    ring_version: Option<u64>,
    ranges: Vec<PositionRange>,
    progress: Option<RepairProgress>,
    peer_nodes: Vec<String>,
}

impl RepairSessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn session_type(mut self, session_type: RepairType) -> Self {
        self.session_type = Some(session_type);
        self
    }

    pub fn status(mut self, status: RepairSessionStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn ring_version(mut self, ring_version: u64) -> Self {
        self.ring_version = Some(ring_version);
        self
    }

    pub fn range(mut self, range: PositionRange) -> Self {
        self.ranges.push(range);
        self
    }

    pub fn ranges(mut self, ranges: Vec<PositionRange>) -> Self {
        self.ranges = ranges;
        self
    }

    pub fn progress(mut self, progress: RepairProgress) -> Self {
        self.progress = Some(progress);
        self
    }

    pub fn peer_node(mut self, node: impl Into<String>) -> Self {
        self.peer_nodes.push(node.into());
        self
    }

    pub fn peer_nodes(mut self, nodes: Vec<String>) -> Self {
        self.peer_nodes = nodes;
        self
    }

    pub fn build(self) -> Result<RepairSession, &'static str> {
        let id = self.id.ok_or("id is required")?;
        let session_type = self.session_type.ok_or("session_type is required")?;
        let ring_version = self.ring_version.ok_or("ring_version is required")?;

        Ok(RepairSession {
            id,
            session_type,
            status: self.status.unwrap_or(RepairSessionStatus::Pending),
            ring_version,
            ranges: self.ranges,
            progress: self.progress.unwrap_or_default(),
            peer_nodes: self.peer_nodes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_range_new() {
        let range = PositionRange::new(0, 100);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 100);
    }

    #[test]
    fn test_position_range_contains() {
        let range = PositionRange::new(10, 20);
        assert!(!range.contains(9));
        assert!(range.contains(10));
        assert!(range.contains(15));
        assert!(range.contains(19));
        assert!(!range.contains(20));
    }

    #[test]
    fn test_position_range_len() {
        let range = PositionRange::new(10, 100);
        assert_eq!(range.len(), 90);

        let empty = PositionRange::new(100, 10);
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_position_range_is_empty() {
        let range = PositionRange::new(10, 100);
        assert!(!range.is_empty());

        let empty = PositionRange::new(100, 100);
        assert!(empty.is_empty());

        let inverted = PositionRange::new(100, 10);
        assert!(inverted.is_empty());
    }

    #[test]
    fn test_position_range_overlaps() {
        let range1 = PositionRange::new(0, 100);
        let range2 = PositionRange::new(50, 150);
        let range3 = PositionRange::new(100, 200);
        let range4 = PositionRange::new(200, 300);

        assert!(range1.overlaps(&range2));
        assert!(range2.overlaps(&range1));
        assert!(!range1.overlaps(&range3));
        assert!(!range1.overlaps(&range4));
    }

    #[test]
    fn test_repair_session_status_is_terminal() {
        assert!(!RepairSessionStatus::Pending.is_terminal());
        assert!(!RepairSessionStatus::InProgress.is_terminal());
        assert!(RepairSessionStatus::Completed.is_terminal());
        assert!(RepairSessionStatus::Failed.is_terminal());
        assert!(RepairSessionStatus::Cancelled.is_terminal());
    }

    #[test]
    fn test_repair_session_status_is_active() {
        assert!(!RepairSessionStatus::Pending.is_active());
        assert!(RepairSessionStatus::InProgress.is_active());
        assert!(!RepairSessionStatus::Completed.is_active());
        assert!(!RepairSessionStatus::Failed.is_active());
        assert!(!RepairSessionStatus::Cancelled.is_active());
    }

    #[test]
    fn test_rebalance_direction_debug() {
        assert_eq!(format!("{:?}", RebalanceDirection::Incoming), "Incoming");
        assert_eq!(format!("{:?}", RebalanceDirection::Outgoing), "Outgoing");
    }

    #[test]
    fn test_repair_type_crash_recovery() {
        let repair_type = RepairType::crash_recovery(1000, 2000);
        match repair_type {
            RepairType::CrashRecovery {
                downtime_start,
                downtime_end,
            } => {
                assert_eq!(downtime_start, 1000);
                assert_eq!(downtime_end, 2000);
            }
            _ => panic!("expected CrashRecovery"),
        }
    }

    #[test]
    fn test_repair_type_rebalance() {
        let incoming = RepairType::rebalance_incoming();
        match incoming {
            RepairType::Rebalance { direction } => {
                assert_eq!(direction, RebalanceDirection::Incoming);
            }
            _ => panic!("expected Rebalance"),
        }

        let outgoing = RepairType::rebalance_outgoing();
        match outgoing {
            RepairType::Rebalance { direction } => {
                assert_eq!(direction, RebalanceDirection::Outgoing);
            }
            _ => panic!("expected Rebalance"),
        }
    }

    #[test]
    fn test_repair_type_anti_entropy() {
        let repair_type = RepairType::AntiEntropy;
        assert_eq!(repair_type, RepairType::AntiEntropy);
    }

    #[test]
    fn test_repair_checkpoint_new() {
        let oid = Oid::hash(b"test");
        let checkpoint = RepairCheckpoint::new(100, oid, 12345);
        assert_eq!(checkpoint.position, 100);
        assert_eq!(checkpoint.oid, oid);
        assert_eq!(checkpoint.timestamp, 12345);
    }

    #[test]
    fn test_repair_priority_ordering() {
        assert!(RepairPriority::FailedWrite < RepairPriority::CrashRecovery);
        assert!(RepairPriority::CrashRecovery < RepairPriority::Rebalance);
        assert!(RepairPriority::Rebalance < RepairPriority::AntiEntropy);
    }

    #[test]
    fn test_repair_priority_value() {
        assert_eq!(RepairPriority::FailedWrite.value(), 1);
        assert_eq!(RepairPriority::CrashRecovery.value(), 2);
        assert_eq!(RepairPriority::Rebalance.value(), 3);
        assert_eq!(RepairPriority::AntiEntropy.value(), 4);
    }

    #[test]
    fn test_repair_priority_sorting() {
        let mut priorities = [
            RepairPriority::AntiEntropy,
            RepairPriority::FailedWrite,
            RepairPriority::Rebalance,
            RepairPriority::CrashRecovery,
        ];
        priorities.sort();
        assert_eq!(priorities[0], RepairPriority::FailedWrite);
        assert_eq!(priorities[1], RepairPriority::CrashRecovery);
        assert_eq!(priorities[2], RepairPriority::Rebalance);
        assert_eq!(priorities[3], RepairPriority::AntiEntropy);
    }

    #[test]
    fn test_repair_progress_new() {
        let progress = RepairProgress::new();
        assert_eq!(progress.objects_compared, 0);
        assert_eq!(progress.objects_missing, 0);
        assert_eq!(progress.objects_transferred, 0);
        assert_eq!(progress.bytes_transferred, 0);
        assert!(progress.last_checkpoint.is_none());
    }

    #[test]
    fn test_repair_progress_add_compared() {
        let mut progress = RepairProgress::new();
        progress.add_compared(10);
        assert_eq!(progress.objects_compared, 10);
        progress.add_compared(5);
        assert_eq!(progress.objects_compared, 15);
    }

    #[test]
    fn test_repair_progress_add_missing() {
        let mut progress = RepairProgress::new();
        progress.add_missing(5);
        assert_eq!(progress.objects_missing, 5);
        progress.add_missing(3);
        assert_eq!(progress.objects_missing, 8);
    }

    #[test]
    fn test_repair_progress_add_transferred() {
        let mut progress = RepairProgress::new();
        progress.add_transferred(2, 1024);
        assert_eq!(progress.objects_transferred, 2);
        assert_eq!(progress.bytes_transferred, 1024);
        progress.add_transferred(3, 2048);
        assert_eq!(progress.objects_transferred, 5);
        assert_eq!(progress.bytes_transferred, 3072);
    }

    #[test]
    fn test_repair_progress_checkpoint() {
        let mut progress = RepairProgress::new();
        let oid = Oid::hash(b"test");
        let checkpoint = RepairCheckpoint::new(100, oid, 12345);

        progress.set_checkpoint(checkpoint);
        assert!(progress.last_checkpoint.is_some());
        assert_eq!(progress.last_checkpoint.unwrap().position, 100);

        progress.clear_checkpoint();
        assert!(progress.last_checkpoint.is_none());
    }

    #[test]
    fn test_repair_progress_missing_rate() {
        let mut progress = RepairProgress::new();
        assert_eq!(progress.missing_rate(), 0.0);

        progress.objects_compared = 100;
        progress.objects_missing = 25;
        assert!((progress.missing_rate() - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_repair_progress_transfer_rate() {
        let mut progress = RepairProgress::new();
        assert_eq!(progress.transfer_rate(), 0.0);

        progress.objects_missing = 10;
        progress.objects_transferred = 8;
        assert!((progress.transfer_rate() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_repair_progress_saturating_add() {
        let mut progress = RepairProgress::new();
        progress.objects_compared = u64::MAX;
        progress.add_compared(1);
        assert_eq!(progress.objects_compared, u64::MAX);
    }

    #[test]
    fn test_repair_session_builder_minimal() {
        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        assert_eq!(session.id(), "session-1");
        assert_eq!(session.session_type(), RepairType::AntiEntropy);
        assert_eq!(session.status(), RepairSessionStatus::Pending);
        assert_eq!(session.ring_version(), 1);
        assert!(session.ranges().is_empty());
        assert!(session.peer_nodes().is_empty());
    }

    #[test]
    fn test_repair_session_builder_full() {
        let session = RepairSession::builder()
            .id("session-2")
            .session_type(RepairType::crash_recovery(1000, 2000))
            .status(RepairSessionStatus::InProgress)
            .ring_version(5)
            .range(PositionRange::new(0, 100))
            .range(PositionRange::new(200, 300))
            .peer_node("node-1")
            .peer_node("node-2")
            .build()
            .unwrap();

        assert_eq!(session.id(), "session-2");
        assert_eq!(session.status(), RepairSessionStatus::InProgress);
        assert_eq!(session.ring_version(), 5);
        assert_eq!(session.ranges().len(), 2);
        assert_eq!(session.peer_nodes().len(), 2);
    }

    #[test]
    fn test_repair_session_builder_missing_id() {
        let result = RepairSession::builder()
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "id is required");
    }

    #[test]
    fn test_repair_session_builder_missing_session_type() {
        let result = RepairSession::builder()
            .id("session-1")
            .ring_version(1)
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "session_type is required");
    }

    #[test]
    fn test_repair_session_builder_missing_ring_version() {
        let result = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .build();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ring_version is required");
    }

    #[test]
    fn test_repair_session_builder_with_ranges() {
        let ranges = vec![PositionRange::new(0, 100), PositionRange::new(100, 200)];
        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .ranges(ranges.clone())
            .build()
            .unwrap();

        assert_eq!(session.ranges(), &ranges);
    }

    #[test]
    fn test_repair_session_builder_with_peer_nodes() {
        let nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .peer_nodes(nodes.clone())
            .build()
            .unwrap();

        assert_eq!(session.peer_nodes(), &nodes);
    }

    #[test]
    fn test_repair_session_builder_with_progress() {
        let mut progress = RepairProgress::new();
        progress.objects_compared = 100;
        progress.objects_missing = 10;

        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .progress(progress)
            .build()
            .unwrap();

        assert_eq!(session.progress().objects_compared, 100);
        assert_eq!(session.progress().objects_missing, 10);
    }

    #[test]
    fn test_repair_session_status_transitions() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        assert_eq!(session.status(), RepairSessionStatus::Pending);
        assert!(!session.is_active());
        assert!(!session.is_terminal());

        session.start();
        assert_eq!(session.status(), RepairSessionStatus::InProgress);
        assert!(session.is_active());
        assert!(!session.is_terminal());

        session.complete();
        assert_eq!(session.status(), RepairSessionStatus::Completed);
        assert!(!session.is_active());
        assert!(session.is_terminal());
    }

    #[test]
    fn test_repair_session_fail() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        session.start();
        session.fail();
        assert_eq!(session.status(), RepairSessionStatus::Failed);
        assert!(session.is_terminal());
    }

    #[test]
    fn test_repair_session_cancel() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        session.cancel();
        assert_eq!(session.status(), RepairSessionStatus::Cancelled);
        assert!(session.is_terminal());
    }

    #[test]
    fn test_repair_session_set_status() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        session.set_status(RepairSessionStatus::InProgress);
        assert_eq!(session.status(), RepairSessionStatus::InProgress);
    }

    #[test]
    fn test_repair_session_progress_updates() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        session.add_compared(100);
        assert_eq!(session.progress().objects_compared, 100);

        session.add_missing(10);
        assert_eq!(session.progress().objects_missing, 10);

        session.add_transferred(5, 1024);
        assert_eq!(session.progress().objects_transferred, 5);
        assert_eq!(session.progress().bytes_transferred, 1024);
    }

    #[test]
    fn test_repair_session_checkpoint() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        let oid = Oid::hash(b"test");
        let checkpoint = RepairCheckpoint::new(500, oid, 12345);

        session.set_checkpoint(checkpoint);
        assert!(session.progress().last_checkpoint.is_some());
        assert_eq!(session.progress().last_checkpoint.unwrap().position, 500);
    }

    #[test]
    fn test_repair_session_progress_mut() {
        let mut session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();

        session.progress_mut().objects_compared = 50;
        assert_eq!(session.progress().objects_compared, 50);
    }

    #[test]
    fn test_position_range_clone() {
        let range = PositionRange::new(10, 20);
        let cloned = range.clone();
        assert_eq!(range, cloned);
    }

    #[test]
    fn test_position_range_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(PositionRange::new(0, 100));
        set.insert(PositionRange::new(0, 100));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_repair_session_status_clone() {
        let status = RepairSessionStatus::InProgress;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_repair_type_clone() {
        let repair_type = RepairType::crash_recovery(1000, 2000);
        let cloned = repair_type.clone();
        assert_eq!(repair_type, cloned);
    }

    #[test]
    fn test_repair_progress_clone() {
        let mut progress = RepairProgress::new();
        progress.objects_compared = 100;
        let cloned = progress.clone();
        assert_eq!(progress.objects_compared, cloned.objects_compared);
    }

    #[test]
    fn test_repair_session_clone() {
        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .peer_node("node-1")
            .build()
            .unwrap();

        let cloned = session.clone();
        assert_eq!(session.id(), cloned.id());
        assert_eq!(session.session_type(), cloned.session_type());
    }

    #[test]
    fn test_repair_session_builder_clone() {
        let builder = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy);
        let cloned = builder.clone();
        let session = cloned.ring_version(1).build().unwrap();
        assert_eq!(session.id(), "session-1");
    }

    #[test]
    fn test_repair_checkpoint_clone() {
        let oid = Oid::hash(b"test");
        let checkpoint = RepairCheckpoint::new(100, oid, 12345);
        let cloned = checkpoint.clone();
        assert_eq!(checkpoint.position, cloned.position);
        assert_eq!(checkpoint.oid, cloned.oid);
    }

    #[test]
    fn test_repair_session_builder_new() {
        let builder = RepairSessionBuilder::new();
        let result = builder
            .id("test")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_repair_priority_partial_cmp() {
        assert!(RepairPriority::FailedWrite
            .partial_cmp(&RepairPriority::AntiEntropy)
            .is_some());
    }

    #[test]
    fn test_position_range_debug() {
        let range = PositionRange::new(10, 20);
        let debug = format!("{:?}", range);
        assert!(debug.contains("PositionRange"));
        assert!(debug.contains("10"));
        assert!(debug.contains("20"));
    }

    #[test]
    fn test_repair_session_status_debug() {
        let status = RepairSessionStatus::InProgress;
        let debug = format!("{:?}", status);
        assert!(debug.contains("InProgress"));
    }

    #[test]
    fn test_repair_type_debug() {
        let repair_type = RepairType::crash_recovery(1000, 2000);
        let debug = format!("{:?}", repair_type);
        assert!(debug.contains("CrashRecovery"));
        assert!(debug.contains("1000"));
        assert!(debug.contains("2000"));
    }

    #[test]
    fn test_repair_checkpoint_debug() {
        let oid = Oid::hash(b"test");
        let checkpoint = RepairCheckpoint::new(100, oid, 12345);
        let debug = format!("{:?}", checkpoint);
        assert!(debug.contains("RepairCheckpoint"));
        assert!(debug.contains("100"));
        assert!(debug.contains("12345"));
    }

    #[test]
    fn test_repair_priority_debug() {
        let priority = RepairPriority::FailedWrite;
        let debug = format!("{:?}", priority);
        assert!(debug.contains("FailedWrite"));
    }

    #[test]
    fn test_repair_progress_debug() {
        let progress = RepairProgress::new();
        let debug = format!("{:?}", progress);
        assert!(debug.contains("RepairProgress"));
    }

    #[test]
    fn test_repair_session_debug() {
        let session = RepairSession::builder()
            .id("session-1")
            .session_type(RepairType::AntiEntropy)
            .ring_version(1)
            .build()
            .unwrap();
        let debug = format!("{:?}", session);
        assert!(debug.contains("RepairSession"));
        assert!(debug.contains("session-1"));
    }

    #[test]
    fn test_repair_session_builder_debug() {
        let builder = RepairSession::builder().id("session-1");
        let debug = format!("{:?}", builder);
        assert!(debug.contains("RepairSessionBuilder"));
    }

    #[test]
    fn test_repair_priority_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RepairPriority::FailedWrite);
        set.insert(RepairPriority::FailedWrite);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_repair_session_status_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RepairSessionStatus::Pending);
        set.insert(RepairSessionStatus::Pending);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_rebalance_direction_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RebalanceDirection::Incoming);
        set.insert(RebalanceDirection::Incoming);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_repair_type_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RepairType::AntiEntropy);
        set.insert(RepairType::AntiEntropy);
        assert_eq!(set.len(), 1);
    }
}
