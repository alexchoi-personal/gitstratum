use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceOperation {
    pub id: String,
    pub source_node: String,
    pub target_node: String,
    pub key_range_start: u64,
    pub key_range_end: u64,
    pub status: RebalanceOperationStatus,
    pub bytes_to_move: u64,
    pub bytes_moved: u64,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceOperationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl Default for RebalanceOperationStatus {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceStatus {
    pub id: String,
    pub in_progress: bool,
    pub progress_percent: f32,
    pub bytes_moved: u64,
    pub bytes_remaining: u64,
    pub operations: Vec<RebalanceOperation>,
    pub started_at: u64,
    pub estimated_completion: Option<u64>,
}

impl RebalanceStatus {
    pub fn new(id: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id: id.into(),
            in_progress: false,
            progress_percent: 0.0,
            bytes_moved: 0,
            bytes_remaining: 0,
            operations: Vec::new(),
            started_at: now,
            estimated_completion: None,
        }
    }

    pub fn update_progress(&mut self) {
        let total_bytes: u64 = self.operations.iter().map(|op| op.bytes_to_move).sum();
        let moved_bytes: u64 = self.operations.iter().map(|op| op.bytes_moved).sum();

        self.bytes_moved = moved_bytes;
        self.bytes_remaining = total_bytes.saturating_sub(moved_bytes);

        if total_bytes > 0 {
            self.progress_percent = (moved_bytes as f32 / total_bytes as f32) * 100.0;
        } else {
            self.progress_percent = 100.0;
        }

        self.in_progress = self.operations.iter().any(|op| {
            matches!(
                op.status,
                RebalanceOperationStatus::Pending | RebalanceOperationStatus::InProgress
            )
        });
    }

    pub fn is_complete(&self) -> bool {
        !self.in_progress && self.progress_percent >= 100.0
    }
}

impl Default for RebalanceStatus {
    fn default() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string())
    }
}

pub struct Rebalancer {
    current_rebalance: Option<RebalanceStatus>,
    history: Vec<RebalanceStatus>,
    max_concurrent_operations: usize,
}

impl Rebalancer {
    pub fn new() -> Self {
        Self {
            current_rebalance: None,
            history: Vec::new(),
            max_concurrent_operations: 4,
        }
    }

    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent_operations = max;
        self
    }

    pub fn start_rebalance(&mut self, _reason: &str) -> Result<String, RebalanceError> {
        if self.current_rebalance.is_some() {
            return Err(RebalanceError::AlreadyInProgress);
        }

        let id = uuid::Uuid::new_v4().to_string();
        let mut status = RebalanceStatus::new(&id);
        status.in_progress = true;

        self.current_rebalance = Some(status);
        Ok(id)
    }

    pub fn add_operation(&mut self, operation: RebalanceOperation) -> Result<(), RebalanceError> {
        let rebalance = self
            .current_rebalance
            .as_mut()
            .ok_or(RebalanceError::NotStarted)?;

        rebalance.operations.push(operation);
        rebalance.update_progress();
        Ok(())
    }

    pub fn update_operation(
        &mut self,
        operation_id: &str,
        status: RebalanceOperationStatus,
        bytes_moved: u64,
    ) -> Result<(), RebalanceError> {
        let rebalance = self
            .current_rebalance
            .as_mut()
            .ok_or(RebalanceError::NotStarted)?;

        if let Some(op) = rebalance
            .operations
            .iter_mut()
            .find(|op| op.id == operation_id)
        {
            op.status = status;
            op.bytes_moved = bytes_moved;

            if matches!(
                status,
                RebalanceOperationStatus::Completed | RebalanceOperationStatus::Failed
            ) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                op.completed_at = Some(now);
            }
        }

        rebalance.update_progress();
        Ok(())
    }

    pub fn complete_rebalance(&mut self) -> Result<RebalanceStatus, RebalanceError> {
        let mut rebalance = self
            .current_rebalance
            .take()
            .ok_or(RebalanceError::NotStarted)?;

        rebalance.in_progress = false;
        rebalance.update_progress();

        self.history.push(rebalance.clone());
        Ok(rebalance)
    }

    pub fn cancel_rebalance(&mut self) -> Result<(), RebalanceError> {
        let mut rebalance = self
            .current_rebalance
            .take()
            .ok_or(RebalanceError::NotStarted)?;

        for op in &mut rebalance.operations {
            if matches!(
                op.status,
                RebalanceOperationStatus::Pending | RebalanceOperationStatus::InProgress
            ) {
                op.status = RebalanceOperationStatus::Cancelled;
            }
        }

        rebalance.in_progress = false;
        self.history.push(rebalance);
        Ok(())
    }

    pub fn current_status(&self) -> Option<&RebalanceStatus> {
        self.current_rebalance.as_ref()
    }

    pub fn get_status(&self, id: &str) -> Option<&RebalanceStatus> {
        if let Some(current) = &self.current_rebalance {
            if current.id == id {
                return Some(current);
            }
        }
        self.history.iter().find(|s| s.id == id)
    }

    pub fn history(&self) -> &[RebalanceStatus] {
        &self.history
    }

    pub fn is_rebalancing(&self) -> bool {
        self.current_rebalance
            .as_ref()
            .map(|r| r.in_progress)
            .unwrap_or(false)
    }
}

impl Default for Rebalancer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceError {
    AlreadyInProgress,
    NotStarted,
    OperationNotFound,
}

impl std::fmt::Display for RebalanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInProgress => write!(f, "rebalance already in progress"),
            Self::NotStarted => write!(f, "no rebalance in progress"),
            Self::OperationNotFound => write!(f, "operation not found"),
        }
    }
}

impl std::error::Error for RebalanceError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_operation(
        id: &str,
        source: &str,
        target: &str,
        key_start: u64,
        key_end: u64,
        bytes_to_move: u64,
    ) -> RebalanceOperation {
        RebalanceOperation {
            id: id.to_string(),
            source_node: source.to_string(),
            target_node: target.to_string(),
            key_range_start: key_start,
            key_range_end: key_end,
            status: RebalanceOperationStatus::Pending,
            bytes_to_move,
            bytes_moved: 0,
            started_at: 1000,
            completed_at: None,
        }
    }

    #[test]
    fn test_successful_rebalance_workflow() {
        let mut rebalancer = Rebalancer::new().with_max_concurrent(8);
        assert_eq!(rebalancer.max_concurrent_operations, 8);
        assert!(rebalancer.current_status().is_none());
        assert!(rebalancer.history().is_empty());
        assert!(!rebalancer.is_rebalancing());

        let id = rebalancer.start_rebalance("node scale-out").unwrap();
        assert!(!id.is_empty());
        assert!(rebalancer.is_rebalancing());
        assert!(rebalancer.current_status().is_some());

        let op1 = create_operation("op1", "node-a", "node-c", 0, 1000, 5000);
        let op2 = create_operation("op2", "node-b", "node-c", 1000, 2000, 3000);
        rebalancer.add_operation(op1).unwrap();
        rebalancer.add_operation(op2).unwrap();

        let status = rebalancer.current_status().unwrap();
        assert_eq!(status.operations.len(), 2);
        assert!(status.in_progress);
        assert_eq!(status.bytes_remaining, 8000);
        assert_eq!(status.progress_percent, 0.0);

        rebalancer
            .update_operation("op1", RebalanceOperationStatus::InProgress, 2500)
            .unwrap();

        let status = rebalancer.current_status().unwrap();
        assert_eq!(
            status.operations[0].status,
            RebalanceOperationStatus::InProgress
        );
        assert_eq!(status.operations[0].bytes_moved, 2500);
        assert!(status.operations[0].completed_at.is_none());
        assert_eq!(status.bytes_moved, 2500);
        assert_eq!(status.progress_percent, 31.25);

        rebalancer
            .update_operation("op1", RebalanceOperationStatus::Completed, 5000)
            .unwrap();
        rebalancer
            .update_operation("op2", RebalanceOperationStatus::Completed, 3000)
            .unwrap();

        let status = rebalancer.current_status().unwrap();
        assert!(status.operations[0].completed_at.is_some());
        assert!(status.operations[1].completed_at.is_some());
        assert_eq!(status.bytes_moved, 8000);
        assert_eq!(status.bytes_remaining, 0);
        assert_eq!(status.progress_percent, 100.0);
        assert!(!status.in_progress);

        let found_status = rebalancer.get_status(&id).unwrap();
        assert_eq!(found_status.id, id);

        let completed = rebalancer.complete_rebalance().unwrap();
        assert_eq!(completed.id, id);
        assert!(!completed.in_progress);
        assert!(completed.is_complete());
        assert_eq!(completed.operations.len(), 2);

        assert!(rebalancer.current_status().is_none());
        assert!(!rebalancer.is_rebalancing());
        assert_eq!(rebalancer.history().len(), 1);

        let historical = rebalancer.get_status(&id).unwrap();
        assert_eq!(historical.id, id);
    }

    #[test]
    fn test_cancelled_rebalance_workflow() {
        let mut rebalancer = Rebalancer::new();

        rebalancer.start_rebalance("maintenance").unwrap();

        let mut op1 = create_operation("op1", "node-a", "node-b", 0, 500, 1000);
        op1.status = RebalanceOperationStatus::Pending;

        let mut op2 = create_operation("op2", "node-a", "node-c", 500, 1000, 2000);
        op2.status = RebalanceOperationStatus::InProgress;
        op2.bytes_moved = 500;

        let mut op3 = create_operation("op3", "node-b", "node-c", 1000, 1500, 1500);
        op3.status = RebalanceOperationStatus::Completed;
        op3.bytes_moved = 1500;

        let mut op4 = create_operation("op4", "node-c", "node-a", 1500, 2000, 800);
        op4.status = RebalanceOperationStatus::Failed;
        op4.bytes_moved = 200;

        rebalancer.add_operation(op1).unwrap();
        rebalancer.add_operation(op2).unwrap();
        rebalancer.add_operation(op3).unwrap();
        rebalancer.add_operation(op4).unwrap();

        assert!(rebalancer.is_rebalancing());

        rebalancer.cancel_rebalance().unwrap();

        assert!(!rebalancer.is_rebalancing());
        assert!(rebalancer.current_status().is_none());
        assert_eq!(rebalancer.history().len(), 1);

        let cancelled = &rebalancer.history()[0];
        assert!(!cancelled.in_progress);
        assert_eq!(
            cancelled.operations[0].status,
            RebalanceOperationStatus::Cancelled
        );
        assert_eq!(
            cancelled.operations[1].status,
            RebalanceOperationStatus::Cancelled
        );
        assert_eq!(
            cancelled.operations[2].status,
            RebalanceOperationStatus::Completed
        );
        assert_eq!(
            cancelled.operations[3].status,
            RebalanceOperationStatus::Failed
        );
    }

    #[test]
    fn test_error_handling_scenarios() {
        let mut rebalancer = Rebalancer::new();

        let op = create_operation("op1", "a", "b", 0, 100, 1000);
        assert_eq!(
            rebalancer.add_operation(op).unwrap_err(),
            RebalanceError::NotStarted
        );
        assert_eq!(
            rebalancer
                .update_operation("op1", RebalanceOperationStatus::InProgress, 0)
                .unwrap_err(),
            RebalanceError::NotStarted
        );
        assert_eq!(
            rebalancer.complete_rebalance().unwrap_err(),
            RebalanceError::NotStarted
        );
        assert_eq!(
            rebalancer.cancel_rebalance().unwrap_err(),
            RebalanceError::NotStarted
        );

        rebalancer.start_rebalance("first").unwrap();
        assert_eq!(
            rebalancer.start_rebalance("second").unwrap_err(),
            RebalanceError::AlreadyInProgress
        );

        rebalancer
            .update_operation("nonexistent", RebalanceOperationStatus::Completed, 100)
            .unwrap();

        let errors = [
            RebalanceError::AlreadyInProgress,
            RebalanceError::NotStarted,
            RebalanceError::OperationNotFound,
        ];

        for error in &errors {
            let cloned = error.clone();
            assert_eq!(&cloned, error);
            let _ = format!("{:?}", error);
        }

        assert_eq!(
            format!("{}", RebalanceError::AlreadyInProgress),
            "rebalance already in progress"
        );
        assert_eq!(
            format!("{}", RebalanceError::NotStarted),
            "no rebalance in progress"
        );
        assert_eq!(
            format!("{}", RebalanceError::OperationNotFound),
            "operation not found"
        );

        let boxed: Box<dyn std::error::Error> = Box::new(RebalanceError::NotStarted);
        assert_eq!(boxed.to_string(), "no rebalance in progress");

        assert_ne!(
            RebalanceError::AlreadyInProgress,
            RebalanceError::NotStarted
        );
    }

    #[test]
    fn test_serialization_and_trait_implementations() {
        let op = RebalanceOperation {
            id: "op-serialize".to_string(),
            source_node: "node1".to_string(),
            target_node: "node2".to_string(),
            key_range_start: 100,
            key_range_end: 200,
            status: RebalanceOperationStatus::InProgress,
            bytes_to_move: 5000,
            bytes_moved: 2500,
            started_at: 1234567890,
            completed_at: Some(1234567900),
        };

        let op_json = serde_json::to_string(&op).unwrap();
        let op_back: RebalanceOperation = serde_json::from_str(&op_json).unwrap();
        assert_eq!(op_back.id, "op-serialize");
        assert_eq!(op_back.source_node, "node1");
        assert_eq!(op_back.target_node, "node2");
        assert_eq!(op_back.key_range_start, 100);
        assert_eq!(op_back.key_range_end, 200);
        assert_eq!(op_back.status, RebalanceOperationStatus::InProgress);
        assert_eq!(op_back.bytes_to_move, 5000);
        assert_eq!(op_back.bytes_moved, 2500);
        assert_eq!(op_back.started_at, 1234567890);
        assert_eq!(op_back.completed_at, Some(1234567900));

        let op_cloned = op.clone();
        assert_eq!(op_cloned.id, op.id);
        let _ = format!("{:?}", op);

        let statuses = [
            RebalanceOperationStatus::Pending,
            RebalanceOperationStatus::InProgress,
            RebalanceOperationStatus::Completed,
            RebalanceOperationStatus::Failed,
            RebalanceOperationStatus::Cancelled,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let back: RebalanceOperationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, status);

            let cloned = status.clone();
            let copied = status;
            assert_eq!(cloned, status);
            assert_eq!(copied, status);

            let _ = format!("{:?}", status);
        }

        assert_eq!(
            RebalanceOperationStatus::default(),
            RebalanceOperationStatus::Pending
        );

        let mut status = RebalanceStatus::new("test-status");
        status.in_progress = true;
        status.progress_percent = 50.0;
        status.bytes_moved = 2500;
        status.bytes_remaining = 2500;
        status.estimated_completion = Some(9999999999);

        let status_json = serde_json::to_string(&status).unwrap();
        let status_back: RebalanceStatus = serde_json::from_str(&status_json).unwrap();
        assert_eq!(status_back.id, "test-status");
        assert!(status_back.in_progress);
        assert_eq!(status_back.progress_percent, 50.0);
        assert_eq!(status_back.bytes_moved, 2500);
        assert_eq!(status_back.bytes_remaining, 2500);
        assert_eq!(status_back.estimated_completion, Some(9999999999));

        let status_cloned = status.clone();
        assert_eq!(status_cloned.id, status.id);
        let _ = format!("{:?}", status);

        let status_from_string = RebalanceStatus::new(String::from("string-id"));
        assert_eq!(status_from_string.id, "string-id");

        let default_status = RebalanceStatus::default();
        assert!(!default_status.id.is_empty());
        assert!(!default_status.in_progress);
        assert_eq!(default_status.progress_percent, 0.0);
        assert!(default_status.started_at > 0);

        let default_rebalancer = Rebalancer::default();
        assert!(default_rebalancer.current_status().is_none());
        assert!(!default_rebalancer.is_rebalancing());
    }

    #[test]
    fn test_multiple_rebalance_cycles_and_history() {
        let mut rebalancer = Rebalancer::new();

        let mut ids = Vec::new();
        for i in 0..3 {
            let id = rebalancer.start_rebalance(&format!("cycle-{}", i)).unwrap();
            ids.push(id.clone());

            for j in 0..2 {
                let op = create_operation(
                    &format!("op-{}-{}", i, j),
                    &format!("source-{}", j),
                    &format!("target-{}", j),
                    j as u64 * 1000,
                    (j as u64 + 1) * 1000,
                    1000,
                );
                rebalancer.add_operation(op).unwrap();
            }

            for j in 0..2 {
                rebalancer
                    .update_operation(
                        &format!("op-{}-{}", i, j),
                        RebalanceOperationStatus::Completed,
                        1000,
                    )
                    .unwrap();
            }

            let completed = rebalancer.complete_rebalance().unwrap();
            assert_eq!(completed.id, id);
            assert!(!completed.in_progress);
            assert_eq!(completed.operations.len(), 2);
        }

        assert_eq!(rebalancer.history().len(), 3);
        assert!(rebalancer.current_status().is_none());
        assert!(!rebalancer.is_rebalancing());

        for id in &ids {
            let status = rebalancer.get_status(id);
            assert!(status.is_some());
            assert_eq!(status.unwrap().id, *id);
        }

        assert!(rebalancer.get_status("nonexistent").is_none());

        let id4 = rebalancer.start_rebalance("cycle-3").unwrap();
        let current_status = rebalancer.get_status(&id4).unwrap();
        assert_eq!(current_status.id, id4);

        let historical_status = rebalancer.get_status(&ids[0]).unwrap();
        assert_eq!(historical_status.id, ids[0]);
    }

    #[test]
    fn test_progress_tracking_edge_cases() {
        let mut status = RebalanceStatus::new("progress-test");
        status.update_progress();
        assert_eq!(status.progress_percent, 100.0);
        assert_eq!(status.bytes_moved, 0);
        assert_eq!(status.bytes_remaining, 0);
        assert!(!status.in_progress);

        let mut status = RebalanceStatus::new("partial-progress");
        let mut op1 = create_operation("op1", "a", "b", 0, 100, 1000);
        op1.bytes_moved = 500;
        op1.status = RebalanceOperationStatus::InProgress;

        let mut op2 = create_operation("op2", "b", "c", 100, 200, 1000);
        op2.bytes_moved = 1000;
        op2.status = RebalanceOperationStatus::Completed;

        status.operations.push(op1);
        status.operations.push(op2);
        status.update_progress();

        assert_eq!(status.bytes_moved, 1500);
        assert_eq!(status.bytes_remaining, 500);
        assert_eq!(status.progress_percent, 75.0);
        assert!(status.in_progress);

        let mut status = RebalanceStatus::new("over-moved");
        let mut op = create_operation("op1", "a", "b", 0, 100, 1000);
        op.bytes_moved = 1500;
        op.status = RebalanceOperationStatus::Completed;
        status.operations.push(op);
        status.update_progress();
        assert_eq!(status.bytes_moved, 1500);
        assert_eq!(status.bytes_remaining, 0);

        let mut status = RebalanceStatus::new("failed-ops");
        let mut op = create_operation("op1", "a", "b", 0, 100, 1000);
        op.status = RebalanceOperationStatus::Failed;
        op.bytes_moved = 500;
        status.operations.push(op);
        status.update_progress();
        assert!(!status.in_progress);
        assert_eq!(status.bytes_moved, 500);

        let mut status = RebalanceStatus::new("cancelled-ops");
        let mut op = create_operation("op1", "a", "b", 0, 100, 1000);
        op.status = RebalanceOperationStatus::Cancelled;
        status.operations.push(op);
        status.update_progress();
        assert!(!status.in_progress);

        let mut status = RebalanceStatus::new("completion-test");
        status.in_progress = false;
        status.progress_percent = 99.9;
        assert!(!status.is_complete());

        status.progress_percent = 100.0;
        assert!(status.is_complete());

        status.progress_percent = 100.1;
        assert!(status.is_complete());

        status.in_progress = true;
        status.progress_percent = 100.0;
        assert!(!status.is_complete());

        let rebalancer = Rebalancer::new()
            .with_max_concurrent(10)
            .with_max_concurrent(5);
        assert_eq!(rebalancer.max_concurrent_operations, 5);

        let mut rebalancer = Rebalancer::new();
        rebalancer.start_rebalance("test").unwrap();
        for i in 0..5 {
            let op = create_operation(&format!("op{}", i), "a", "b", i * 100, (i + 1) * 100, 1000);
            rebalancer.add_operation(op).unwrap();
        }
        let status = rebalancer.current_status().unwrap();
        assert_eq!(status.operations.len(), 5);
    }
}
