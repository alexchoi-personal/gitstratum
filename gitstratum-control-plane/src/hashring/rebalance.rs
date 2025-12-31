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

        if let Some(op) = rebalance.operations.iter_mut().find(|op| op.id == operation_id) {
            op.status = status;
            op.bytes_moved = bytes_moved;

            if matches!(status, RebalanceOperationStatus::Completed | RebalanceOperationStatus::Failed) {
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
