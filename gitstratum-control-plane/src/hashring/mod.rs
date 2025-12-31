mod notify;
mod rebalance;
mod state;

pub use notify::{HashRingNotification, HashRingNotifier, RingType};
pub use rebalance::{
    RebalanceError, RebalanceOperation, RebalanceOperationStatus, RebalanceStatus, Rebalancer,
};
pub use state::{HashRingState, HashRingTopology};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_re_exports_notify() {
        let _notification = HashRingNotification::RebalanceStarted {
            rebalance_id: "test".to_string(),
        };
        let _notifier = HashRingNotifier::new();
        let _ring_type = RingType::Object;
    }

    #[test]
    fn test_re_exports_rebalance() {
        let _operation = RebalanceOperation {
            id: "op1".to_string(),
            source_node: "source".to_string(),
            target_node: "target".to_string(),
            key_range_start: 0,
            key_range_end: 100,
            status: RebalanceOperationStatus::Pending,
            bytes_to_move: 1000,
            bytes_moved: 0,
            started_at: 0,
            completed_at: None,
        };
        let _status = RebalanceStatus::new("test");
        let _rebalancer = Rebalancer::new();
        let _error = RebalanceError::NotStarted;
        let _op_status = RebalanceOperationStatus::Pending;
    }

    #[test]
    fn test_re_exports_state() {
        let _topology = HashRingTopology::new(3);
        let _state = HashRingState::new(16, 3);
    }
}
