pub mod prelude;
pub mod types;

pub(crate) mod anti_entropy;
pub(crate) mod constants;
pub(crate) mod coordinator;
pub(crate) mod crash_recovery;
pub(crate) mod downtime;
pub(crate) mod merkle;
pub(crate) mod rebalance;
pub(crate) mod session;

#[allow(unused_imports)]
pub(crate) use anti_entropy::{
    AntiEntropyConfig, AntiEntropyRepairer, AntiEntropyStats, RepairItem, RepairQueue,
};
#[allow(unused_imports)]
pub(crate) use coordinator::RepairStats;
pub use coordinator::{RepairCoordinator, RepairCoordinatorConfig};
#[allow(unused_imports)]
pub(crate) use crash_recovery::{
    CrashRecoveryConfig, CrashRecoveryHandler, RecoveryNeeded, RepairRateLimiter,
};
#[allow(unused_imports)]
pub(crate) use downtime::DowntimeTracker;
#[allow(unused_imports)]
pub(crate) use merkle::{MerkleNode, MerkleTreeBuilder, ObjectMerkleTree};
#[allow(unused_imports)]
pub(crate) use rebalance::{
    RangeTransfer, RebalanceConfig, RebalanceHandler, RebalanceState, RebalanceStats,
};
#[allow(unused_imports)]
pub(crate) use session::{generate_session_id, PositionRange, RepairCheckpoint, RepairProgress};
pub use session::{
    RebalanceDirection, RepairPriority, RepairSession, RepairSessionStatus, RepairType,
};
pub use types::{NodeId, SessionId};

#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use super::anti_entropy::{
        AntiEntropyConfig, AntiEntropyRepairer, AntiEntropyStats, RepairItem, RepairQueue,
    };
    pub use super::coordinator::RepairStats;
    pub use super::crash_recovery::{
        CrashRecoveryConfig, CrashRecoveryHandler, RecoveryNeeded, RepairRateLimiter,
    };
    pub use super::downtime::DowntimeTracker;
    pub use super::merkle::{MerkleNode, MerkleTreeBuilder, ObjectMerkleTree};
    pub use super::rebalance::{
        RangeTransfer, RebalanceConfig, RebalanceHandler, RebalanceState, RebalanceStats,
    };
    pub use super::session::{
        generate_session_id, PositionRange, RebalanceDirection, RepairCheckpoint, RepairPriority,
        RepairProgress,
    };
}
