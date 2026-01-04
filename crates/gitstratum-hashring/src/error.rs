use thiserror::Error;

pub type Result<T> = std::result::Result<T, HashRingError>;

#[derive(Error, Debug)]
pub enum HashRingError {
    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("ring is empty")]
    EmptyRing,

    #[error("insufficient nodes for replication factor {0}, have {1}")]
    InsufficientNodes(usize, usize),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}
