use thiserror::Error;

pub type Result<T> = std::result::Result<T, OperatorError>;

#[derive(Error, Debug)]
pub enum OperatorError {
    #[error("Kubernetes API error: {0}")]
    KubeApi(#[from] kube::Error),

    #[error("reconciliation failed: {0}")]
    Reconciliation(String),

    #[error("invalid spec: {0}")]
    InvalidSpec(String),

    #[error("resource not found: {0}")]
    ResourceNotFound(String),

    #[error("scaling error: {0}")]
    Scaling(String),

    #[error("rebalancing in progress")]
    RebalancingInProgress,

    #[error("insufficient replicas: expected at least {expected}, got {actual}")]
    InsufficientReplicas { expected: i32, actual: i32 },

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("finalizer error: {0}")]
    Finalizer(String),
}

impl OperatorError {
    pub fn reconciliation(msg: impl Into<String>) -> Self {
        Self::Reconciliation(msg.into())
    }

    pub fn invalid_spec(msg: impl Into<String>) -> Self {
        Self::InvalidSpec(msg.into())
    }

    pub fn resource_not_found(msg: impl Into<String>) -> Self {
        Self::ResourceNotFound(msg.into())
    }

    pub fn scaling(msg: impl Into<String>) -> Self {
        Self::Scaling(msg.into())
    }

    pub fn finalizer(msg: impl Into<String>) -> Self {
        Self::Finalizer(msg.into())
    }
}
