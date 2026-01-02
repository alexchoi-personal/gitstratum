#[allow(clippy::result_large_err)]
pub mod negotiation;

pub use negotiation::{
    compute_common_commits, negotiate_refs, AckType, NegotiationLine, NegotiationRequest,
    NegotiationResponse, ObjectWalker,
};
