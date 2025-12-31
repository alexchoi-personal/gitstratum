#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod server;
pub mod protocol;
pub mod commands;
pub mod pack;
pub mod middleware;
pub mod cache;
pub mod client;

pub use error::{FrontendError, Result};
pub use server::{
    ClusterState, ControlPlaneConnection, FrontendBuilder, GitFrontend, MetadataConnection,
    ObjectConnection,
};
pub use cache::negotiation::{
    compute_common_commits, negotiate_refs, AckType, NegotiationLine, NegotiationRequest,
    NegotiationResponse, ObjectWalker,
};
pub use pack::assembly::{PackEntry, PackReader, PackWriter};
pub use commands::receive_pack::{
    parse_push_command, ControlPlaneClient, GitReceivePack, MetadataWriter, ObjectWriter,
    PushResult, ReceivePackCapabilities, RefUpdate, RefUpdateResult,
};
pub use commands::upload_pack::{
    format_ref_advertisement, GitUploadPack, MetadataClient, ObjectClient, UploadPackCapabilities,
};
