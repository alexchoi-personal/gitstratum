#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod auth;
pub mod cache;
pub mod client;
pub mod coalesce;
pub mod commands;
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod middleware;
pub mod pack;
pub mod protocol;
pub mod server;
pub mod ssh;

pub use cache::negotiation::{
    compute_common_commits, negotiate_refs, AckType, NegotiationLine, NegotiationRequest,
    NegotiationResponse, ObjectWalker,
};
pub use commands::receive_pack::{
    parse_push_command, ControlPlaneClient, GitReceivePack, MetadataWriter, ObjectWriter,
    PushResult, ReceivePackCapabilities, RefUpdate, RefUpdateResult,
};
pub use commands::upload_pack::{
    format_ref_advertisement, GitUploadPack, MetadataClient, ObjectClient, UploadPackCapabilities,
};
pub use error::{FrontendError, Result};
pub use pack::assembly::{PackEntry, PackReader, PackWriter};
pub use server::{
    ClusterState, ControlPlaneConnection, FrontendBuilder, GitFrontend, MetadataConnection,
    ObjectConnection,
};
