#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg_attr(coverage_nightly, coverage(off))]
pub mod error;
pub mod frontend;
pub mod negotiate;
pub mod pack;
pub mod receive_pack;
pub mod upload_pack;

pub use error::{FrontendError, Result};
pub use frontend::{
    ClusterState, ControlPlaneConnection, FrontendBuilder, GitFrontend, MetadataConnection,
    ObjectConnection,
};
pub use negotiate::{
    compute_common_commits, negotiate_refs, AckType, NegotiationLine, NegotiationRequest,
    NegotiationResponse, ObjectWalker,
};
pub use pack::{PackEntry, PackReader, PackWriter};
pub use receive_pack::{
    parse_push_command, ControlPlaneClient, GitReceivePack, MetadataWriter, ObjectWriter,
    PushResult, ReceivePackCapabilities, RefUpdate, RefUpdateResult,
};
pub use upload_pack::{
    format_ref_advertisement, GitUploadPack, MetadataClient, ObjectClient, UploadPackCapabilities,
};
