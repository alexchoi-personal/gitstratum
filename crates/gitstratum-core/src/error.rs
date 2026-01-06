use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid OID: {0}")]
    InvalidOid(String),

    #[error("invalid object type: {0}")]
    InvalidObjectType(String),

    #[error("invalid object format: {0}")]
    InvalidObjectFormat(String),

    #[error("object not found: {0}")]
    ObjectNotFound(String),

    #[error("ref not found: {0}")]
    RefNotFound(String),

    #[error("repo not found: {0}")]
    RepoNotFound(String),

    #[error("invalid ref name: {0}")]
    InvalidRefName(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("OID mismatch: expected {expected}, computed {computed}")]
    OidMismatch { expected: String, computed: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
