//! Extractor-layer error type.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractorError {
    /// No registered extractor claimed the URL.
    #[error("no extractor supports url: {0}")]
    Unsupported(String),

    /// An HTTP request to fetch a manifest failed.
    #[error("network error: {0}")]
    Network(String),

    /// The manifest was fetched but could not be parsed.
    #[error("parse error ({format}): {reason}")]
    Parse {
        format: &'static str,
        reason: String,
    },

    /// The manifest is valid but contains no playable streams.
    #[error("no streams found in manifest")]
    NoStreams,

    /// URL resolution / joining error.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// Generic wrapper for unexpected errors.
    #[error("internal: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for ExtractorError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e.to_string())
    }
}
