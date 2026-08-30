//! Crate-wide error type and ingest-edge HTTP status mapping.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

use crate::limits::LimitError;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn status_codes_map_to_ingest_edge_failures() {
        for (err, want) in [
            (TracesError::UnsupportedContentType("x".into()), 415),
            (TracesError::Decode("x".into()), 400),
            (TracesError::Invalid("x".into()), 400),
            (TracesError::Limit("x".into()), 400),
            (TracesError::RateLimit("x".into()), 429),
            (TracesError::TooLarge { limit: 1 }, 400),
            (TracesError::Wal("x".into()), 500),
            (TracesError::Produce("x".into()), 500),
            (TracesError::Block("x".into()), 500),
        ] {
            assert2::assert!(err.status_code() == want);
        }
    }
}

// === split-modules: generated submodules ===
mod tempo_error_response;
mod tempo_limit_error_response;
mod traces_error;

pub use tempo_error_response::tempo_error_response;
pub use tempo_limit_error_response::tempo_limit_error_response;
pub use traces_error::TracesError;
