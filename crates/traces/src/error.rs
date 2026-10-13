//! Crate-wide error type and ingest-edge HTTP status mapping.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use krabka_blockstore::TenantResolveError;
use krabka_observability::server_security::TenantDenied;
use serde_json::json;

use crate::limits::LimitError;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn status_codes_map_to_ingest_edge_failures() {
        struct StatusCase {
            error: TracesError,
            status: u16,
        }
        for case in [
            StatusCase {
                error: TracesError::UnsupportedContentType("x".into()),
                status: 415,
            },
            StatusCase {
                error: TracesError::Decode("x".into()),
                status: 400,
            },
            StatusCase {
                error: TracesError::Invalid("x".into()),
                status: 400,
            },
            StatusCase {
                error: TracesError::Tenant(TenantResolveError::Missing),
                status: 400,
            },
            StatusCase {
                error: TracesError::TenantDenied(TenantDenied {
                    principal: "grafana".into(),
                    tenant: krabka_blockstore::TenantId::new("tenant-b").unwrap(),
                }),
                status: 403,
            },
            StatusCase {
                error: TracesError::Limit("x".into()),
                status: 400,
            },
            StatusCase {
                error: TracesError::RateLimit("x".into()),
                status: 429,
            },
            StatusCase {
                error: TracesError::TooLarge { limit: 1 },
                status: 400,
            },
            StatusCase {
                error: TracesError::Wal("x".into()),
                status: 500,
            },
            StatusCase {
                error: TracesError::Produce("x".into()),
                status: 500,
            },
            StatusCase {
                error: TracesError::Block("x".into()),
                status: 500,
            },
        ] {
            assert2::assert!(case.error.status_code() == case.status);
        }
    }
}

mod tempo_error_response;
mod tempo_limit_error_response;
mod traces_error;

pub use tempo_error_response::tempo_error_response;
pub use tempo_limit_error_response::tempo_limit_error_response;
pub use traces_error::TracesError;
