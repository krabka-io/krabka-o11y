use std::time::Instant;

use axum::http::StatusCode;
use krabka_units::convert::StdDurationExt as _;

use super::{ByteSize, IngestRequest, RequestOutcome};

/// What a push handler measured about one ingest request before its response
/// status is known.
#[derive(Debug, Clone, Copy)]
pub struct IngestPushMeasurement {
    /// Request-body size.
    pub body: ByteSize,
    /// Accepted items: zero until the body decodes.
    pub items: u64,
    /// When the handler started.
    pub start: Instant,
}

impl IngestPushMeasurement {
    /// A push of a `body`-sized request that the handler started at `start`
    /// and that has accepted no items yet.
    #[must_use]
    pub const fn before_decode(body: ByteSize, start: Instant) -> Self {
        Self {
            body,
            items: 0,
            start,
        }
    }

    /// This push once it has accepted `items` items.
    #[must_use]
    pub const fn with_items(self, items: u64) -> Self {
        Self { items, ..self }
    }

    /// The ingest outcome of this push when its handler answers with
    /// `status`: [`RequestOutcome::Ok`] for any 2xx. Its latency runs to now.
    #[must_use]
    pub fn ingest_request(self, status: StatusCode) -> IngestRequest {
        IngestRequest {
            outcome: RequestOutcome::from_success_status(status),
            body: self.body,
            items: self.items,
            elapsed: self.start.elapsed().as_time(),
        }
    }
}
