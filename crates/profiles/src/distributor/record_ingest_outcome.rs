use std::time::Instant;

use krabka_units::{
    ByteSize,
    convert::{ByteSizeExt as _, StdDurationExt as _},
};

use super::{DistributorState, IngestRequest, RequestOutcome, TenantId};

/// What one ingest request did, for its span and the ingest metrics.
pub(crate) struct IngestOutcome<'a> {
    pub tenant: Option<&'a TenantId>,
    pub outcome: RequestOutcome,
    pub bytes: u64,
    pub items: u64,
    pub start: Instant,
}

/// Records the item count on the request's span and the request in the
/// ingest metrics.
pub(crate) fn record_ingest_outcome(
    state: &DistributorState,
    span: &tracing::Span,
    outcome: &IngestOutcome<'_>,
) {
    span.record("krabka.ingest.samples", outcome.items);
    if let Some(tenant) = outcome.tenant {
        state
            .metrics
            .record_ingest_samples(tenant.as_str(), outcome.items);
    }
    state.metrics.record_ingest(IngestRequest {
        outcome: outcome.outcome,
        body: ByteSize::from_bytes(outcome.bytes),
        items: outcome.items,
        elapsed: outcome.start.elapsed().as_time(),
    });
}
