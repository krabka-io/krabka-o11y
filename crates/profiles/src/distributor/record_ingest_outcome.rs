use std::time::Instant;

use krabka_units::convert::StdDurationExt as _;

use super::{DistributorState, IngestBytes, IngestItems, TenantId};

/// What one ingest request did, for its span and the ingest metrics.
pub(crate) struct IngestOutcome<'a> {
    pub tenant: Option<&'a TenantId>,
    pub ok: bool,
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
    state.metrics.record_ingest(
        outcome.ok,
        IngestBytes(outcome.bytes),
        IngestItems(outcome.items),
        outcome.start.elapsed().as_time(),
    );
}
