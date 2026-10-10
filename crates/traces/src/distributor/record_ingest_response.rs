use krabka_observability::service_metrics::IngestPushMeasurement;

use super::{DistributorState, Response};

/// Record one push-handler ingest outcome from the response status, and return
/// the response unchanged.
///
/// The outcome is `status="ok"` for any 2xx. The [`produce_spans`](super::produce_spans) error site
/// bumps the WAL/produce failure counter separately, so a 4xx validation or
/// rate-limit reject here does not inflate that counter.
pub(crate) fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    measurement: IngestPushMeasurement,
) -> Response {
    state
        .metrics
        .record_ingest(measurement.ingest_request(resp.status()));
    resp
}
