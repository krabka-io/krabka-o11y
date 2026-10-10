use super::{DistributorState, Response};
use crate::service_metrics::IngestPushMeasurement;

/// Records one push-handler ingest outcome from the response status and returns
/// the response unchanged.
///
/// The outcome is `status="ok"` for any 2xx. The WAL/produce failure counter
/// is bumped separately at the [`append_distributor_wal_records`] error site,
/// so a 4xx validation or quota reject here does not inflate it.
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
