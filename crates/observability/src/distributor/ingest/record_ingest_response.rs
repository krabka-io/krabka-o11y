use super::{ByteSize, DistributorState, Instant, Response, StdDurationExt};

/// Records one push-handler ingest outcome from the response status and returns
/// the response unchanged.
///
/// `ok` is true for any 2xx. The WAL/produce failure counter is bumped
/// separately at the [`append_distributor_wal_records`] error site, so a 4xx
/// validation or quota reject here does not inflate it.
pub(crate) fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    body: ByteSize,
    items: u64,
    start: Instant,
) -> Response {
    let ok = resp.status().is_success();
    state
        .metrics
        .record_ingest(ok, body, items, start.elapsed().as_time());
    resp
}
