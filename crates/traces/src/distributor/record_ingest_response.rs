use super::*;

/// Record one push-handler ingest outcome from the response status, and return
/// the response unchanged.
///
/// `ok` is true for any 2xx. The [`produce_spans`] error site bumps the
/// WAL/produce failure counter separately, so a 4xx validation or rate-limit
/// reject here does not inflate that counter.
///
/// `start` stays an [`Instant`](std::time::Instant). An instant is a
/// coordinate, not a magnitude, and only the elapsed extent is a quantity.
pub(crate) fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    body: ByteSize,
    items: u64,
    start: std::time::Instant,
) -> Response {
    let ok = resp.status().is_success();
    state
        .metrics
        .record_ingest(ok, body, items, start.elapsed().as_time());
    resp
}
