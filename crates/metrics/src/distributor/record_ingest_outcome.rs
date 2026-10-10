use super::{
    ByteSize, DistributorState, IngestRequest, PushError, PushSuccess, RequestOutcome, Time,
};

/// Records an ingest request outcome on the distributor metrics bundle, if one
/// is configured. `body_size` is the compressed request-body length. `items` is
/// the decoded series count on success and `0` on error.
pub(crate) fn record_ingest_outcome(
    state: &DistributorState,
    result: &Result<(PushSuccess, u64), PushError>,
    body_size: ByteSize,
    elapsed: Time,
) {
    let Some(metrics) = &state.metrics else {
        return;
    };
    metrics.record_ingest(IngestRequest {
        outcome: RequestOutcome::from_result(result),
        body: body_size,
        items: result.as_ref().map_or(0, |(_, items)| *items),
        elapsed,
    });
}
