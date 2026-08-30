use super::*;

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
    match result {
        Ok((_, items)) => metrics.record_ingest(true, body_size, *items, elapsed),
        Err(_) => metrics.record_ingest(false, body_size, 0, elapsed),
    }
}
