use super::{
    DistributorState, HeaderMap, IntoResponse, Response, Span, TracesError, error_response,
    produce_spans, tenant,
};

pub(crate) async fn append_decoded_response(
    state: &DistributorState,
    headers: &HeaderMap,
    spans: Vec<Span>,
    success: Response,
) -> Response {
    let tenant = tenant(headers);
    if let Err(err) = state.enforce_ingest(&tenant, &spans) {
        return error_response(&err);
    }
    let accepted = spans.len() as u64;
    match produce_spans(state.sink.as_ref(), &tenant, spans).await {
        Ok(()) => {
            // Attribute accepted spans to the tenant once per request (batch
            // size), not per span-record, keeping cardinality bounded.
            state.metrics.record_ingest_spans(&tenant, accepted);
            success.into_response()
        }
        Err(err) => {
            // A produce failure is an actual WAL-append error (distinct from a
            // 4xx validation/rate-limit reject handled above).
            state.metrics.record_wal_append_failure();
            if let TracesError::ProduceBatch {
                appended, total, ..
            } = &err
            {
                state
                    .metrics
                    .wal_produce
                    .record_batch_failure(*appended, *total);
            }
            error_response(&err)
        }
    }
}
