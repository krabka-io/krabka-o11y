use super::{Span, SpanRecord, TracesError, WalSink};

/// Appends one request's decoded spans to the WAL as one pipelined batch.
///
/// A sequential append would cost one produce-and-ack round trip per span. On
/// a single-partition WAL with `max.in.flight=1`, that turned a few-hundred-span
/// batch into seconds and overran the OTLP client's deadline. One batch lets
/// the producer coalesce the records and drain them in about one round trip.
///
/// The sink enqueues the records in this order and waits for the acks out of
/// it, so per-partition order and idempotent sequencing do not change.
///
/// # Errors
/// Returns [`TracesError::ProduceBatch`] when the batch does not append in
/// full. The error carries how many records reached the broker.
pub async fn produce_spans(
    sink: &dyn WalSink,
    tenant: &str,
    spans: Vec<Span>,
) -> Result<(), TracesError> {
    let records = spans
        .into_iter()
        .map(|span| SpanRecord {
            tenant: tenant.to_string(),
            span,
        })
        .collect();
    sink.append_batch(records).await.map_err(TracesError::from)
}
