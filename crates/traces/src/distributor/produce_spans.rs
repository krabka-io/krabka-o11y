use super::{WalSink, Span, TracesError, SpanRecord};

/// Append decoded spans to the WAL sink.
///
/// This function appends all spans in one request concurrently. Each `append`
/// enqueues its record into the producer's per-partition accumulator, a fast
/// hop that does not touch the broker, and then awaits the broker ack.
///
/// A sequential await would force N serial produce-and-ack round-trips. On a
/// single-partition WAL with `max.in.flight=1`, that serialized a
/// few-hundred-span batch into seconds and overran the OTLP client's deadline.
/// One concurrent fire lets the producer coalesce the records into a handful of
/// batches, drained in about one round-trip.
///
/// Per-partition ordering and idempotent sequencing do not change. The sender
/// still drains each partition in order with one batch in flight. Traces carry
/// no cross-span WAL-order dependency, because the block-builder regroups by
/// `trace_id`.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn produce_spans(
    sink: &dyn WalSink,
    tenant: &str,
    spans: Vec<Span>,
) -> Result<(), TracesError> {
    let appends = spans.into_iter().map(|span| {
        sink.append(SpanRecord {
            tenant: tenant.to_string(),
            span,
        })
    });
    futures::future::try_join_all(appends).await?;
    Ok(())
}
