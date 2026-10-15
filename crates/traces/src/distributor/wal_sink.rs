use super::{Span, SpanRecord, TenantId, TracesError, WalBatchError, write_batch_serially};

/// Append one already-encoded logical span record to the traces WAL.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    /// Appends one record and waits for its ack.
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError>;

    /// Appends one request's span records as a batch.
    ///
    /// An OTLP export becomes one record per span, so this is the call that
    /// decides how many broker round trips one export costs. The default
    /// appends serially and stops at the first failure, which is all a sink
    /// with no produce pipeline can do. [`super::KafkaSink`] overrides it.
    ///
    /// # Errors
    /// Returns [`WalBatchError`] when a record fails to append. The error
    /// carries the count of records that did append, so the handler can report
    /// a partial batch as the partial batch it is.
    async fn append_batch(
        &self,
        records: Vec<SpanRecord>,
    ) -> Result<(), WalBatchError<TracesError>> {
        write_batch_serially(records, |record| self.append(record)).await
    }
}

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
    tenant: &TenantId,
    spans: Vec<Span>,
) -> Result<(), TracesError> {
    let records = spans
        .into_iter()
        .map(|span| SpanRecord {
            tenant: tenant.as_str().to_owned(),
            span,
        })
        .collect();
    sink.append_batch(records).await.map_err(TracesError::from)
}
