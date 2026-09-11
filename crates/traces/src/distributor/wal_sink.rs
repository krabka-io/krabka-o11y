use super::{SpanRecord, TracesError, WalBatchError};

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
        let total = records.len();
        for (appended, record) in records.into_iter().enumerate() {
            if let Err(source) = self.append(record).await {
                return Err(WalBatchError::new(appended, total, source));
            }
        }
        Ok(())
    }
}
