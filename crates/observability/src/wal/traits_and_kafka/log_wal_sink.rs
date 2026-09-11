use super::{WalLogRecord, WalSinkError, async_trait};
use crate::wal_produce::WalBatchError;

#[async_trait]
pub trait LogWalSink: Send + Sync + 'static {
    /// Appends one record and waits for its ack.
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError>;

    /// Appends one push request's records as a batch.
    ///
    /// A Loki push becomes one record per entry, so this is the call that
    /// decides how many broker round trips one push costs. The default appends
    /// serially and stops at the first failure, which is all a sink with no
    /// produce pipeline can do. [`super::KafkaLogWalSink`] overrides it.
    ///
    /// # Errors
    /// Returns [`WalBatchError`] when a record fails to append. The error
    /// carries the count of records that did append, so the handler can report
    /// a partial batch as the partial batch it is.
    async fn append_batch(
        &self,
        records: Vec<WalLogRecord>,
    ) -> Result<(), WalBatchError<WalSinkError>> {
        let total = records.len();
        for (appended, record) in records.into_iter().enumerate() {
            if let Err(source) = self.append(record).await {
                return Err(WalBatchError::new(appended, total, source));
            }
        }
        Ok(())
    }
}
