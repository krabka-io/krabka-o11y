use super::{Bytes, ProduceError, WalBatchError, WalRecord};

/// A WAL record and the partitioning key that selects its partition.
#[derive(Clone, Debug, PartialEq)]
pub struct WalBatchRecord {
    pub key: Bytes,
    pub record: WalRecord,
}

/// Testable sink for metrics WAL records.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    /// Appends one record and waits for its ack.
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError>;

    /// Appends one request's records as a batch.
    ///
    /// A remote-write body becomes one record per sample, so this is the call
    /// that decides how many broker round trips one request costs. The default
    /// appends serially and stops at the first failure, which is all a sink
    /// with no produce pipeline can do. [`super::KafkaSink`] overrides it.
    ///
    /// # Errors
    /// Returns [`WalBatchError`] when a record fails to append. The error
    /// carries the count of records that did append, so the handler can report
    /// a partial batch as the partial batch it is.
    async fn append_batch(
        &self,
        records: Vec<WalBatchRecord>,
    ) -> Result<(), WalBatchError<ProduceError>> {
        let total = records.len();
        for batch_entry in records.into_iter().enumerate() {
            if let Err(source) = self.append(batch_entry.1.key, batch_entry.1.record).await {
                return Err(WalBatchError::new(batch_entry.0, total, source));
            }
        }
        Ok(())
    }
}
