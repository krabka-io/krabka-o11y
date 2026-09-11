use super::{ProfileRecord, ProfilesError, WalBatchError};

#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    /// Appends one record and waits for its ack.
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError>;

    /// Appends one request's profile records as a batch.
    ///
    /// One ingest request becomes one record per sample type, so this is the
    /// call that decides how many broker round trips one request costs. The
    /// default appends serially and stops at the first failure, which is all a
    /// sink with no produce pipeline can do. [`super::KafkaSink`] overrides it.
    ///
    /// # Errors
    /// Returns [`WalBatchError`] when a record fails to append. The error
    /// carries the count of records that did append, so the handler can report
    /// a partial batch as the partial batch it is.
    async fn append_batch(
        &self,
        records: Vec<ProfileRecord>,
    ) -> Result<(), WalBatchError<ProfilesError>> {
        let total = records.len();
        for (appended, record) in records.into_iter().enumerate() {
            if let Err(source) = self.append(record).await {
                return Err(WalBatchError::new(appended, total, source));
            }
        }
        Ok(())
    }
}
