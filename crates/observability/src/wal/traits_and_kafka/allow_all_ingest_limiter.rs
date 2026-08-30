use super::*;

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllIngestLimiter;

#[async_trait]
impl LogIngestLimiter for AllowAllIngestLimiter {
    async fn check(
        &self,
        _tenant: &str,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        Ok(())
    }
}
