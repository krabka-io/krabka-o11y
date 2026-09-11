use super::{IngestLimitError, LogIngestLimiter, Principal, TenantId, WalLogRecord, async_trait};

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllIngestLimiter;

#[async_trait]
impl LogIngestLimiter for AllowAllIngestLimiter {
    async fn check(
        &self,
        _principal: &Principal,
        _tenant: &TenantId,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        Ok(())
    }
}
