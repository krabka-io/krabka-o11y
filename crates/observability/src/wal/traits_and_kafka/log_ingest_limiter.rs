use super::*;

#[async_trait]
pub trait LogIngestLimiter: Send + Sync + 'static {
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError>;
}
