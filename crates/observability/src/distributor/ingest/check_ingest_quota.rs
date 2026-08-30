use super::{DistributorError, LogIngestLimiter, WalLogRecord};

pub(crate) async fn check_ingest_quota(
    limiter: &dyn LogIngestLimiter,
    records: &[WalLogRecord],
) -> Result<(), DistributorError> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    limiter
        .check(&first.tenant, records)
        .await
        .map_err(DistributorError::IngestQuota)
}
