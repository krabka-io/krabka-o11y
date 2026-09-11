use super::{DistributorError, LogIngestLimiter, Principal, TenantId, WalLogRecord};

/// Asks the limiter whether `principal` may write `records` for `tenant`.
///
/// The limiter checks one tenant per call. Every record the distributor
/// normalizes carries the one tenant it resolved from the request, so a record
/// under any other tenant is a defect in the distributor. Such a batch is
/// refused before the limiter sees it, because a check under the wrong tenant
/// would let one tenant's quota and ACLs admit another tenant's records.
pub(crate) async fn check_ingest_quota(
    limiter: &dyn LogIngestLimiter,
    principal: &Principal,
    tenant: &TenantId,
    records: &[WalLogRecord],
) -> Result<(), DistributorError> {
    if let Some(record) = records
        .iter()
        .find(|record| record.tenant != tenant.as_str())
    {
        return Err(DistributorError::RecordTenantMismatch {
            tenant: tenant.to_string(),
            record_tenant: record.tenant.clone(),
        });
    }
    if records.is_empty() {
        return Ok(());
    }
    limiter
        .check(principal, tenant, records)
        .await
        .map_err(DistributorError::IngestQuota)
}
