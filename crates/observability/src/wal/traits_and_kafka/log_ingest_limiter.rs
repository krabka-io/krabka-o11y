use super::{CancellationToken, IngestLimitError, Principal, TenantId, WalLogRecord, async_trait};

/// Decides whether a principal may write one push's records for a tenant.
///
/// Every record in `records` belongs to `tenant`. The distributor resolves the
/// tenant once per request and stamps it on every record it normalizes.
#[async_trait]
pub trait LogIngestLimiter: Send + Sync + 'static {
    /// Allows the push, or reports why it is refused or cannot be checked.
    ///
    /// The caller has already checked `principal` against its tenant grant.
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
        records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError>;

    /// Keeps the limiter's cached state fresh until `token` is cancelled.
    ///
    /// The role runs this as a supervised task. The default holds no cached
    /// state, so it only waits for the cancel.
    async fn keep_fresh(&self, token: CancellationToken) {
        token.cancelled().await;
    }
}
