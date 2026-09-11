use super::{Principal, QueryAuthorizationError, TenantId, async_trait};

/// Decides whether a principal may read the logs of a tenant.
///
/// The querier asks before every read of a tenant's data, and the ruler and
/// the delete-request API ask before they read or change a tenant's rule
/// groups or delete requests. An implementation is called from many requests
/// at once, so it should answer from memory and not wait on a remote call per
/// request.
#[async_trait]
pub trait LogQueryAuthorizer: Send + Sync + 'static {
    /// Allows `principal` to read `tenant`, or reports why it is refused or
    /// cannot be checked.
    ///
    /// The caller has already checked `principal` against its tenant grant.
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError>;
}
