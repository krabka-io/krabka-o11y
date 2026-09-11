use super::{LogQueryAuthorizer, Principal, QueryAuthorizationError, TenantId, async_trait};

#[derive(Clone, Debug, Default)]
pub(crate) struct UnavailableQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for UnavailableQueryAuthorizer {
    async fn check(
        &self,
        _principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        Err(QueryAuthorizationError::Unavailable {
            tenant: tenant.to_string(),
            reason: "broker-backed query authorization is not connected".to_string(),
        })
    }
}
