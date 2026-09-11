use super::{LogQueryAuthorizer, Principal, QueryAuthorizationError, TenantId, async_trait};

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for AllowAllQueryAuthorizer {
    async fn check(
        &self,
        _principal: &Principal,
        _tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        Ok(())
    }
}
