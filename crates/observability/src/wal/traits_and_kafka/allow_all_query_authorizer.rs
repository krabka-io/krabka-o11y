use super::{LogQueryAuthorizer, QueryAuthorizationError, async_trait};

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for AllowAllQueryAuthorizer {
    async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
        Ok(())
    }
}
