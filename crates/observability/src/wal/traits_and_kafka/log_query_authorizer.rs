use super::{QueryAuthorizationError, async_trait};

#[async_trait]
pub trait LogQueryAuthorizer: Send + Sync + 'static {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError>;
}
