use super::{
    Arc, LogQueryAuthorizer, QueryAuthorizationError, UnavailableQueryAuthorizer, async_trait,
};

/// A [`LogQueryAuthorizer`] whose underlying implementation can change after
/// construction.
///
/// The querier uses it to fail closed while the real
/// [`BrokerBackedQueryAuthorizer`] connects asynchronously.
pub(crate) struct SwappableQueryAuthorizer {
    pub(crate) inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
}

impl SwappableQueryAuthorizer {
    /// Creates a new swappable authorizer that starts unavailable.
    pub(crate) fn new() -> (Self, Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>) {
        let inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>> = Arc::new(
            tokio::sync::RwLock::new(Arc::new(UnavailableQueryAuthorizer)),
        );
        (
            Self {
                inner: inner.clone(),
            },
            inner,
        )
    }
}

#[async_trait]
impl LogQueryAuthorizer for SwappableQueryAuthorizer {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        let authorizer = self.inner.read().await.clone();
        authorizer.check(tenant).await
    }
}
