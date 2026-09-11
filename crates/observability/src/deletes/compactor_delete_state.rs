use super::{Arc, LogQueryAuthorizer, SharedLogDeleteRequests};

/// What the delete-request API reads and changes.
///
/// A delete request removes a tenant's logs for good, so every route asks
/// `query_authorizer` before it reads or changes that tenant's requests.
#[derive(Clone)]
pub(crate) struct CompactorDeleteState {
    pub(crate) delete_requests: SharedLogDeleteRequests,
    pub(crate) query_authorizer: Arc<dyn LogQueryAuthorizer>,
}
