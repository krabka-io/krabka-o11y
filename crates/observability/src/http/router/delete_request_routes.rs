use super::{
    CompactorDeleteState, Router, SharedLogDeleteRequests, cancel_delete_request,
    create_delete_request, get, list_delete_requests,
};

/// `Loki`'s delete-request API, with no ops routes on it.
///
/// The block builder owns these because it is the role that materialises a
/// delete while it writes, which is what `Loki`'s compactor does too.
pub(crate) fn delete_request_routes(delete_requests: SharedLogDeleteRequests) -> Router {
    Router::new()
        .route(
            "/loki/api/v1/delete",
            get(list_delete_requests)
                .post(create_delete_request)
                .put(create_delete_request)
                .delete(cancel_delete_request),
        )
        .with_state(CompactorDeleteState { delete_requests })
}
