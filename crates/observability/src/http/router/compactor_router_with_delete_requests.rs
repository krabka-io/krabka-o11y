use super::*;

pub(crate) fn compactor_router_with_delete_requests(
    delete_requests: SharedLogDeleteRequests,
) -> Router {
    let delete_state = CompactorDeleteState { delete_requests };
    with_role_ops_routes(Router::new(), COMPACTOR_OPS, ServiceReadiness::ready())
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/loki/api/v1/delete",
            get(list_delete_requests)
                .post(create_delete_request)
                .put(create_delete_request)
                .delete(cancel_delete_request),
        )
        .with_state(delete_state)
}
