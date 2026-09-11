use super::{
    Arc, BLOCK_BUILDER_OPS, LogQueryAuthorizer, RoleReadiness, Router, SharedLogDeleteRequests,
    delete_request_routes, format_query, format_query_post, get, with_role_ops_routes,
};

pub(crate) fn compactor_router_with_delete_requests(
    delete_requests: SharedLogDeleteRequests,
    query_authorizer: Arc<dyn LogQueryAuthorizer>,
    readiness: RoleReadiness,
) -> Router {
    with_role_ops_routes(Router::new(), BLOCK_BUILDER_OPS, readiness)
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .merge(delete_request_routes(delete_requests, query_authorizer))
}
