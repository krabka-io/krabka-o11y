use super::{PostedQueryRequest, QuerierState, QueryKind, Response, State, handle_posted_query};

pub(crate) async fn query_range_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Response {
    handle_posted_query(state, request, QueryKind::Range).await
}
