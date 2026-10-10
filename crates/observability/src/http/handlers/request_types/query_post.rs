use super::{
    Bytes, HeaderMap, PostedQueryRequest, QuerierState, QueryKind, RawQuery, RequestSecurity,
    Response, State, handle_posted_query,
};

pub(crate) async fn query_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let request = PostedQueryRequest {
        security,
        headers,
        raw_query,
        body,
    };
    handle_posted_query(state, request, QueryKind::Instant).await
}
