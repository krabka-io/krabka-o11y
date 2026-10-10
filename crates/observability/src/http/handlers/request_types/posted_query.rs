use super::{
    Bytes, HeaderMap, Instant, IntoResponse, QuerierState, QueryKind, RequestSecurity, Response,
    handle_query, post_query_params_body_first,
};

/// The extracted parts of a Loki query POST: the URL query string and the
/// form body that carries the remaining parameters.
pub(crate) struct PostedQueryRequest {
    pub security: RequestSecurity,
    pub headers: HeaderMap,
    pub raw_query: Option<String>,
    pub body: Bytes,
}

/// Runs a posted `query` or `query_range` request and records its outcome
/// under the route label of `kind`.
pub(crate) async fn handle_posted_query(
    state: QuerierState,
    request: PostedQueryRequest,
    kind: QueryKind,
) -> Response {
    let start = Instant::now();
    let route = kind.route_label();
    let resp = match post_query_params_body_first(request.raw_query.as_deref(), &request.body) {
        Ok(raw_query) => {
            // Boxed so the handler's own future stays small; the query
            // future is the large part of it.
            Box::pin(handle_query(
                state.clone(),
                request.security,
                request.headers,
                Some(&raw_query),
                kind,
            ))
            .await
        }
        Err(error) => error.into_response(),
    };
    state.record_query(route, resp.status(), start);
    resp
}
