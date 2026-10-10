use super::{
    Bytes, HeaderMap, HttpQueryError, Path, PostedQueryRequest, QuerierState, RawQuery,
    RequestSecurity, Response, State, api_prom_label_names_post,
};

/// Loki's legacy `/api/prom/label/{name}/values` answers a POST exactly as
/// `/api/prom/label` does, so the path's label name is unused.
pub(crate) async fn api_prom_label_values_post(
    state: State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(_name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Result<Response, HttpQueryError> {
    let request = PostedQueryRequest {
        security,
        headers,
        raw_query,
        body,
    };
    api_prom_label_names_post(state, request).await
}
