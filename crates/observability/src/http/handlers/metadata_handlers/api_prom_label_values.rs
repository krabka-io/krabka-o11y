use super::{
    HeaderMap, Path, QuerierState, RawQuery, RequestSecurity, Response, State, api_prom_label_names,
};

/// Loki's legacy `/api/prom/label/{name}/values` answers exactly as
/// `/api/prom/label` does, so the path's label name is unused.
pub(crate) async fn api_prom_label_values(
    state: State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(_name): Path<String>,
    raw_query: RawQuery,
) -> Response {
    api_prom_label_names(state, security, headers, raw_query).await
}
