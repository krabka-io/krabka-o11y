use super::{
    Arc, BlockCatalog, HeaderMap, IntoResponse, Json, QuerierBackend, QueryFrontend, Response,
    State, StatusCode, Uri, backend_error_response, json, optional_time_bounds, scope_name,
    scope_param, tenant,
};

pub(crate) async fn search_tags_v2<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scope = match scope_param(&uri) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (tags, _metrics) = match qf.tag_names(&tenant, scope, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let scopes: Vec<_> = tags
        .iter()
        .map(|st| json!({ "name": scope_name(st.scope), "tags": &st.tags }))
        .collect();
    Json(json!({ "scopes": scopes, "metrics": { "inspectedBytes": "0" } })).into_response()
}
