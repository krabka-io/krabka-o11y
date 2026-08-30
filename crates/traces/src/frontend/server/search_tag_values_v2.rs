use super::{json, QuerierBackend, BlockCatalog, State, Path, Arc, QueryFrontend, HeaderMap, Uri, Response, tenant, optional_time_bounds, IntoResponse, StatusCode, backend_error_response, Json};

pub(crate) async fn search_tag_values_v2<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
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
    let (values, _metrics) = match qf.tag_values(&tenant, &tag, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let tag_values: Vec<_> = values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    Json(json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } })).into_response()
}
