use std::collections::BTreeSet;

use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Path, Principal, QuerierBackend,
    QueryFrontend, Response, State, Uri, backend_error_response, json, optional_time_bounds,
    tenant_and_bounds, with_warnings,
};

/// `/api/search/tag/{tag}/values`, or with `V2` the typed
/// `/api/v2/search/tag/{tag}/values`.
pub(crate) async fn search_tag_values<B, C, const V2: bool>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let (tenant, start_ns, end_ns) = match tenant_and_bounds(
        &headers,
        &principal,
        &qf.cfg.tenant_policy,
        &uri,
        optional_time_bounds,
    ) {
        Ok(request) => request,
        Err(rejection) => return *rejection,
    };
    let (values, _metrics, warnings) = match qf.tag_values(&tenant, &tag, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if !V2 {
        let tag_values = values
            .into_iter()
            .map(|value| value.value)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagValues": tag_values })).into_response();
    }
    let tag_values: Vec<_> = values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    with_warnings(
        json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } }),
        &warnings,
    )
}
