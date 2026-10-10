use std::collections::BTreeSet;

use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, json,
    optional_time_bounds, scope_name, scope_param, tenant_and_bounds, with_warnings,
};

/// `/api/search/tags`, or with `V2` the scoped `/api/v2/search/tags`.
pub(crate) async fn search_tags<B, C, const V2: bool>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
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
    // The v1 endpoint lists every scope's names as one flat set.
    let scope = if V2 {
        match scope_param(&uri) {
            Ok(scope) => scope,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        }
    } else {
        None
    };
    let (tags, _metrics, warnings) = match qf.tag_names(&tenant, scope, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if !V2 {
        let tag_names = tags
            .into_iter()
            .flat_map(|scope| scope.tags)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagNames": tag_names })).into_response();
    }
    let scopes: Vec<_> = tags
        .iter()
        .map(|st| json!({ "name": scope_name(st.scope), "tags": &st.tags }))
        .collect();
    with_warnings(
        json!({ "scopes": scopes, "metrics": { "inspectedBytes": "0" } }),
        &warnings,
    )
}
